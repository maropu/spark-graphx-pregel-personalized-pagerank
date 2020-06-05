/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import breeze.linalg.{Vector => BV}

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{Vector, Vectors}

object PageRankEx extends Logging {

  private lazy val loggingEnabled = {
    SparkContext.getActive.exists { sc =>
      sc.getConf.getBoolean("spark.graphx.pageRank.loggingEnabled", false)
    }
  }

  /**
   * Run a dynamic version of Personalized PageRank returning vertex attributes
   * containing the PageRank.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param sources The list of sources to compute personalized pagerank fro
   * @param tol the tolerance allowed at convergence (smaller => more accurate)
   * @param maxIterations the maximum number of iterations to run for
   * @param resetProb the random reset probability (alpha)
   *
   * @return the vertices containing the personalized PageRank
   */
  def runParallelPersonalizedPageRankUntilConvergence[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      sources: Seq[VertexId],
      tol: Double = 0.01,
      maxIterations: Int = Int.MaxValue,
      resetProb: Double = 0.15): VertexRDD[Vector] = {
    require(tol >= 0, s"Tolerance must be no less than 0, but got $tol")
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0, " +
      s"but got $maxIterations")
    require(resetProb >= 0 && resetProb <= 1, "Random reset probability must belong " +
      s"to [0, 1], but got $resetProb")

    // Vertex type of a PageRange graph
    type VecType = (BV[Double], BV[Double])

    val sourcesNum = sources.size
    val sourcesInitMap = {
      val sc = graph.vertices.sparkContext
      sc.broadcast(sources.zipWithIndex.map { case (vid, i) => (vid, i) }.toMap)
    }

    def zeros(size: Int) = Vectors.zeros(size).asBreeze

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 0.
    val pagerankGraph = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (_, _, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, _) =>
      if (sourcesInitMap.value.contains(id)) {
        val initValues = (0 until sourcesNum).map { i =>
          if (i == sourcesInitMap.value(id)) {
            Double.NegativeInfinity
          } else {
            0.0
          }
        }
        val delta = Vectors.dense(initValues.toArray).asBreeze
        (zeros(sourcesNum), delta)
      } else {
        (zeros(sourcesNum), zeros(sourcesNum))
      }
    }.cache()

    // Define the three functions needed to implement PageRank
    // in the GraphX version of Pregel.
    def personalizedVertexProgram(id: VertexId, attr: VecType, msgSums: BV[Double]): VecType = {
      val (oldPRs, lastDeltas) = attr
      val vetexValues = oldPRs.valuesIterator.zip(msgSums.valuesIterator)
        .zip(lastDeltas.valuesIterator).map {
        case ((oldPR, msgSum), lastDelta) =>
          if (lastDelta != Double.NegativeInfinity) {
            oldPR + (1.0 - resetProb) * msgSum
          } else {
            1.0
          }
      }
      val newPRs = BV(vetexValues.toArray)
      (newPRs, newPRs - oldPRs)
    }

    def sendMessage(edge: EdgeTriplet[VecType, Double]) = {
      if (edge.srcAttr._2.max > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: BV[Double], b: BV[Double]) = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = Vectors.zeros(sources.size).asBreeze

    // Execute a dynamic version of Pregel
    val vp = (id: VertexId, attr: (BV[Double], BV[Double]), msgSum: BV[Double]) =>
      personalizedVertexProgram(id, attr, msgSum)

    val funcToComputeGlobalMetricOpt = if (loggingEnabled) {
      val reduceFunc = (v1: VecType, v2: VecType) => {
        (v1._1 + v2._1, v1._2 + v2._2)
      }
      val formatFunc = (v: VecType) => {
        val value = v._1.toArray.mkString("(", ",", ")")
        val delta = v._2.toArray.mkString("(", ",", ")")
        s"value:$value delta:$delta"
      }
      Some((reduceFunc, formatFunc))
    } else {
      None
    }

    val rankGraph = PregelEx(
      pagerankGraph,
      initialMessage,
      maxIterations,
      activeDirection = EdgeDirection.Out,
      funcToComputeGlobalMetricOpt
    )(
      vp, sendMessage, messageCombiner
    ).mapVertices { (_, attr) => attr._1 }

    // SPARK-18847 If the graph has sinks (vertices with no outgoing edges) correct the sum of ranks
    val rankSums = rankGraph.vertices.values.fold(zeros(sourcesNum))(_ +:+ _)
    rankGraph.vertices.mapValues { (_, attr) =>
      Vectors.fromBreeze(attr /:/ rankSums)
    }
  }
}
