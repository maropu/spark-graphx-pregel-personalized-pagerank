[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/spark-graphx-pregel-personalized-pagerank/blob/master/LICENSE)

This package provides interfaces for personalized PageRank on Pregel/GraphX.

## How to use this package

    // Gets test graph data from `https://snap.stanford.edu/data/ca-CondMat.html`
    $ wget https://snap.stanford.edu/data/ca-CondMat.txt.gz

    // Launches a Spark v2.4.5 shell with this package
    // Note that you can set true to `spark.graphx.pageRank.loggingEnabled` for checking the progress
    $ git clone https://github.com/maropu/spark-graphx-pregel-personalized-pagerank.git
    $ $SPARK_HOME/bin/spark-shell --conf spark.jars ./spark-graphx-pregel-personalized-pagerank/spark-graphx-pregel-personalized-pagerank_2.11_2.4.5-0.1.0-SNAPSHOT-with-dependencies.jar --conf spark.graphx.pageRank.loggingEnabled=true -v
    ...

    scala> import org.apache.spark.graphx._

    // Loads the downloaded graph data
    scala> val graph = GraphLoader.edgeListFile(sc, "ca-CondMat.txt.gz")

    // Computes personalized PageRank
    scala> lib.PageRankEx.runParallelPersonalizedPageRankUntilConvergence(graph, sources = Seq(130, 81626), tol = 0.01)

    Pregel finished iteration 0 (# of active messages: 1090)
    Aggregated metrics: value:(1.8499999999999999,1.8499999999999996) delta:(1.8499999999999999,1.8499999999999996)

    Pregel finished iteration 1 (# of active messages: 424)
    Aggregated metrics: value:(2.5724999999999874,2.572500000000004) delta:(0.7832142857142885,0.7697222222222233)

    Pregel finished iteration 2 (# of active messages: 64)
    Aggregated metrics: value:(2.952715671543223,2.6335382923099413) delta:(0.43964207846964315,0.5921036916707042)

    Pregel finished iteration 3 (# of active messages: 22)
    Aggregated metrics: value:(3.145608510503435,2.633552646467126) delta:(0.2908540302238811,0.5914781586882217)

    Pregel finished iteration 4 (# of active messages: 0)
    Aggregated metrics: value:(3.1912734581542,2.633554118907456) delta:(0.2230614608527432,0.5914717973238139)

For API details, see [PageRankEx.scala](https://github.com/maropu/spark-graphx-pregel-personalized-pagerank/blob/0e392178e6bd753028f11d135e067d1a1f1edb21/src/main/scala/org/apache/spark/graphx/lib/PageRankEx.scala#L37-L51).

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/spark-sql-server/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

