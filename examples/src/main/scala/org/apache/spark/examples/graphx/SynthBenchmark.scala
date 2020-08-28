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

// scalastyle:off println
package org.apache.spark.examples.graphx

import java.io.{FileOutputStream, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphXUtils, PartitionStrategy}
import org.apache.spark.graphx.util.GraphGenerators

/**
 * The SynthBenchmark application can be used to run various GraphX algorithms on
 * synthetic log-normal graphs.  The intent of this code is to enable users to
 * profile the GraphX system without access to large graph datasets.
 */
object SynthBenchmark {

  /**
   * To run this program use the following:
   *
   * MASTER=spark://foobar bin/run-example graphx.SynthBenchmark -app=pagerank
   *
   * Options:
   *   -app "pagerank" or "cc" for pagerank or connected components. (Default: pagerank)
   *   -niters the number of iterations of pagerank to use (Default: 10)
   *   -nverts the number of vertices in the graph (Default: 1000000)
   *   -numEPart the number of edge partitions in the graph (Default: number of cores)
   *   -partStrategy the graph partitioning strategy to use
   *   -mu the mean parameter for the log-normal graph (Default: 4.0)
   *   -sigma the stdev parameter for the log-normal graph (Default: 1.3)
   *   -degFile the local file to save the degree information (Default: Empty)
   *   -seed seed to use for RNGs (Default: -1, picks seed randomly)
   */
  def main(args: Array[String]): Unit = {
    val options = args.map {
      arg =>
        arg.dropWhile(_ == '-').split('=') match {
          case Array(opt, v) => (opt -> v)
          case _ => throw new IllegalArgumentException(s"Invalid argument: $arg")
        }
    }

    var app = "pagerank"
    var niter = 10
    var numVertices = 100000
    var numEPart: Option[Int] = None
    var partitionStrategy: Option[PartitionStrategy] = None
    var mu: Double = 4.0
    var sigma: Double = 1.3
    var degFile: String = ""
    var seed: Int = -1

    options.foreach {
      case ("app", v) => app = v
      case ("niters", v) => niter = v.toInt
      case ("nverts", v) => numVertices = v.toInt
      case ("numEPart", v) => numEPart = Some(v.toInt)
      case ("partStrategy", v) => partitionStrategy = Some(PartitionStrategy.fromString(v))
      case ("mu", v) => mu = v.toDouble
      case ("sigma", v) => sigma = v.toDouble
      case ("degFile", v) => degFile = v
      case ("seed", v) => seed = v.toInt
      case (opt, _) => throw new IllegalArgumentException(s"Invalid option: $opt")
    }

    val conf = new SparkConf()
      .setAppName(s"GraphX Synth Benchmark (nverts = $numVertices, app = $app)")
    GraphXUtils.registerKryoClasses(conf)

    val sc = new SparkContext(conf)

    // Create the graph
    println("Creating graph...")
    val unpartitionedGraph = GraphGenerators.logNormalGraph(sc, numVertices,
      numEPart.getOrElse(sc.defaultParallelism), mu, sigma, seed)
    // Repartition the graph
    val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_)).cache()

    var startTime = System.currentTimeMillis()
    val numEdges = graph.edges.count()
    println(s"Done creating graph. Num Vertices = $numVertices, Num Edges = $numEdges")
    val loadTime = System.currentTimeMillis() - startTime

    // Collect the degree distribution (if desired)
    if (!degFile.isEmpty) {
      val fos = new FileOutputStream(degFile)
      val pos = new PrintWriter(fos)
      val hist = graph.vertices.leftJoin(graph.degrees)((id, _, optDeg) => optDeg.getOrElse(0))
        .map(p => p._2).countByValue()
      hist.foreach {
        case (deg, count) => pos.println(s"$deg \t $count")
      }
    }

    // Run PageRank
    startTime = System.currentTimeMillis()
    if (app == "pagerank") {
      println("Running PageRank")
      val totalPR = graph.staticPageRank(niter).vertices.map(_._2).sum()
      println(s"Total PageRank = $totalPR")
    } else if (app == "cc") {
      println("Running Connected Components")
      val numComponents = graph.connectedComponents.vertices.map(_._2).distinct().count()
      println(s"Number of components = $numComponents")
    }
    val runTime = System.currentTimeMillis() - startTime

    println(s"Num Vertices = $numVertices")
    println(s"Num Edges = $numEdges")
    println(s"Creation time = ${loadTime/1000.0} seconds")
    println(s"Run time = ${runTime/1000.0} seconds")

    sc.stop()
  }
}
// scalastyle:on println
