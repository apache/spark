package org.apache.spark.examples.graphx

import org.apache.spark.SparkContext._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.PartitionStrategy.{CanonicalRandomVertexCut, EdgePartition2D, EdgePartition1D, RandomVertexCut}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.util.GraphGenerators
import java.io.{PrintWriter, FileOutputStream}

/**
 * The SynthBenchmark application can be used to run various GraphX algorithms on
 * synthetic log-normal graphs.  The intent of this code is to enable users to
 * profile the GraphX system without access to large graph datasets.
 */
object SynthBenchmark {

  def pickPartitioner(v: String): PartitionStrategy = {
    // TODO: Use reflection rather than listing all the partitioning strategies here.
    v match {
      case "RandomVertexCut" => RandomVertexCut
      case "EdgePartition1D" => EdgePartition1D
      case "EdgePartition2D" => EdgePartition2D
      case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
      case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + v)
    }
  }

  /**
   * To run this program use the following:
   *
   * bin/spark-class org.apache.spark.graphx.lib.SynthBenchmark -host="local[4]"
   *
   * Required Options:
   *   -host The spark job scheduler
   *
   * Additional Options:
   *   -app "pagerank" or "cc" for pagerank or connected components. (Default: pagerank)
   *   -niters the number of iterations of pagerank to use (Default: 10)
   *   -numVertices the number of vertices in the graph (Default: 1000000)
   *   -numEPart the number of edge partitions in the graph (Default: number of cores)
   *   -partStrategy the graph partitioning strategy to use
   *   -mu the mean parameter for the log-normal graph (Default: 4.0)
   *   -sigma the stdev parameter for the log-normal graph (Default: 1.3)
   *   -degFile the local file to save the degree information (Default: Empty)
   */
  def main(args: Array[String]): Unit = {
    val options = args.map {
      arg =>
        arg.dropWhile(_ == '-').split('=') match {
          case Array(opt, v) => (opt -> v)
          case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
        }
    }

    var host: String = null
    var app = "pagerank"
    var niter = 10
    var numVertices = 1000000
    var numEPart: Option[Int] = None
    var partitionStrategy: Option[PartitionStrategy] = None
    var mu: Double = 4.0
    var sigma: Double = 1.3
    var degFile: String = ""

    options.foreach {
      case ("host", v) => host = v
      case ("app", v) => app = v
      case ("niter", v) => niter = v.toInt
      case ("nverts", v) => numVertices = v.toInt
      case ("numEPart", v) => numEPart = Some(v.toInt)
      case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
      case ("mu", v) => mu = v.toDouble
      case ("sigma", v) => sigma = v.toDouble
      case ("degFile", v) => degFile = v
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }

    if (host == null) {
      println("No -host option specified!")
      System.exit(1)
    }

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

    val sc = new SparkContext(host, s"GraphX Synth Benchmark (nverts = $numVertices)", conf)

    // Create the graph
    var graph = GraphGenerators.logNormalGraph(sc, numVertices, numEPart.getOrElse(sc.defaultParallelism), mu, sigma)
    // Repartition the graph
    if (!partitionStrategy.isEmpty) {
      graph = graph.partitionBy(partitionStrategy.get)
    }
    graph.cache

    var startTime = System.currentTimeMillis()
    val numEdges = graph.edges.count()
    println(s"Num Vertices: $numVertices")
    println(s"Num Edges: $numEdges}")
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
      val totalPR = graph.staticPageRank(niter).vertices.map(p => p._2).sum
      println(s"Total pagerank = $totalPR")
    } else if (app == "cc") {
      println("Connected Components")
      val maxCC = graph.staticPageRank(niter).vertices.map(v => v._2).reduce((a,b)=>math.max(a,b))
      println(s"Max CC = $maxCC")
    }
    val runTime = System.currentTimeMillis() - startTime

    sc.stop
    println(s"Num Vertices: $numVertices")
    println(s"Num Edges: $numEdges")
    println(s"Load time: ${loadTime/1000.0} seconds")
    println(s"Run time:  ${runTime/1000.0} seconds")

  }
}
