package org.apache.spark.graphx

import org.apache.spark._
import org.apache.spark.graphx.algorithms._


/**
 * The Analytics object contains a collection of basic graph analytics
 * algorithms that operate largely on the graph structure.
 *
 * In addition the Analytics object contains a driver `main` which can
 * be used to apply the various functions to graphs in standard
 * formats.
 */
object Analytics extends Logging {

  def main(args: Array[String]) = {
    val host = args(0)
    val taskType = args(1)
    val fname = args(2)
    val options =  args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
      loggers.map{
        loggerName =>
          val logger = org.apache.log4j.Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel()
        logger.setLevel(level)
        loggerName -> prevLevel
      }.toMap
    }

    def pickPartitioner(v: String): PartitionStrategy = {
      v match {
         case "RandomVertexCut" => RandomVertexCut
         case "EdgePartition1D" => EdgePartition1D
         case "EdgePartition2D" => EdgePartition2D
         case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
         case _ => throw new IllegalArgumentException("Invalid Partition Strategy: " + v)
       }
    }
     val serializer = "org.apache.spark.serializer.KryoSerializer"
     System.setProperty("spark.serializer", serializer)
     System.setProperty("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

     taskType match {
       case "pagerank" => {

         var tol:Float = 0.001F
         var outFname = ""
         var numVPart = 4
         var numEPart = 4
         var partitionStrategy: Option[PartitionStrategy] = None

         options.foreach{
           case ("tol", v) => tol = v.toFloat
           case ("output", v) => outFname = v
           case ("numVPart", v) => numVPart = v.toInt
           case ("numEPart", v) => numEPart = v.toInt
           case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
           case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
         }

         println("======================================")
         println("|             PageRank               |")
         println("======================================")

         val sc = new SparkContext(host, "PageRank(" + fname + ")")

         val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
           minEdgePartitions = numEPart).cache()
         val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

         println("GRAPHX: Number of vertices " + graph.vertices.count)
         println("GRAPHX: Number of edges " + graph.edges.count)

         val pr = graph.pageRank(tol).vertices.cache()

         println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_+_))

         if (!outFname.isEmpty) {
           logWarning("Saving pageranks of pages to " + outFname)
           pr.map{case (id, r) => id + "\t" + r}.saveAsTextFile(outFname)
         }

         sc.stop()
       }

        case "cc" => {

          var numIter = Int.MaxValue
          var numVPart = 4
          var numEPart = 4
          var isDynamic = false
          var partitionStrategy: Option[PartitionStrategy] = None

          options.foreach{
            case ("numIter", v) => numIter = v.toInt
            case ("dynamic", v) => isDynamic = v.toBoolean
            case ("numEPart", v) => numEPart = v.toInt
            case ("numVPart", v) => numVPart = v.toInt
            case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
            case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
          }

          if(!isDynamic && numIter == Int.MaxValue) {
            println("Set number of iterations!")
            sys.exit(1)
          }
          println("======================================")
          println("|      Connected Components          |")
          println("--------------------------------------")
          println(" Using parameters:")
          println(" \tDynamic:  " + isDynamic)
          println(" \tNumIter:  " + numIter)
          println("======================================")

          val sc = new SparkContext(host, "ConnectedComponents(" + fname + ")")
          val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
            minEdgePartitions = numEPart).cache()
          val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

          val cc = ConnectedComponents.run(graph)
          println("Components: " + cc.vertices.map{ case (vid,data) => data}.distinct())
          sc.stop()
        }

       case "triangles" => {
         var numVPart = 4
         var numEPart = 4
         // TriangleCount requires the graph to be partitioned
         var partitionStrategy: PartitionStrategy = RandomVertexCut

         options.foreach{
           case ("numEPart", v) => numEPart = v.toInt
           case ("numVPart", v) => numVPart = v.toInt
           case ("partStrategy", v) => partitionStrategy = pickPartitioner(v)
           case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
         }
         println("======================================")
         println("|      Triangle Count                |")
         println("--------------------------------------")
         val sc = new SparkContext(host, "TriangleCount(" + fname + ")")
         val graph = GraphLoader.edgeListFile(sc, fname, canonicalOrientation = true,
           minEdgePartitions = numEPart).partitionBy(partitionStrategy).cache()
         val triangles = TriangleCount.run(graph)
         println("Triangles: " + triangles.vertices.map {
            case (vid,data) => data.toLong
          }.reduce(_+_) / 3)
         sc.stop()
       }

       case _ => {
         println("Invalid task type.")
       }
     }
   }
}
