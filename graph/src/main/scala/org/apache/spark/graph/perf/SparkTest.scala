///// This file creates circular dependencies between examples bagle and graph


// package org.apache.spark.graph.perf

// import org.apache.spark._
// import org.apache.spark.SparkContext._
// import org.apache.spark.bagel.Bagel
// import org.apache.spark.bagel.examples._
// import org.apache.spark.graph._


// object SparkTest {

//   def main(args: Array[String]) {
//     val host = args(0)
//     val taskType = args(1)
//     val fname = args(2)
//     val options =  args.drop(3).map { arg =>
//       arg.dropWhile(_ == '-').split('=') match {
//         case Array(opt, v) => (opt -> v)
//         case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
//       }
//     }

//     System.setProperty("spark.serializer", "org.apache.spark.KryoSerializer")
//     //System.setProperty("spark.shuffle.compress", "false")
//     System.setProperty("spark.kryo.registrator", "spark.bagel.examples.PRKryoRegistrator")

//     var numIter = Int.MaxValue
//     var isDynamic = false
//     var tol:Float = 0.001F
//     var outFname = ""
//     var numVPart = 4
//     var numEPart = 4

//     options.foreach{
//       case ("numIter", v) => numIter = v.toInt
//       case ("dynamic", v) => isDynamic = v.toBoolean
//       case ("tol", v) => tol = v.toFloat
//       case ("output", v) => outFname = v
//       case ("numVPart", v) => numVPart = v.toInt
//       case ("numEPart", v) => numEPart = v.toInt
//       case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
//     }

//     val sc = new SparkContext(host, "PageRank(" + fname + ")")
//     val g = GraphLoader.textFile(sc, fname, a => 1.0F).withPartitioner(numVPart, numEPart).cache()
//     val startTime = System.currentTimeMillis

//     val numVertices = g.vertices.count()

//     val vertices = g.collectNeighborIds(EdgeDirection.Out).map { case (vid, neighbors) =>
//       (vid.toString, new PRVertex(1.0, neighbors.map(_.toString)))
//     }

//     // Do the computation
//     val epsilon = 0.01 / numVertices
//     val messages = sc.parallelize(Array[(String, PRMessage)]())
//     val utils = new PageRankUtils
//     val result =
//         Bagel.run(
//           sc, vertices, messages, combiner = new PRCombiner(),
//           numPartitions = numVPart)(
//           utils.computeWithCombiner(numVertices, epsilon, numIter))

//     println("Total rank: " + result.map{ case (id, r) => r.value }.reduce(_+_) )
//     if (!outFname.isEmpty) {
//       println("Saving pageranks of pages to " + outFname)
//       result.map{ case (id, r) => id + "\t" + r.value }.saveAsTextFile(outFname)
//     }
//     println("Runtime:    " + ((System.currentTimeMillis - startTime)/1000.0) + " seconds")
//     sc.stop()
//   }
// }
