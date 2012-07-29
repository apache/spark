package spark.stream

import SparkStreamContext._

import scala.util.Sorting

import spark.SparkContext
import spark.storage.StorageLevel

object GrepCountApprox {
  var inputFile : String = null
  var hdfs : String = null
  var idealPartitions : Int = 0

  def main (args: Array[String]) {
    
    if (args.length != 5) {
      println ("Usage: GrepCountApprox <host> <HDFS> <Input file> <Ideal Partitions> <Timeout>")
      System.exit(1)
    }

    hdfs = args(1)
    inputFile = hdfs + args(2)
    idealPartitions = args(3).toInt
    val timeout = args(4).toLong
    println ("Input file: " + inputFile)
    
    val ssc = new SparkStreamContext(args(0), "GrepCount")
    
    SparkContext.idealPartitions = idealPartitions
    SparkContext.inputFile = inputFile
    ssc.setTempDir(hdfs + "/tmp")
    
    val sentences = ssc.readNetworkStream[String]("Sentences", Array("localhost:55119"), 1000)
    //sentences.print
    val matching = sentences.filter(_.contains("light"))
    var i = 0
    val startTime = System.currentTimeMillis
    matching.foreachRDD { rdd => 
      val myNum = i
      val result = rdd.countApprox(timeout)
      val initialTime = (System.currentTimeMillis - startTime) / 1000.0
      printf("APPROX\t%.2f\t%d\tinitial\t%.1f\t%.1f\n", initialTime, myNum, result.initialValue.mean,
        result.initialValue.high - result.initialValue.low)
      result.onComplete { r =>
        val finalTime = (System.currentTimeMillis - startTime) / 1000.0
        printf("APPROX\t%.2f\t%d\tfinal\t%.1f\t0.0\t%.1f\n", finalTime, myNum, r.mean, finalTime - initialTime)
      }
      i += 1
    }

    ssc.run
  }
}
