package spark.streaming

import SparkStreamContext._

import scala.util.Sorting

import spark.SparkContext
import spark.storage.StorageLevel

object GrepCount {
  var inputFile : String = null
  var HDFS : String = null
  var idealPartitions : Int = 0

  def main (args: Array[String]) {
    
    if (args.length != 4) {
      println ("Usage: GrepCount <host> <HDFS> <Input file> <Ideal Partitions>")
      System.exit(1)
    }

    HDFS = args(1)
    inputFile = HDFS + args(2)
    idealPartitions = args(3).toInt
    println ("Input file: " + inputFile)
    
    val ssc = new SparkStreamContext(args(0), "GrepCount")
    
    SparkContext.idealPartitions = idealPartitions
    SparkContext.inputFile = inputFile
    
    val sentences = ssc.readNetworkStream[String]("Sentences", Array("localhost:55119"), 1000)
    //sentences.print
    val matching = sentences.filter(_.contains("light"))
    matching.foreachRDD(rdd => println(rdd.count))

    ssc.run
  }
}
