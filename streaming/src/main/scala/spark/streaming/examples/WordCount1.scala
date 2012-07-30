package spark.streaming

import SparkStreamContext._

import scala.util.Sorting

import spark.SparkContext
import spark.storage.StorageLevel

object WordCount1 {
  var inputFile : String = null
  var HDFS : String = null
  var idealPartitions : Int = 0

  def main (args: Array[String]) {
    
    if (args.length != 4) {
      println ("Usage: WordCount <host> <HDFS> <Input file> <Ideal Partitions>")
      System.exit(1)
    }

    HDFS = args(1)
    inputFile = HDFS + args(2)
    idealPartitions = args(3).toInt
    println ("Input file: " + inputFile)
    
    val ssc = new SparkStreamContext(args(0), "WordCountWindow")
    
    SparkContext.idealPartitions = idealPartitions
    SparkContext.inputFile = inputFile
    
    val sentences = ssc.readNetworkStream[String]("Sentences", Array("localhost:55119"), 1000)
    //sentences.print

    val words = sentences.flatMap(_.split(" "))
   
    def add(v1: Int, v2: Int) = (v1 + v2) 
    def subtract(v1: Int, v2: Int) = (v1 - v2) 
    
    val windowedCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(add _, subtract _, Seconds(10), Seconds(1), 10)
    windowedCounts.persist(StorageLevel.MEMORY_ONLY_DESER, StorageLevel.MEMORY_ONLY_DESER, Seconds(1))
    windowedCounts.foreachRDD(_.collect)

    ssc.run
  }
}
