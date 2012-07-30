package spark.streaming

import SparkStreamContext._

import scala.util.Sorting

import spark.SparkContext
import spark.storage.StorageLevel

object WordMax {
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
    
    val sentences = ssc.readNetworkStream[String]("Sentences", Array("localhost:55119"), 2000)
    //sentences.print

    val words = sentences.flatMap(_.split(" "))
   
    def add(v1: Int, v2: Int) = (v1 + v2) 
    def subtract(v1: Int, v2: Int) = (v1 - v2) 
    def max(v1: Int, v2: Int) = (if (v1 > v2) v1 else v2)
    
    //val windowedCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(add _, subtract _, Seconds(30), Milliseconds(2000),
    //  System.getProperty("spark.default.parallelism", "1").toInt)
    //windowedCounts.persist(StorageLevel.MEMORY_ONLY_DESER, StorageLevel.DISK_AND_MEMORY_DESER_2, Seconds(5))
    //windowedCounts.print
    
    val parallelism = System.getProperty("spark.default.parallelism", "1").toInt

    val localCounts = words.map(x => (x, 1)).reduceByKey(add _, parallelism)
    //localCounts.persist(StorageLevel.MEMORY_ONLY_DESER)
    localCounts.persist(StorageLevel.MEMORY_ONLY_DESER_2)
    val windowedCounts = localCounts.window(Seconds(30), Seconds(2)).reduceByKey(max _)

    //val windowedCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(add _, subtract _, Seconds(30), Seconds(2), 
    //  parallelism)
    //windowedCounts.persist(StorageLevel.MEMORY_ONLY_DESER, StorageLevel.MEMORY_ONLY_DESER_2, Seconds(6))

    //windowedCounts.print
    windowedCounts.register
    //windowedCounts.foreachRDD(rdd => rdd.collect.foreach(x => print(x+ " ")))
    //windowedCounts.foreachRDD(rdd => rdd.collect.foreach(x => x))

    ssc.run
  }
}
