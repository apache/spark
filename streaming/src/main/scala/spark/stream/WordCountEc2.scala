package spark.stream

import SparkStreamContext._
import spark.SparkContext

object WordCountEc2 {
  var inputFile : String = null
  var HDFS : String = null
  var idealPartitions : Int = 0

  def main (args: Array[String]) {
    
    if (args.length != 4) {
      println ("Usage: SparkStreamContext <host> <HDFS> <Input file> <Ideal Partitions>")
      System.exit(1)
    }

    HDFS = args(1)
    inputFile = HDFS + args(2)
    idealPartitions = args(3).toInt
    println ("Input file: " + inputFile)
    
    SparkContext.idealPartitions = idealPartitions
    SparkContext.inputFile = inputFile
    
    val ssc = new SparkStreamContext(args(0), "Test")
    val sentences = ssc.readNetworkStream[String]("Sentences", Array("localhost:55119"), 1000)
    /*sentences.foreach(println)*/
    
    val words = sentences.flatMap(_.split(" "))
    /*words.foreach(println)*/
    
    val counts = words.map(x => (x, 1)).reduceByKey(_ + _)
    /*counts.foreach(println)*/
    
    counts.foreachRDD(rdd => rdd.collect.foreach(x => x))
    /*counts.register*/
    
    ssc.run
  }
}
