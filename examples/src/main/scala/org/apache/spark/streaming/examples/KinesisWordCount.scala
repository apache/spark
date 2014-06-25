package org.apache.spark.streaming.examples

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.StreamingContext._


object KinesisWordCount {

  def main(args: Array[String]): Unit = {
  
  
     if (args.length < 1) {
      System.err.println("Usage: KinesisWordCount <master> <streamname>" + " [accesskey] [accessSecretKey]")
      System.exit(1)
    }
    
	  val master=args(0)
	  val kinesisStream=args(1)
	  val accesskey=args(2)
	  val accessSecretKey=args(3)
	 
  
   val ssc = new StreamingContext(master, "KinesisWordCOunt", Seconds(2),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))
  
   val lines = KinesisUtils.createStream(ssc, accesskey, accessSecretKey, kinesisStream)
  
   val words = lines.flatMap(_.split(" "))
   val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
   wordCounts.print
   ssc.start

  }
}