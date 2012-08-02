package spark.streaming.examples

import spark.RDD
import spark.streaming.SparkStreamContext
import spark.streaming.SparkStreamContext._
import spark.streaming.Seconds

import scala.collection.mutable.SynchronizedQueue

object ExampleOne {
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: ExampleOne <master>")
      System.exit(1)
    }
    
    // Create the context and set the batch size
    val ssc = new SparkStreamContext(args(0), "ExampleOne")
    ssc.setBatchDuration(Seconds(1))
    
    // Create the queue through which RDDs can be pushed to 
    // a QueueInputRDS
    val rddQueue = new SynchronizedQueue[RDD[Int]]()
    
    // Create the QueueInputRDs and use it do some processing
    val inputStream = ssc.createQueueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()    
    ssc.start()
    
    // Create and push some RDDs into
    for (i <- 1 to 30) {
      rddQueue += ssc.sc.makeRDD(1 to 1000, 10)
      Thread.sleep(1000)
    }
    ssc.stop()
    System.exit(0)
  }
}