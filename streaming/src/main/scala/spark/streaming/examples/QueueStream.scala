package spark.streaming.examples

import spark.RDD
import spark.streaming.StreamingContext
import spark.streaming.StreamingContext._
import spark.streaming.Seconds

import scala.collection.mutable.SynchronizedQueue

object QueueStream {
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: QueueStream <master>")
      System.exit(1)
    }
    
    // Create the context and set the batch size
    val ssc = new StreamingContext(args(0), "QueueStream")
    ssc.setBatchDuration(Seconds(1))
    
    // Create the queue through which RDDs can be pushed to 
    // a QueueInputDStream
    val rddQueue = new SynchronizedQueue[RDD[Int]]()
    
    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
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