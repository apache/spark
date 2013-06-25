package spark.streaming.examples

import spark.RDD
import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._

import scala.collection.mutable.SynchronizedQueue

object QueueStream {
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: QueueStream <master>")
      System.exit(1)
    }
    
    // Create the context
    val ssc = new StreamingContext(args(0), "QueueStream", Seconds(1),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

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
      rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      Thread.sleep(1000)
    }
    ssc.stop()
    System.exit(0)
  }
}
