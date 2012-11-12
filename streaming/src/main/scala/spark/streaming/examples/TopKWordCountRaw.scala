package spark.streaming.examples

import spark.storage.StorageLevel
import spark.util.IntParam

import spark.streaming._
import spark.streaming.StreamingContext._
import spark.streaming.util.RawTextHelper._

import java.util.UUID

object TopKWordCountRaw {
  
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: WordCountRaw <master> <# streams> <port> <HDFS checkpoint directory> ")
      System.exit(1)
    }

    val Array(master, IntParam(numStreams), IntParam(port), checkpointDir) = args
    val k = 10
    
    // Create the context, set the batch size and checkpoint directory.
    // Checkpoint directory is necessary for achieving fault-tolerance, by saving counts 
    // periodically to HDFS 
    val ssc = new StreamingContext(master, "TopKWordCountRaw")
    ssc.setBatchDuration(Seconds(1))
    ssc.checkpoint(checkpointDir + "/" + UUID.randomUUID.toString, Seconds(1)) 
   
    // Warm up the JVMs on master and slave for JIT compilation to kick in  
    /*warmUp(ssc.sc)*/

    // Set up the raw network streams that will connect to localhost:port to raw test
    // senders on the slaves and generate top K words of last 30 seconds
    val lines = (1 to numStreams).map(_ => {
        ssc.rawNetworkStream[String]("localhost", port, StorageLevel.MEMORY_ONLY_SER_2)
    })
    val union = new UnionDStream(lines.toArray)
    val counts = union.mapPartitions(splitAndCountPartitions)
    val windowedCounts = counts.reduceByKeyAndWindow(add _, subtract _, Seconds(30), Seconds(1), 10)
    val partialTopKWindowedCounts = windowedCounts.mapPartitions(topK(_, k))
    partialTopKWindowedCounts.foreachRDD(rdd => {
      val collectedCounts = rdd.collect
      println("Collected " + collectedCounts.size + " words from partial top words") 
      println("Top " + k + " words are " + topK(collectedCounts.toIterator, k).mkString(","))
    })

    ssc.start()
  }
}
