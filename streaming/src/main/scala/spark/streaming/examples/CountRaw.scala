package spark.streaming.examples

import spark.util.IntParam
import spark.storage.StorageLevel
import spark.streaming._
import spark.streaming.StreamingContext._

object CountRaw {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: WordCountNetwork <master> <numStreams> <hostname> <port>")
      System.exit(1)
    }

    val Array(master, IntParam(numStreams), hostname, IntParam(port)) = args

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "CountRaw")
    ssc.setBatchDuration(Seconds(1))

    // Make sure some tasks have started on each node
    ssc.sc.parallelize(1 to 1000, 1000).count()
    ssc.sc.parallelize(1 to 1000, 1000).count()
    ssc.sc.parallelize(1 to 1000, 1000).count()

    val rawStreams = (1 to numStreams).map(_ =>
      ssc.createRawNetworkStream[String](hostname, port, StorageLevel.MEMORY_ONLY_2)).toArray
    val union = new UnifiedDStream(rawStreams)
    union.map(_.length).reduce(_ + _).foreachRDD(r => println("Byte count: " + r.collect().mkString))
    ssc.start()
  }
}
