package spark.streaming.examples

import spark.util.IntParam
import spark.storage.StorageLevel
import spark.streaming._
import spark.streaming.StreamingContext._

object CountRaw {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: CountRaw <master> <numStreams> <host> <port> <batchMillis>")
      System.exit(1)
    }

    val Array(master, IntParam(numStreams), host, IntParam(port), IntParam(batchMillis)) = args

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "CountRaw")
    ssc.setBatchDuration(Milliseconds(batchMillis))

    // Make sure some tasks have started on each node
    ssc.sc.parallelize(1 to 1000, 1000).count()
    ssc.sc.parallelize(1 to 1000, 1000).count()
    ssc.sc.parallelize(1 to 1000, 1000).count()

    val rawStreams = (1 to numStreams).map(_ =>
      ssc.createRawNetworkStream[String](host, port, StorageLevel.MEMORY_ONLY_2)).toArray
    val union = new UnifiedDStream(rawStreams)
    union.map(_.length + 2).reduce(_ + _).foreachRDD(r => println("Byte count: " + r.collect().mkString))
    ssc.start()
  }
}
