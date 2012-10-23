package spark.streaming.examples

import spark.util.IntParam
import spark.storage.StorageLevel
import spark.streaming._
import spark.streaming.StreamingContext._

object GrepRaw {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: GrepRaw <master> <numStreams> <host> <port> <batchMillis>")
      System.exit(1)
    }

    val Array(master, IntParam(numStreams), host, IntParam(port), IntParam(batchMillis)) = args

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "GrepRaw")
    ssc.setBatchDuration(Milliseconds(batchMillis))

    // Make sure some tasks have started on each node
    ssc.sc.parallelize(1 to 1000, 1000).count()
    ssc.sc.parallelize(1 to 1000, 1000).count()
    ssc.sc.parallelize(1 to 1000, 1000).count()

    val rawStreams = (1 to numStreams).map(_ =>
      ssc.rawNetworkStream[String](host, port, StorageLevel.MEMORY_ONLY_2)).toArray
    val union = new UnionDStream(rawStreams)
    union.filter(_.contains("Culpepper")).count().foreachRDD(r =>
      println("Grep count: " + r.collect().mkString))
    ssc.start()
  }
}
