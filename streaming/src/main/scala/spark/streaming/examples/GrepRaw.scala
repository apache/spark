package spark.streaming.examples

import spark.util.IntParam
import spark.storage.StorageLevel

import spark.streaming._
import spark.streaming.StreamingContext._
import spark.streaming.util.RawTextHelper._

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
    warmUp(ssc.sc)


    val rawStreams = (1 to numStreams).map(_ =>
      ssc.rawNetworkStream[String](host, port, StorageLevel.MEMORY_ONLY_SER_2)).toArray
    val union = new UnionDStream(rawStreams)
    union.filter(_.contains("Alice")).count().foreachRDD(r =>
      println("Grep count: " + r.collect().mkString))
    ssc.start()
  }
}
