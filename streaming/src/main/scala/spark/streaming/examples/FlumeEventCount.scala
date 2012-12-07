package spark.streaming.examples

import spark.util.IntParam
import spark.storage.StorageLevel
import spark.streaming._

object FlumeEventCount {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: FlumeEventCount <master> <host> <port> <batchMillis>")
      System.exit(1)
    }

    val Array(master, host, IntParam(port), IntParam(batchMillis)) = args

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "FlumeEventCount",
      Milliseconds(batchMillis))

    // Create a flume stream
    val stream = ssc.flumeStream(host,port,StorageLevel.MEMORY_ONLY)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    ssc.start()
  }
}
