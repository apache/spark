package spark.streaming.examples

import spark.util.IntParam
import spark.storage.StorageLevel
import spark.streaming._

/**
 *  Produces a count of events received from Flume.
 *
 *  This should be used in conjunction with an AvroSink in Flume. It will start
 *  an Avro server on at the request host:port address and listen for requests.
 *  Your Flume AvroSink should be pointed to this address.
 *
 *  Usage: FlumeEventCount <master> <host> <port>
 *
 *    <master> is a Spark master URL
 *    <host> is the host the Flume receiver will be started on - a receiver
 *           creates a server and listens for flume events.
 *    <port> is the port the Flume receiver will listen on.
 */
object FlumeEventCount {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: FlumeEventCount <master> <host> <port>")
      System.exit(1)
    }

    val Array(master, host, IntParam(port)) = args

    val batchInterval = Milliseconds(2000)
    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "FlumeEventCount", batchInterval,
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    // Create a flume stream
    val stream = ssc.flumeStream(host,port,StorageLevel.MEMORY_ONLY)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    ssc.start()
  }
}
