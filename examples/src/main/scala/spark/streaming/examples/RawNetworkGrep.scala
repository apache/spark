package spark.streaming.examples

import spark.util.IntParam
import spark.storage.StorageLevel

import spark.streaming._
import spark.streaming.util.RawTextHelper

/**
 * Receives text from multiple rawNetworkStreams and counts how many '\n' delimited
 * lines have the word 'the' in them. This is useful for benchmarking purposes. This
 * will only work with spark.streaming.util.RawTextSender running on all worker nodes
 * and with Spark using Kryo serialization (set Java property "spark.serializer" to
 * "spark.KryoSerializer").
 * Usage: RawNetworkGrep <master> <numStreams> <host> <port> <batchMillis>
 *   <master> is the Spark master URL
 *   <numStream> is the number rawNetworkStreams, which should be same as number
 *               of work nodes in the cluster
 *   <host> is "localhost".
 *   <port> is the port on which RawTextSender is running in the worker nodes.
 *   <batchMillise> is the Spark Streaming batch duration in milliseconds.
 */

object RawNetworkGrep {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: RawNetworkGrep <master> <numStreams> <host> <port> <batchMillis>")
      System.exit(1)
    }

    val Array(master, IntParam(numStreams), host, IntParam(port), IntParam(batchMillis)) = args

    // Create the context
    val ssc = new StreamingContext(master, "RawNetworkGrep", Milliseconds(batchMillis),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    // Warm up the JVMs on master and slave for JIT compilation to kick in
    RawTextHelper.warmUp(ssc.sparkContext)

    val rawStreams = (1 to numStreams).map(_ =>
      ssc.rawSocketStream[String](host, port, StorageLevel.MEMORY_ONLY_SER_2)).toArray
    val union = ssc.union(rawStreams)
    union.filter(_.contains("the")).count().foreach(r =>
      println("Grep count: " + r.collect().mkString))
    ssc.start()
  }
}
