package spark.streaming.examples

import akka.actor.ActorSystem
import akka.actor.actorRef2Scala
import akka.zeromq._
import spark.streaming.{ Seconds, StreamingContext }
import spark.streaming.StreamingContext._
import akka.zeromq.Subscribe

/**
 * A simple publisher for demonstration purposes, repeatedly publishes random Messages
 * every one second.
 */
object SimpleZeroMQPublisher {

  def main(args: Array[String]) = {
    if (args.length < 2) {
      System.err.println("Usage: SimpleZeroMQPublisher <zeroMQUrl> <topic> ")
      System.exit(1)
    }

    val Seq(url, topic) = args.toSeq
    val acs: ActorSystem = ActorSystem()

    val pubSocket = ZeroMQExtension(acs).newSocket(SocketType.Pub, Bind(url))
    val messages: Array[String] = Array("words ", "may ", "count ")
    while (true) {
      Thread.sleep(1000)
      pubSocket ! ZMQMessage(Frame(topic) :: messages.map(x => Frame(x.getBytes)).toList)
    }
    acs.awaitTermination()
  }
}

/**
 * A sample wordcount with ZeroMQStream stream
 *
 * To work with zeroMQ, some native libraries have to be installed.
 * Install zeroMQ (release 2.1) core libraries. [ZeroMQ Install guide](http://www.zeromq.org/intro:get-the-software)
 * 
 * Usage: ZeroMQWordCount <master> <zeroMQurl> <topic>
 * In local mode, <master> should be 'local[n]' with n > 1
 *   <zeroMQurl> and <topic> describe where zeroMq publisher is running.
 *
 * To run this example locally, you may run publisher as
 *    `$ ./run spark.streaming.examples.SimpleZeroMQPublisher tcp://127.0.1.1:1234 foo.bar`
 * and run the example as
 *    `$ ./run spark.streaming.examples.ZeroMQWordCount local[2] tcp://127.0.1.1:1234 foo`
 */
object ZeroMQWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: ZeroMQWordCount <master> <zeroMQurl> <topic>" +
          "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }
    val Seq(master, url, topic) = args.toSeq

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "ZeroMQWordCount", Seconds(2),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    def bytesToStringIterator(x: Seq[Seq[Byte]]) = (x.map(x => new String(x.toArray))).iterator

    //For this stream, a zeroMQ publisher should be running.
    val lines = ssc.zeroMQStream(url, Subscribe(topic), bytesToStringIterator)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
  }

}
