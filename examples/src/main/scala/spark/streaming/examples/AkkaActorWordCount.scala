package spark.streaming.examples

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala

import spark.streaming.Seconds
import spark.streaming.StreamingContext
import spark.streaming.StreamingContext.toPairDStreamFunctions
import spark.streaming.receivers.Data

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

/**
 * A sample actor as receiver is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives
 * data, thus it is important to have feeder running before this example
 * can be run. Please see FileTextStreamFeeder(sample) for feeder of this 
 * receiver.
 */
class SampleActorReceiver[T: ClassManifest](urlOfPublisher: String)
  extends Actor {

  lazy private val remotePublisher = context.actorFor(urlOfPublisher)

  override def preStart = remotePublisher ! SubscribeReceiver(context.self)

  def receive = {
    case msg => context.parent ! Data(msg.asInstanceOf[T])
  }

  override def postStop() = remotePublisher ! UnsubscribeReceiver(context.self)

}

/**
 * A sample word count program demonstrating the use of plugging in
 * AkkaActor as Receiver
 */
object AkkaActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        "Usage: AkkaActorWordCount <master> <batch-duration in seconds>" +
          " <remoteAkkaHost> <remoteAkkaPort>" +
          "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    val Seq(master, batchDuration, remoteAkkaHost, remoteAkkaPort) = args.toSeq

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "AkkaActorWordCount",
      Seconds(batchDuration.toLong))

    /* 
     * Following is the use of pluggableActorStream to plug in custom actor as receiver
     * 
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility 
     * to ensure the type safety, i.e type of data received and PluggableInputDstream 
     * should be same.
     * 
     * For example: Both pluggableActorStream and SampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */

    val lines = ssc.pluggableActorStream[String](
      Props(new SampleActorReceiver[String]("akka://spark@%s:%s/user/FeederActor".format(
        remoteAkkaHost, remoteAkkaPort.toInt))), "SampleReceiver")

    //compute wordcount 
    lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).print()

    ssc.start()

  }
}
