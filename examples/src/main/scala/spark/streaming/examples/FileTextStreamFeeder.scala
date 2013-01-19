package spark.streaming.examples

import java.util.concurrent.CountDownLatch

import scala.collection.mutable.LinkedList
import scala.io.Source

import akka.actor.{ Actor, ActorRef, actorRef2Scala }
import akka.actor.Props

import spark.util.AkkaUtils

/**
 * A feeder to which multiple message receiver (specified by "noOfReceivers")actors 
 * subscribe and receive file(s)'s text as stream of messages. This is provided
 * as a demonstration application for trying out Actor as receiver feature. Please see 
 * SampleActorReceiver or AkkaActorWordCount example for details about the 
 * receiver of this feeder.
 */

object FileTextStreamFeeder {

  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()
  var countdownLatch: CountDownLatch = _
  def main(args: Array[String]) = args.toList match {

    case host :: port :: noOfReceivers :: fileNames =>
      val acs = AkkaUtils.createActorSystem("spark", host, port.toInt)._1
      countdownLatch = new CountDownLatch(noOfReceivers.toInt)
      val actor = acs.actorOf(Props(new FeederActor), "FeederActor")
      countdownLatch.await() //wait for all the receivers to subscribe
      for (fileName <- fileNames;line <- Source.fromFile(fileName).getLines) {
        actor ! line
      }
      acs.awaitTermination();

    case _ =>
      System.err.println("Usage: FileTextStreamFeeder <hostname> <port> <no_of_receivers> <filenames>")
      System.exit(1)
  }

  /**
   * Sends the content to every receiver subscribed
   */
  class FeederActor extends Actor {

    def receive: Receive = {

      case SubscribeReceiver(receiverActor: ActorRef) =>
        println("received subscribe from %s".format(receiverActor.toString))
        receivers = LinkedList(receiverActor) ++ receivers
        countdownLatch.countDown()

      case UnsubscribeReceiver(receiverActor: ActorRef) =>
        println("received unsubscribe from %s".format(receiverActor.toString))
        receivers = receivers.dropWhile(x => x eq receiverActor)

      case textMessage: String =>
        receivers.foreach(_ ! textMessage)

    }
  }
}