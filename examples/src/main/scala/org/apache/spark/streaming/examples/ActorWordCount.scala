/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.examples

import scala.collection.mutable.LinkedList
import scala.util.Random
import scala.reflect.ClassTag

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.receivers.Receiver
import org.apache.spark.util.AkkaUtils

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

/**
 * Sends the random content to every receiver subscribed with 1/2
 *  second delay.
 */
class FeederActor extends Actor {

  val rand = new Random()
  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()

  val strings: Array[String] = Array("words ", "may ", "count ")

  def makeMessage(): String = {
    val x = rand.nextInt(3)
    strings(x) + strings(2 - x)
  }

  /*
   * A thread to generate random messages
   */
  new Thread() {
    override def run() {
      while (true) {
        Thread.sleep(500)
        receivers.foreach(_ ! makeMessage)
      }
    }
  }.start()

  def receive: Receive = {

    case SubscribeReceiver(receiverActor: ActorRef) =>
      println("received subscribe from %s".format(receiverActor.toString))
    receivers = LinkedList(receiverActor) ++ receivers

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      println("received unsubscribe from %s".format(receiverActor.toString))
    receivers = receivers.dropWhile(x => x eq receiverActor)

  }
}

/**
 * A sample actor as receiver, is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives
 * data.
 *
 * @see [[org.apache.spark.streaming.examples.FeederActor]]
 */
class SampleActorReceiver[T: ClassTag](urlOfPublisher: String)
extends Actor with Receiver {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart = remotePublisher ! SubscribeReceiver(context.self)

  def receive = {
    case msg ⇒ context.parent ! pushBlock(msg.asInstanceOf[T])
  }

  override def postStop() = remotePublisher ! UnsubscribeReceiver(context.self)

}

/**
 * A sample feeder actor
 *
 * Usage: FeederActor <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder would start on.
 */
object FeederActor {

  def main(args: Array[String]) {
    if(args.length < 2){
      System.err.println(
        "Usage: FeederActor <hostname> <port>\n"
      )
      System.exit(1)
    }
    val Seq(host, port) = args.toSeq


    val actorSystem = AkkaUtils.createActorSystem("test", host, port.toInt)._1
    val feeder = actorSystem.actorOf(Props[FeederActor], "FeederActor")

    println("Feeder started as:" + feeder)

    actorSystem.awaitTermination()
  }
}

/**
 * A sample word count program demonstrating the use of plugging in
 * Actor as Receiver
 * Usage: ActorWordCount <master> <hostname> <port>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
 *
 * To run this example locally, you may run Feeder Actor as
 *    `$ ./run-example spark.streaming.examples.FeederActor 127.0.1.1 9999`
 * and then run the example
 *    `$ ./run-example spark.streaming.examples.ActorWordCount local[2] 127.0.1.1 9999`
 */
object ActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: ActorWordCount <master> <hostname> <port>" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    val Seq(master, host, port) = args.toSeq

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "ActorWordCount", Seconds(2),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    /*
     * Following is the use of actorStream to plug in custom actor as receiver
     *
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility
     * to ensure the type safety, i.e type of data received and InputDstream
     * should be same.
     *
     * For example: Both actorStream and SampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */

    val lines = ssc.actorStream[String](
      Props(new SampleActorReceiver[String]("akka.tcp://test@%s:%s/user/FeederActor".format(
        host, port.toInt))), "SampleReceiver")

    //compute wordcount
    lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()

    ssc.start()
  }
}
