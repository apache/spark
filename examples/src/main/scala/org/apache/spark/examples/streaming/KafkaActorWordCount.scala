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

// scalastyle:off println
package org.apache.spark.examples.streaming

import java.util.Properties

import scala.collection.mutable.LinkedHashSet
import scala.reflect.classTag

import akka.actor._
import com.typesafe.config.ConfigFactory
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.akka.{ActorReceiver, AkkaUtils}
import org.apache.spark.util.ThreadUtils

case class RegisterReceiver(receiverActor: ActorRef)
case class UnRegisterReceiver(receiverActor: ActorRef)
case class AddKafkaTopics(kafkaParams: Map[String, String], topicMap: Map[String, Int])

/**
  * Sends kafka messages to every receiver subscribed
  */
class KafkaFeederActor extends Actor with Logging{

  val receivers = new LinkedHashSet[ActorRef]()

  def receive: Receive = {
    case RegisterReceiver(receiverActor: ActorRef) =>
      println("received subscribe from %s".format(receiverActor.toString))
      receivers += receiverActor

    case UnRegisterReceiver(receiverActor: ActorRef) =>
      println("received unsubscribe from %s".format(receiverActor.toString))
      receivers -= receiverActor

    case AddKafkaTopics(kafkaParams: Map[String, String], topics: Map[String, Int]) =>
      // Kafka connection properties
      val props = new Properties()
      kafkaParams.foreach(param => props.put(param._1, param._2))

      val zkConnect = kafkaParams("zookeeper.connect")
      // Create the connection to the cluster
      logInfo("Connecting to Zookeeper: " + zkConnect)
      val consumerConfig = new ConsumerConfig(props)
      val consumerConnector = Consumer.create(consumerConfig)
      logInfo("Connected to " + zkConnect)

      val keyDecoder =
        classTag[StringDecoder].runtimeClass.getConstructor(classOf[VerifiableProperties])
          .newInstance(consumerConfig.props).asInstanceOf[Decoder[String]]
      val valueDecoder =
        classTag[StringDecoder].runtimeClass.getConstructor(classOf[VerifiableProperties])
          .newInstance(consumerConfig.props).asInstanceOf[Decoder[String]]

      // Create threads for each topic/message Stream we are listening
      val topicMessageStreams = consumerConnector.createMessageStreams(
        topics, keyDecoder, valueDecoder)

      val executorPool =
        ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "KafkaMessageHandler")
      try {
        // Start the messages handler for each partition
        topicMessageStreams.values.foreach { streams =>
          streams.foreach { stream => executorPool.submit(new MessageHandler(stream)) }
        }
      } finally {
        executorPool.shutdown() // Just causes threads to terminate after work is done
      }
  }

  // Handles Kafka messages
  private class MessageHandler(stream: KafkaStream[String, String])
    extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      try {
        val streamIterator = stream.iterator()
        while (streamIterator.hasNext()) {
          val msgAndMetadata = streamIterator.next()
          receivers.foreach(_ ! (msgAndMetadata.key + msgAndMetadata.message))
        }
      } catch {
        case e: Throwable => System.err.println("Error handling message; exiting", e)
      }
    }
  }
}

/**
  * A sample actor as receiver, is also simplest. This receiver actor
  * goes and subscribe to a typical publisher/feeder actor and receives
  * data.
  *
  * @see [[org.apache.spark.examples.streaming.KafkaFeederActor]]
  */
class KafkaActorReceiver[T](urlOfPublisher: String) extends ActorReceiver {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart(): Unit = remotePublisher ! RegisterReceiver(context.self)

  def receive: PartialFunction[Any, Unit] = {
    case msg => store(msg.asInstanceOf[T])
  }

  override def postStop(): Unit = remotePublisher ! UnRegisterReceiver(context.self)

}

/**
  * A sample feeder actor
  *
  * Usage: KafkaFeederActor <hostname> <port>
  *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder would start on.
  */
object KafkaFeederActor {

  def main(args: Array[String]) {
    if (args.length < 2){
      System.err.println("Usage: KafkaFeederActor <hostname> <port>\n")
      System.exit(1)
    }
    val Seq(host, port) = args.toSeq

    val akkaConf = ConfigFactory.parseString(
      s"""akka.actor.provider = "akka.remote.RemoteActorRefProvider"
          |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
          |akka.remote.netty.tcp.hostname = "$host"
          |akka.remote.netty.tcp.port = $port
          |""".stripMargin)
    val actorSystem = ActorSystem("test", akkaConf)
    val feeder = actorSystem.actorOf(Props[KafkaFeederActor], "KafkaFeederActor")

    println("Feeder started as:" + feeder)

    actorSystem.awaitTermination()
  }
}

/**
  * A sample word count program demonstrating the use of plugging in
  *
  * Actor as Receiver
  * Usage: KafkaActorWordCount <hostname> <port>
  *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
  *
  * To run this example locally, you may run Feeder Actor as
  *    `$ bin/run-example org.apache.spark.examples.streaming.KafkaFeederActor localhost 9999`
  * and then run the example
  *    `$ bin/run-example org.apache.spark.examples.streaming.KafkaActorWordCount localhost 9999`
  * and then add topics
  *     `$ bin/run-example org.apache.spark.examples.streaming.AddKafkaTopic localhost 9999 \
  *    zoo01,zoo02,zoo03 my-consumer-group topic1,topic2 1`
  */
private object KafkaActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: KafkaActorWordCount <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Seq(host, port) = args.toSeq
    val sparkConf = new SparkConf().setAppName("KafkaActorWordCount")
    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    /*
     * Following is the use of AkkaUtils.createStream to plug in custom actor as receiver
     *
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility
     * to ensure the type safety, i.e type of data received and InputDStream
     * should be same.
     *
     * For example: Both AkkaUtils.createStream and SampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */
    val lines = AkkaUtils.createStream[String](
      ssc,
      Props(classOf[KafkaActorReceiver[String]],
        "akka.tcp://test@%s:%s/user/KafkaFeederActor".format(host, port.toInt)),
      "SampleReceiver")
    // compute wordcount
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

/**
  * A sample program to add kafka topics
  *
  * Usage: AddKafkaTopic <hostname> <port> <zkQuorum> <group> <topics> <numThreads>
  *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
  *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
  *   <group> is the name of kafka consumer group
  *   <topics> is a list of one or more kafka topics to consume from
  *   <numThreads> is the number of threads the kafka consumer should use
  *
  */
private object AddKafkaTopic {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: AddKafkaTopic <hostname> <port> <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()

    val Array(host, port, zkQuorum, group, topics, numThreads) = args
    val urlOfPublisher = "akka.tcp://test@%s:%s/user/KafkaFeederActor".format(host, port.toInt)

    val akkaConf = ConfigFactory.parseString(
      s"""akka.actor.provider = "akka.remote.RemoteActorRefProvider"
          |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
          |akka.remote.netty.tcp.hostname = "localhost"
          |akka.remote.netty.tcp.port = 2553
          |""".stripMargin)
    val system = ActorSystem("test", akkaConf)
    val topic = system.actorSelection(urlOfPublisher)

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> group,
      "zookeeper.connection.timeout.ms" -> "10000")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    topic ! AddKafkaTopics(kafkaParams, topicMap)
    system.awaitTermination()
  }
}
// scalastyle:on println
