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

import akka.actor.ActorSystem
import akka.actor.actorRef2Scala
import akka.zeromq._
import akka.zeromq.Subscribe
import akka.util.ByteString

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.zeromq._

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
    implicit def stringToByteString(x: String) = ByteString(x)
    val messages: List[ByteString] = List("words ", "may ", "count ")
    while (true) {
      Thread.sleep(1000)
      pubSocket ! ZMQMessage(ByteString(topic) :: messages)
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
 *    `$ ./bin/run-example org.apache.spark.streaming.examples.SimpleZeroMQPublisher tcp://127.0.1.1:1234 foo.bar`
 * and run the example as
 *    `$ ./bin/run-example org.apache.spark.streaming.examples.ZeroMQWordCount local[2] tcp://127.0.1.1:1234 foo`
 */
object ZeroMQWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: ZeroMQWordCount <master> <zeroMQurl> <topic>" +
          "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()
    val Seq(master, url, topic) = args.toSeq

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "ZeroMQWordCount", Seconds(2),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))

    def bytesToStringIterator(x: Seq[ByteString]) = (x.map(_.utf8String)).iterator

    //For this stream, a zeroMQ publisher should be running.
    val lines = ZeroMQUtils.createStream(ssc, url, Subscribe(topic), bytesToStringIterator _)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
