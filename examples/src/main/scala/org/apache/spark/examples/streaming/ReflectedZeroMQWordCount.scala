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

package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

// scalastyle:off
/**
 * An example using StreamingContext.reflectedStream, based on the ZeroMQWordCount example.
 *
 * To work with zeroMQ, some native libraries have to be installed.
 * Install zeroMQ (release 2.1) core libraries. [ZeroMQ Install guide]
 * (http://www.zeromq.org/intro:get-the-software)
 *
 * Usage: ReflectedZeroMQWordCount <zeroMQurl> <topic>
 *   <zeroMQurl> and <topic> describe where zeroMq publisher is running.
 *
 * To run this example locally, you may run publisher as
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.SimpleZeroMQPublisher tcp://127.0.1.1:1234 foo.bar`
 * and run the example as
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.ReflectedZeroMQWordCount tcp://127.0.1.1:1234 foo`
 */
// scalastyle:on
object ReflectedZeroMQWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ReflectedZeroMQWordCount <zeroMQurl> <topic>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()
    val Seq(url, topic) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ReflectedZeroMQWordCount")
    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // For this stream, a zeroMQ publisher should be running.
    val lines = ssc.reflectedStream[String](
      "org.apache.spark.streaming.zeromq.ReflectedZeroMQStreamFactory", url, topic)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    }
}
