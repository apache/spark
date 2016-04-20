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

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam

/**
 *  Produces a count of events received from Flume.
 *
 *  This should be used in conjunction with the Spark Sink running in a Flume agent. See
 *  the Spark Streaming programming guide for more details.
 *
 *  Usage: FlumePollingEventCount <host> <port>
 *    `host` is the host on which the Spark Sink is running.
 *    `port` is the port at which the Spark Sink is listening.
 *
 *  To run this example:
 *    `$ bin/run-example org.apache.spark.examples.streaming.FlumePollingEventCount [host] [port] `
 */
object FlumePollingEventCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumePollingEventCount <host> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(host, IntParam(port)) = args

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumePollingEventCount")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    val stream = FlumeUtils.createPollingStream(ssc, host, port)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
