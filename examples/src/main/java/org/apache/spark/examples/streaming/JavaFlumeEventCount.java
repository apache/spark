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

package org.apache.spark.examples.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

/**
 *  Produces a count of events received from Flume.
 *
 *  This should be used in conjunction with an AvroSink in Flume. It will start
 *  an Avro server on at the request host:port address and listen for requests.
 *  Your Flume AvroSink should be pointed to this address.
 *
 *  Usage: JavaFlumeEventCount <host> <port>
 *    <host> is the host the Flume receiver will be started on - a receiver
 *           creates a server and listens for flume events.
 *    <port> is the port the Flume receiver will listen on.
 *
 *  To run this example:
 *     `$ bin/run-example org.apache.spark.examples.streaming.JavaFlumeEventCount <host> <port>`
 */
public final class JavaFlumeEventCount {
  private JavaFlumeEventCount() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: JavaFlumeEventCount <host> <port>");
      System.exit(1);
    }

    StreamingExamples.setStreamingLogLevels();

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    Duration batchInterval = new Duration(2000);
    SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCount");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
    JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
      FlumeUtils.createStream(ssc, host, port);

    flumeStream.count();

    flumeStream.count().map(in -> "Received " + in + " flume events.").print();

    ssc.start();
    ssc.awaitTermination();
  }
}
