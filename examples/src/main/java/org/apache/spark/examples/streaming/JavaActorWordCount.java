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

import java.util.Arrays;
import java.util.Iterator;

import scala.Tuple2;

import akka.actor.ActorSelection;
import akka.actor.Props;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.akka.AkkaUtils;
import org.apache.spark.streaming.akka.JavaActorReceiver;

/**
 * A sample actor as receiver, is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives
 * data.
 *
 * @see [[org.apache.spark.examples.streaming.FeederActor]]
 */
class JavaSampleActorReceiver<T> extends JavaActorReceiver {

  private final String urlOfPublisher;

  public JavaSampleActorReceiver(String urlOfPublisher) {
    this.urlOfPublisher = urlOfPublisher;
  }

  private ActorSelection remotePublisher;

  @Override
  public void preStart() {
    remotePublisher = getContext().actorSelection(urlOfPublisher);
    remotePublisher.tell(new SubscribeReceiver(getSelf()), getSelf());
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    @SuppressWarnings("unchecked")
    T msgT = (T) msg;
    store(msgT);
  }

  @Override
  public void postStop() {
    remotePublisher.tell(new UnsubscribeReceiver(getSelf()), getSelf());
  }
}

/**
 * A sample word count program demonstrating the use of plugging in
 * Actor as Receiver
 * Usage: JavaActorWordCount <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
 *
 * To run this example locally, you may run Feeder Actor as
 * <code><pre>
 *     $ bin/run-example org.apache.spark.examples.streaming.FeederActor localhost 9999
 * </pre></code>
 * and then run the example
 * <code><pre>
 *     $ bin/run-example org.apache.spark.examples.streaming.JavaActorWordCount localhost 9999
 * </pre></code>
 */
public class JavaActorWordCount {

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: JavaActorWordCount <hostname> <port>");
      System.exit(1);
    }

    StreamingExamples.setStreamingLogLevels();

    final String host = args[0];
    final String port = args[1];
    SparkConf sparkConf = new SparkConf().setAppName("JavaActorWordCount");
    // Create the context and set the batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

    String feederActorURI = "akka.tcp://test@" + host + ":" + port + "/user/FeederActor";

    /*
     * Following is the use of AkkaUtils.createStream to plug in custom actor as receiver
     *
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility
     * to ensure the type safety, i.e type of data received and InputDstream
     * should be same.
     *
     * For example: Both AkkaUtils.createStream and JavaSampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */
    JavaDStream<String> lines = AkkaUtils.createStream(
        jssc,
        Props.create(JavaSampleActorReceiver.class, feederActorURI),
        "SampleReceiver");

    // compute wordcount
    lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) {
        return Arrays.asList(s.split("\\s+")).iterator();
      }
    }).mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<>(s, 1);
      }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    }).print();

    jssc.start();
    jssc.awaitTermination();
  }
}
