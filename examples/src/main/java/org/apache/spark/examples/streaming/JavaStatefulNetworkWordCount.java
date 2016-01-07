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
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;

/**
 * Counts words cumulatively in UTF8 encoded, '\n' delimited text received from the network every
 * second starting with initial value of word count.
 * Usage: JavaStatefulNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 * data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaStatefulNetworkWordCount localhost 9999`
 */
public class JavaStatefulNetworkWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: JavaStatefulNetworkWordCount <hostname> <port>");
      System.exit(1);
    }

    StreamingExamples.setStreamingLogLevels();

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaStatefulNetworkWordCount");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    ssc.checkpoint(".");

    // Initial state RDD input to mapWithState
    @SuppressWarnings("unchecked")
    List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<String, Integer>("hello", 1),
            new Tuple2<String, Integer>("world", 1));
    JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2);

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Lists.newArrayList(SPACE.split(x));
      }
    });

    JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
        });

    // Update the cumulative count function
    final Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
        new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {

          @Override
          public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) {
            int sum = one.or(0) + (state.exists() ? state.get() : 0);
            Tuple2<String, Integer> output = new Tuple2<String, Integer>(word, sum);
            state.update(sum);
            return output;
          }
        };

    // DStream made of get cumulative counts that get updated in every batch
    JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
        wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

    stateDstream.print();
    ssc.start();
    ssc.awaitTermination();
  }
}
