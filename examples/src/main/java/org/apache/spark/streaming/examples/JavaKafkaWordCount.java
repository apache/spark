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

package org.apache.spark.streaming.examples;

import java.util.Map;
import java.util.HashMap;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaKafkaWordCount <master> <zkQuorum> <group> <topics> <numThreads>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `./run-example org.apache.spark.streaming.examples.JavaKafkaWordCount local[2] zoo01,zoo02,
 *    zoo03 my-consumer-group topic1,topic2 1`
 */

public class JavaKafkaWordCount {
  public static void main(String[] args) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaWordCount <master> <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }

    // Create the context with a 1 second batch size
    JavaStreamingContext ssc = new JavaStreamingContext(args[0], "NetworkWordCount",
            new Duration(2000), System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));

    int numThreads = Integer.parseInt(args[4]);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = args[3].split(",");
    for (String topic: topics) {
      topicMap.put(topic, numThreads);
    }

    JavaPairDStream<String, String> messages = ssc.kafkaStream(args[1], args[2], topicMap);

    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) throws Exception {
        return tuple2._2();
      }
    });

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Lists.newArrayList(x.split(" "));
      }
    });

    JavaPairDStream<String, Integer> wordCounts = words.map(
      new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) throws Exception {
          return i1 + i2;
        }
      });

    wordCounts.print();
    ssc.start();
  }
}
