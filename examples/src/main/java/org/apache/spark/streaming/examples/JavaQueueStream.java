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

import com.google.common.collect.Lists;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class JavaQueueStream {
  public static void main(String[] args) throws InterruptedException {
    if (args.length < 1) {
      System.err.println("Usage: JavaQueueStream <master>");
      System.exit(1);
    }

    // Create the context
    JavaStreamingContext ssc = new JavaStreamingContext(args[0], "QueueStream", new Duration(1000),
            System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    Queue<JavaRDD<Integer>> rddQueue = new LinkedList<JavaRDD<Integer>>();

    // Create and push some RDDs into the queue
    List<Integer> list = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      list.add(i);
    }

    for (int i = 0; i < 30; i++) {
      rddQueue.add(ssc.sc().parallelize(list));
    }


    // Create the QueueInputDStream and use it do some processing
    JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
    JavaPairDStream<Integer, Integer> mappedStream = inputStream.map(
        new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer i) throws Exception {
            return new Tuple2<Integer, Integer>(i % 10, 1);
          }
        });
    JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) throws Exception {
          return i1 + i2;
        }
    });

    reducedStream.print();
    ssc.start();
  }
}
