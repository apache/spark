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

package org.apache.spark.examples.mllib;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.stat.test.BinarySample;
import org.apache.spark.mllib.stat.test.StreamingTest;
import org.apache.spark.mllib.stat.test.StreamingTestResult;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.util.Utils;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.LinkedList;

/**
 * Example for Streaming Testing.
 */
public class JavaStreamingTestExample {
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Usage: JavaStreamingTestExample " +
        "<dataDir> <batchDuration> <numBatchesTimeout>");
        System.exit(1);
    }

    String dataDir = args[0];
    Duration batchDuration = Seconds.apply(Long.valueOf(args[1]));
    int numBatchesTimeout = Integer.valueOf(args[2]);

    SparkConf conf = new SparkConf().setMaster("local").setAppName("StreamingTestExample");
    JavaStreamingContext ssc = new JavaStreamingContext(conf, batchDuration);

    ssc.checkpoint(Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark").toString());

    // $example on$
    JavaDStream<BinarySample> data = ssc.textFileStream(dataDir).map(
      new Function<String, BinarySample>() {
        @Override
        public BinarySample call(String line) throws Exception {
          String[] ts = line.split(",");
          boolean label = Boolean.valueOf(ts[0]);
          double value = Double.valueOf(ts[1]);
          return new BinarySample(label, value);
        }
      });

    StreamingTest streamingTest = new StreamingTest()
      .setPeacePeriod(0)
      .setWindowSize(0)
      .setTestMethod("welch");

    JavaDStream<StreamingTestResult> out = streamingTest.registerStream(data);
    out.print();
    // $example off$

    // Stop processing if test becomes significant or we time out
    final Accumulator<Integer> timeoutCounter =
      ssc.sparkContext().accumulator(numBatchesTimeout);

    out.foreachRDD(new VoidFunction<JavaRDD<StreamingTestResult>>() {
      @Override
      public void call(JavaRDD<StreamingTestResult> rdd) throws Exception {
        timeoutCounter.add(-1);

        long cntSignificant = rdd.filter(new Function<StreamingTestResult, Boolean>() {
          @Override
          public Boolean call(StreamingTestResult v) throws Exception {
            return v.pValue() < 0.05;
          }
        }).count();

        if (timeoutCounter.value() <= 0 || cntSignificant > 0) {
          rdd.context().stop();
        }
      }
    });

    ssc.start();
    ssc.awaitTermination();
  }
}
