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


import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
// $example on$
import org.apache.spark.mllib.stat.test.BinarySample;
import org.apache.spark.mllib.stat.test.StreamingTest;
import org.apache.spark.mllib.stat.test.StreamingTestResult;
// $example off$
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.Utils;


/**
 * Perform streaming testing using Welch's 2-sample t-test on a stream of data, where the data
 * stream arrives as text files in a directory. Stops when the two groups are statistically
 * significant (p-value < 0.05) or after a user-specified timeout in number of batches is exceeded.
 *
 * The rows of the text files must be in the form `Boolean, Double`. For example:
 *   false, -3.92
 *   true, 99.32
 *
 * Usage:
 *   JavaStreamingTestExample <dataDir> <batchDuration> <numBatchesTimeout>
 *
 * To run on your local machine using the directory `dataDir` with 5 seconds between each batch and
 * a timeout after 100 insignificant batches, call:
 *    $ bin/run-example mllib.JavaStreamingTestExample dataDir 5 100
 *
 * As you add text files to `dataDir` the significance test wil continually update every
 * `batchDuration` seconds until the test becomes significant (p-value < 0.05) or the number of
 * batches processed exceeds `numBatchesTimeout`.
 */
public class JavaStreamingTestExample {

  private static int timeoutCounter = 0;

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: JavaStreamingTestExample " +
        "<dataDir> <batchDuration> <numBatchesTimeout>");
        System.exit(1);
    }

    String dataDir = args[0];
    Duration batchDuration = Seconds.apply(Long.parseLong(args[1]));
    int numBatchesTimeout = Integer.parseInt(args[2]);

    SparkConf conf = new SparkConf().setMaster("local").setAppName("StreamingTestExample");
    JavaStreamingContext ssc = new JavaStreamingContext(conf, batchDuration);

    ssc.checkpoint(Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark").toString());

    // $example on$
    JavaDStream<BinarySample> data = ssc.textFileStream(dataDir).map(
      new Function<String, BinarySample>() {
        @Override
        public BinarySample call(String line) {
          String[] ts = line.split(",");
          boolean label = Boolean.parseBoolean(ts[0]);
          double value = Double.parseDouble(ts[1]);
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
    timeoutCounter = numBatchesTimeout;

    out.foreachRDD(new VoidFunction<JavaRDD<StreamingTestResult>>() {
      @Override
      public void call(JavaRDD<StreamingTestResult> rdd) {
        timeoutCounter -= 1;

        boolean anySignificant = !rdd.filter(new Function<StreamingTestResult, Boolean>() {
          @Override
          public Boolean call(StreamingTestResult v) {
            return v.pValue() < 0.05;
          }
        }).isEmpty();

        if (timeoutCounter <= 0 || anySignificant) {
          rdd.context().stop();
        }
      }
    });

    ssc.start();
    ssc.awaitTermination();
  }
}
