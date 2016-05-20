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

package org.apache.spark.mllib.stat;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.test.BinarySample;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult;
import org.apache.spark.mllib.stat.test.StreamingTest;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import static org.apache.spark.streaming.JavaTestUtils.*;

public class JavaStatisticsSuite {
  private transient SparkSession spark;
  private transient JavaSparkContext jsc;
  private transient JavaStreamingContext ssc;

  @Before
  public void setUp() {
    SparkConf conf = new SparkConf()
      .set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("JavaStatistics")
      .config(conf)
      .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
    ssc = new JavaStreamingContext(jsc, new Duration(1000));
    ssc.checkpoint("checkpoint");
  }

  @After
  public void tearDown() {
    spark.stop();
    ssc.stop();
    spark = null;
  }

  @Test
  public void testCorr() {
    JavaRDD<Double> x = jsc.parallelize(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    JavaRDD<Double> y = jsc.parallelize(Arrays.asList(1.1, 2.2, 3.1, 4.3));

    Double corr1 = Statistics.corr(x, y);
    Double corr2 = Statistics.corr(x, y, "pearson");
    // Check default method
    assertEquals(corr1, corr2, 1e-5);
  }

  @Test
  public void kolmogorovSmirnovTest() {
    JavaDoubleRDD data = jsc.parallelizeDoubles(Arrays.asList(0.2, 1.0, -1.0, 2.0));
    KolmogorovSmirnovTestResult testResult1 = Statistics.kolmogorovSmirnovTest(data, "norm");
    KolmogorovSmirnovTestResult testResult2 = Statistics.kolmogorovSmirnovTest(
      data, "norm", 0.0, 1.0);
  }

  @Test
  public void chiSqTest() {
    JavaRDD<LabeledPoint> data = jsc.parallelize(Arrays.asList(
      new LabeledPoint(0.0, Vectors.dense(0.1, 2.3)),
      new LabeledPoint(1.0, Vectors.dense(1.5, 5.1)),
      new LabeledPoint(0.0, Vectors.dense(2.4, 8.1))));
    ChiSqTestResult[] testResults = Statistics.chiSqTest(data);
  }

  @Test
  public void streamingTest() {
    List<BinarySample> trainingBatch = Arrays.asList(
      new BinarySample(true, 1.0),
      new BinarySample(false, 2.0));
    JavaDStream<BinarySample> training =
      attachTestInputStream(ssc, Arrays.asList(trainingBatch, trainingBatch), 2);
    int numBatches = 2;
    StreamingTest model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod("welch");
    model.registerStream(training);
    attachTestOutputStream(training);
    runStreams(ssc, numBatches, numBatches);
  }
}
