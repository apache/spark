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

package org.apache.spark.mllib.clustering;

import java.io.Serializable;
import java.util.List;

import scala.Tuple2;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.spark.streaming.JavaTestUtils.*;

import org.apache.spark.SparkConf;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaStreamingKMeansSuite implements Serializable {

  protected transient JavaStreamingContext ssc;

  @Before
  public void setUp() {
    SparkConf conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
    ssc = new JavaStreamingContext(conf, new Duration(1000));
    ssc.checkpoint("checkpoint");
  }

  @After
  public void tearDown() {
    ssc.stop();
    ssc = null;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void javaAPI() {
    List<Vector> trainingBatch = Lists.newArrayList(
      Vectors.dense(1.0),
      Vectors.dense(0.0));
    JavaDStream<Vector> training =
      attachTestInputStream(ssc, Lists.newArrayList(trainingBatch, trainingBatch), 2);
    List<Tuple2<Integer, Vector>> testBatch = Lists.newArrayList(
      new Tuple2<Integer, Vector>(10, Vectors.dense(1.0)),
      new Tuple2<Integer, Vector>(11, Vectors.dense(0.0)));
    JavaPairDStream<Integer, Vector> test = JavaPairDStream.fromJavaDStream(
      attachTestInputStream(ssc, Lists.newArrayList(testBatch, testBatch), 2));
    StreamingKMeans skmeans = new StreamingKMeans()
      .setK(1)
      .setDecayFactor(1.0)
      .setInitialCenters(new Vector[]{Vectors.dense(1.0)}, new double[]{0.0});
    skmeans.trainOn(training);
    JavaPairDStream<Integer, Integer> prediction = skmeans.predictOnValues(test);
    attachTestOutputStream(prediction.count());
    runStreams(ssc, 2, 2);
  }
}
