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

package org.apache.spark.mllib.regression;

import java.io.Serializable;
import java.util.List;

import scala.Tuple3;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaIsotonicRegressionSuite implements Serializable {
  private transient JavaSparkContext sc;

  private List<Tuple3<Double, Double, Double>> generateIsotonicInput(double[] labels) {
    List<Tuple3<Double, Double, Double>> input = Lists.newArrayList();

    for (int i = 1; i <= labels.length; i++) {
      input.add(new Tuple3<Double, Double, Double>(labels[i-1], (double) i, 1d));
    }

    return input;
  }

  private IsotonicRegressionModel runIsotonicRegression(double[] labels) {
    JavaRDD<Tuple3<Double, Double, Double>> trainRDD =
      sc.parallelize(generateIsotonicInput(labels), 2).cache();

    return new IsotonicRegression().run(trainRDD);
  }

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaLinearRegressionSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void testIsotonicRegressionJavaRDD() {
    IsotonicRegressionModel model =
      runIsotonicRegression(new double[]{1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12});

    Assert.assertArrayEquals(
      new double[] {1, 2, 7d/3, 7d/3, 6, 7, 8, 10, 10, 12}, model.predictions(), 1e-14);
  }

  @Test
  public void testIsotonicRegressionPredictionsJavaRDD() {
    IsotonicRegressionModel model =
      runIsotonicRegression(new double[]{1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12});

    JavaDoubleRDD testRDD = sc.parallelizeDoubles(Lists.newArrayList(0.0, 1.0, 9.5, 12.0, 13.0));
    List<Double> predictions = model.predict(testRDD).collect();

    Assert.assertTrue(predictions.get(0) == 1d);
    Assert.assertTrue(predictions.get(1) == 1d);
    Assert.assertTrue(predictions.get(2) == 10d);
    Assert.assertTrue(predictions.get(3) == 12d);
    Assert.assertTrue(predictions.get(4) == 12d);
  }
}
