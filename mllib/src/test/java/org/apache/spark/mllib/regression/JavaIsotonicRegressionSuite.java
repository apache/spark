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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.util.IsotonicDataGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class JavaIsotonicRegressionSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaLinearRegressionSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  double difference(List<Tuple2<Double, Double>> expected, IsotonicRegressionModel model) {
    double diff = 0;

    for(int i = 0; i < model.predictions().length(); i++) {
      Tuple2<Double, Double> exp = expected.get(i);
      diff += Math.abs(model.predict(exp._2()) - exp._1());
    }

    return diff;
  }

  @Test
  public void runIsotonicRegressionUsingStaticMethod() {
    JavaPairRDD<Double, Double> trainRDD = sc.parallelizePairs(
      IsotonicDataGenerator.generateIsotonicInputAsList(
        new double[]{1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12})).cache();

    IsotonicRegressionModel model = IsotonicRegression.train(trainRDD, true);

    List<Tuple2<Double, Double>> expected = IsotonicDataGenerator
      .generateIsotonicInputAsList(
        new double[] {1, 2, 7d/3, 7d/3, 7d/3, 6, 7, 8, 10, 10, 10, 12});

    Assert.assertTrue(difference(expected, model) == 0);
  }

  @Test
  public void testPredictJavaRDD() {
    JavaPairRDD<Double, Double> trainRDD = sc.parallelizePairs(
      IsotonicDataGenerator.generateIsotonicInputAsList(
        new double[]{1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12})).cache();

    IsotonicRegressionModel model = IsotonicRegression.train(trainRDD, true);

    JavaRDD<Double> testRDD = trainRDD.map(new Function<Tuple2<Double, Double>, Double>() {
      @Override
      public Double call(Tuple2<Double, Double> v) throws Exception {
        return v._2();
      }
    });

    List<Double> predictions = model.predict(testRDD).collect();

    Assert.assertTrue(predictions.get(0) == 1d);
    Assert.assertTrue(predictions.get(11) == 12d);
  }
}

