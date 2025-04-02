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

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.util.LinearDataGenerator;

public class JavaLinearRegressionSuite extends SharedSparkSession {

  private static int validatePrediction(
      List<LabeledPoint> validationData, LinearRegressionModel model) {
    int numAccurate = 0;
    for (LabeledPoint point : validationData) {
      double prediction = model.predict(point.features());
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      if (Math.abs(prediction - point.label()) <= 0.5) {
        numAccurate++;
      }
    }
    return numAccurate;
  }

  @Test
  public void runLinearRegressionUsingConstructor() {
    int nPoints = 100;
    double A = 3.0;
    double[] weights = {10, 10};

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 42, 0.1), 2).cache();
    List<LabeledPoint> validationData =
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 17, 0.1);

    LinearRegressionWithSGD linSGDImpl = new LinearRegressionWithSGD(1.0, 100, 0.0, 1.0);
    linSGDImpl.setIntercept(true);
    LinearRegressionModel model = linSGDImpl.run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assertions.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void runLinearRegressionUsingStaticMethods() {
    int nPoints = 100;
    double A = 0.0;
    double[] weights = {10, 10};

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 42, 0.1), 2).cache();
    List<LabeledPoint> validationData =
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 17, 0.1);

    LinearRegressionModel model = new LinearRegressionWithSGD(1.0, 100, 0.0, 1.0)
        .run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assertions.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void testPredictJavaRDD() {
    int nPoints = 100;
    double A = 0.0;
    double[] weights = {10, 10};
    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 42, 0.1), 2).cache();
    LinearRegressionWithSGD linSGDImpl = new LinearRegressionWithSGD(1.0, 100, 0.0, 1.0);
    LinearRegressionModel model = linSGDImpl.run(testRDD.rdd());
    JavaRDD<Vector> vectors = testRDD.map(LabeledPoint::features);
    JavaRDD<Double> predictions = model.predict(vectors);
    // Should be able to get the first prediction.
    predictions.first();
  }
}
