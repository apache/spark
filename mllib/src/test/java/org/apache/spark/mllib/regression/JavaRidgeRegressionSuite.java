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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.util.LinearDataGenerator;

public class JavaRidgeRegressionSuite extends SharedSparkSession {

  private static double predictionError(List<LabeledPoint> validationData,
                                        RidgeRegressionModel model) {
    double errorSum = 0;
    for (LabeledPoint point : validationData) {
      double prediction = model.predict(point.features());
      errorSum += (prediction - point.label()) * (prediction - point.label());
    }
    return errorSum / validationData.size();
  }

  private static List<LabeledPoint> generateRidgeData(int numPoints, int numFeatures, double std) {
    // Pick weights as random values distributed uniformly in [-0.5, 0.5]
    Random random = new Random(42);
    double[] w = new double[numFeatures];
    for (int i = 0; i < w.length; i++) {
      w[i] = random.nextDouble() - 0.5;
    }
    return LinearDataGenerator.generateLinearInputAsList(0.0, w, numPoints, 42, std);
  }

  @Test
  public void runRidgeRegressionUsingConstructor() {
    int numExamples = 50;
    int numFeatures = 20;
    List<LabeledPoint> data = generateRidgeData(2 * numExamples, numFeatures, 10.0);

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
            new ArrayList<>(data.subList(0, numExamples)));
    List<LabeledPoint> validationData = data.subList(numExamples, 2 * numExamples);

    RidgeRegressionWithSGD ridgeSGDImpl = new RidgeRegressionWithSGD(1.0, 200, 0.0, 1.0);
    RidgeRegressionModel model = ridgeSGDImpl.run(testRDD.rdd());
    double unRegularizedErr = predictionError(validationData, model);

    ridgeSGDImpl.optimizer().setRegParam(0.1);
    model = ridgeSGDImpl.run(testRDD.rdd());
    double regularizedErr = predictionError(validationData, model);

    Assertions.assertTrue(regularizedErr < unRegularizedErr);
  }

  @Test
  public void runRidgeRegressionUsingStaticMethods() {
    int numExamples = 50;
    int numFeatures = 20;
    List<LabeledPoint> data = generateRidgeData(2 * numExamples, numFeatures, 10.0);

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
            new ArrayList<>(data.subList(0, numExamples)));
    List<LabeledPoint> validationData = data.subList(numExamples, 2 * numExamples);

    RidgeRegressionModel model = new RidgeRegressionWithSGD(1.0, 200, 0.0, 1.0)
        .run(testRDD.rdd());
    double unRegularizedErr = predictionError(validationData, model);

    model = new RidgeRegressionWithSGD(1.0, 200, 0.1, 1.0)
        .run(testRDD.rdd());
    double regularizedErr = predictionError(validationData, model);

    Assertions.assertTrue(regularizedErr < unRegularizedErr);
  }
}
