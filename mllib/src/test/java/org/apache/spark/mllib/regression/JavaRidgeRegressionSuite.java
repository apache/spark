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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.jblas.DoubleMatrix;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.util.LinearDataGenerator;

public class JavaRidgeRegressionSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
      sc = new JavaSparkContext("local", "JavaRidgeRegressionSuite");
  }

  @After
  public void tearDown() {
      sc.stop();
      sc = null;
      System.clearProperty("spark.driver.port");
  }

  double predictionError(List<LabeledPoint> validationData, RidgeRegressionModel model) {
    double errorSum = 0;
    for (LabeledPoint point: validationData) {
      Double prediction = model.predict(point.features());
      errorSum += (prediction - point.label()) * (prediction - point.label());
    }
    return errorSum / validationData.size();
  }

  List<LabeledPoint> generateRidgeData(int numPoints, int nfeatures, double eps) {
    org.jblas.util.Random.seed(42);
    // Pick weights as random values distributed uniformly in [-0.5, 0.5]
    DoubleMatrix w = DoubleMatrix.rand(nfeatures, 1).subi(0.5);
    // Set first two weights to eps
    w.put(0, 0, eps);
    w.put(1, 0, eps);
    return LinearDataGenerator.generateLinearInputAsList(0.0, w.data, numPoints, 42, eps);
  }

  @Test
  public void runRidgeRegressionUsingConstructor() {
    int nexamples = 200;
    int nfeatures = 20;
    double eps = 10.0;
    List<LabeledPoint> data = generateRidgeData(2*nexamples, nfeatures, eps);

    JavaRDD<LabeledPoint> testRDD = sc.parallelize(data.subList(0, nexamples));
    List<LabeledPoint> validationData = data.subList(nexamples, 2*nexamples);

    RidgeRegressionWithSGD ridgeSGDImpl = new RidgeRegressionWithSGD();
    ridgeSGDImpl.optimizer().setStepSize(1.0)
                            .setRegParam(0.0)
                            .setNumIterations(200);
    RidgeRegressionModel model = ridgeSGDImpl.run(testRDD.rdd());
    double unRegularizedErr = predictionError(validationData, model);

    ridgeSGDImpl.optimizer().setRegParam(0.1);
    model = ridgeSGDImpl.run(testRDD.rdd());
    double regularizedErr = predictionError(validationData, model);

    Assert.assertTrue(regularizedErr < unRegularizedErr);
  }

  @Test
  public void runRidgeRegressionUsingStaticMethods() {
    int nexamples = 200;
    int nfeatures = 20;
    double eps = 10.0;
    List<LabeledPoint> data = generateRidgeData(2*nexamples, nfeatures, eps);

    JavaRDD<LabeledPoint> testRDD = sc.parallelize(data.subList(0, nexamples));
    List<LabeledPoint> validationData = data.subList(nexamples, 2*nexamples);

    RidgeRegressionModel model = RidgeRegressionWithSGD.train(testRDD.rdd(), 200, 1.0, 0.0);
    double unRegularizedErr = predictionError(validationData, model);

    model = RidgeRegressionWithSGD.train(testRDD.rdd(), 200, 1.0, 0.1);
    double regularizedErr = predictionError(validationData, model);

    Assert.assertTrue(regularizedErr < unRegularizedErr);
  }
}
