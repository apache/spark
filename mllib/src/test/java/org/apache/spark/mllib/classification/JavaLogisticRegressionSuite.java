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

package org.apache.spark.mllib.classification;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

public class JavaLogisticRegressionSuite extends SharedSparkSession {

  int validatePrediction(List<LabeledPoint> validationData, LogisticRegressionModel model) {
    int numAccurate = 0;
    for (LabeledPoint point : validationData) {
      Double prediction = model.predict(point.features());
      if (prediction == point.label()) {
        numAccurate++;
      }
    }
    return numAccurate;
  }

  @Test
  public void runLRUsingConstructor() {
    int nPoints = 10000;
    double A = 2.0;
    double B = -1.5;

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 42), 2).cache();
    List<LabeledPoint> validationData =
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 17);

    LogisticRegressionWithSGD lrImpl = new LogisticRegressionWithSGD(1.0, 100, 1.0, 1.0);
    lrImpl.setIntercept(true);
    LogisticRegressionModel model = lrImpl.run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void runLRUsingStaticMethods() {
    int nPoints = 10000;
    double A = 0.0;
    double B = -2.5;

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 42), 2).cache();
    List<LabeledPoint> validationData =
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 17);

    LogisticRegressionModel model = new LogisticRegressionWithSGD(1.0, 100, 0.01, 1.0)
        .run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }
}
