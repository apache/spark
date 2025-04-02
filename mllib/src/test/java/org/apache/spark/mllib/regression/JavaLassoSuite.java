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
import org.apache.spark.mllib.util.LinearDataGenerator;

public class JavaLassoSuite extends SharedSparkSession {

  int validatePrediction(List<LabeledPoint> validationData, LassoModel model) {
    int numAccurate = 0;
    for (LabeledPoint point : validationData) {
      Double prediction = model.predict(point.features());
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      if (Math.abs(prediction - point.label()) <= 0.5) {
        numAccurate++;
      }
    }
    return numAccurate;
  }

  @Test
  public void runLassoUsingConstructor() {
    int nPoints = 10000;
    double A = 0.0;
    double[] weights = {-1.5, 1.0e-2};

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(LinearDataGenerator.generateLinearInputAsList(A,
      weights, nPoints, 42, 0.1), 2).cache();
    List<LabeledPoint> validationData =
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 17, 0.1);

    LassoWithSGD lassoSGDImpl = new LassoWithSGD(1.0, 20, 0.01, 1.0);
    LassoModel model = lassoSGDImpl.run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assertions.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void runLassoUsingStaticMethods() {
    int nPoints = 10000;
    double A = 0.0;
    double[] weights = {-1.5, 1.0e-2};

    JavaRDD<LabeledPoint> testRDD = jsc.parallelize(LinearDataGenerator.generateLinearInputAsList(A,
      weights, nPoints, 42, 0.1), 2).cache();
    List<LabeledPoint> validationData =
      LinearDataGenerator.generateLinearInputAsList(A, weights, nPoints, 17, 0.1);

    LassoModel model = new LassoWithSGD(1.0, 100, 0.01, 1.0).run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assertions.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

}
