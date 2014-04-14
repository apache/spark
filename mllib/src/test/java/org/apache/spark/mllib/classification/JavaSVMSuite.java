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


import java.io.Serializable;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.regression.LabeledPoint;

public class JavaSVMSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaSVMSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
    System.clearProperty("spark.driver.port");
  }

  int validatePrediction(List<LabeledPoint> validationData, SVMModel model) {
    int numAccurate = 0;
    for (LabeledPoint point: validationData) {
      Double prediction = model.predict(point.features());
      if (prediction == point.label()) {
        numAccurate++;
      }
    }
    return numAccurate;
  }

  @Test
  public void runSVMUsingConstructor() {
    int nPoints = 10000;
    double A = 2.0;
    double[] weights = {-1.5, 1.0};

    JavaRDD<LabeledPoint> testRDD = sc.parallelize(SVMSuite.generateSVMInputAsList(A,
        weights, nPoints, 42), 2).cache();
    List<LabeledPoint> validationData =
        SVMSuite.generateSVMInputAsList(A, weights, nPoints, 17);

    SVMWithSGD svmSGDImpl = new SVMWithSGD();
    svmSGDImpl.optimizer().setStepSize(1.0)
                          .setRegParam(1.0)
                          .setNumIterations(100);
    SVMModel model = svmSGDImpl.run(testRDD.rdd());

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

  @Test
  public void runSVMUsingStaticMethods() {
    int nPoints = 10000;
    double A = 2.0;
    double[] weights = {-1.5, 1.0};

    JavaRDD<LabeledPoint> testRDD = sc.parallelize(SVMSuite.generateSVMInputAsList(A,
        weights, nPoints, 42), 2).cache();
    List<LabeledPoint> validationData =
        SVMSuite.generateSVMInputAsList(A, weights, nPoints, 17);

    SVMModel model = SVMWithSGD.train(testRDD.rdd(), 100, 1.0, 1.0, 1.0);

    int numAccurate = validatePrediction(validationData, model);
    Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
  }

}
