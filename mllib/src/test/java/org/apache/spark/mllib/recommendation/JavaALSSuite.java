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

package org.apache.spark.mllib.recommendation;

import java.io.Serializable;
import java.util.List;

import scala.Tuple2;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.jblas.DoubleMatrix;

public class JavaALSSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaALS");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
    System.clearProperty("spark.driver.port");
  }

  void validatePrediction(MatrixFactorizationModel model, int users, int products, int features, 
      DoubleMatrix trueRatings, double matchThreshold) {
    DoubleMatrix predictedU = new DoubleMatrix(users, features);
    List<scala.Tuple2<Object, double[]>> userFeatures = model.userFeatures().toJavaRDD().collect();
    for (int i = 0; i < features; ++i) {
      for (scala.Tuple2<Object, double[]> userFeature : userFeatures) {
        predictedU.put((Integer)userFeature._1(), i, userFeature._2()[i]);
      }
    }
    DoubleMatrix predictedP = new DoubleMatrix(products, features);

    List<scala.Tuple2<Object, double[]>> productFeatures =
      model.productFeatures().toJavaRDD().collect();
    for (int i = 0; i < features; ++i) {
      for (scala.Tuple2<Object, double[]> productFeature : productFeatures) {
        predictedP.put((Integer)productFeature._1(), i, productFeature._2()[i]);
      }
    }

    DoubleMatrix predictedRatings = predictedU.mmul(predictedP.transpose());

    for (int u = 0; u < users; ++u) {
      for (int p = 0; p < products; ++p) {
        double prediction = predictedRatings.get(u, p);
        double correct = trueRatings.get(u, p);
        Assert.assertTrue(Math.abs(prediction - correct) < matchThreshold);
      }
    }
  }

  @Test
  public void runALSUsingStaticMethods() {
    int features = 1;
    int iterations = 15;
    int users = 10;
    int products = 10;
    scala.Tuple2<List<Rating>, DoubleMatrix> testData = ALSSuite.generateRatingsAsJavaList(
        users, products, features, 0.7);

    JavaRDD<Rating> data = sc.parallelize(testData._1());
    MatrixFactorizationModel model = ALS.train(data.rdd(), features, iterations);
    validatePrediction(model, users, products, features, testData._2(), 0.3);
  }

  @Test
  public void runALSUsingConstructor() {
    int features = 2;
    int iterations = 15;
    int users = 20;
    int products = 30;
    scala.Tuple2<List<Rating>, DoubleMatrix> testData = ALSSuite.generateRatingsAsJavaList(
        users, products, features, 0.7);

    JavaRDD<Rating> data = sc.parallelize(testData._1());

    MatrixFactorizationModel model = new ALS().setRank(features)
                                              .setIterations(iterations)
                                              .run(data.rdd());
    validatePrediction(model, users, products, features, testData._2(), 0.3);
  }
}
