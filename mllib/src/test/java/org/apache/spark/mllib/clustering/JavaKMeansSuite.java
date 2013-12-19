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
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaKMeansSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaKMeans");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
    System.clearProperty("spark.driver.port");
  }

  // L1 distance between two points
  double distance1(double[] v1, double[] v2) {
    double distance = 0.0;
    for (int i = 0; i < v1.length; ++i) {
      distance = Math.max(distance, Math.abs(v1[i] - v2[i]));
    }
    return distance;
  }

  // Assert that two sets of points are equal, within EPSILON tolerance
  void assertSetsEqual(double[][] v1, double[][] v2) {
    double EPSILON = 1e-4;
    Assert.assertTrue(v1.length == v2.length);
    for (int i = 0; i < v1.length; ++i) {
      double minDistance = Double.MAX_VALUE;
      for (int j = 0; j < v2.length; ++j) {
        minDistance = Math.min(minDistance, distance1(v1[i], v2[j]));
      }
      Assert.assertTrue(minDistance <= EPSILON);
    }

    for (int i = 0; i < v2.length; ++i) {
      double minDistance = Double.MAX_VALUE;
      for (int j = 0; j < v1.length; ++j) {
        minDistance = Math.min(minDistance, distance1(v2[i], v1[j]));
      }
      Assert.assertTrue(minDistance <= EPSILON);
    }
  }


  @Test
  public void runKMeansUsingStaticMethods() {
    List<double[]> points = new ArrayList<double[]>();
    points.add(new double[]{1.0, 2.0, 6.0});
    points.add(new double[]{1.0, 3.0, 0.0});
    points.add(new double[]{1.0, 4.0, 6.0});

    double[][] expectedCenter = { {1.0, 3.0, 4.0} };

    JavaRDD<double[]> data = sc.parallelize(points, 2);
    KMeansModel model = KMeans.train(data.rdd(), 1, 1);
    assertSetsEqual(model.clusterCenters(), expectedCenter);

    model = KMeans.train(data.rdd(), 1, 1, 1, KMeans.RANDOM());
    assertSetsEqual(model.clusterCenters(), expectedCenter);
  }

  @Test
  public void runKMeansUsingConstructor() {
    List<double[]> points = new ArrayList<double[]>();
    points.add(new double[]{1.0, 2.0, 6.0});
    points.add(new double[]{1.0, 3.0, 0.0});
    points.add(new double[]{1.0, 4.0, 6.0});

    double[][] expectedCenter = { {1.0, 3.0, 4.0} };

    JavaRDD<double[]> data = sc.parallelize(points, 2);
    KMeansModel model = new KMeans().setK(1).setMaxIterations(5).run(data.rdd());
    assertSetsEqual(model.clusterCenters(), expectedCenter);

    model = new KMeans().setK(1)
                        .setMaxIterations(1)
                        .setRuns(1)
                        .setInitializationMode(KMeans.RANDOM())
                        .run(data.rdd());
    assertSetsEqual(model.clusterCenters(), expectedCenter);
  }
}
