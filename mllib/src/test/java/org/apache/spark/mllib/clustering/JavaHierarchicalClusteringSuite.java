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

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JavaHierarchicalClusteringSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaHierarchicalClustering");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void runWithSmallData() {
    List<Vector> points = Lists.newArrayList(
        Vectors.dense(1.0, 2.0, 6.0),
        Vectors.dense(1.0, 3.0, 0.0),
        Vectors.dense(1.0, 4.0, 6.0)
    );

    Vector expectedCenter = Vectors.dense(1.0, 3.0, 4.0);

    JavaRDD<Vector> data = sc.parallelize(points, 2);
    HierarchicalClustering algo = new HierarchicalClustering().setNumClusters(1);
    HierarchicalClusteringModel model = algo.run(data.rdd());
    assertEquals(1, model.getCenters().length);
    assertEquals(expectedCenter, model.getCenters()[0]);
  }

  @Test
  public void runWithDenseVectors() {
    int numClusters = 5;
    List<Vector> points = Lists.newArrayList();
    for (int i = 0; i < 99; i++) {
      Double elm = (double)(i % numClusters);
      Vector point = Vectors.dense(elm, elm);
      points.add(point);
    }
    JavaRDD<Vector> data = sc.parallelize(points, 2);
    HierarchicalClustering algo = new HierarchicalClustering().setNumClusters(numClusters);
    HierarchicalClusteringModel model = algo.run(data.rdd());
    Vector[] centers = model.getCenters();
    assertEquals(numClusters, centers.length);
    assertEquals(Vectors.dense(0.0, 0.0), centers[0]);
    assertEquals(Vectors.dense(1.0, 1.0), centers[1]);
    assertEquals(Vectors.dense(2.0, 2.0), centers[2]);
    assertEquals(Vectors.dense(3.0, 3.0), centers[3]);
    assertEquals(Vectors.dense(4.0, 4.0), centers[4]);

    // adjacency list
    ArrayList<ArrayList<Double>> edges = model.toJavaAdjacencyList();
    assertEquals(8, edges.size());
    // linkage matrix
    ArrayList<ArrayList<Double>> matrix = model.toJavaLinkageMatrix();
    assertEquals(4, matrix.size());
  }

  @Test
  public void runWithSparseVectors() {
    int numClusters = 5;
    List<Vector> points = Lists.newArrayList();
    for (int i = 0; i < 99; i++) {
      int elm = i % numClusters;
      int indexes[] = {elm};
      double values[] = {elm};
      Vector point = Vectors.sparse(numClusters, indexes, values);
      points.add(point);
    }
    JavaRDD<Vector> data = sc.parallelize(points, 2);
    HierarchicalClustering algo = new HierarchicalClustering().setNumClusters(numClusters);
    HierarchicalClusteringModel model = algo.run(data.rdd());
    Vector[] centers = model.getCenters();
    assertEquals(numClusters, centers.length);
    assertEquals(points.get(0), centers[0]);
    assertEquals(points.get(1), centers[1]);
    assertEquals(points.get(2), centers[2]);
    assertEquals(points.get(3), centers[3]);
    assertEquals(points.get(4), centers[4]);

    // adjacency list
    ArrayList<ArrayList<Double>> edges = model.toJavaAdjacencyList();
    assertEquals(8, edges.size());
    // linkage matrix
    ArrayList<ArrayList<Double>> matrix = model.toJavaLinkageMatrix();
    assertEquals(4, matrix.size());
  }
}
