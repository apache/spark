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

package org.apache.spark.examples.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.HierarchicalClustering;
import org.apache.spark.mllib.clustering.HierarchicalClusteringModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class JavaHierarchicalClustering {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Hierarchical Clustering Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load and parse data
    String path = "data/mllib/sample_hierarchical_data.csv";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
        new Function<String, Vector>() {
          public Vector call(String s) {
            String[] sarray = s.split(",");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++)
              values[i] = Double.parseDouble(sarray[i]);
            return Vectors.dense(values);
          }
        }
    );
    parsedData.cache();

    // Cluster the data into three classes using KMeans
    int numClusters = 10;
    HierarchicalClusteringModel model =
        HierarchicalClustering.train(parsedData.rdd(), numClusters);
    System.out.println("# Clusters: " + model.getClusters().length);

    // Predict a point
    Vector vector = Vectors.dense(6.0, 3.0, 4.0, 1.0);
    int clusterIndex = model.predict(vector);
    System.out.println("Predicted the Closest Cluster Index: " + clusterIndex);

    // Evaluate clustering by computing total variance
    double variance = model.getSumOfVariance();
    System.out.println("Sum of Variance of the Clusters = " + variance);

    // Cut the cluster tree by height
    HierarchicalClusteringModel cutModel = model.cut(4.0);
    System.out.println("# Clusters: " + cutModel.getClusters().length);
    int cutClusterIndex = model.predict(vector);
    System.out.println("Predicted the Closest Cluster Index: " + cutClusterIndex);
    double cutVariance = cutModel.getSumOfVariance();
    System.out.println("Sum of Variance of the Clusters = " + cutVariance);
  }
}
