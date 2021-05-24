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

// $example on$
import java.util.Arrays;
import java.util.List;
// $example off$
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off$

/**
 * Java example for bisecting k-means clustering.
 */
public class JavaBisectingKMeansExample {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaBisectingKMeansExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // $example on$
    List<Vector> localData = Arrays.asList(
      Vectors.dense(0.1, 0.1),   Vectors.dense(0.3, 0.3),
      Vectors.dense(10.1, 10.1), Vectors.dense(10.3, 10.3),
      Vectors.dense(20.1, 20.1), Vectors.dense(20.3, 20.3),
      Vectors.dense(30.1, 30.1), Vectors.dense(30.3, 30.3)
    );
    JavaRDD<Vector> data = sc.parallelize(localData, 2);

    BisectingKMeans bkm = new BisectingKMeans()
      .setK(4);
    BisectingKMeansModel model = bkm.run(data);

    System.out.println("Compute Cost: " + model.computeCost(data));

    Vector[] clusterCenters = model.clusterCenters();
    for (int i = 0; i < clusterCenters.length; i++) {
      Vector clusterCenter = clusterCenters[i];
      System.out.println("Cluster Center " + i + ": " + clusterCenter);
    }
    // $example off$

    sc.stop();
  }
}
