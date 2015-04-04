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

import scala.Tuple3;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;

/**
 * Java example for graph clustering using power iteration clustering (PIC).
 */
public class JavaPowerIterationClusteringExample {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaPowerIterationClusteringExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    @SuppressWarnings("unchecked")
    JavaRDD<Tuple3<Long, Long, Double>> similarities = sc.parallelize(Lists.newArrayList(
      new Tuple3<Long, Long, Double>(0L, 1L, 0.9),
      new Tuple3<Long, Long, Double>(1L, 2L, 0.9),
      new Tuple3<Long, Long, Double>(2L, 3L, 0.9),
      new Tuple3<Long, Long, Double>(3L, 4L, 0.1),
      new Tuple3<Long, Long, Double>(4L, 5L, 0.9)));

    PowerIterationClustering pic = new PowerIterationClustering()
      .setK(2)
      .setMaxIterations(10);
    PowerIterationClusteringModel model = pic.run(similarities);

    for (PowerIterationClustering.Assignment a: model.assignments().toJavaRDD().collect()) {
      System.out.println(a.id() + " -> " + a.cluster());
    }

    sc.stop();
  }
}
