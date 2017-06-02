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
import org.apache.spark.mllib.clustering.AffinityPropagation;
import org.apache.spark.mllib.clustering.AffinityPropagationCluster;
import org.apache.spark.mllib.clustering.AffinityPropagationModel;

/**
 * Java example for graph clustering using affinity propagation (AP).
 */
public class JavaAffinityPropagation {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaAffinityPropagationExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    @SuppressWarnings("unchecked")
    JavaRDD<Tuple3<Long, Long, Double>> similarities = sc.parallelize(Lists.newArrayList(
      new Tuple3<Long, Long, Double>(0L, 1L, 0.9), // similarities
      new Tuple3<Long, Long, Double>(1L, 2L, 0.9),
      new Tuple3<Long, Long, Double>(1L, 3L, 0.9),
      new Tuple3<Long, Long, Double>(3L, 4L, 0.1),
      new Tuple3<Long, Long, Double>(4L, 5L, 0.9),
      new Tuple3<Long, Long, Double>(4L, 6L, 0.9),
      new Tuple3<Long, Long, Double>(0L, 0L, 0.2), // preferences
      new Tuple3<Long, Long, Double>(1L, 1L, 0.2),
      new Tuple3<Long, Long, Double>(2L, 2L, 0.2),
      new Tuple3<Long, Long, Double>(3L, 3L, 0.2),
      new Tuple3<Long, Long, Double>(4L, 4L, 0.2),
      new Tuple3<Long, Long, Double>(5L, 5L, 0.2),
      new Tuple3<Long, Long, Double>(6L, 6L, 0.2)));

    AffinityPropagation ap = new AffinityPropagation()
      .setMaxIterations(20);
    AffinityPropagationModel model = ap.run(similarities);

    for (AffinityPropagationCluster c: model.fromAssignToClusters().toJavaRDD().collect()) {
      StringBuilder builder = new StringBuilder();
      builder.append("cluster id: " + c.id() + " -> ");
      builder.append(" exemplar: " + c.exemplar() + " members:");
      for (Long node: c.members()) {
        builder.append(" " + node);
      }
      System.out.println(builder.toString());
    }

    JavaRDD<Tuple3<Long, Long, Double>> similarities2 = sc.parallelize(Lists.newArrayList(
      new Tuple3<Long, Long, Double>(0L, 1L, -0.12),
      new Tuple3<Long, Long, Double>(1L, 2L, -0.08),
      new Tuple3<Long, Long, Double>(1L, 3L, -0.22),
      new Tuple3<Long, Long, Double>(3L, 4L, -0.93),
      new Tuple3<Long, Long, Double>(3L, 5L, -0.82),
      new Tuple3<Long, Long, Double>(4L, 1L, -0.85),
      new Tuple3<Long, Long, Double>(4L, 2L, -0.73),
      new Tuple3<Long, Long, Double>(4L, 5L, -0.19),
      new Tuple3<Long, Long, Double>(4L, 6L, -0.12)));

    AffinityPropagationModel model2 = ap.run(ap.determinePreferences(similarities2));

    for (AffinityPropagationCluster c: model2.fromAssignToClusters().toJavaRDD().collect()) {
      StringBuilder builder = new StringBuilder();
      builder.append("cluster id: " + c.id() + " -> ");
      builder.append(" exemplar: " + c.exemplar() + " members:");
      for (Long node: c.members()) {
        builder.append(" " + node);
      }
      System.out.println(builder.toString());
    }

    AffinityPropagationModel model3 = ap.run(ap.embedPreferences(similarities2, -0.5));

    for (AffinityPropagationCluster c: model3.fromAssignToClusters().toJavaRDD().collect()) {
      StringBuilder builder = new StringBuilder();
      builder.append("cluster id: " + c.id() + " -> ");
      builder.append(" exemplar: " + c.exemplar() + " members:");
      for (Long node: c.members()) {
        builder.append(" " + node);
      }
      System.out.println(builder.toString());
    }

    sc.stop();
  }
}
