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
import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
// $example off$

public class JavaSummaryStatisticsExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaSummaryStatisticsExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    JavaRDD<Vector> mat = jsc.parallelize(
      Arrays.asList(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    ); // an RDD of Vectors

    // Compute column summary statistics.
    MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
    System.out.println(summary.mean());  // a dense vector containing the mean value for each column
    System.out.println(summary.variance());  // column-wise variance
    System.out.println(summary.numNonzeros());  // number of nonzeros in each column
    // $example off$

    jsc.stop();
  }
}
