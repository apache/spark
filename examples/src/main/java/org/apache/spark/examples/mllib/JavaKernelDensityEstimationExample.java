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
import org.apache.spark.mllib.stat.KernelDensity;
// $example off$

public class JavaKernelDensityEstimationExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaKernelDensityEstimationExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    // an RDD of sample data
    JavaRDD<Double> data = jsc.parallelize(
      Arrays.asList(1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 6.0, 7.0, 8.0, 9.0, 9.0));

    // Construct the density estimator with the sample data
    // and a standard deviation for the Gaussian kernels
    KernelDensity kd = new KernelDensity().setSample(data).setBandwidth(3.0);

    // Find density estimates for the given values
    double[] densities = kd.estimate(new double[]{-1.0, 2.0, 5.0});

    System.out.println(Arrays.toString(densities));
    // $example off$

    jsc.stop();
  }
}

