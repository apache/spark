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

package org.apache.spark.mllib.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * Example using MLLib KMeans from Java.
 */
public class JavaKMeans {

  static class ParsePoint extends Function<String, double[]> {
    public double[] call(String line) {
      StringTokenizer tok = new StringTokenizer(line, " ");
      int numTokens = tok.countTokens();
      double[] point = new double[numTokens];
      for (int i = 0; i < numTokens; ++i) {
        point[i] = Double.parseDouble(tok.nextToken());
      }
      return point;
    }
  }

  public static void main(String[] args) {

    if (args.length < 4) {
      System.err.println(
          "Usage: JavaKMeans <master> <input_file> <k> <max_iterations> [<runs>]");
      System.exit(1);
    }

    String inputFile = args[1];
    int k = Integer.parseInt(args[2]);
    int iterations = Integer.parseInt(args[3]);
    int runs = 1;

    if (args.length >= 5) {
      runs = Integer.parseInt(args[4]);
    }

    JavaSparkContext sc = new JavaSparkContext(args[0], "JavaKMeans",
        System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));
    JavaRDD<String> lines = sc.textFile(args[1]);

    JavaRDD<double[]> points = lines.map(new ParsePoint());

    KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs);

    System.out.println("Cluster centers:");
    for (double[] center : model.clusterCenters()) {
      System.out.println(" " + Arrays.toString(center));
    }
    double cost = model.computeCost(points.rdd());
    System.out.println("Cost: " + cost);

    System.exit(0);
  }
}
