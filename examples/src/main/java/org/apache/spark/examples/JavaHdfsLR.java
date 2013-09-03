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

package org.apache.spark.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.Random;

/**
 * Logistic regression based classification.
 */
public class JavaHdfsLR {

  static int D = 10;   // Number of dimensions
  static Random rand = new Random(42);

  static class DataPoint implements Serializable {
    public DataPoint(double[] x, double y) {
      this.x = x;
      this.y = y;
    }

    double[] x;
    double y;
  }

  static class ParsePoint extends Function<String, DataPoint> {
    public DataPoint call(String line) {
      StringTokenizer tok = new StringTokenizer(line, " ");
      double y = Double.parseDouble(tok.nextToken());
      double[] x = new double[D];
      int i = 0;
      while (i < D) {
        x[i] = Double.parseDouble(tok.nextToken());
        i += 1;
      }
      return new DataPoint(x, y);
    }
  }

  static class VectorSum extends Function2<double[], double[], double[]> {
    public double[] call(double[] a, double[] b) {
      double[] result = new double[D];
      for (int j = 0; j < D; j++) {
        result[j] = a[j] + b[j];
      }
      return result;
    }
  }

  static class ComputeGradient extends Function<DataPoint, double[]> {
    double[] weights;

    public ComputeGradient(double[] weights) {
      this.weights = weights;
    }

    public double[] call(DataPoint p) {
      double[] gradient = new double[D];
      for (int i = 0; i < D; i++) {
        double dot = dot(weights, p.x);
        gradient[i] = (1 / (1 + Math.exp(-p.y * dot)) - 1) * p.y * p.x[i];
      }
      return gradient;
    }
  }

  public static double dot(double[] a, double[] b) {
    double x = 0;
    for (int i = 0; i < D; i++) {
      x += a[i] * b[i];
    }
    return x;
  }

  public static void printWeights(double[] a) {
    System.out.println(Arrays.toString(a));
  }

  public static void main(String[] args) {

    if (args.length < 3) {
      System.err.println("Usage: JavaHdfsLR <master> <file> <iters>");
      System.exit(1);
    }

    JavaSparkContext sc = new JavaSparkContext(args[0], "JavaHdfsLR",
        System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));
    JavaRDD<String> lines = sc.textFile(args[1]);
    JavaRDD<DataPoint> points = lines.map(new ParsePoint()).cache();
    int ITERATIONS = Integer.parseInt(args[2]);

    // Initialize w to a random value
    double[] w = new double[D];
    for (int i = 0; i < D; i++) {
      w[i] = 2 * rand.nextDouble() - 1;
    }

    System.out.print("Initial w: ");
    printWeights(w);

    for (int i = 1; i <= ITERATIONS; i++) {
      System.out.println("On iteration " + i);

      double[] gradient = points.map(
        new ComputeGradient(w)
      ).reduce(new VectorSum());

      for (int j = 0; j < D; j++) {
        w[j] -= gradient[j];
      }

    }

    System.out.print("Final w: ");
    printWeights(w);
    System.exit(0);
  }
}
