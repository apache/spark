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

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Logistic regression based classification using ML Lib.
 */
public final class JavaLR {

  static class ParsePoint extends Function<String, LabeledPoint> {
    private static final Pattern COMMA = Pattern.compile(",");
    private static final Pattern SPACE = Pattern.compile(" ");

    @Override
    public LabeledPoint call(String line) {
      String[] parts = COMMA.split(line);
      double y = Double.parseDouble(parts[0]);
      String[] tok = SPACE.split(parts[1]);
      double[] x = new double[tok.length];
      for (int i = 0; i < tok.length; ++i) {
        x[i] = Double.parseDouble(tok[i]);
      }
      return new LabeledPoint(y, x);
    }
  }

  public static void printWeights(double[] a) {
    System.out.println(Arrays.toString(a));
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Usage: JavaLR <master> <input_dir> <step_size> <niters>");
      System.exit(1);
    }

    JavaSparkContext sc = new JavaSparkContext(args[0], "JavaLR",
        System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(JavaLR.class));
    JavaRDD<String> lines = sc.textFile(args[1]);
    JavaRDD<LabeledPoint> points = lines.map(new ParsePoint()).cache();
    double stepSize = Double.parseDouble(args[2]);
    int iterations = Integer.parseInt(args[3]);

    // Another way to configure LogisticRegression
    //
    // LogisticRegressionWithSGD lr = new LogisticRegressionWithSGD();
    // lr.optimizer().setNumIterations(iterations)
    //               .setStepSize(stepSize)
    //               .setMiniBatchFraction(1.0);
    // lr.setIntercept(true);
    // LogisticRegressionModel model = lr.train(points.rdd());

    LogisticRegressionModel model = LogisticRegressionWithSGD.train(points.rdd(),
        iterations, stepSize);

    System.out.print("Final w: ");
    printWeights(model.weights());

    System.exit(0);
  }
}
