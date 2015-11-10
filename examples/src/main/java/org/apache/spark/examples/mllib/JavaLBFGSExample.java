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

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.optimization.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
// $example off$

public class JavaLBFGSExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("L-BFGS Example");
    SparkContext sc = new SparkContext(conf);

    // $example on$
    String path = "data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();
    int numFeatures = data.take(1).get(0).features().size();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint> trainingInit = data.sample(false, 0.6, 11L);
    JavaRDD<LabeledPoint> test = data.subtract(trainingInit);

    // Append 1 into the training data as intercept.
    JavaRDD<Tuple2<Object, Vector>> training = data.map(
      new Function<LabeledPoint, Tuple2<Object, Vector>>() {
        public Tuple2<Object, Vector> call(LabeledPoint p) {
          return new Tuple2<Object, Vector>(p.label(), MLUtils.appendBias(p.features()));
        }
      });
    training.cache();

    // Run training algorithm to build the model.
    int numCorrections = 10;
    double convergenceTol = 1e-4;
    int maxNumIterations = 20;
    double regParam = 0.1;
    Vector initialWeightsWithIntercept = Vectors.dense(new double[numFeatures + 1]);

    Tuple2<Vector, double[]> result = LBFGS.runLBFGS(
      training.rdd(),
      new LogisticGradient(),
      new SquaredL2Updater(),
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept);
    Vector weightsWithIntercept = result._1();
    double[] loss = result._2();

    final LogisticRegressionModel model = new LogisticRegressionModel(
      Vectors.dense(Arrays.copyOf(weightsWithIntercept.toArray(), weightsWithIntercept.size() - 1)),
      (weightsWithIntercept.toArray())[weightsWithIntercept.size() - 1]);

    // Clear the default threshold.
    model.clearThreshold();

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double score = model.predict(p.features());
          return new Tuple2<Object, Object>(score, p.label());
        }
      });

    // Get evaluation metrics.
    BinaryClassificationMetrics metrics =
      new BinaryClassificationMetrics(scoreAndLabels.rdd());
    double auROC = metrics.areaUnderROC();

    System.out.println("Loss of each step in training process");
    for (double l : loss)
      System.out.println(l);
    System.out.println("Area under ROC = " + auROC);
    // $example off$
  }
}

