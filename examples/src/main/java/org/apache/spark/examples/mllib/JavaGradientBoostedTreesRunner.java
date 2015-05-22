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

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;

/**
 * Classification and regression using gradient-boosted decision trees.
 */
public final class JavaGradientBoostedTreesRunner {

  private static void usage() {
    System.err.println("Usage: JavaGradientBoostedTreesRunner <libsvm format data file>" +
        " <Classification/Regression>");
    System.exit(-1);
  }

  public static void main(String[] args) {
    String datapath = "data/mllib/sample_libsvm_data.txt";
    String algo = "Classification";
    if (args.length >= 1) {
      datapath = args[0];
    }
    if (args.length >= 2) {
      algo = args[1];
    }
    if (args.length > 2) {
      usage();
    }
    SparkConf sparkConf = new SparkConf().setAppName("JavaGradientBoostedTreesRunner");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD().cache();

    // Set parameters.
    //  Note: All features are treated as continuous.
    BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams(algo);
    boostingStrategy.setNumIterations(10);
    boostingStrategy.treeStrategy().setMaxDepth(5);

    if (algo.equals("Classification")) {
      // Compute the number of classes from the data.
      Integer numClasses = data.map(new Function<LabeledPoint, Double>() {
        @Override public Double call(LabeledPoint p) {
          return p.label();
        }
      }).countByValue().size();
      boostingStrategy.treeStrategy().setNumClasses(numClasses);

      // Train a GradientBoosting model for classification.
      final GradientBoostedTreesModel model = GradientBoostedTrees.train(data, boostingStrategy);

      // Evaluate model on training instances and compute training error
      JavaPairRDD<Double, Double> predictionAndLabel =
          data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
            @Override public Tuple2<Double, Double> call(LabeledPoint p) {
              return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
            }
          });
      Double trainErr =
          1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override public Boolean call(Tuple2<Double, Double> pl) {
              return !pl._1().equals(pl._2());
            }
          }).count() / data.count();
      System.out.println("Training error: " + trainErr);
      System.out.println("Learned classification tree model:\n" + model);
    } else if (algo.equals("Regression")) {
      // Train a GradientBoosting model for classification.
      final GradientBoostedTreesModel model = GradientBoostedTrees.train(data, boostingStrategy);

      // Evaluate model on training instances and compute training error
      JavaPairRDD<Double, Double> predictionAndLabel =
          data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
            @Override public Tuple2<Double, Double> call(LabeledPoint p) {
              return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
            }
          });
      Double trainMSE =
          predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
            @Override public Double call(Tuple2<Double, Double> pl) {
              Double diff = pl._1() - pl._2();
              return diff * diff;
            }
          }).reduce(new Function2<Double, Double, Double>() {
            @Override public Double call(Double a, Double b) {
              return a + b;
            }
          }) / data.count();
      System.out.println("Training Mean Squared Error: " + trainMSE);
      System.out.println("Learned regression tree model:\n" + model);
    } else {
      usage();
    }

    sc.stop();
  }
}
