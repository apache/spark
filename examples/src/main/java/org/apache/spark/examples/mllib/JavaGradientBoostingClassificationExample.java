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
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;
// $example off$

public class JavaGradientBoostingClassificationExample {
  public static void main(String[] args) {
    // $example on$
    SparkConf sparkConf = new SparkConf()
      .setAppName("JavaGradientBoostedTreesClassificationExample");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    // Load and parse the data file.
    String datapath = "data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), datapath).toJavaRDD();
    // Split the data into training and test sets (30% held out for testing)
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
    JavaRDD<LabeledPoint> trainingData = splits[0];
    JavaRDD<LabeledPoint> testData = splits[1];

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
    boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
    boostingStrategy.getTreeStrategy().setNumClasses(2);
    boostingStrategy.getTreeStrategy().setMaxDepth(5);
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

    GradientBoostedTreesModel model = GradientBoostedTrees.train(trainingData, boostingStrategy);

    // Evaluate model on test instances and compute test error
    JavaPairRDD<Double, Double> predictionAndLabel =
      testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
    double testErr =
      predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();
    System.out.println("Test Error: " + testErr);
    System.out.println("Learned classification GBT model:\n" + model.toDebugString());

    // Save and load model
    model.save(jsc.sc(), "target/tmp/myGradientBoostingClassificationModel");
    GradientBoostedTreesModel sameModel = GradientBoostedTreesModel.load(jsc.sc(),
      "target/tmp/myGradientBoostingClassificationModel");
    // $example off$

    jsc.stop();
  }

}
