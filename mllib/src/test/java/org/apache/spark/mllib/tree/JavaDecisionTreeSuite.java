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

package org.apache.spark.mllib.tree;

import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.configuration.Strategy;
import org.apache.spark.mllib.tree.impurity.Gini;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

public class JavaDecisionTreeSuite extends SharedSparkSession {

  private static int validatePrediction(
      List<LabeledPoint> validationData, DecisionTreeModel model) {
    int numCorrect = 0;
    for (LabeledPoint point : validationData) {
      Double prediction = model.predict(point.features());
      if (prediction == point.label()) {
        numCorrect++;
      }
    }
    return numCorrect;
  }

  @Test
  public void runDTUsingConstructor() {
    List<LabeledPoint> arr = DecisionTreeSuite.generateCategoricalDataPointsAsJavaList();
    JavaRDD<LabeledPoint> rdd = jsc.parallelize(arr);
    HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    categoricalFeaturesInfo.put(1, 2); // feature 1 has 2 categories

    int maxDepth = 4;
    int numClasses = 2;
    int maxBins = 100;
    Strategy strategy = new Strategy(Algo.Classification(), Gini.instance(), maxDepth, numClasses,
      maxBins, categoricalFeaturesInfo);

    DecisionTree learner = new DecisionTree(strategy);
    DecisionTreeModel model = learner.run(rdd.rdd());

    int numCorrect = validatePrediction(arr, model);
    Assertions.assertEquals(numCorrect, rdd.count());
  }

  @Test
  public void runDTUsingStaticMethods() {
    List<LabeledPoint> arr = DecisionTreeSuite.generateCategoricalDataPointsAsJavaList();
    JavaRDD<LabeledPoint> rdd = jsc.parallelize(arr);
    HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    categoricalFeaturesInfo.put(1, 2); // feature 1 has 2 categories

    int maxDepth = 4;
    int numClasses = 2;
    int maxBins = 100;
    Strategy strategy = new Strategy(Algo.Classification(), Gini.instance(), maxDepth, numClasses,
      maxBins, categoricalFeaturesInfo);

    DecisionTreeModel model = DecisionTree$.MODULE$.train(rdd.rdd(), strategy);

    // java compatibility test
    JavaRDD<Double> predictions = model.predict(rdd.map(LabeledPoint::features));

    int numCorrect = validatePrediction(arr, model);
    Assertions.assertEquals(numCorrect, rdd.count());
  }

}
