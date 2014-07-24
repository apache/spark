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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.rdd.DatasetInfo;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.configuration.DTClassifierParams;
import org.apache.spark.mllib.tree.model.DecisionTreeClassifierModel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

public class JavaDecisionTreeSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaDecisionTreeSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  int validatePrediction(List<LabeledPoint> validationData, DecisionTreeClassifierModel model) {
    int numAccurate = 0;
    for (LabeledPoint point: validationData) {
      Double prediction = model.predict(point.features());
      if (prediction == point.label()) {
        numAccurate++;
      }
    }
    return numAccurate;
  }

  @Test
  public void runDTUsingConstructor() {
    scala.Tuple2<java.util.List<LabeledPoint>, DatasetInfo> arr_datasetInfo =
        DecisionTreeSuite.generateCategoricalDataPointsAsList();
    JavaRDD<LabeledPoint> rdd = sc.parallelize(arr_datasetInfo._1());
    DatasetInfo datasetInfo = arr_datasetInfo._2();

    DTClassifierParams dtParams = DecisionTreeClassifier.defaultParams();
    dtParams.setMaxBins(200);
    dtParams.setImpurity("entropy");
    DecisionTreeClassifier dtLearner = new DecisionTreeClassifier(dtParams);
    DecisionTreeClassifierModel model = dtLearner.run(rdd.rdd(), datasetInfo);

    int numAccurate = validatePrediction(arr_datasetInfo._1(), model);
    Assert.assertTrue(numAccurate == rdd.count());
  }

  @Test
  public void runDTUsingStaticMethods() {
    scala.Tuple2<List<LabeledPoint>, DatasetInfo> arr_datasetInfo =
        DecisionTreeSuite.generateCategoricalDataPointsAsList();
    JavaRDD<LabeledPoint> rdd = sc.parallelize(arr_datasetInfo._1());
    DatasetInfo datasetInfo = arr_datasetInfo._2();

    DTClassifierParams dtParams = DecisionTreeClassifier.defaultParams();
    DecisionTreeClassifierModel model =
        DecisionTreeClassifier.train(rdd.rdd(), datasetInfo, dtParams);

    int numAccurate = validatePrediction(arr_datasetInfo._1(), model);
    Assert.assertTrue(numAccurate == rdd.count());
  }

}
