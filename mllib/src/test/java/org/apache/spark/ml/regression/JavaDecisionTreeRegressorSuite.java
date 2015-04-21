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

package org.apache.spark.ml.regression;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.impl.TreeTests;
import org.apache.spark.mllib.classification.LogisticRegressionSuite;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.util.Utils;


public class JavaDecisionTreeRegressorSuite implements Serializable {

  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaDecisionTreeRegressorSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void runDT() {
    int nPoints = 20;
    double A = 2.0;
    double B = -1.5;

    JavaRDD<LabeledPoint> data = sc.parallelize(
        LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 42), 2).cache();
    Map<Integer, Integer> categoricalFeatures = new HashMap<Integer, Integer>();
    DataFrame dataFrame = TreeTests.setMetadata(data, categoricalFeatures, 2);

    // This tests setters. Training with various options is tested in Scala.
    DecisionTreeRegressor dt = new DecisionTreeRegressor()
        .setMaxDepth(2)
        .setMaxBins(10)
        .setMinInstancesPerNode(5)
        .setMinInfoGain(0.0)
        .setMaxMemoryInMB(256)
        .setCacheNodeIds(false)
        .setCheckpointInterval(10)
        .setMaxDepth(2); // duplicate setMaxDepth to check builder pattern
    for (int i = 0; i < DecisionTreeRegressor.supportedImpurities().length; ++i) {
      dt.setImpurity(DecisionTreeRegressor.supportedImpurities()[i]);
    }
    DecisionTreeRegressionModel model = dt.fit(dataFrame);

    model.transform(dataFrame);
    model.numNodes();
    model.depth();
    model.toDebugString();

    /*
    // TODO: Add test once save/load are implemented.
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    String path = tempDir.toURI().toString();
    try {
      model2.save(sc.sc(), path);
      DecisionTreeRegressionModel sameModel = DecisionTreeRegressionModel.load(sc.sc(), path);
      TreeTests.checkEqual(model2, sameModel);
    } finally {
      Utils.deleteRecursively(tempDir);
    }
    */
  }
}
