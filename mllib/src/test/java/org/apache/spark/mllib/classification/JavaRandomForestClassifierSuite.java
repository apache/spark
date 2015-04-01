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

package org.apache.spark.mllib.classification;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.impl.TreeTests;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.util.Utils;


public class JavaRandomForestClassifierSuite implements Serializable {

  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaRandomForestClassifierSuite");
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
    JavaRDD<Vector> featureRDD = data.map(new Function<LabeledPoint, Vector>() {
      @Override
      public Vector call(LabeledPoint lp) throws Exception {
        return lp.features();
      }
    });

    RandomForestClassifier rf = new RandomForestClassifier()
        .setMaxDepth(2)
        .setMaxBins(10)
        .setMinInstancesPerNode(5)
        .setMinInfoGain(0.0)
        .setMaxMemoryInMB(256)
        .setCacheNodeIds(false)
        .setCheckpointInterval(10)
        .setSubsamplingRate(1.0)
        .setSeed(1234)
        .setNumTrees(3)
        .setMaxDepth(2); // duplicate setMaxDepth to check builder pattern
    for (int i = 0; i < RandomForestClassifier.supportedImpurities().length; ++i) {
      rf.setImpurity(RandomForestClassifier.supportedImpurities()[i]);
    }
    for (int i = 0; i < RandomForestClassifier.supportedFeaturesPerNode().length; ++i) {
      rf.setFeaturesPerNode(RandomForestClassifier.supportedFeaturesPerNode()[i]);
    }
    Map<Integer, Integer> categoricalFeatures = new HashMap<Integer, Integer>();
    RandomForestClassificationModel model1 = rf.run(data);
    RandomForestClassificationModel model2 = rf.run(data, categoricalFeatures);
    RandomForestClassificationModel model3 = rf.run(data, categoricalFeatures, 2);

    model1.predict(featureRDD);
    model2.predict(featureRDD.take(1).get(0));
    model2.totalNumNodes();
    model2.toDebugString();
    model2.getTrees();
    model2.getTreeWeights();

    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    String path = tempDir.toURI().toString();
    try {
      model3.save(sc.sc(), path);
      RandomForestClassificationModel sameModel =
          RandomForestClassificationModel.load(sc.sc(), path);
      TreeTests.checkEqual(model3, sameModel);
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }
}
