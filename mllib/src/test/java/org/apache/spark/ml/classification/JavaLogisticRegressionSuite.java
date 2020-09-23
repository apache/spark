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

package org.apache.spark.ml.classification;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import static org.apache.spark.ml.classification.LogisticRegressionSuite.generateLogisticInputAsList;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaLogisticRegressionSuite extends SharedSparkSession {

  private transient Dataset<Row> dataset;

  private transient JavaRDD<LabeledPoint> datasetRDD;
  private double eps = 1e-5;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    List<LabeledPoint> points = generateLogisticInputAsList(1.0, 1.0, 100, 42);
    datasetRDD = jsc.parallelize(points, 2);
    dataset = spark.createDataFrame(datasetRDD, LabeledPoint.class);
    dataset.createOrReplaceTempView("dataset");
  }

  @Test
  public void logisticRegressionDefaultParams() {
    LogisticRegression lr = new LogisticRegression();
    Assert.assertEquals("label", lr.getLabelCol());
    LogisticRegressionModel model = lr.fit(dataset);
    model.transform(dataset).createOrReplaceTempView("prediction");
    Dataset<Row> predictions = spark.sql("SELECT label, probability, prediction FROM prediction");
    predictions.collectAsList();
    // Check defaults
    Assert.assertEquals(0.5, model.getThreshold(), eps);
    Assert.assertEquals("features", model.getFeaturesCol());
    Assert.assertEquals("prediction", model.getPredictionCol());
    Assert.assertEquals("probability", model.getProbabilityCol());
  }

  @Test
  public void logisticRegressionWithSetters() {
    // Set params, train, and check as many params as we can.
    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
      .setProbabilityCol("myProbability");
    LogisticRegressionModel model = lr.fit(dataset);
    LogisticRegression parent = (LogisticRegression) model.parent();
    Assert.assertEquals(10, parent.getMaxIter());
    Assert.assertEquals(1.0, parent.getRegParam(), eps);
    Assert.assertEquals(0.4, parent.getThresholds()[0], eps);
    Assert.assertEquals(0.6, parent.getThresholds()[1], eps);
    Assert.assertEquals(0.6, parent.getThreshold(), eps);
    Assert.assertEquals(0.6, model.getThreshold(), eps);

    // Modify model params, and check that the params worked.
    model.setThreshold(1.0);
    model.transform(dataset).createOrReplaceTempView("predAllZero");
    Dataset<Row> predAllZero = spark.sql("SELECT prediction, myProbability FROM predAllZero");
    for (Row r : predAllZero.collectAsList()) {
      Assert.assertEquals(0.0, r.getDouble(0), eps);
    }
    // Call transform with params, and check that the params worked.
    model.transform(dataset, model.threshold().w(0.0), model.probabilityCol().w("myProb"))
      .createOrReplaceTempView("predNotAllZero");
    Dataset<Row> predNotAllZero = spark.sql("SELECT prediction, myProb FROM predNotAllZero");
    boolean foundNonZero = false;
    for (Row r : predNotAllZero.collectAsList()) {
      if (r.getDouble(0) != 0.0) foundNonZero = true;
    }
    Assert.assertTrue(foundNonZero);

    // Call fit() with new params, and check as many params as we can.
    LogisticRegressionModel model2 = lr.fit(dataset, lr.maxIter().w(5), lr.regParam().w(0.1),
      lr.threshold().w(0.4), lr.probabilityCol().w("theProb"));
    LogisticRegression parent2 = (LogisticRegression) model2.parent();
    Assert.assertEquals(5, parent2.getMaxIter());
    Assert.assertEquals(0.1, parent2.getRegParam(), eps);
    Assert.assertEquals(0.4, parent2.getThreshold(), eps);
    Assert.assertEquals(0.4, model2.getThreshold(), eps);
    Assert.assertEquals("theProb", model2.getProbabilityCol());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void logisticRegressionPredictorClassifierMethods() {
    LogisticRegression lr = new LogisticRegression();
    LogisticRegressionModel model = lr.fit(dataset);
    Assert.assertEquals(2, model.numClasses());

    model.transform(dataset).createOrReplaceTempView("transformed");
    Dataset<Row> trans1 = spark.sql("SELECT rawPrediction, probability FROM transformed");
    for (Row row : trans1.collectAsList()) {
      Vector raw = (Vector) row.get(0);
      Vector prob = (Vector) row.get(1);
      Assert.assertEquals(2, raw.size());
      Assert.assertEquals(2, prob.size());
      double probFromRaw1 = 1.0 / (1.0 + Math.exp(-raw.apply(1)));
      Assert.assertEquals(0, Math.abs(prob.apply(1) - probFromRaw1), eps);
      Assert.assertEquals(0, Math.abs(prob.apply(0) - (1.0 - probFromRaw1)), eps);
    }

    Dataset<Row> trans2 = spark.sql("SELECT prediction, probability FROM transformed");
    for (Row row : trans2.collectAsList()) {
      double pred = row.getDouble(0);
      Vector prob = (Vector) row.get(1);
      double probOfPred = prob.apply((int) pred);
      for (int i = 0; i < prob.size(); ++i) {
        Assert.assertTrue(probOfPred >= prob.apply(i));
      }
    }
  }

  @Test
  public void logisticRegressionTrainingSummary() {
    LogisticRegression lr = new LogisticRegression();
    LogisticRegressionModel model = lr.fit(dataset);

    LogisticRegressionTrainingSummary summary = model.summary();
    Assert.assertEquals(summary.totalIterations(), summary.objectiveHistory().length - 1);
  }
}
