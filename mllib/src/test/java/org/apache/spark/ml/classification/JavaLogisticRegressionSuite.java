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

import java.io.Serializable;
import java.lang.Math;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import static org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInputAsList;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;


public class JavaLogisticRegressionSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;
  private transient DataFrame dataset;

  private transient JavaRDD<LabeledPoint> datasetRDD;
  private double eps = 1e-5;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLogisticRegressionSuite");
    jsql = new SQLContext(jsc);
    List<LabeledPoint> points = generateLogisticInputAsList(1.0, 1.0, 100, 42);
    datasetRDD = jsc.parallelize(points, 2);
    dataset = jsql.createDataFrame(datasetRDD, LabeledPoint.class);
    dataset.registerTempTable("dataset");
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void logisticRegressionDefaultParams() {
    LogisticRegression lr = new LogisticRegression();
    assert(lr.getLabelCol().equals("label"));
    LogisticRegressionModel model = lr.fit(dataset);
    model.transform(dataset).registerTempTable("prediction");
    DataFrame predictions = jsql.sql("SELECT label, probability, prediction FROM prediction");
    predictions.collectAsList();
    // Check defaults
    assert(model.getThreshold() == 0.5);
    assert(model.getFeaturesCol().equals("features"));
    assert(model.getPredictionCol().equals("prediction"));
    assert(model.getProbabilityCol().equals("probability"));
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
    assert(model.fittingParamMap().apply(lr.maxIter()).equals(10));
    assert(model.fittingParamMap().apply(lr.regParam()).equals(1.0));
    assert(model.fittingParamMap().apply(lr.threshold()).equals(0.6));
    assert(model.getThreshold() == 0.6);

    // Modify model params, and check that the params worked.
    model.setThreshold(1.0);
    model.transform(dataset).registerTempTable("predAllZero");
    DataFrame predAllZero = jsql.sql("SELECT prediction, myProbability FROM predAllZero");
    for (Row r: predAllZero.collectAsList()) {
      assert(r.getDouble(0) == 0.0);
    }
    // Call transform with params, and check that the params worked.
    model.transform(dataset, model.threshold().w(0.0), model.probabilityCol().w("myProb"))
      .registerTempTable("predNotAllZero");
    DataFrame predNotAllZero = jsql.sql("SELECT prediction, myProb FROM predNotAllZero");
    boolean foundNonZero = false;
    for (Row r: predNotAllZero.collectAsList()) {
      if (r.getDouble(0) != 0.0) foundNonZero = true;
    }
    assert(foundNonZero);

    // Call fit() with new params, and check as many params as we can.
    LogisticRegressionModel model2 = lr.fit(dataset, lr.maxIter().w(5), lr.regParam().w(0.1),
        lr.threshold().w(0.4), lr.probabilityCol().w("theProb"));
    assert(model2.fittingParamMap().apply(lr.maxIter()).equals(5));
    assert(model2.fittingParamMap().apply(lr.regParam()).equals(0.1));
    assert(model2.fittingParamMap().apply(lr.threshold()).equals(0.4));
    assert(model2.getThreshold() == 0.4);
    assert(model2.getProbabilityCol().equals("theProb"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void logisticRegressionPredictorClassifierMethods() {
    LogisticRegression lr = new LogisticRegression();
    LogisticRegressionModel model = lr.fit(dataset);
    assert(model.numClasses() == 2);

    model.transform(dataset).registerTempTable("transformed");
    DataFrame trans1 = jsql.sql("SELECT rawPrediction, probability FROM transformed");
    for (Row row: trans1.collect()) {
      Vector raw = (Vector)row.get(0);
      Vector prob = (Vector)row.get(1);
      assert(raw.size() == 2);
      assert(prob.size() == 2);
      double probFromRaw1 = 1.0 / (1.0 + Math.exp(-raw.apply(1)));
      assert(Math.abs(prob.apply(1) - probFromRaw1) < eps);
      assert(Math.abs(prob.apply(0) - (1.0 - probFromRaw1)) < eps);
    }

    DataFrame trans2 = jsql.sql("SELECT prediction, probability FROM transformed");
    for (Row row: trans2.collect()) {
      double pred = row.getDouble(0);
      Vector prob = (Vector)row.get(1);
      double probOfPred = prob.apply((int)pred);
      for (int i = 0; i < prob.size(); ++i) {
        assert(probOfPred >= prob.apply(i));
      }
    }
  }
}
