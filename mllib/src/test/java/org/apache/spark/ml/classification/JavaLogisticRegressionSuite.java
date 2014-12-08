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

import scala.Tuple2;

import java.io.Serializable;
import java.lang.Math;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import static org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInputAsList;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.ml.LabeledPoint;
import org.apache.spark.sql.Row;


public class JavaLogisticRegressionSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;
  private transient DataFrame dataset;

  private transient JavaRDD<LabeledPoint> datasetRDD;
  private transient JavaRDD<Vector> featuresRDD;
  private double eps = 1e-5;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLogisticRegressionSuite");
    jsql = new SQLContext(jsc);
    List<LabeledPoint> points = new ArrayList<LabeledPoint>();
    for (org.apache.spark.mllib.regression.LabeledPoint lp:
        generateLogisticInputAsList(1.0, 1.0, 100, 42)) {
      points.add(new LabeledPoint(lp.label(), lp.features()));
    }
    datasetRDD = jsc.parallelize(points, 2);
    featuresRDD = datasetRDD.map(new Function<LabeledPoint, Vector>() {
      @Override public Vector call(LabeledPoint lp) { return lp.features(); }
    });
    dataset = jsql.applySchema(datasetRDD, LabeledPoint.class);
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
    DataFrame predictions = jsql.sql("SELECT label, score, prediction FROM prediction");
    predictions.collectAsList();
    // Check defaults
    assert(model.getThreshold() == 0.5);
    assert(model.getFeaturesCol().equals("features"));
    assert(model.getPredictionCol().equals("prediction"));
    assert(model.getScoreCol().equals("score"));
  }

  @Test
  public void logisticRegressionWithSetters() {
    // Set params, train, and check as many params as we can.
    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
      .setScoreCol("probability");
    LogisticRegressionModel model = lr.fit(dataset);
    assert(model.fittingParamMap().get(lr.maxIter()).get() == 10);
    assert(model.fittingParamMap().get(lr.regParam()).get() == 1.0);
    assert(model.fittingParamMap().get(lr.threshold()).get() == 0.6);
    assert(model.getThreshold() == 0.6);

    // Modify model params, and check that the params worked.
    model.setThreshold(1.0);
    model.transform(dataset).registerTempTable("predAllZero");
    SchemaRDD predAllZero = jsql.sql("SELECT prediction, probability FROM predAllZero");
    for (Row r: predAllZero.collectAsList()) {
      assert(r.getDouble(0) == 0.0);
    }
    // Call transform with params, and check that the params worked.
    /* TODO: USE THIS
    model.transform(dataset, model.threshold().w(0.8)) // overwrite threshold
        .registerTempTable("prediction");
    DataFrame predictions = jsql.sql("SELECT label, score, prediction FROM prediction");
    predictions.collectAsList();
    */

    model.transform(dataset, model.threshold().w(0.0), model.scoreCol().w("myProb"))
      .registerTempTable("predNotAllZero");
    SchemaRDD predNotAllZero = jsql.sql("SELECT prediction, myProb FROM predNotAllZero");
    boolean foundNonZero = false;
    for (Row r: predNotAllZero.collectAsList()) {
      if (r.getDouble(0) != 0.0) foundNonZero = true;
    }
    assert(foundNonZero);

    // Call fit() with new params, and check as many params as we can.
    LogisticRegressionModel model2 = lr.fit(dataset, lr.maxIter().w(5), lr.regParam().w(0.1),
        lr.threshold().w(0.4), lr.scoreCol().w("theProb"));
    assert(model2.fittingParamMap().get(lr.maxIter()).get() == 5);
    assert(model2.fittingParamMap().get(lr.regParam()).get() == 0.1);
    assert(model2.fittingParamMap().get(lr.threshold()).get() == 0.4);
    assert(model2.getThreshold() == 0.4);
    assert(model2.getScoreCol().equals("theProb"));
  }

  @Test
  public void logisticRegressionPredictorClassifierMethods() {
    LogisticRegression lr = new LogisticRegression();

    // fit() vs. train()
    LogisticRegressionModel model1 = lr.fit(dataset);
    LogisticRegressionModel model2 = lr.train(datasetRDD);
    assert(model1.intercept() == model2.intercept());
    assert(model1.weights().equals(model2.weights()));
    assert(model1.numClasses() == model2.numClasses());
    assert(model1.numClasses() == 2);

    // transform() vs. predict()
    model1.transform(dataset).registerTempTable("transformed");
    SchemaRDD trans = jsql.sql("SELECT prediction FROM transformed");
    JavaRDD<Double> preds = model1.predict(featuresRDD);
    for (scala.Tuple2<Row, Double> trans_pred: trans.toJavaRDD().zip(preds).collect()) {
      double t = trans_pred._1().getDouble(0);
      double p = trans_pred._2();
      assert(t == p);
    }

    // Check various types of predictions.
    JavaRDD<Vector> rawPredictions = model1.predictRaw(featuresRDD);
    JavaRDD<Vector> probabilities = model1.predictProbabilities(featuresRDD);
    JavaRDD<Double> predictions = model1.predict(featuresRDD);
    double threshold = model1.getThreshold();
    for (Tuple2<Vector, Vector> raw_prob: rawPredictions.zip(probabilities).collect()) {
      Vector raw = raw_prob._1();
      Vector prob = raw_prob._2();
      for (int i = 0; i < raw.size(); ++i) {
        double r = raw.apply(i);
        double p = prob.apply(i);
        double pFromR = 1.0 / (1.0 + Math.exp(-r));
        assert(Math.abs(r - pFromR) < eps);
      }
    }
    for (Tuple2<Vector, Double> prob_pred: probabilities.zip(predictions).collect()) {
      Vector prob = prob_pred._1();
      double pred = prob_pred._2();
      double probOfPred = prob.apply((int)pred);
      for (int i = 0; i < prob.size(); ++i) {
        assert(probOfPred >= prob.apply(i));
      }
    }
  }
}
