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
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.LabeledPoint;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import static org.apache.spark.mllib.classification.LogisticRegressionSuite
    .generateLogisticInputAsList;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;


public class JavaLinearRegressionSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient JavaSQLContext jsql;
  private transient JavaSchemaRDD dataset;
  private transient JavaRDD<LabeledPoint> datasetRDD;
  private transient JavaRDD<Vector> featuresRDD;
  private double eps = 1e-5;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLinearRegressionSuite");
    jsql = new JavaSQLContext(jsc);
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
  public void linearRegressionDefaultParams() {
    LinearRegression lr = new LinearRegression();
    assert(lr.getLabelCol().equals("label"));
    LinearRegressionModel model = lr.fit(dataset);
    model.transform(dataset).registerTempTable("prediction");
    JavaSchemaRDD predictions = jsql.sql("SELECT label, prediction FROM prediction");
    predictions.collect();
    // Check defaults
    assert(model.getFeaturesCol().equals("features"));
    assert(model.getPredictionCol().equals("prediction"));
  }

  @Test
  public void linearRegressionWithSetters() {
    // Set params, train, and check as many params as we can.
    LinearRegression lr = new LinearRegression()
        .setMaxIter(10)
        .setRegParam(1.0);
    LinearRegressionModel model = lr.fit(dataset);
    assert(model.fittingParamMap().get(lr.maxIter()).get() == 10);
    assert(model.fittingParamMap().get(lr.regParam()).get() == 1.0);

    // Call fit() with new params, and check as many params as we can.
    LinearRegressionModel model2 =
        lr.fit(dataset, lr.maxIter().w(5), lr.regParam().w(0.1), lr.predictionCol().w("thePred"));
    assert(model2.fittingParamMap().get(lr.maxIter()).get() == 5);
    assert(model2.fittingParamMap().get(lr.regParam()).get() == 0.1);
    assert(model2.getPredictionCol().equals("thePred"));
  }

  @Test
  public void linearRegressionPredictorClassifierMethods() {
    LinearRegression lr = new LinearRegression();

    // fit() vs. train()
    LinearRegressionModel model1 = lr.fit(dataset);
    LinearRegressionModel model2 = lr.train(datasetRDD);
    assert(model1.intercept() == model2.intercept());
    assert(model1.weights().equals(model2.weights()));

    // transform() vs. predict()
    model1.transform(dataset).registerTempTable("transformed");
    JavaSchemaRDD trans = jsql.sql("SELECT prediction FROM transformed");
    JavaRDD<Double> preds = model1.predict(featuresRDD);
    for (Tuple2<Row, Double> trans_pred: trans.zip(preds).collect()) {
      double t = trans_pred._1().getDouble(0);
      double p = trans_pred._2();
      assert(t == p);
    }
  }
}
