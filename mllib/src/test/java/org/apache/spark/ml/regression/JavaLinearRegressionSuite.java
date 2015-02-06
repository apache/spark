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

import java.io.Serializable;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import static org.apache.spark.mllib.classification.LogisticRegressionSuite
    .generateLogisticInputAsList;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class JavaLinearRegressionSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;
  private transient DataFrame dataset;
  private transient JavaRDD<LabeledPoint> datasetRDD;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLinearRegressionSuite");
    jsql = new SQLContext(jsc);
    List<LabeledPoint> points = generateLogisticInputAsList(1.0, 1.0, 100, 42);
    datasetRDD = jsc.parallelize(points, 2);
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
    DataFrame predictions = jsql.sql("SELECT label, prediction FROM prediction");
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
    assert(model.fittingParamMap().apply(lr.maxIter()) == 10);
    assert(model.fittingParamMap().apply(lr.regParam()).equals(1.0));

    // Call fit() with new params, and check as many params as we can.
    LinearRegressionModel model2 =
        lr.fit(dataset, lr.maxIter().w(5), lr.regParam().w(0.1), lr.predictionCol().w("thePred"));
    assert(model2.fittingParamMap().apply(lr.maxIter()) == 5);
    assert(model2.fittingParamMap().apply(lr.regParam()).equals(0.1));
    assert(model2.getPredictionCol().equals("thePred"));
  }
}
