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
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.SQLContext;
import static org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInputAsList;

public class JavaLogisticRegressionSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;
  private transient SchemaRDD dataset;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLogisticRegressionSuite");
    jsql = new SQLContext(jsc);
    List<LabeledPoint> points = generateLogisticInputAsList(1.0, 1.0, 100, 42);
    dataset = jsql.applySchema(jsc.parallelize(points, 2), LabeledPoint.class);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void logisticRegression() {
    LogisticRegression lr = new LogisticRegression();
    LogisticRegressionModel model = lr.fit(dataset);
    model.transform(dataset).registerTempTable("prediction");
    SchemaRDD predictions = jsql.sql("SELECT label, score, prediction FROM prediction");
    predictions.collectAsList();
  }

  @Test
  public void logisticRegressionWithSetters() {
    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0);
    LogisticRegressionModel model = lr.fit(dataset);
    model.transform(dataset, model.threshold().w(0.8)) // overwrite threshold
      .registerTempTable("prediction");
    SchemaRDD predictions = jsql.sql("SELECT label, score, prediction FROM prediction");
    predictions.collectAsList();
  }

  @Test
  public void logisticRegressionFitWithVarargs() {
    LogisticRegression lr = new LogisticRegression();
    lr.fit(dataset, lr.maxIter().w(10), lr.regParam().w(1.0));
  }
}
