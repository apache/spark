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

package org.apache.spark.ml.example;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.ParamGridBuilder;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JavaLogisticRegressionSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient JavaSQLContext jsql;
  private transient JavaSchemaRDD dataset;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLogisticRegressionSuite");
    jsql = new JavaSQLContext(jsc);
    JavaRDD<LabeledPoint> points =
      MLUtils.loadLibSVMFile(jsc.sc(), "../data/mllib/sample_binary_classification_data.txt")
        .toJavaRDD();
    dataset = jsql.applySchema(points, LabeledPoint.class);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void logisticRegression() {
    LogisticRegression lr = new LogisticRegression();
    LogisticRegressionModel model = lr.fit(dataset.schemaRDD());
    model.transform(dataset.schemaRDD()).registerTempTable("prediction");
    JavaSchemaRDD predictions = jsql.sql("SELECT label, score, prediction FROM prediction");
    for (Row r: predictions.collect()) {
      System.out.println(r);
    }
  }

  @Test
  public void logisticRegressionWithSetters() {
    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0);
    LogisticRegressionModel model = lr.fit(dataset.schemaRDD());
    model.transform(dataset.schemaRDD(), model.threshold().w(0.8)) // overwrite threshold
      .registerTempTable("prediction");
    JavaSchemaRDD predictions = jsql.sql("SELECT label, score, prediction FROM prediction");
    for (Row r: predictions.collect()) {
      System.out.println(r);
    }
  }

  @Test
  public void chainModelParams() {
    LogisticRegression lr = new LogisticRegression();
    lr.modelParams()
      .setFeaturesCol("features")
      .setScoreCol("score")
      .setThreshold(0.5);
  }

  @Test
  public void logisticRegressionFitWithVarargs() {
    LogisticRegression lr = new LogisticRegression();
    lr.fit(dataset.schemaRDD(), lr.maxIter().w(10), lr.regParam().w(1.0));
  }

  @Test
  public void logisticRegressionWithCrossValidation() {
    LogisticRegression lr = new LogisticRegression();
    ParamMap[] lrParamMaps = new ParamGridBuilder()
      .addMulti(lr.regParam(), new double[] {0.1, 100.0})
      .addMulti(lr.maxIter(), new int[] {0, 5})
      .build();
    BinaryClassificationEvaluator eval = new BinaryClassificationEvaluator();
    CrossValidator cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3);
    CrossValidatorModel bestModel = cv.fit(dataset.baseSchemaRDD());
  }

  @Test
  public void logisticRegressionWithPipeline() {
    StandardScaler scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures");
    LogisticRegression lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures");
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[] {scaler, lr});
    PipelineModel model = pipeline.fit(dataset.baseSchemaRDD());
    model.transform(dataset.baseSchemaRDD()).registerTempTable("prediction");
    JavaSchemaRDD predictions = jsql.sql("SELECT label, score, prediction FROM prediction");
    for (Row r: predictions.collect()) {
      System.out.println(r);
    }
  }
}
