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

package org.apache.spark.ml;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import static org.apache.spark.mllib.classification.LogisticRegressionSuite
  .generateLogisticInputAsList;

/**
 * Test Pipeline construction and fitting in Java.
 */
public class JavaPipelineSuite {

  private transient JavaSparkContext jsc;
  private transient JavaSQLContext jsql;
  private transient JavaSchemaRDD dataset;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaPipelineSuite");
    jsql = new JavaSQLContext(jsc);
    JavaRDD<LabeledPoint> points =
      jsc.parallelize(generateLogisticInputAsList(1.0, 1.0, 100, 42), 2);
    dataset = jsql.applySchema(points, LabeledPoint.class);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void pipeline() {
    StandardScaler scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures");
    LogisticRegression lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures");
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[] {scaler, lr});
    PipelineModel model = pipeline.fit(dataset);
    model.transform(dataset).registerTempTable("prediction");
    JavaSchemaRDD predictions = jsql.sql("SELECT label, score, prediction FROM prediction");
    predictions.collect();
  }
}
