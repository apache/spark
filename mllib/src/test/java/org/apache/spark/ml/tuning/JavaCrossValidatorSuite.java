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

package org.apache.spark.ml.tuning;

import java.io.Serializable;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.SQLContext;
import static org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInputAsList;

public class JavaCrossValidatorSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;
  private transient SchemaRDD dataset;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaCrossValidatorSuite");
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
  public void crossValidationWithLogisticRegression() {
    LogisticRegression lr = new LogisticRegression();
    ParamMap[] lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam(), new double[] {0.001, 1000.0})
      .addGrid(lr.maxIter(), new int[] {0, 10})
      .build();
    BinaryClassificationEvaluator eval = new BinaryClassificationEvaluator();
    CrossValidator cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3);
    CrossValidatorModel cvModel = cv.fit(dataset);
    ParamMap bestParamMap = cvModel.bestModel().fittingParamMap();
    Assert.assertEquals(0.001, bestParamMap.apply(lr.regParam()));
    Assert.assertEquals(10, bestParamMap.apply(lr.maxIter()));
  }
}
