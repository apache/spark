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

import scala.jdk.javaapi.CollectionConverters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.ml.classification.LogisticRegressionSuite.generateMultinomialLogisticInput;

public class JavaOneVsRestSuite extends SharedSparkSession {

  private transient Dataset<Row> dataset;
  private transient JavaRDD<LabeledPoint> datasetRDD;

  @Override
  @BeforeEach
  public void setUp() throws IOException {
    super.setUp();
    int nPoints = 3;

    // The following coefficients and xMean/xVariance are computed from iris dataset with
    // lambda=0.2.
    // As a result, we are drawing samples from probability distribution of an actual model.
    double[] coefficients = {
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682};

    double[] xMean = {5.843, 3.057, 3.758, 1.199};
    double[] xVariance = {0.6856, 0.1899, 3.116, 0.581};
    List<LabeledPoint> points = CollectionConverters.asJava(generateMultinomialLogisticInput(
      coefficients, xMean, xVariance, true, nPoints, 42));
    datasetRDD = jsc.parallelize(points, 2);
    dataset = spark.createDataFrame(datasetRDD, LabeledPoint.class);
  }

  @Test
  public void oneVsRestDefaultParams() {
    OneVsRest ova = new OneVsRest();
    ova.setClassifier(new LogisticRegression());
    Assertions.assertEquals("label", ova.getLabelCol());
    Assertions.assertEquals("prediction", ova.getPredictionCol());
    OneVsRestModel ovaModel = ova.fit(dataset);
    Dataset<Row> predictions = ovaModel.transform(dataset).select("label", "prediction");
    predictions.collectAsList();
    Assertions.assertEquals("label", ovaModel.getLabelCol());
    Assertions.assertEquals("prediction", ovaModel.getPredictionCol());
  }
}
