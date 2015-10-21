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
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaNaiveBayesSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLogisticRegressionSuite");
    jsql = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  public void validatePrediction(DataFrame predictionAndLabels) {
    for (Row r : predictionAndLabels.collect()) {
      double prediction = r.getAs(0);
      double label = r.getAs(1);
      assertEquals(label, prediction, 1E-5);
    }
  }

  @Test
  public void naiveBayesDefaultParams() {
    NaiveBayes nb = new NaiveBayes();
    assertEquals("label", nb.getLabelCol());
    assertEquals("features", nb.getFeaturesCol());
    assertEquals("prediction", nb.getPredictionCol());
    assertEquals(1.0, nb.getSmoothing(), 1E-5);
    assertEquals("multinomial", nb.getModelType());
  }

  @Test
  public void testNaiveBayes() {
    List<Row> data = Arrays.asList(
      RowFactory.create(0.0, Vectors.dense(1.0, 0.0, 0.0)),
      RowFactory.create(0.0, Vectors.dense(2.0, 0.0, 0.0)),
      RowFactory.create(1.0, Vectors.dense(0.0, 1.0, 0.0)),
      RowFactory.create(1.0, Vectors.dense(0.0, 2.0, 0.0)),
      RowFactory.create(2.0, Vectors.dense(0.0, 0.0, 1.0)),
      RowFactory.create(2.0, Vectors.dense(0.0, 0.0, 2.0)));

    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty())
    });

    DataFrame dataset = jsql.createDataFrame(data, schema);
    NaiveBayes nb = new NaiveBayes().setSmoothing(0.5).setModelType("multinomial");
    NaiveBayesModel model = nb.fit(dataset);

    DataFrame predictionAndLabels = model.transform(dataset).select("prediction", "label");
    validatePrediction(predictionAndLabels);
  }
}
