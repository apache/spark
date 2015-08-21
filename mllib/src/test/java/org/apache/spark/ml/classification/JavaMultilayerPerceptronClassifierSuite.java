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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class JavaMultilayerPerceptronClassifierSuite implements Serializable {

  private transient JavaSparkContext jsc;
  private transient SQLContext sqlContext;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLogisticRegressionSuite");
    sqlContext = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
    sqlContext = null;
  }

  @Test
  public void testMLPC() {
    DataFrame dataFrame = sqlContext.createDataFrame(
      jsc.parallelize(Arrays.asList(
        new LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
        new LabeledPoint(1.0, Vectors.dense(0.0, 1.0)),
        new LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
        new LabeledPoint(0.0, Vectors.dense(1.0, 1.0)))),
      LabeledPoint.class);
    MultilayerPerceptronClassifier mlpc = new MultilayerPerceptronClassifier()
      .setLayers(new int[] {2, 5, 2})
      .setBlockSize(1)
      .setSeed(11L)
      .setMaxIter(100);
    MultilayerPerceptronClassificationModel model = mlpc.fit(dataFrame);
    DataFrame result = model.transform(dataFrame);
    Row[] predictionAndLabels = result.select("prediction", "label").collect();
    for (Row r: predictionAndLabels) {
      Assert.assertEquals((int) r.getDouble(0), (int) r.getDouble(1));
    }
  }
}
