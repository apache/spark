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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaMultilayerPerceptronClassifierSuite extends SharedSparkSession {

  @Test
  public void testMLPC() {
    List<LabeledPoint> data = Arrays.asList(
      new LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      new LabeledPoint(1.0, Vectors.dense(0.0, 1.0)),
      new LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      new LabeledPoint(0.0, Vectors.dense(1.0, 1.0))
    );
    Dataset<Row> dataFrame = spark.createDataFrame(data, LabeledPoint.class);

    MultilayerPerceptronClassifier mlpc = new MultilayerPerceptronClassifier()
      .setLayers(new int[]{2, 5, 2})
      .setBlockSize(1)
      .setSeed(123L)
      .setMaxIter(100);
    MultilayerPerceptronClassificationModel model = mlpc.fit(dataFrame);
    Dataset<Row> result = model.transform(dataFrame);
    List<Row> predictionAndLabels = result.select("prediction", "label").collectAsList();
    for (Row r : predictionAndLabels) {
      Assertions.assertEquals((int) r.getDouble(0), (int) r.getDouble(1));
    }
  }
}
