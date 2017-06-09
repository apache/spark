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

package org.apache.spark.ml.feature;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaPolynomialExpansionSuite extends SharedSparkSession {

  @Test
  public void polynomialExpansionTest() {
    PolynomialExpansion polyExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3);

    List<Row> data = Arrays.asList(
      RowFactory.create(
        Vectors.dense(-2.0, 2.3),
        Vectors.dense(-2.0, 4.0, -8.0, 2.3, -4.6, 9.2, 5.29, -10.58, 12.17)
      ),
      RowFactory.create(Vectors.dense(0.0, 0.0), Vectors.dense(new double[9])),
      RowFactory.create(
        Vectors.dense(0.6, -1.1),
        Vectors.dense(0.6, 0.36, 0.216, -1.1, -0.66, -0.396, 1.21, 0.726, -1.331)
      )
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
      new StructField("expected", new VectorUDT(), false, Metadata.empty())
    });

    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    List<Row> pairs = polyExpansion.transform(dataset)
      .select("polyFeatures", "expected")
      .collectAsList();

    for (Row r : pairs) {
      double[] polyFeatures = ((Vector) r.get(0)).toArray();
      double[] expected = ((Vector) r.get(1)).toArray();
      Assert.assertArrayEquals(polyFeatures, expected, 1e-1);
    }
  }
}
