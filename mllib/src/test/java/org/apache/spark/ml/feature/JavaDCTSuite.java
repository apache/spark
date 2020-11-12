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

import org.jtransforms.dct.DoubleDCT_1D;

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

public class JavaDCTSuite extends SharedSparkSession {

  @Test
  public void javaCompatibilityTest() {
    double[] input = new double[]{1D, 2D, 3D, 4D};
    Dataset<Row> dataset = spark.createDataFrame(
      Arrays.asList(RowFactory.create(Vectors.dense(input))),
      new StructType(new StructField[]{
        new StructField("vec", (new VectorUDT()), false, Metadata.empty())
      }));

    double[] expectedResult = input.clone();
    (new DoubleDCT_1D(input.length)).forward(expectedResult, true);

    DCT dct = new DCT()
      .setInputCol("vec")
      .setOutputCol("resultVec");

    List<Row> result = dct.transform(dataset).select("resultVec").collectAsList();
    Vector resultVec = result.get(0).getAs("resultVec");

    Assert.assertArrayEquals(expectedResult, resultVec.toArray(), 1e-6);
  }
}
