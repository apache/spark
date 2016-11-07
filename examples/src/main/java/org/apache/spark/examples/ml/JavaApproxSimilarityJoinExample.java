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

package org.apache.spark.examples.ml;

import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.MinHash;
import org.apache.spark.ml.feature.MinHashModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaApproxSimilarityJoinExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaApproxNearestNeighborExample")
      .getOrCreate();

    // $example on$
    List<Row> dataA = Arrays.asList(
      RowFactory.create(0, Vectors.sparse(6, new int[]{0, 1, 2}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(1, Vectors.sparse(6, new int[]{2, 3, 4}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(2, Vectors.sparse(6, new int[]{0, 2, 4}, new double[]{1.0, 1.0, 1.0}))
    );

    List<Row> dataB = Arrays.asList(
      RowFactory.create(0, Vectors.sparse(6, new int[]{1, 3, 5}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(1, Vectors.sparse(6, new int[]{2, 3, 5}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(2, Vectors.sparse(6, new int[]{1, 2, 4}, new double[]{1.0, 1.0, 1.0}))
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("keys", new VectorUDT(), false, Metadata.empty())
    });
    Dataset<Row> dfA = spark.createDataFrame(dataA, schema);
    Dataset<Row> dfB = spark.createDataFrame(dataB, schema);

    MinHash mh = new MinHash()
      .setOutputDim(5)
      .setInputCol("keys")
      .setOutputCol("values");

    MinHashModel model = mh.fit(dfA);
    model.approxSimilarityJoin(dfA, dfB, 0.6).show();

    // Cache the transformed columns
    Dataset<Row> transformedA = model.transform(dfA);
    Dataset<Row> transformedB = model.transform(dfB);
    model.approxSimilarityJoin(transformedA, transformedB, 0.6).show();

    // Self Join
    model.approxSimilarityJoin(dfA, dfA, 0.6).filter("datasetA.id < datasetB.id").show();
    // $example off$

    spark.stop();
  }
}
