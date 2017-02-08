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

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.MinHashLSH;
import org.apache.spark.ml.feature.MinHashLSHModel;
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

public class JavaMinHashLSHExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaMinHashLSHExample")
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

    int[] indicies = {1, 3};
    double[] values = {1.0, 1.0};
    Vector key = Vectors.sparse(6, indicies, values);

    MinHashLSH mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("keys")
      .setOutputCol("values");

    MinHashLSHModel model = mh.fit(dfA);

    // Feature Transformation
    System.out.println("The hashed dataset where hashed values are stored in the column 'values':");
    model.transform(dfA).show();
    // Cache the transformed columns
    Dataset<Row> transformedA = model.transform(dfA).cache();
    Dataset<Row> transformedB = model.transform(dfB).cache();

    // Approximate similarity join
    System.out.println("Approximately joining dfA and dfB on distance smaller than 0.6:");
    model.approxSimilarityJoin(dfA, dfB, 0.6).show();
    System.out.println("Joining cached datasets to avoid recomputing the hash values:");
    model.approxSimilarityJoin(transformedA, transformedB, 0.6).show();

    // Self Join
    System.out.println("Approximately self join of dfB on distance smaller than 0.6:");
    model.approxSimilarityJoin(dfA, dfA, 0.6).filter("datasetA.id < datasetB.id").show();

    // Approximate nearest neighbor search
    System.out.println("Approximately searching dfA for 2 nearest neighbors of the key:");
    System.out.println("Note: It may return less than 2 rows because of lack of elements in " +
      "the hash buckets.");
    model.approxNearestNeighbors(dfA, key, 2).show();
    System.out.println("Searching cached dataset to avoid recomputing the hash values:");
    model.approxNearestNeighbors(transformedA, key, 2).show();
    // $example off$

    spark.stop();
  }
}
