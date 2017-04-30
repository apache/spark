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

// $example on$
import org.apache.spark.ml.feature.MinHash;
import org.apache.spark.ml.feature.MinHashModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
// $example off$

public class JavaMinHashExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaMinHashExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(Vectors.sparse(100, new int[]{1, 3, 4}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(Vectors.sparse(100, new int[]{1, 3, 4}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(Vectors.sparse(100, new int[]{3, 4, 6}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(Vectors.sparse(100, new int[]{2, 8}, new double[]{1.0, 1.0}))
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("signatures", new VectorUDT(), false, Metadata.empty()),
    });
    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    MinHash minHash = new MinHash()
      .setInputCol("signatures")
      .setOutputCol("buckets")
      .setOutputDim(2);
    MinHashModel model = minHash.fit(dataset);

    // basic transformation with a new hash column
    Dataset<Row> transformedDataset = model.transform(dataset);
    transformedDataset.select("signatures", "buckets").show();

    // approximate nearest neighbor search with a dataset and a key
    Vector key = Vectors.sparse(100, new int[]{2, 3, 4}, new double[]{1.0, 1.0, 1.0});
    Dataset approxNearestNeighbors = model.approxNearestNeighbors(dataset, key, 3, false, "distance");
    approxNearestNeighbors.select("signatures", "distance").show();

    // approximate similarity join of two datasets
    List<Row> dataToJoin = Arrays.asList(RowFactory.create(key));
    Dataset<Row> datasetToJoin = spark.createDataFrame(dataToJoin, schema);
    Dataset approxSimilarityJoin = model.approxSimilarityJoin(dataset, datasetToJoin, 1);
    approxSimilarityJoin.select("datasetA", "datasetB", "distCol").show();
    // $example off$

    spark.stop();
  }
}
