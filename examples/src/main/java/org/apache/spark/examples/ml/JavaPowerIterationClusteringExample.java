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
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.clustering.PowerIterationClustering;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaPowerIterationClusteringExample {
  public static void main(String[] args) {
    // Create a SparkSession.
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaPowerIterationClustering")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0L, 1L, 1.0),
      RowFactory.create(0L, 2L, 1.0),
      RowFactory.create(1L, 2L, 1.0),
      RowFactory.create(3L, 4L, 1.0),
      RowFactory.create(4L, 0L, 0.1)
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("src", DataTypes.LongType, false, Metadata.empty()),
      new StructField("dst", DataTypes.LongType, false, Metadata.empty()),
      new StructField("weight", DataTypes.DoubleType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    PowerIterationClustering model = new PowerIterationClustering()
      .setK(2)
      .setMaxIter(10)
      .setInitMode("degree")
      .setWeightCol("weight");

    Dataset<Row> result = model.assignClusters(df);
    result.show(false);
    // $example off$
    spark.stop();
  }
}
