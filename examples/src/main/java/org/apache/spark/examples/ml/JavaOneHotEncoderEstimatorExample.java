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

import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaOneHotEncoderEstimatorExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaOneHotEncoderEstimatorExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0, "a", "x"),
      RowFactory.create(1, "b", "y"),
      RowFactory.create(2, "c", "y"),
      RowFactory.create(3, "a", "z"),
      RowFactory.create(4, "a", "y"),
      RowFactory.create(5, "c", "z")
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("category1", DataTypes.StringType, false, Metadata.empty()),
      new StructField("category2", DataTypes.StringType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // TODO: Replace this with multi-column API of StringIndexer once SPARK-11215 is merged.
    StringIndexerModel indexer1 = new StringIndexer()
      .setInputCol("category1")
      .setOutputCol("categoryIndex1")
      .fit(df);
    StringIndexerModel indexer2 = new StringIndexer()
      .setInputCol("category2")
      .setOutputCol("categoryIndex2")
      .fit(df);
    Dataset<Row> indexed1 = indexer1.transform(df);
    Dataset<Row> indexed2 = indexer2.transform(indexed1);

    OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
      .setInputCols(new String[] {"categoryIndex1", "categoryIndex2"})
      .setOutputCols(new String[] {"categoryVec1", "categoryVec2"});

    OneHotEncoderModel model = encoder.fit(indexed2);
    Dataset<Row> encoded = model.transform(indexed2);
    encoded.show();
    // $example off$

    spark.stop();
  }
}

