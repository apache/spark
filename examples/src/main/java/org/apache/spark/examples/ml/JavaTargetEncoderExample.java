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
import org.apache.spark.ml.feature.TargetEncoder;
import org.apache.spark.ml.feature.TargetEncoderModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
// $example off$

public class JavaTargetEncoderExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaTargetEncoderExample")
      .getOrCreate();

    // Note: categorical features are usually first encoded with StringIndexer
    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0.0, 1.0, 0, 10.0),
      RowFactory.create(1.0, 0.0, 1, 20.0),
      RowFactory.create(2.0, 1.0, 0, 30.0),
      RowFactory.create(0.0, 2.0, 1, 40.0),
      RowFactory.create(0.0, 1.0, 0, 50.0),
      RowFactory.create(2.0, 0.0, 1, 60.0)
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("categoryIndex1", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("categoryIndex2", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("binaryLabel", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("continuousLabel", DataTypes.DoubleType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // binary target
    TargetEncoder bin_encoder = new TargetEncoder()
      .setInputCols(new String[] {"categoryIndex1", "categoryIndex2"})
      .setOutputCols(new String[] {"categoryIndex1Target", "categoryIndex2Target"})
      .setLabelCol("binaryLabel")
      .setTargetType("binary");

    TargetEncoderModel bin_model = bin_encoder.fit(df);
    Dataset<Row> bin_encoded = bin_model.transform(df);
    bin_encoded.show();

    // continuous target
    TargetEncoder cont_encoder = new TargetEncoder()
      .setInputCols(new String[] {"categoryIndex1", "categoryIndex2"})
      .setOutputCols(new String[] {"categoryIndex1Target", "categoryIndex2Target"})
      .setLabelCol("continuousLabel")
      .setTargetType("continuous");

    TargetEncoderModel cont_model = cont_encoder.fit(df);
    Dataset<Row> cont_encoded = cont_model.transform(df);
    cont_encoded.show();
    // $example off$

    spark.stop();
  }
}

