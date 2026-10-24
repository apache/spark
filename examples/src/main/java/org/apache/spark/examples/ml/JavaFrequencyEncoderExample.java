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
import org.apache.spark.ml.feature.FrequencyEncoder;
import org.apache.spark.ml.feature.FrequencyEncoderModel;
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

public class JavaFrequencyEncoderExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaFrequencyEncoderExample")
      .getOrCreate();

    // Note: categorical features are usually first encoded with StringIndexer
    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0.0, 1.0),
      RowFactory.create(1.0, 0.0),
      RowFactory.create(2.0, 1.0),
      RowFactory.create(0.0, 2.0),
      RowFactory.create(0.0, 1.0),
      RowFactory.create(2.0, 0.0)
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("categoryIndex1", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("categoryIndex2", DataTypes.DoubleType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // proportions of the training rows, the default
    FrequencyEncoder encoder = new FrequencyEncoder()
      .setInputCols(new String[]{"categoryIndex1", "categoryIndex2"})
      .setOutputCols(new String[]{"categoryIndex1Freq", "categoryIndex2Freq"});

    FrequencyEncoderModel model = encoder.fit(df);
    Dataset<Row> encoded = model.transform(df);
    encoded.show();

    // raw counts instead of proportions
    FrequencyEncoder countEncoder = new FrequencyEncoder()
      .setInputCols(new String[]{"categoryIndex1", "categoryIndex2"})
      .setOutputCols(new String[]{"categoryIndex1Count", "categoryIndex2Count"})
      .setNormalize(false);

    FrequencyEncoderModel countModel = countEncoder.fit(df);
    Dataset<Row> countEncoded = countModel.transform(df);
    countEncoded.show();
    // $example off$

    spark.stop();
  }
}
