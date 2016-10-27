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

import org.apache.spark.ml.feature.Interaction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.lang.StringBuffer;

// $example on$
// $example off$

public class JavaInteractionExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaInteractionExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0, 1, 2),
      RowFactory.create(1, 4, 3),
      RowFactory.create(2, 6, 1),
      RowFactory.create(3, 10, 8),
      RowFactory.create(4, 9, 2),
      RowFactory.create(5, 1, 1)
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("id1", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("id2", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("id3", DataTypes.IntegerType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    Interaction interaction = new Interaction()
      .setInputCols(new String[]{"id1","id2","id3"})
      .setOutputCol("interactedCol");
    Dataset<Row> interacted = interaction.transform(df);

    interacted.show();
    // $example off$

    spark.stop();
  }
}

