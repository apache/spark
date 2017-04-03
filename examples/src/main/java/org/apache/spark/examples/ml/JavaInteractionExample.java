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
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

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
      RowFactory.create(1, 1, 2, 3, 8, 4, 5),
      RowFactory.create(2, 4, 3, 8, 7, 9, 8),
      RowFactory.create(3, 6, 1, 9, 2, 3, 6),
      RowFactory.create(4, 10, 8, 6, 9, 4, 5),
      RowFactory.create(5, 9, 2, 7, 10, 7, 3),
      RowFactory.create(6, 1, 1, 4, 2, 8, 4)
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("id1", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("id2", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("id3", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("id4", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("id5", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("id6", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("id7", DataTypes.IntegerType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    VectorAssembler assembler1 = new VectorAssembler()
            .setInputCols(new String[]{"id2", "id3", "id4"})
            .setOutputCol("vec1");

    Dataset<Row> assembled1 = assembler1.transform(df);

    VectorAssembler assembler2 = new VectorAssembler()
            .setInputCols(new String[]{"id5", "id6", "id7"})
            .setOutputCol("vec2");

    Dataset<Row> assembled2 = assembler2.transform(assembled1).select("id1", "vec1", "vec2");

    Interaction interaction = new Interaction()
            .setInputCols(new String[]{"id1","vec1","vec2"})
            .setOutputCol("interactedCol");

    Dataset<Row> interacted = interaction.transform(assembled2);

    interacted.show(false);
    // $example off$

    spark.stop();
  }
}

