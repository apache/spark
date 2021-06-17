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

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class JavaStopWordsRemoverSuite extends SharedSparkSession {

  @Test
  public void javaCompatibilityTest() {
    StopWordsRemover remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered");

    List<Row> data = Arrays.asList(
      RowFactory.create(Arrays.asList("I", "saw", "the", "red", "balloon")),
      RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false,
        Metadata.empty())
    });
    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    remover.transform(dataset).collect();
  }
}
