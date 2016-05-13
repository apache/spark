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

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.types.DataTypes.*;
// $example off$

public class JavaVectorAssemblerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaVectorAssemblerExample")
      .getOrCreate();

    // $example on$
    StructType schema = createStructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("hour", IntegerType, false),
      createStructField("mobile", DoubleType, false),
      createStructField("userFeatures", new VectorUDT(), false),
      createStructField("clicked", DoubleType, false)
    });
    Row row = RowFactory.create(0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0);
    Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row), schema);

    VectorAssembler assembler = new VectorAssembler()
      .setInputCols(new String[]{"hour", "mobile", "userFeatures"})
      .setOutputCol("features");

    Dataset<Row> output = assembler.transform(dataset);
    System.out.println(output.select("features", "clicked").first());
    // $example off$
    spark.stop();
  }
}

