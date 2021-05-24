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

import org.apache.spark.sql.*;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.stat.Summarizer;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaSummarizerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaSummarizerExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(Vectors.dense(2.0, 3.0, 5.0), 1.0),
      RowFactory.create(Vectors.dense(4.0, 6.0, 7.0), 2.0)
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
      new StructField("weight", DataTypes.DoubleType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    Row result1 = df.select(Summarizer.metrics("mean", "variance")
      .summary(new Column("features"), new Column("weight")).as("summary"))
      .select("summary.mean", "summary.variance").first();
    System.out.println("with weight: mean = " + result1.<Vector>getAs(0).toString() +
      ", variance = " + result1.<Vector>getAs(1).toString());

    Row result2 = df.select(
      Summarizer.mean(new Column("features")),
      Summarizer.variance(new Column("features"))
    ).first();
    System.out.println("without weight: mean = " + result2.<Vector>getAs(0).toString() +
      ", variance = " + result2.<Vector>getAs(1).toString());
    // $example off$
    spark.stop();
  }
}
