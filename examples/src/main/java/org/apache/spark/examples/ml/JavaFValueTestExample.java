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

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.stat.FValueTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
// $example off$

/**
 * An example for FValue testing.
 * Run with
 * <pre>
 * bin/run-example ml.JavaFValueTestExample
 * </pre>
 */
public class JavaFValueTestExample {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaFValueTestExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(4.6, Vectors.dense(6.0, 7.0, 0.0, 7.0, 6.0, 0.0)),
      RowFactory.create(6.6, Vectors.dense(0.0, 9.0, 6.0, 0.0, 5.0, 9.0)),
      RowFactory.create(5.1, Vectors.dense(0.0, 9.0, 3.0, 0.0, 5.0, 5.0)),
      RowFactory.create(7.6, Vectors.dense(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)),
      RowFactory.create(9.0, Vectors.dense(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)),
      RowFactory.create(9.0, Vectors.dense(8.0, 9.0, 6.0, 4.0, 0.0, 0.0))
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);
    Row r = FValueTest.test(df, "features", "label").head();
    System.out.println("pValues: " + r.get(0).toString());
    System.out.println("degreesOfFreedom: " + r.getList(1).toString());
    System.out.println("fvalues: " + r.get(2).toString());

    // $example off$

    spark.stop();
  }
}
