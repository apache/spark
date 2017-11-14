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
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
// $example off$

/**
 * An example for computing correlation matrix.
 * Run with
 * <pre>
 * bin/run-example ml.JavaCorrelationExample
 * </pre>
 */
public class JavaCorrelationExample {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaCorrelationExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{1.0, -2.0})),
      RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
      RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)),
      RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{9.0, 1.0}))
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);
    Row r1 = Correlation.corr(df, "features").head();
    System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

    Row r2 = Correlation.corr(df, "features", "spearman").head();
    System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
    // $example off$

    spark.stop();
  }
}
