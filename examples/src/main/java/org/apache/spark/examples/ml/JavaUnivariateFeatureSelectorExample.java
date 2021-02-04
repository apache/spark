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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.UnivariateFeatureSelector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
// $example off$

/**
 * An example for UnivariateFeatureSelector.
 * Run with
 * <pre>
 * bin/run-example ml.JavaUnivariateFeatureSelectorExample
 * </pre>
 */
public class JavaUnivariateFeatureSelectorExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaUnivariateFeatureSelectorExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(1, Vectors.dense(1.7, 4.4, 7.6, 5.8, 9.6, 2.3), 3.0),
      RowFactory.create(2, Vectors.dense(8.8, 7.3, 5.7, 7.3, 2.2, 4.1), 2.0),
      RowFactory.create(3, Vectors.dense(1.2, 9.5, 2.5, 3.1, 8.7, 2.5), 3.0),
      RowFactory.create(4, Vectors.dense(3.7, 9.2, 6.1, 4.1, 7.5, 3.8), 2.0),
      RowFactory.create(5, Vectors.dense(8.9, 5.2, 7.8, 8.3, 5.2, 3.0), 4.0),
      RowFactory.create(6, Vectors.dense(7.9, 8.5, 9.2, 4.0, 9.4, 2.1), 4.0)
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    UnivariateFeatureSelector selector = new UnivariateFeatureSelector()
      .setFeatureType("continuous")
      .setLabelType("categorical")
      .setSelectionMode("numTopFeatures")
      .setSelectionThreshold(1)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures");

    Dataset<Row> result = selector.fit(df).transform(df);

    System.out.println("UnivariateFeatureSelector output with top "
        + selector.getSelectionThreshold() + " features selected using f_classif");
    result.show();

    // $example off$
    spark.stop();
  }
}
