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

// $example on$

import org.apache.spark.ml.regression.IsotonicRegression;
import org.apache.spark.ml.regression.IsotonicRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;

/**
 * An example demonstrating IsotonicRegression.
 * Run with
 * <pre>
 * bin/run-example ml.JavaIsotonicRegressionExample
 * </pre>
 */
public class JavaIsotonicRegressionExample {

  public static void main(String[] args) {
    // Create a SparkSession.
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaIsotonicRegressionExample")
      .getOrCreate();

    // $example on$
    // Loads data.
    Dataset<Row> dataset = spark.read().format("libsvm")
      .load("data/mllib/sample_isotonic_regression_libsvm_data.txt");

    // Trains an isotonic regression model.
    IsotonicRegression ir = new IsotonicRegression();
    IsotonicRegressionModel model = ir.fit(dataset);

    System.out.println("Boundaries in increasing order: " + model.boundaries());
    System.out.println("Predictions associated with the boundaries: " + model.predictions());

    // Makes predictions.
    model.transform(dataset).show();
    // $example off$

    spark.stop();
  }
}
