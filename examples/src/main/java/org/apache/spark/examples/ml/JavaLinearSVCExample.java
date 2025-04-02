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
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

public class JavaLinearSVCExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaLinearSVCExample")
      .getOrCreate();

    // $example on$
    // Load training data
    Dataset<Row> training = spark.read().format("libsvm")
      .load("data/mllib/sample_libsvm_data.txt");

    LinearSVC lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1);

    // Fit the model
    LinearSVCModel lsvcModel = lsvc.fit(training);

    // Print the coefficients and intercept for LinearSVC
    System.out.println("Coefficients: "
      + lsvcModel.coefficients() + " Intercept: " + lsvcModel.intercept());
    // $example off$

    spark.stop();
  }
}
