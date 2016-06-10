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

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.regression.IsotonicRegression
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating Isotonic Regression.
 * Run with
 * {{{
 * bin/run-example ml.IsotonicRegressionExample
 * }}}
 */
object IsotonicRegressionExample {

  def main(args: Array[String]): Unit = {

    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("data/mllib/sample_isotonic_regression_libsvm_data.txt")

    // Trains an isotonic regression model.
    val ir = new IsotonicRegression()
    val model = ir.fit(dataset)

    println(s"Boundaries in increasing order: ${model.boundaries}")
    println(s"Predictions associated with the boundaries: ${model.predictions}")

    // Makes predictions.
    model.transform(dataset).show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
