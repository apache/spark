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

// $example on$
package org.apache.spark.examples.ml
// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, DoubleType}
import org.apache.spark.ml.regression.IsotonicRegression

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
    val ir = new IsotonicRegression().setIsotonic(true)
      .setLabelCol("label").setFeaturesCol("features").setWeightCol("weight")

    // $example on$
    val dataReader = spark.read
    dataReader.schema(new StructType().add("label", DoubleType).add("features", DoubleType))

    var data = dataReader.csv("data/mllib/sample_isotonic_regression_data.txt")
    data = data.select(col("label"), col("features"), lit(1.0).as("weight"))

    // Split data into training (60%) and test (40%) sets.
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    // Create isotonic regression model from training data.
    // Isotonic parameter defaults to true so it is only shown for demonstration
    val model = ir.fit(training).setPredictionCol("prediction")

    // Create tuples of predicted and real labels.
    val result = model.transform(test)

    result.show
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println