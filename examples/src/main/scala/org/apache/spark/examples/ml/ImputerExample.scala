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

package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.feature.Imputer
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating Imputer.
 * Run with:
 *   bin/run-example ml.ImputerExample
 */
object ImputerExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ImputerExample")
      .getOrCreate()

    // $example on$
    val df = spark.createDataFrame(Seq(
      (1.0, Double.NaN),
      (2.0, Double.NaN),
      (Double.NaN, 3.0),
      (4.0, 4.0),
      (5.0, 5.0)
    )).toDF("a", "b")

    val imputer = new Imputer()
      .setInputCols(Array("a", "b"))
      .setOutputCols(Array("out_a", "out_b"))

    val model = imputer.fit(df)
    model.transform(df).show()
    // $example off$

    spark.stop()
  }
}
