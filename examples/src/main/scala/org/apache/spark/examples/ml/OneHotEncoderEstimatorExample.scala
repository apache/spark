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
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
// $example off$
import org.apache.spark.sql.SparkSession

object OneHotEncoderEstimatorExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("OneHotEncoderEstimatorExample")
      .getOrCreate()

    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, "a", "x"),
      (1, "b", "y"),
      (2, "c", "y"),
      (3, "a", "z"),
      (4, "a", "y"),
      (5, "c", "z")
    )).toDF("id", "category1", "category2")

    // TODO: Replace this with multi-column API of StringIndexer once SPARK-11215 is merged.
    val indexer1 = new StringIndexer()
      .setInputCol("category1")
      .setOutputCol("categoryIndex1")
      .fit(df)
    val indexer2 = new StringIndexer()
      .setInputCol("category2")
      .setOutputCol("categoryIndex2")
      .fit(df)
    val indexed1 = indexer1.transform(df)
    val indexed2 = indexer2.transform(indexed1)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2"))
    val model = encoder.fit(indexed2)

    val encoded = model.transform(indexed2)
    encoded.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
