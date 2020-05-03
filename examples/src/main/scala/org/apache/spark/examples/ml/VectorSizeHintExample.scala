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
import org.apache.spark.ml.feature.{VectorAssembler, VectorSizeHint}
import org.apache.spark.ml.linalg.Vectors
// $example off$
import org.apache.spark.sql.SparkSession

object VectorSizeHintExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("VectorSizeHintExample")
      .getOrCreate()

    // $example on$
    val dataset = spark.createDataFrame(
      Seq(
        (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
        (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val sizeHint = new VectorSizeHint()
      .setInputCol("userFeatures")
      .setHandleInvalid("skip")
      .setSize(3)

    val datasetWithSize = sizeHint.transform(dataset)
    println("Rows where 'userFeatures' is not the right size are filtered out")
    datasetWithSize.show(false)

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    // This dataframe can be used by downstream transformers as before
    val output = assembler.transform(datasetWithSize)
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
