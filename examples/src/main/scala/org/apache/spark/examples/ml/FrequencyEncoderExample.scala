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
import org.apache.spark.ml.feature.FrequencyEncoder
// $example off$
import org.apache.spark.sql.SparkSession

object FrequencyEncoderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FrequencyEncoderExample")
      .getOrCreate()

    // Note: categorical features are usually first encoded with StringIndexer
    // $example on$
    val df = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    // proportions of the training rows, the default
    val encoder = new FrequencyEncoder()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryIndex1Freq", "categoryIndex2Freq"))

    val model = encoder.fit(df)
    val encoded = model.transform(df)
    encoded.show()

    // raw counts instead of proportions
    val countEncoder = new FrequencyEncoder()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryIndex1Count", "categoryIndex2Count"))
      .setNormalize(false)

    val countModel = countEncoder.fit(df)
    val countEncoded = countModel.transform(df)
    countEncoded.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
