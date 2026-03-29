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
import org.apache.spark.ml.feature.TargetEncoder
// $example off$
import org.apache.spark.sql.SparkSession

object TargetEncoderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TargetEncoderExample")
      .getOrCreate()

    // Note: categorical features are usually first encoded with StringIndexer
    // $example on$
    val df = spark.createDataFrame(Seq(
      (0.0, 1.0, 0, 10.0),
      (1.0, 0.0, 1, 20.0),
      (2.0, 1.0, 0, 30.0),
      (0.0, 2.0, 1, 40.0),
      (0.0, 1.0, 0, 50.0),
      (2.0, 0.0, 1, 60.0)
    )).toDF("categoryIndex1", "categoryIndex2",
            "binaryLabel", "continuousLabel")

    // binary target
    val bin_encoder = new TargetEncoder()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryIndex1Target", "categoryIndex2Target"))
      .setLabelCol("binaryLabel")
      .setTargetType("binary");

    val bin_model = bin_encoder.fit(df)
    val bin_encoded = bin_model.transform(df)
    bin_encoded.show()

    // continuous target
    val cont_encoder = new TargetEncoder()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryIndex1Target", "categoryIndex2Target"))
      .setLabelCol("continuousLabel")
      .setTargetType("continuous");

    val cont_model = cont_encoder.fit(df)
    val cont_encoded = cont_model.transform(df)
    cont_encoded.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
