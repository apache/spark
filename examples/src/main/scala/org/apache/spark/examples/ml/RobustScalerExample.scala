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
import org.apache.spark.ml.feature.RobustScaler
// $example off$
import org.apache.spark.sql.SparkSession

object RobustScalerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RobustScalerExample")
      .getOrCreate()

    // $example on$
    val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val scaler = new RobustScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithScaling(true)
      .setWithCentering(false)
      .setLower(0.25)
      .setUpper(0.75)

    // Compute summary statistics by fitting the RobustScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Transform each feature to have unit quantile range.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
