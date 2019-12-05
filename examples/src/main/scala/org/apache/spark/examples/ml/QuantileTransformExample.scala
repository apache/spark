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
import org.apache.spark.ml.feature.QuantileTransform
import org.apache.spark.ml.linalg._
// $example off$
import org.apache.spark.sql.SparkSession

object QuantileTransformExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("QuantileTransformExample")
      .getOrCreate()

    // $example on$
    val data = Seq(
      Vectors.dense(0.5, 0.5),
      Vectors.dense(1.0, -1.0),
      Vectors.dense(2.0, -2.0),
      Vectors.dense(3.0, -3.0),
      Vectors.dense(4.0, -4.0)
    )

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val qt = new QuantileTransform()
      .setDistribution("gaussian")
      .setInputCol("features")
      .setOutputCol("transformed")
      .setNumQuantiles(3)

    val result = qt.fit(df).transform(df)
    result.show(false)
    // $example off$

    spark.stop()
  }
}
