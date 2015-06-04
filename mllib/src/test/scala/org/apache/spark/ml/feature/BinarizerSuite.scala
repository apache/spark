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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

class BinarizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var data: Array[Double] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = Array(0.1, -0.5, 0.2, -0.3, 0.8, 0.7, -0.1, -0.4)
  }

  test("Binarize continuous features with default parameter") {
    val defaultBinarized: Array[Double] = data.map(x => if (x > 0.0) 1.0 else 0.0)
    val dataFrame: DataFrame = sqlContext.createDataFrame(
      data.zip(defaultBinarized)).toDF("feature", "expected")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")

    binarizer.transform(dataFrame).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Double, y: Double) =>
        assert(x === y, "The feature value is not correct after binarization.")
    }
  }

  test("Binarize continuous features with setter") {
    val threshold: Double = 0.2
    val thresholdBinarized: Array[Double] = data.map(x => if (x > threshold) 1.0 else 0.0)
    val dataFrame: DataFrame = sqlContext.createDataFrame(
        data.zip(thresholdBinarized)).toDF("feature", "expected")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(threshold)

    binarizer.transform(dataFrame).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Double, y: Double) =>
        assert(x === y, "The feature value is not correct after binarization.")
    }
  }
}
