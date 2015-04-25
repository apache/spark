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

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


class BinarizerSuite extends FunSuite with MLlibTestSparkContext {

  @transient var data: Array[Double] = _
  @transient var dataFrame: DataFrame = _
  @transient var binarizer: Binarizer = _
  @transient val threshold = 0.2
  @transient var defaultBinarized: Array[Double] = _
  @transient var thresholdBinarized: Array[Double] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = Array(0.1, -0.5, 0.2, -0.3, 0.8, 0.7, -0.1, -0.4)
    defaultBinarized = data.map(x => if (x > 0.0) 1.0 else 0.0)
    thresholdBinarized = data.map(x => if (x > threshold) 1.0 else 0.0)

    val sqlContext = new SQLContext(sc)
    dataFrame = sqlContext.createDataFrame(sc.parallelize(data, 2).map(BinarizerSuite.FeatureData))
    binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
  }

  def collectResult(result: DataFrame): Array[Double] = {
    result.select("binarized_feature").collect().map {
      case Row(feature: Double) => feature
    }
  }

  def assertValues(lhs: Array[Double], rhs: Array[Double]): Unit = {
    assert((lhs, rhs).zipped.forall { (x1, x2) =>
      x1 === x2
    }, "The feature value is not correct after binarization.")
  }

  test("Binarize continuous features with default parameter") {
    val result = collectResult(binarizer.transform(dataFrame))
    assertValues(result, defaultBinarized)
  }

  test("Binarize continuous features with setter") {
    binarizer.setThreshold(threshold)
    val result = collectResult(binarizer.transform(dataFrame))
    assertValues(result, thresholdBinarized)
  }
}

private object BinarizerSuite {
  case class FeatureData(feature: Double)
}
