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

import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkFunSuite}

class QuantileDiscretizerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import org.apache.spark.ml.feature.QuantileDiscretizerSuite._

  test("Test quantile discretizer") {
    checkDiscretizedData(sc,
      Array[Double](1, 2, 3, 3, 3, 3, 3, 3, 3),
      10,
      Array[Double](1, 2, 3, 3, 3, 3, 3, 3, 3),
      Array("-Infinity, 1.0", "1.0, 2.0", "2.0, 3.0", "3.0, Infinity"))

    checkDiscretizedData(sc,
      Array[Double](1, 2, 3, 3, 3, 3, 3, 3, 3),
      4,
      Array[Double](1, 2, 3, 3, 3, 3, 3, 3, 3),
      Array("-Infinity, 1.0", "1.0, 2.0", "2.0, 3.0", "3.0, Infinity"))

    checkDiscretizedData(sc,
      Array[Double](1, 2, 3, 3, 3, 3, 3, 3, 3),
      3,
      Array[Double](0, 1, 2, 2, 2, 2, 2, 2, 2),
      Array("-Infinity, 2.0", "2.0, 3.0", "3.0, Infinity"))

    checkDiscretizedData(sc,
      Array[Double](1, 2, 3, 3, 3, 3, 3, 3, 3),
      2,
      Array[Double](0, 1, 1, 1, 1, 1, 1, 1, 1),
      Array("-Infinity, 2.0", "2.0, Infinity"))

  }

  test("Test getting splits") {
    val splitTestPoints = Array(
      Array[Double]() -> Array(Double.NegativeInfinity, 0, Double.PositiveInfinity),
      Array(Double.NegativeInfinity) -> Array(Double.NegativeInfinity, 0, Double.PositiveInfinity),
      Array(Double.PositiveInfinity) -> Array(Double.NegativeInfinity, 0, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, Double.PositiveInfinity)
        -> Array(Double.NegativeInfinity, 0, Double.PositiveInfinity),
      Array(0.0) -> Array(Double.NegativeInfinity, 0, Double.PositiveInfinity),
      Array(1.0) -> Array(Double.NegativeInfinity, 1, Double.PositiveInfinity),
      Array(0.0, 1.0) -> Array(Double.NegativeInfinity, 0, 1, Double.PositiveInfinity)
    )
    for ((ori, res) <- splitTestPoints) {
      assert(QuantileDiscretizer.getSplits(ori) === res, "Returned splits are invalid.")
    }
  }

  test("Test splits on dataset larger than minSamplesRequired") {
    val sqlCtx = SQLContext.getOrCreate(sc)
    import sqlCtx.implicits._

    val datasetSize = QuantileDiscretizer.minSamplesRequired + 1
    val numBuckets = 5
    val df = sc.parallelize((1.0 to datasetSize by 1.0).map(Tuple1.apply)).toDF("input")
    val discretizer = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("result")
      .setNumBuckets(numBuckets)
      .setSeed(1)

    val result = discretizer.fit(df).transform(df)
    val observedNumBuckets = result.select("result").distinct.count

    assert(observedNumBuckets === numBuckets,
      "Observed number of buckets does not equal expected number of buckets.")
  }

  test("read/write") {
    val t = new QuantileDiscretizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setNumBuckets(6)
    testDefaultReadWrite(t)
  }
}

private object QuantileDiscretizerSuite extends SparkFunSuite {

  def checkDiscretizedData(
      sc: SparkContext,
      data: Array[Double],
      numBucket: Int,
      expectedResult: Array[Double],
      expectedAttrs: Array[String]): Unit = {
    val sqlCtx = SQLContext.getOrCreate(sc)
    import sqlCtx.implicits._

    val df = sc.parallelize(data.map(Tuple1.apply)).toDF("input")
    val discretizer = new QuantileDiscretizer().setInputCol("input").setOutputCol("result")
      .setNumBuckets(numBucket).setSeed(1)
    val result = discretizer.fit(df).transform(df)

    val transformedFeatures = result.select("result").collect()
      .map { case Row(transformedFeature: Double) => transformedFeature }
    val transformedAttrs = Attribute.fromStructField(result.schema("result"))
      .asInstanceOf[NominalAttribute].values.get

    assert(transformedFeatures === expectedResult,
      "Transformed features do not equal expected features.")
    assert(transformedAttrs === expectedAttrs,
      "Transformed attributes do not equal expected attributes.")
  }
}
