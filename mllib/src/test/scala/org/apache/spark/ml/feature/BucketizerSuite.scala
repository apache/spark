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

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class BucketizerSuite extends FunSuite with MLlibTestSparkContext {

  test("Bucket continuous features with setter") {
    val sqlContext = new SQLContext(sc)
    val data = Array(0.1, -0.5, 0.2, -0.3, 0.8, 0.7, -0.1, -0.4, -0.9)
    val splits = Array(-0.5, 0.0, 0.5)
    val bucketizedData = Array(2.0, 1.0, 2.0, 1.0, 3.0, 3.0, 1.0, 1.0, 0.0)
    val dataFrame: DataFrame = sqlContext.createDataFrame(
        data.zip(bucketizedData)).toDF("feature", "expected")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCol("feature")
      .setOutputCol("result")
      .setSplits(splits)

    bucketizer.transform(dataFrame).select("result", "expected").collect().foreach {
      case Row(x: Double, y: Double) =>
        assert(x === y, "The feature value is not correct after bucketing.")
    }
  }

  test("Binary search for finding buckets") {
    val data = Array.fill[Double](100)(Random.nextDouble())
    val splits = Array.fill[Double](10)(Random.nextDouble()).sorted
    val wrappedSplits = Array(Double.MinValue) ++ splits ++ Array(Double.MaxValue)
    val bsResult = Vectors.dense(
      data.map(x => Bucketizer.binarySearchForBuckets(wrappedSplits, x, true, true)))
    val lsResult = Vectors.dense(data.map(x => BucketizerSuite.linearSearchForBuckets(splits, x)))
    assert(bsResult ~== lsResult absTol 1e-5)
  }
}

private object BucketizerSuite {
  private def linearSearchForBuckets(splits: Array[Double], feature: Double): Double = {
    var i = 0
    while (i < splits.size) {
      if (feature < splits(i)) return i
      i += 1
    }
    i
  }
}
