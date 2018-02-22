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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class BucketizerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new Bucketizer)
  }

  test("Bucket continuous features, without -inf,inf") {
    // Check a set of valid feature values.
    val splits = Array(-0.5, 0.0, 0.5)
    val validData = Array(-0.5, -0.3, 0.0, 0.2)
    val expectedBuckets = Array(0.0, 0.0, 1.0, 1.0)
    val dataFrame: DataFrame = validData.zip(expectedBuckets).toSeq.toDF("feature", "expected")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCol("feature")
      .setOutputCol("result")
      .setSplits(splits)

    bucketizer.transform(dataFrame).select("result", "expected").collect().foreach {
      case Row(x: Double, y: Double) =>
        assert(x === y,
          s"The feature value is not correct after bucketing.  Expected $y but found $x")
    }

    // Check for exceptions when using a set of invalid feature values.
    val invalidData1: Array[Double] = Array(-0.9) ++ validData
    val invalidData2 = Array(0.51) ++ validData
    val badDF1 = invalidData1.zipWithIndex.toSeq.toDF("feature", "idx")
    withClue("Invalid feature value -0.9 was not caught as an invalid feature!") {
      intercept[SparkException] {
        bucketizer.transform(badDF1).collect()
      }
    }
    val badDF2 = invalidData2.zipWithIndex.toSeq.toDF("feature", "idx")
    withClue("Invalid feature value 0.51 was not caught as an invalid feature!") {
      intercept[SparkException] {
        bucketizer.transform(badDF2).collect()
      }
    }
  }

  test("Bucket continuous features, with -inf,inf") {
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)
    val validData = Array(-0.9, -0.5, -0.3, 0.0, 0.2, 0.5, 0.9)
    val expectedBuckets = Array(0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0)
    val dataFrame: DataFrame = validData.zip(expectedBuckets).toSeq.toDF("feature", "expected")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCol("feature")
      .setOutputCol("result")
      .setSplits(splits)

    bucketizer.transform(dataFrame).select("result", "expected").collect().foreach {
      case Row(x: Double, y: Double) =>
        assert(x === y,
          s"The feature value is not correct after bucketing.  Expected $y but found $x")
    }
  }

  test("Bucket continuous features, with NaN data but non-NaN splits") {
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)
    val validData = Array(-0.9, -0.5, -0.3, 0.0, 0.2, 0.5, 0.9, Double.NaN, Double.NaN, Double.NaN)
    val expectedBuckets = Array(0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 4.0)
    val dataFrame: DataFrame = validData.zip(expectedBuckets).toSeq.toDF("feature", "expected")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCol("feature")
      .setOutputCol("result")
      .setSplits(splits)

    bucketizer.setHandleInvalid("keep")
    bucketizer.transform(dataFrame).select("result", "expected").collect().foreach {
      case Row(x: Double, y: Double) =>
        assert(x === y,
          s"The feature value is not correct after bucketing.  Expected $y but found $x")
    }

    bucketizer.setHandleInvalid("skip")
    val skipResults: Array[Double] = bucketizer.transform(dataFrame)
      .select("result").as[Double].collect()
    assert(skipResults.length === 7)
    assert(skipResults.forall(_ !== 4.0))

    bucketizer.setHandleInvalid("error")
    withClue("Bucketizer should throw error when setHandleInvalid=error and given NaN values") {
      intercept[SparkException] {
        bucketizer.transform(dataFrame).collect()
      }
    }
  }

  test("Bucket continuous features, with NaN splits") {
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity, Double.NaN)
    withClue("Invalid NaN split was not caught during Bucketizer initialization") {
      intercept[IllegalArgumentException] {
        new Bucketizer().setSplits(splits)
      }
    }
  }

  test("Binary search correctness on hand-picked examples") {
    import BucketizerSuite.checkBinarySearch
    // length 3, with -inf
    checkBinarySearch(Array(Double.NegativeInfinity, 0.0, 1.0))
    // length 4
    checkBinarySearch(Array(-1.0, -0.5, 0.0, 1.0))
    // length 5
    checkBinarySearch(Array(-1.0, -0.5, 0.0, 1.0, 1.5))
    // length 3, with inf
    checkBinarySearch(Array(0.0, 1.0, Double.PositiveInfinity))
    // length 3, with -inf and inf
    checkBinarySearch(Array(Double.NegativeInfinity, 1.0, Double.PositiveInfinity))
    // length 4, with -inf and inf
    checkBinarySearch(Array(Double.NegativeInfinity, 0.0, 1.0, Double.PositiveInfinity))
  }

  test("Binary search correctness in contrast with linear search, on random data") {
    val data = Array.fill(100)(Random.nextDouble())
    val splits: Array[Double] = Double.NegativeInfinity +:
      Array.fill(10)(Random.nextDouble()).sorted :+ Double.PositiveInfinity
    val bsResult = Vectors.dense(data.map(x =>
      Bucketizer.binarySearchForBuckets(splits, x, false)))
    val lsResult = Vectors.dense(data.map(x => BucketizerSuite.linearSearchForBuckets(splits, x)))
    assert(bsResult ~== lsResult absTol 1e-5)
  }

  test("read/write") {
    val t = new Bucketizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setSplits(Array(0.1, 0.8, 0.9))
    testDefaultReadWrite(t)
  }

  test("Bucket numeric features") {
    val splits = Array(-3.0, 0.0, 3.0)
    val data = Array(-2.0, -1.0, 0.0, 1.0, 2.0)
    val expectedBuckets = Array(0.0, 0.0, 1.0, 1.0, 1.0)
    val dataFrame: DataFrame = data.zip(expectedBuckets).toSeq.toDF("feature", "expected")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCol("feature")
      .setOutputCol("result")
      .setSplits(splits)

    val types = Seq(ShortType, IntegerType, LongType, FloatType, DoubleType,
      ByteType, DecimalType(10, 0))
    for (mType <- types) {
      val df = dataFrame.withColumn("feature", col("feature").cast(mType))
      bucketizer.transform(df).select("result", "expected").collect().foreach {
        case Row(x: Double, y: Double) =>
          assert(x === y, "The result is not correct after bucketing in type " +
            mType.toString + ". " + s"Expected $y but found $x.")
      }
    }
  }

  test("Bucketizer should only drop NaN in input columns, with handleInvalid=skip") {
    val df = spark.createDataFrame(Seq((2.3, 3.0), (Double.NaN, 3.0), (6.7, Double.NaN)))
      .toDF("a", "b")
    val splits = Array(Double.NegativeInfinity, 3.0, Double.PositiveInfinity)
    val bucketizer = new Bucketizer().setInputCol("a").setOutputCol("x").setSplits(splits)
    bucketizer.setHandleInvalid("skip")
    assert(bucketizer.transform(df).count() == 2)
  }
}

private object BucketizerSuite extends SparkFunSuite {
  /** Brute force search for buckets.  Bucket i is defined by the range [split(i), split(i+1)). */
  def linearSearchForBuckets(splits: Array[Double], feature: Double): Double = {
    require(feature >= splits.head)
    var i = 0
    val n = splits.length - 1
    while (i < n) {
      if (feature < splits(i + 1)) return i
      i += 1
    }
    throw new RuntimeException(
      s"linearSearchForBuckets failed to find bucket for feature value $feature")
  }

  /** Check all values in splits, plus values between all splits. */
  def checkBinarySearch(splits: Array[Double]): Unit = {
    def testFeature(feature: Double, expectedBucket: Double): Unit = {
      assert(Bucketizer.binarySearchForBuckets(splits, feature, false) === expectedBucket,
        s"Expected feature value $feature to be in bucket $expectedBucket with splits:" +
          s" ${splits.mkString(", ")}")
    }
    var i = 0
    val n = splits.length - 1
    while (i < n) {
      // Split i should fall in bucket i.
      testFeature(splits(i), i)
      // Value between splits i,i+1 should be in i, which is also true if the (i+1)-th split is inf.
      testFeature((splits(i) + splits(i + 1)) / 2, i)
      i += 1
    }
  }
}
