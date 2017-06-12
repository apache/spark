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
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

class MultipleBucketizerSuite extends SparkFunSuite with MLlibTestSparkContext
    with DefaultReadWriteTest {

  import testImplicits._

  test("Bucket continuous features, without -inf,inf") {
    // Check a set of valid feature values.
    val splits = Array(Array(-0.5, 0.0, 0.5), Array(-0.1, 0.3, 0.5))
    val validData1 = Array(-0.5, -0.3, 0.0, 0.2)
    val validData2 = Array(0.5, 0.3, 0.0, -0.1)
    val expectedBuckets1 = Array(0.0, 0.0, 1.0, 1.0)
    val expectedBuckets2 = Array(1.0, 1.0, 0.0, 0.0)

    val data = (0 until validData1.length).map { idx =>
      (validData1(idx), validData2(idx), expectedBuckets1(idx), expectedBuckets2(idx))
    }
    val dataFrame: DataFrame = data.toSeq.toDF("feature1", "feature2", "expected1", "expected2")

    val bucketizer1: Bucketizer = new Bucketizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setSplitsArray(splits)

    assert(bucketizer1.isBucketizeMultipleInputCols())

    bucketizer1.transform(dataFrame).select("result1", "expected1", "result2", "expected2")
      .collect().foreach {
        case Row(r1: Double, e1: Double, r2: Double, e2: Double) =>
          assert(r1 === e1,
            s"The feature value is not correct after bucketing.  Expected $e1 but found $r1")
          assert(r2 === e2,
            s"The feature value is not correct after bucketing.  Expected $e2 but found $r2")
      }

    // Check for exceptions when using a set of invalid feature values.
    val invalidData1: Array[Double] = Array(-0.9) ++ validData1
    val invalidData2 = Array(0.51) ++ validData1
    val badDF1 = invalidData1.zipWithIndex.toSeq.toDF("feature", "idx")

    val bucketizer2: Bucketizer = new Bucketizer()
      .setInputCols(Array("feature"))
      .setOutputCols(Array("result"))
      .setSplitsArray(Array(splits(0)))

    assert(bucketizer2.isBucketizeMultipleInputCols())

    withClue("Invalid feature value -0.9 was not caught as an invalid feature!") {
      intercept[SparkException] {
        bucketizer2.transform(badDF1).collect()
      }
    }
    val badDF2 = invalidData2.zipWithIndex.toSeq.toDF("feature", "idx")
    withClue("Invalid feature value 0.51 was not caught as an invalid feature!") {
      intercept[SparkException] {
        bucketizer2.transform(badDF2).collect()
      }
    }
  }

  test("Bucket continuous features, with -inf,inf") {
    val splits = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.2, 0.5, Double.PositiveInfinity))

    val validData1 = Array(-0.9, -0.5, -0.3, 0.0, 0.2, 0.5, 0.9)
    val validData2 = Array(-0.1, -0.5, -0.2, 0.0, 0.1, 0.3, 0.5)
    val expectedBuckets1 = Array(0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0)
    val expectedBuckets2 = Array(1.0, 0.0, 1.0, 1.0, 1.0, 2.0, 3.0)

    val data = (0 until validData1.length).map { idx =>
      (validData1(idx), validData2(idx), expectedBuckets1(idx), expectedBuckets2(idx))
    }
    val dataFrame: DataFrame = data.toSeq.toDF("feature1", "feature2", "expected1", "expected2")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setSplitsArray(splits)

    assert(bucketizer.isBucketizeMultipleInputCols())

    bucketizer.transform(dataFrame).select("result1", "expected1", "result2", "expected2")
      .collect().foreach {
        case Row(r1: Double, e1: Double, r2: Double, e2: Double) =>
          assert(r1 === e1,
            s"The feature value is not correct after bucketing.  Expected $e1 but found $r1")
          assert(r2 === e2,
            s"The feature value is not correct after bucketing.  Expected $e2 but found $r2")
      }
  }

  test("Bucket continuous features, with NaN data but non-NaN splits") {
    val splits = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.1, 0.2, 0.6, Double.PositiveInfinity))

    val validData1 = Array(-0.9, -0.5, -0.3, 0.0, 0.2, 0.5, 0.9, Double.NaN, Double.NaN, Double.NaN)
    val validData2 = Array(0.2, -0.1, 0.3, 0.0, 0.1, 0.3, 0.5, 0.8, Double.NaN, Double.NaN)
    val expectedBuckets1 = Array(0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 4.0)
    val expectedBuckets2 = Array(2.0, 1.0, 2.0, 1.0, 1.0, 2.0, 2.0, 3.0, 4.0, 4.0)

    val data = (0 until validData1.length).map { idx =>
      (validData1(idx), validData2(idx), expectedBuckets1(idx), expectedBuckets2(idx))
    }
    val dataFrame: DataFrame = data.toSeq.toDF("feature1", "feature2", "expected1", "expected2")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setSplitsArray(splits)

    assert(bucketizer.isBucketizeMultipleInputCols())

    bucketizer.setHandleInvalid("keep")
    bucketizer.transform(dataFrame).select("result1", "expected1", "result2", "expected2")
      .collect().foreach {
        case Row(r1: Double, e1: Double, r2: Double, e2: Double) =>
          assert(r1 === e1,
            s"The feature value is not correct after bucketing.  Expected $e1 but found $r1")
          assert(r2 === e2,
            s"The feature value is not correct after bucketing.  Expected $e2 but found $r2")
      }

    bucketizer.setHandleInvalid("skip")
    val skipResults1: Array[Double] = bucketizer.transform(dataFrame)
      .select("result1").as[Double].collect()
    assert(skipResults1.length === 7)
    assert(skipResults1.forall(_ !== 4.0))

    val skipResults2: Array[Double] = bucketizer.transform(dataFrame)
      .select("result2").as[Double].collect()
    assert(skipResults2.length === 7)
    assert(skipResults2.forall(_ !== 4.0))

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
        new Bucketizer().setSplitsArray(Array(splits))
      }
    }
  }

  test("read/write") {
    val t = new Bucketizer()
      .setInputCols(Array("myInputCol"))
      .setOutputCols(Array("myOutputCol"))
      .setSplitsArray(Array(Array(0.1, 0.8, 0.9)))
    assert(t.isBucketizeMultipleInputCols())
    testDefaultReadWrite(t)
  }
}
