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
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class BucketizerSuite extends MLTest with DefaultReadWriteTest {

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

    testTransformer[(Double, Double)](dataFrame, bucketizer, "result", "expected") {
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

    testTransformer[(Double, Double)](dataFrame, bucketizer, "result", "expected") {
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
    testTransformer[(Double, Double)](dataFrame, bucketizer, "result", "expected") {
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

  test("Bucketizer should only drop NaN in input columns, with handleInvalid=skip") {
    val df = spark.createDataFrame(Seq((2.3, 3.0), (Double.NaN, 3.0), (6.7, Double.NaN)))
      .toDF("a", "b")
    val splits = Array(Double.NegativeInfinity, 3.0, Double.PositiveInfinity)
    val bucketizer = new Bucketizer().setInputCol("a").setOutputCol("x").setSplits(splits)
    bucketizer.setHandleInvalid("skip")
    assert(bucketizer.transform(df).count() == 2)
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

    val bucketizer = testDefaultReadWrite(t)
    val data = Seq((1.0, 2.0), (10.0, 100.0), (101.0, -1.0)).toDF("myInputCol", "myInputCol2")
    bucketizer.transform(data)
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

  test("multiple columns: Bucket continuous features, without -inf,inf") {
    // Check a set of valid feature values.
    val splits = Array(Array(-0.5, 0.0, 0.5), Array(-0.1, 0.3, 0.5))
    val validData1 = Array(-0.5, -0.3, 0.0, 0.2)
    val validData2 = Array(0.5, 0.3, 0.0, -0.1)
    val expectedBuckets1 = Array(0.0, 0.0, 1.0, 1.0)
    val expectedBuckets2 = Array(1.0, 1.0, 0.0, 0.0)

    val data = validData1.indices.map { idx =>
      (validData1(idx), validData2(idx), expectedBuckets1(idx), expectedBuckets2(idx))
    }
    val dataFrame: DataFrame = data.toDF("feature1", "feature2", "expected1", "expected2")

    val bucketizer1: Bucketizer = new Bucketizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setSplitsArray(splits)

    bucketizer1.transform(dataFrame).select("result1", "expected1", "result2", "expected2")
    BucketizerSuite.checkBucketResults(bucketizer1.transform(dataFrame),
      Seq("result1", "result2"),
      Seq("expected1", "expected2"))

    // Check for exceptions when using a set of invalid feature values.
    val invalidData1 = Array(-0.9) ++ validData1
    val invalidData2 = Array(0.51) ++ validData1
    val badDF1 = invalidData1.zipWithIndex.toSeq.toDF("feature", "idx")

    val bucketizer2: Bucketizer = new Bucketizer()
      .setInputCols(Array("feature"))
      .setOutputCols(Array("result"))
      .setSplitsArray(Array(splits(0)))

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

  test("multiple columns: Bucket continuous features, with -inf,inf") {
    val splits = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.2, 0.5, Double.PositiveInfinity))

    val validData1 = Array(-0.9, -0.5, -0.3, 0.0, 0.2, 0.5, 0.9)
    val validData2 = Array(-0.1, -0.5, -0.2, 0.0, 0.1, 0.3, 0.5)
    val expectedBuckets1 = Array(0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0)
    val expectedBuckets2 = Array(1.0, 0.0, 1.0, 1.0, 1.0, 2.0, 3.0)

    val data = validData1.indices.map { idx =>
      (validData1(idx), validData2(idx), expectedBuckets1(idx), expectedBuckets2(idx))
    }
    val dataFrame: DataFrame = data.toDF("feature1", "feature2", "expected1", "expected2")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setSplitsArray(splits)

    BucketizerSuite.checkBucketResults(bucketizer.transform(dataFrame),
      Seq("result1", "result2"),
      Seq("expected1", "expected2"))
  }

  test("multiple columns: Bucket continuous features, with NaN data but non-NaN splits") {
    val splits = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.1, 0.2, 0.6, Double.PositiveInfinity))

    val validData1 = Array(-0.9, -0.5, -0.3, 0.0, 0.2, 0.5, 0.9, Double.NaN, Double.NaN, Double.NaN)
    val validData2 = Array(0.2, -0.1, 0.3, 0.0, 0.1, 0.3, 0.5, 0.8, Double.NaN, Double.NaN)
    val expectedBuckets1 = Array(0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 4.0)
    val expectedBuckets2 = Array(2.0, 1.0, 2.0, 1.0, 1.0, 2.0, 2.0, 3.0, 4.0, 4.0)

    val data = validData1.indices.map { idx =>
      (validData1(idx), validData2(idx), expectedBuckets1(idx), expectedBuckets2(idx))
    }
    val dataFrame: DataFrame = data.toDF("feature1", "feature2", "expected1", "expected2")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setSplitsArray(splits)

    bucketizer.setHandleInvalid("keep")
    BucketizerSuite.checkBucketResults(bucketizer.transform(dataFrame),
      Seq("result1", "result2"),
      Seq("expected1", "expected2"))

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

  test("multiple columns: Bucket continuous features, with NaN splits") {
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity, Double.NaN)
    withClue("Invalid NaN split was not caught during Bucketizer initialization") {
      intercept[IllegalArgumentException] {
        new Bucketizer().setSplitsArray(Array(splits))
      }
    }
  }

  test("multiple columns: read/write") {
    val t = new Bucketizer()
      .setInputCols(Array("myInputCol"))
      .setOutputCols(Array("myOutputCol"))
      .setSplitsArray(Array(Array(0.1, 0.8, 0.9)))

    val bucketizer = testDefaultReadWrite(t)
    val data = Seq((1.0, 2.0), (10.0, 100.0), (101.0, -1.0)).toDF("myInputCol", "myInputCol2")
    bucketizer.transform(data)
    assert(t.hasDefault(t.outputCol))
    assert(bucketizer.hasDefault(bucketizer.outputCol))
  }

  test("Bucketizer in a pipeline") {
    val df = Seq((0.5, 0.3, 1.0, 1.0), (0.5, -0.4, 1.0, 0.0))
      .toDF("feature1", "feature2", "expected1", "expected2")

    val bucket = new Bucketizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setSplitsArray(Array(Array(-0.5, 0.0, 0.5), Array(-0.5, 0.0, 0.5)))

    val pl = new Pipeline()
      .setStages(Array(bucket))
      .fit(df)
    pl.transform(df).select("result1", "expected1", "result2", "expected2")

    BucketizerSuite.checkBucketResults(pl.transform(df),
      Seq("result1", "result2"), Seq("expected1", "expected2"))
  }

  test("Compare single/multiple column(s) Bucketizer in pipeline") {
    val df = Seq((0.5, 0.3, 1.0, 1.0), (0.5, -0.4, 1.0, 0.0))
      .toDF("feature1", "feature2", "expected1", "expected2")

    val multiColsBucket = new Bucketizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setSplitsArray(Array(Array(-0.5, 0.0, 0.5), Array(-0.5, 0.0, 0.5)))

    val plForMultiCols = new Pipeline()
      .setStages(Array(multiColsBucket))
      .fit(df)

    val bucketForCol1 = new Bucketizer()
      .setInputCol("feature1")
      .setOutputCol("result1")
      .setSplits(Array(-0.5, 0.0, 0.5))
    val bucketForCol2 = new Bucketizer()
      .setInputCol("feature2")
      .setOutputCol("result2")
      .setSplits(Array(-0.5, 0.0, 0.5))

    val plForSingleCol = new Pipeline()
      .setStages(Array(bucketForCol1, bucketForCol2))
      .fit(df)

    val resultForSingleCol = plForSingleCol.transform(df)
      .select("result1", "expected1", "result2", "expected2")
      .collect()
    val resultForMultiCols = plForMultiCols.transform(df)
      .select("result1", "expected1", "result2", "expected2")
      .collect()

    resultForSingleCol.zip(resultForMultiCols).foreach {
        case (rowForSingle, rowForMultiCols) =>
          assert(rowForSingle.getDouble(0) == rowForMultiCols.getDouble(0) &&
            rowForSingle.getDouble(1) == rowForMultiCols.getDouble(1) &&
            rowForSingle.getDouble(2) == rowForMultiCols.getDouble(2) &&
            rowForSingle.getDouble(3) == rowForMultiCols.getDouble(3))
    }
  }

  test("assert exception is thrown if both multi-column and single-column params are set") {
    val df = Seq((0.5, 0.3), (0.5, -0.4)).toDF("feature1", "feature2")
    ParamsSuite.testExclusiveParams(new Bucketizer, df, ("inputCol", "feature1"),
      ("inputCols", Array("feature1", "feature2")))
    ParamsSuite.testExclusiveParams(new Bucketizer, df, ("inputCol", "feature1"),
      ("outputCol", "result1"), ("splits", Array(-0.5, 0.0, 0.5)),
      ("outputCols", Array("result1", "result2")))
    ParamsSuite.testExclusiveParams(new Bucketizer, df, ("inputCol", "feature1"),
      ("outputCol", "result1"), ("splits", Array(-0.5, 0.0, 0.5)),
      ("splitsArray", Array(Array(-0.5, 0.0, 0.5), Array(-0.5, 0.0, 0.5))))

    // this should fail because at least one of inputCol and inputCols must be set
    ParamsSuite.testExclusiveParams(new Bucketizer, df, ("outputCol", "feature1"),
      ("splits", Array(-0.5, 0.0, 0.5)))

    // the following should fail because not all the params are set
    ParamsSuite.testExclusiveParams(new Bucketizer, df, ("inputCol", "feature1"),
      ("outputCol", "result1"))
    ParamsSuite.testExclusiveParams(new Bucketizer, df,
      ("inputCols", Array("feature1", "feature2")),
      ("outputCols", Array("result1", "result2")))
  }

  test("Bucketizer nested input column") {
    // Check a set of valid feature values.
    val splits = Array(-0.5, 0.0, 0.5)
    val validData = Array(-0.5, -0.3, 0.0, 0.2)
    val expectedBuckets = Array(0.0, 0.0, 1.0, 1.0)
    val dataFrame: DataFrame = validData.zip(expectedBuckets).toSeq.toDF("feature", "expected")
      .select(struct(col("feature")).as("nest"), col("expected"))

    val bucketizer1: Bucketizer = new Bucketizer()
      .setInputCol("nest.feature")
      .setOutputCol("result")
      .setSplits(splits)

    val bucketizer2: Bucketizer = new Bucketizer()
      .setInputCols(Array("nest.feature"))
      .setOutputCols(Array("result"))
      .setSplitsArray(Array(splits))

    for (bucketizer <- Seq(bucketizer1, bucketizer2)) {
      val resultDF = bucketizer.transform(dataFrame).select("result", "expected")
      resultDF.collect().foreach {
        case Row(x: Double, y: Double) =>
          assert(x === y,
            s"The feature value is not correct after bucketing.  Expected $y but found $x")
      }
    }
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

  /** Checks if bucketized results match expected ones. */
  def checkBucketResults(
      bucketResult: DataFrame,
      resultColumns: Seq[String],
      expectedColumns: Seq[String]): Unit = {
    assert(resultColumns.length == expectedColumns.length,
      s"Given ${resultColumns.length} result columns doesn't match " +
        s"${expectedColumns.length} expected columns.")
    assert(resultColumns.length > 0, "At least one result and expected columns are needed.")

    val allColumns = resultColumns ++ expectedColumns
    bucketResult.select(allColumns.head, allColumns.tail: _*).collect().foreach {
      case row =>
        for (idx <- 0 until row.length / 2) {
          val result = row.getDouble(idx)
          val expected = row.getDouble(idx + row.length / 2)
          assert(result === expected, "The feature value is not correct after bucketing. " +
            s"Expected $expected but found $result.")
        }
    }
  }
}
