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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf

class QuantileDiscretizerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("Test observed number of buckets and their sizes match expected values") {
    val spark = this.spark
    import spark.implicits._

    val datasetSize = 100000
    val numBuckets = 5
    val df = sc.parallelize(1.0 to datasetSize by 1.0).map(Tuple1.apply).toDF("input")
    val discretizer = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("result")
      .setNumBuckets(numBuckets)
    val result = discretizer.fit(df).transform(df)

    val observedNumBuckets = result.select("result").distinct.count
    assert(observedNumBuckets === numBuckets,
      "Observed number of buckets does not equal expected number of buckets.")

    val relativeError = discretizer.getRelativeError
    val isGoodBucket = udf {
      (size: Int) => math.abs( size - (datasetSize / numBuckets)) <= (relativeError * datasetSize)
    }
    val numGoodBuckets = result.groupBy("result").count.filter(isGoodBucket($"count")).count
    assert(numGoodBuckets === numBuckets,
      "Bucket sizes are not within expected relative error tolerance.")
  }

  test("Test on data with high proportion of duplicated values") {
    val spark = this.spark
    import spark.implicits._

    val numBuckets = 5
    val expectedNumBuckets = 3
    val df = sc.parallelize(Array(1.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 2.0, 2.0, 2.0, 1.0, 3.0))
      .map(Tuple1.apply).toDF("input")
    val discretizer = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("result")
      .setNumBuckets(numBuckets)
    val result = discretizer.fit(df).transform(df)
    val observedNumBuckets = result.select("result").distinct.count
    assert(observedNumBuckets == expectedNumBuckets,
      s"Observed number of buckets are not correct." +
        s" Expected $expectedNumBuckets but found $observedNumBuckets")
  }

  test("Test transform on data with NaN value") {
    val spark = this.spark
    import spark.implicits._

    val numBuckets = 3
    val validData = Array(-0.9, -0.5, -0.3, 0.0, 0.2, 0.5, 0.9, Double.NaN, Double.NaN, Double.NaN)
    val expectedKeep = Array(0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0)
    val expectedSkip = Array(0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 2.0)

    val discretizer = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("result")
      .setNumBuckets(numBuckets)

    withClue("QuantileDiscretizer with handleInvalid=error should throw exception for NaN values") {
      val dataFrame: DataFrame = validData.toSeq.toDF("input")
      intercept[SparkException] {
        discretizer.fit(dataFrame).transform(dataFrame).collect()
      }
    }

    List(("keep", expectedKeep), ("skip", expectedSkip)).foreach{
      case(u, v) =>
        discretizer.setHandleInvalid(u)
        val dataFrame: DataFrame = validData.zip(v).toSeq.toDF("input", "expected")
        val result = discretizer.fit(dataFrame).transform(dataFrame)
        result.select("result", "expected").collect().foreach {
          case Row(x: Double, y: Double) =>
            assert(x === y,
              s"The feature value is not correct after bucketing.  Expected $y but found $x")
        }
    }
  }

  test("Test transform method on unseen data") {
    val spark = this.spark
    import spark.implicits._

    val trainDF = sc.parallelize(1.0 to 100.0 by 1.0).map(Tuple1.apply).toDF("input")
    val testDF = sc.parallelize(-10.0 to 110.0 by 1.0).map(Tuple1.apply).toDF("input")
    val discretizer = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("result")
      .setNumBuckets(5)

    val result = discretizer.fit(trainDF).transform(testDF)
    val firstBucketSize = result.filter(result("result") === 0.0).count
    val lastBucketSize = result.filter(result("result") === 4.0).count

    assert(firstBucketSize === 30L,
      s"Size of first bucket ${firstBucketSize} did not equal expected value of 30.")
    assert(lastBucketSize === 31L,
      s"Size of last bucket ${lastBucketSize} did not equal expected value of 31.")
  }

  test("read/write") {
    val t = new QuantileDiscretizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setNumBuckets(6)
    testDefaultReadWrite(t)
  }

  test("Verify resulting model has parent") {
    val spark = this.spark
    import spark.implicits._

    val df = sc.parallelize(1 to 100).map(Tuple1.apply).toDF("input")
    val discretizer = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("result")
      .setNumBuckets(5)
    val model = discretizer.fit(df)
    assert(model.hasParent)
  }

  test("Multiple Columns: Test observed number of buckets and their sizes match expected values") {
    val spark = this.spark
    import spark.implicits._

    val datasetSize = 100000
    val numBuckets = 5
    val data1 = Array.range(1, 100001, 1).map(_.toDouble)
    val data2 = Array.range(1, 200000, 2).map(_.toDouble)
    val df = data1.zip(data2).toSeq.toDF("input1", "input2")

    val discretizer = new QuantileDiscretizer()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("result1", "result2"))
      .setNumBuckets(numBuckets)
    assert(discretizer.isQuantileDiscretizeMultipleColumns())
    val result = discretizer.fit(df).transform(df)

    val relativeError = discretizer.getRelativeError
    val isGoodBucket = udf {
      (size: Int) => math.abs( size - (datasetSize / numBuckets)) <= (relativeError * datasetSize)
    }

    for (i <- 1 to 2) {
      val observedNumBuckets = result.select("result" + i).distinct.count
      assert(observedNumBuckets === numBuckets,
        "Observed number of buckets does not equal expected number of buckets.")

      val numGoodBuckets = result.groupBy("result" + i).count.filter(isGoodBucket($"count")).count
      assert(numGoodBuckets === numBuckets,
        "Bucket sizes are not within expected relative error tolerance.")
    }
  }

  test("Multiple Columns: Test on data with high proportion of duplicated values") {
    val spark = this.spark
    import spark.implicits._

    val numBuckets = 5
    val expectedNumBucket = 3
    val data1 = Array(1.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 2.0, 2.0, 2.0, 1.0, 3.0)
    val data2 = Array(1.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 3.0, 2.0, 3.0, 1.0, 2.0)
    val df = data1.zip(data2).toSeq.toDF("input1", "input2")
    val discretizer = new QuantileDiscretizer()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("result1", "result2"))
      .setNumBuckets(numBuckets)
    assert(discretizer.isQuantileDiscretizeMultipleColumns())
    val result = discretizer.fit(df).transform(df)
    for (i <- 1 to 2) {
      val observedNumBuckets = result.select("result" + i).distinct.count
      assert(observedNumBuckets == expectedNumBucket,
        s"Observed number of buckets are not correct." +
          s" Expected $expectedNumBucket but found ($observedNumBuckets")
    }
  }

  test("Multiple Columns: Test transform on data with NaN value") {
    val spark = this.spark
    import spark.implicits._

    val numBuckets = 3
    val validData1 = Array(-0.9, -0.5, -0.3, 0.0, 0.2, 0.5, 0.9, Double.NaN, Double.NaN, Double.NaN)
    val expectedKeep1 = Array(0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0)
    val validData2 = Array(0.2, -0.1, 0.3, 0.0, 0.1, 0.3, 0.5, 0.8, Double.NaN, Double.NaN)
    val expectedKeep2 = Array(1.0, 0.0, 2.0, 0.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0)

    val data = (0 until validData1.length).map { idx =>
      (validData1(idx), validData2(idx), expectedKeep1(idx), expectedKeep2(idx))
    }
    val dataFrame = data.toSeq.toDF("input1", "input2", "expected1", "expected2")

    val discretizer = new QuantileDiscretizer()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("result1", "result2"))
      .setNumBuckets(numBuckets)
    assert(discretizer.isQuantileDiscretizeMultipleColumns())

    withClue("QuantileDiscretizer with handleInvalid=error should throw exception for NaN values") {
      intercept[SparkException] {
        discretizer.fit(dataFrame).transform(dataFrame).collect()
      }
    }

    discretizer.setHandleInvalid("keep")
    discretizer.fit(dataFrame).transform(dataFrame).
      select("result1", "expected1", "result2", "expected2")
      .collect().foreach {
      case Row(r1: Double, e1: Double, r2: Double, e2: Double) =>
        assert(r1 === e1,
          s"The result value is not correct after bucketing. Expected $e1 but found $r1")
        assert(r2 === e2,
          s"The result value is not correct after bucketing. Expected $e2 but found $r2")
    }

    discretizer.setHandleInvalid("skip")
    val result = discretizer.fit(dataFrame).transform(dataFrame)
    for (i <- 1 to 2) {
      val skipResults1: Array[Double] = result.select("result" + i).as[Double].collect()
      assert(skipResults1.length === 7)
      assert(skipResults1.forall(_ !== 4.0))
    }
  }

  test("Multiple Columns: Test numBucketsArray") {
    val spark = this.spark
    import spark.implicits._

    val datasetSize = 20
    val numBucketsArray: Array[Int] = Array(2, 5, 10)
    val data1 = Array.range(1, 21, 1).map(_.toDouble)
    val expected1 = Array (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0,
      1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val data2 = Array.range(1, 40, 2).map(_.toDouble)
    val expected2 = Array (0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0,
      2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 4.0, 4.0, 4.0, 4.0)
    val data3 = Array.range(1, 60, 3).map(_.toDouble)
    val expected3 = Array (0.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 4.0, 4.0, 5.0,
      5.0, 5.0, 6.0, 6.0, 7.0, 8.0, 8.0, 9.0, 9.0, 9.0)
    val data = (0 until 20).map { idx =>
      (data1(idx), data2(idx), data3(idx), expected1(idx), expected2(idx), expected3(idx))
    }
    val df =
      data.toSeq.toDF("input1", "input2", "input3", "expected1", "expected2", "expected3")

    val discretizer = new QuantileDiscretizer()
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("result1", "result2", "result3"))
      .setNumBucketsArray(numBucketsArray)
    assert(discretizer.isQuantileDiscretizeMultipleColumns())
    discretizer.fit(df).transform(df).
      select("result1", "expected1", "result2", "expected2", "result3", "expected3")
      .collect().foreach {
      case Row(r1: Double, e1: Double, r2: Double, e2: Double, r3: Double, e3: Double) =>
        assert(r1 === e1,
          s"The result value is not correct after bucketing. Expected $e1 but found $r1")
        assert(r2 === e2,
          s"The result value is not correct after bucketing. Expected $e2 but found $r2")
        assert(r3 === e3,
          s"The result value is not correct after bucketing. Expected $e3 but found $r3")
    }
  }

  test("multiple columns: read/write") {
    val discretizer = new QuantileDiscretizer()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("result1", "result2"))
      .setNumBucketsArray(Array(5, 10))
    assert(discretizer.isQuantileDiscretizeMultipleColumns())
    testDefaultReadWrite(discretizer)
  }

  test("Both inputCol and inputCols are set") {
    val discretizer = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("result")
      .setNumBuckets(3)
      .setInputCols(Array("input1", "input2"))

    // When both are set, we ignore `inputCols` and just map the column specified by `inputCol`.
    assert(discretizer.isQuantileDiscretizeMultipleColumns() == false)
  }
}
