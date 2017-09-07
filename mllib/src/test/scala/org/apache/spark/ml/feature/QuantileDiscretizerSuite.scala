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
}
