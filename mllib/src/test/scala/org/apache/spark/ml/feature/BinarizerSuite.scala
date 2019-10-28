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

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.{DataFrame, Row}

class BinarizerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Array[Double] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = Array(0.1, -0.5, 0.2, -0.3, 0.8, 0.7, -0.1, -0.4)
  }

  test("params") {
    ParamsSuite.checkParams(new Binarizer)
  }

  test("Binarize continuous features with default parameter") {
    val defaultBinarized: Array[Double] = data.map(x => if (x > 0.0) 1.0 else 0.0)
    val dataFrame: DataFrame = data.zip(defaultBinarized).toSeq.toDF("feature", "expected")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")

    testTransformer[(Double, Double)](dataFrame, binarizer, "binarized_feature", "expected") {
      case Row(x: Double, y: Double) =>
        assert(x === y, "The feature value is not correct after binarization.")
    }
  }

  test("Binarize continuous features with setter") {
    val threshold: Double = 0.2
    val thresholdBinarized: Array[Double] = data.map(x => if (x > threshold) 1.0 else 0.0)
    val dataFrame: DataFrame = data.zip(thresholdBinarized).toSeq.toDF("feature", "expected")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(threshold)

    binarizer.transform(dataFrame).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Double, y: Double) =>
        assert(x === y, "The feature value is not correct after binarization.")
    }
  }

  test("Binarize vector of continuous features with default parameter") {
    val defaultBinarized: Array[Double] = data.map(x => if (x > 0.0) 1.0 else 0.0)
    val dataFrame: DataFrame = Seq(
      (Vectors.dense(data), Vectors.dense(defaultBinarized))
    ).toDF("feature", "expected")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")

    binarizer.transform(dataFrame).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x == y, "The feature value is not correct after binarization.")
    }
  }

  test("Binarize vector of continuous features with setter") {
    val threshold: Double = 0.2
    val defaultBinarized: Array[Double] = data.map(x => if (x > threshold) 1.0 else 0.0)
    val dataFrame: DataFrame = Seq(
      (Vectors.dense(data), Vectors.dense(defaultBinarized))
    ).toDF("feature", "expected")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(threshold)

    binarizer.transform(dataFrame).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x == y, "The feature value is not correct after binarization.")
    }
  }

  test("Binarizer should support sparse vector with negative threshold") {
    val data = Seq(
      (Vectors.sparse(3, Array(1), Array(0.5)), Vectors.dense(Array(1.0, 1.0, 1.0))),
      (Vectors.dense(Array(0.0, 0.5, 0.0)), Vectors.dense(Array(1.0, 1.0, 1.0))))
    val df = data.toDF("feature", "expected")
    val binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(-0.5)
    binarizer.transform(df).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x == y, "The feature value is not correct after binarization.")
    }
  }

  test("read/write") {
    val t = new Binarizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setThreshold(0.1)
    testDefaultReadWrite(t)

    val t2 = new Binarizer()
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("result1", "result2", "result3"))
      .setThresholds(Array(30.0, 30.0, 30.0))
    testDefaultReadWrite(t2)
  }

  test("Multiple Columns: Test thresholds") {
    val thresholds = Array(10.0, -0.5, 0.0)

    val data1 = Seq(5.0, 11.0)
    val expected1 = Seq(0.0, 1.0)
    val data2 = Seq(Vectors.sparse(3, Array(1), Array(0.5)),
      Vectors.dense(Array(0.0, 0.5, 0.0)))
    val expected2 = Seq(Vectors.dense(Array(1.0, 1.0, 1.0)),
      Vectors.dense(Array(1.0, 1.0, 1.0)))
    val data3 = Seq(0.0, 1.0)
    val expected3 = Seq(0.0, 1.0)

    val df = Seq(0, 1).map { idx =>
      (data1(idx), data2(idx), data3(idx), expected1(idx), expected2(idx), expected3(idx))
    }.toDF("input1", "input2", "input3", "expected1", "expected2", "expected3")

    val binarizer = new Binarizer()
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("result1", "result2", "result3"))
      .setThresholds(thresholds)

    binarizer.transform(df)
      .select("result1", "expected1", "result2", "expected2", "result3", "expected3")
      .collect().foreach {
      case Row(r1: Double, e1: Double, r2: Vector, e2: Vector, r3: Double, e3: Double) =>
        assert(r1 === e1,
          s"The result value is not correct after bucketing. Expected $e1 but found $r1")
        assert(r2 === e2,
          s"The result value is not correct after bucketing. Expected $e2 but found $r2")
        assert(r3 === e3,
          s"The result value is not correct after bucketing. Expected $e3 but found $r3")
    }
  }

  test("Multiple Columns: Comparing setting threshold with setting thresholds " +
    "explicitly with identical values") {
    val data1 = Array.range(1, 21, 1).map(_.toDouble)
    val data2 = Array.range(1, 40, 2).map(_.toDouble)
    val data3 = Array.range(1, 60, 3).map(_.toDouble)
    val df = (0 until 20).map { idx =>
      (data1(idx), data2(idx), data3(idx))
    }.toDF("input1", "input2", "input3")

    val binarizerSingleThreshold = new Binarizer()
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("result1", "result2", "result3"))
      .setThreshold(30.0)

    val df2 = binarizerSingleThreshold.transform(df)

    val binarizerMultiThreshold = new Binarizer()
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("expected1", "expected2", "expected3"))
      .setThresholds(Array(30.0, 30.0, 30.0))

    binarizerMultiThreshold.transform(df2)
      .select("result1", "expected1", "result2", "expected2", "result3", "expected3")
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

  test("Multiple Columns: Mismatched sizes of inputCols/outputCols") {
    val binarizer = new Binarizer()
      .setInputCols(Array("input"))
      .setOutputCols(Array("result1", "result2"))
      .setThreshold(1.0)
    val df = sc.parallelize(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
      .map(Tuple1.apply).toDF("input")
    intercept[IllegalArgumentException] {
      binarizer.transform(df).count()
    }
  }

  test("Multiple Columns: Mismatched sizes of inputCols/thresholds") {
    val binarizer = new Binarizer()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("result1", "result2"))
      .setThresholds(Array(1.0, 2.0, 3.0))
    val data1 = Array(1.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 2.0, 2.0, 2.0)
    val data2 = Array(1.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 3.0, 2.0, 3.0)
    val df = data1.zip(data2).toSeq.toDF("input1", "input2")
    intercept[IllegalArgumentException] {
      binarizer.transform(df).count()
    }
  }

  test("Multiple Columns: Mismatched sizes of inputCol/thresholds") {
    val binarizer = new Binarizer()
      .setInputCol("input1")
      .setOutputCol("result1")
      .setThresholds(Array(1.0, 2.0))
    val data1 = Array(1.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 2.0, 2.0, 2.0)
    val data2 = Array(1.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 3.0, 2.0, 3.0)
    val df = data1.zip(data2).toSeq.toDF("input1", "input2")
    intercept[IllegalArgumentException] {
      binarizer.transform(df).count()
    }
  }

  test("Multiple Columns: Set both of threshold/thresholds") {
    val binarizer = new Binarizer()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("result1", "result2"))
      .setThresholds(Array(1.0, 2.0))
      .setThreshold(1.0)
    val data1 = Array(1.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 2.0, 2.0, 2.0)
    val data2 = Array(1.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 3.0, 2.0, 3.0)
    val df = data1.zip(data2).toSeq.toDF("input1", "input2")
    intercept[IllegalArgumentException] {
      binarizer.transform(df).count()
    }
  }
}
