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
  @transient var data2: Array[Double] = _
  @transient var expectedBinarizer1: Array[Double] = _
  @transient var expectedBinarizer2: Array[Double] = _
  @transient var expectedBinarizer3: Array[Double] = _
  @transient var expectedBinarizer4: Array[Double] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = Array(0.1, -0.5, 0.2, -0.3, 0.8, 0.7, -0.1, -0.4)
    data2 = Array(-0.1, 0.5, -0.2, 0.3, -0.8, -0.7, 0.1, 0.4)
    expectedBinarizer1 = Array(1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0)
    expectedBinarizer2 = Array(0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0)
    expectedBinarizer3 = Array(0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0)
    expectedBinarizer4 = Array(0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0)
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

  test("multiple columns: Binarize continuous features with default parameter") {
    val dataFrame: DataFrame = (0 until data.length).map { idx =>
      (data(idx), data2(idx), expectedBinarizer1(idx), expectedBinarizer2(idx))
    }.toDF("feature1", "feature2", "expected1", "expected2")

    val binarizer: Binarizer = new Binarizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setThresholds(Array(0.0, 0.0))

    binarizer.transform(dataFrame).select("result1", "expected1", "result2", "expected2").collect().
      foreach {
        case Row(r1: Double, e1: Double, r2: Double, e2: Double ) => assert(r1 == e1 && r2 == e2,
          "The feature value is not correct after binarization.")
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

  test("multiple columns:Binarize continuous features with setter") {
    val dataFrame: DataFrame = (0 until data.length).map { idx =>
      (data(idx), data2(idx), expectedBinarizer3(idx), expectedBinarizer4(idx))
    }.toDF("feature1", "feature2", "expected1", "expected2")

    val binarizer: Binarizer = new Binarizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setThresholds(Array(0.2, 0.2))

    binarizer.transform(dataFrame).select("result1", "expected1", "result2", "expected2").collect().
      foreach {
        case Row(r1: Double, e1: Double, r2: Double, e2: Double ) => assert(r1 == e1 && r2 == e2,
          "The feature value is not correct after binarization.")
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

  test("multiple column: Binarize vector of continuous features with default parameter") {
    val dataFrame: DataFrame = Seq(
      (Vectors.dense(data), Vectors.dense(data2),
        Vectors.dense(expectedBinarizer1), Vectors.dense(expectedBinarizer2))
    ).toDF("feature1", "feature2", "expected1", "expected2")

    val binarizer: Binarizer = new Binarizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setThresholds(Array(0.0, 0.0))

    binarizer.transform(dataFrame).select("result1", "expected1", "result2", "expected2").collect().
      foreach {
      case Row(r1: Vector, e1: Vector, r2: Vector, e2: Vector ) => assert(r1 == e1 && r2 == e2,
          "The feature value is not correct after binarization.")
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

  test("multiple column: Binarize vector of continuous features with setter") {
    val dataFrame: DataFrame = Seq(
      (Vectors.dense(data), Vectors.dense(data2),
        Vectors.dense(expectedBinarizer3), Vectors.dense(expectedBinarizer4))
    ).toDF("feature1", "feature2", "expected1", "expected2")

    val binarizer: Binarizer = new Binarizer()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCols(Array("result1", "result2"))
      .setThresholds(Array(0.2, 0.2))

    binarizer.transform(dataFrame).select("result1", "expected1", "result2", "expected2").collect().
      foreach {
        case Row(r1: Vector, e1: Vector, r2: Vector, e2: Vector ) => assert(r1 == e1 && r2 == e2,
          "The feature value is not correct after binarization.")
    }
  }

  test("read/write") {
    val t = new Binarizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setThreshold(0.1)
    testDefaultReadWrite(t)
  }
}


