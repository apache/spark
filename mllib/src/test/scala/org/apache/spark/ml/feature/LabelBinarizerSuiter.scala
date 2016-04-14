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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql._

class LabelBinarizerSuiter extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("Label binarize one-class case") {
    val data = Array("pos,pos,pos,pos")
    val expected: Array[Vector] = Array(
      Vectors.dense(1, 1, 1, 1))

    val dataFrame: DataFrame = sqlContext.createDataFrame(
      data.zip(expected)).toDF("feature", "expected")

    val lBinarizer: LabelBinarizer = new LabelBinarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")

    lBinarizer.transform(dataFrame).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x === y, "The feature value is not correct after binarization.")
    }
  }

  test("Label binarize two-class case") {
    val data = Array("pos,neg,neg,pos")
    val expected: Array[Vector] = Array(
      Vectors.dense(0, 1, 1, 0, 1, 0, 0, 1))
    val dataFrame: DataFrame = sqlContext.createDataFrame(
      data.zip(expected)).toDF("feature", "expected")

    val lBinarizer: LabelBinarizer = new LabelBinarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")

    lBinarizer.transform(dataFrame).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x === y, "The feature value is not correct after binarization.")
    }
  }

  test("Label binarize multi-class case") {
    val data = Array("yellow,green,red,green,0")
    val expected: Array[Vector] = Array(
      Vectors.dense(0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0))
    val dataFrame: DataFrame = sqlContext.createDataFrame(
      data.zip(expected)).toDF("feature", "expected")

    val lBinarizer: LabelBinarizer = new LabelBinarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")

    lBinarizer.transform(dataFrame).select("binarized_feature", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x === y, "The feature value is not correct after binarization.")
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
