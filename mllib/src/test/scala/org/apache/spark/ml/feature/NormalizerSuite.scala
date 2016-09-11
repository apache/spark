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
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}


class NormalizerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var data: Array[Vector] = _
  @transient var dataFrame: DataFrame = _
  @transient var normalizer: Normalizer = _
  @transient var l1Normalized: Array[Vector] = _
  @transient var l2Normalized: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = Array(
      Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(0.6, -1.1, -3.0),
      Vectors.sparse(3, Seq((1, 0.91), (2, 3.2))),
      Vectors.sparse(3, Seq((0, 5.7), (1, 0.72), (2, 2.7))),
      Vectors.sparse(3, Seq())
    )
    l1Normalized = Array(
      Vectors.sparse(3, Seq((0, -0.465116279), (1, 0.53488372))),
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(0.12765957, -0.23404255, -0.63829787),
      Vectors.sparse(3, Seq((1, 0.22141119), (2, 0.7785888))),
      Vectors.dense(0.625, 0.07894737, 0.29605263),
      Vectors.sparse(3, Seq())
    )
    l2Normalized = Array(
      Vectors.sparse(3, Seq((0, -0.65617871), (1, 0.75460552))),
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(0.184549876, -0.3383414, -0.922749378),
      Vectors.sparse(3, Seq((1, 0.27352993), (2, 0.96186349))),
      Vectors.dense(0.897906166, 0.113419726, 0.42532397),
      Vectors.sparse(3, Seq())
    )

    dataFrame = spark.createDataFrame(sc.parallelize(data, 2).map(NormalizerSuite.FeatureData))
    normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normalized_features")
  }

  def collectResult(result: DataFrame): Array[Vector] = {
    result.select("normalized_features").collect().map {
      case Row(features: Vector) => features
    }
  }

  def assertTypeOfVector(lhs: Array[Vector], rhs: Array[Vector]): Unit = {
    assert((lhs, rhs).zipped.forall {
      case (v1: DenseVector, v2: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after normalization.")
  }

  def assertValues(lhs: Array[Vector], rhs: Array[Vector]): Unit = {
    assert((lhs, rhs).zipped.forall { (vector1, vector2) =>
      vector1 ~== vector2 absTol 1E-5
    }, "The vector value is not correct after normalization.")
  }

  test("Normalization with default parameter") {
    val result = collectResult(normalizer.transform(dataFrame))

    assertTypeOfVector(data, result)

    assertValues(result, l2Normalized)
  }

  test("Normalization with setter") {
    normalizer.setP(1)

    val result = collectResult(normalizer.transform(dataFrame))

    assertTypeOfVector(data, result)

    assertValues(result, l1Normalized)
  }

  test("read/write") {
    val t = new Normalizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setP(3.0)
    testDefaultReadWrite(t)
  }
}

private object NormalizerSuite {
  case class FeatureData(features: Vector)
}
