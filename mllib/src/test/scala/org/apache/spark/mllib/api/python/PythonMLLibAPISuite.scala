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

package org.apache.spark.mllib.api.python

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

class PythonMLLibAPISuite extends FunSuite {

  test("vector serialization") {
    val vectors = Seq(
      Vectors.dense(Array.empty[Double]),
      Vectors.dense(0.0),
      Vectors.dense(0.0, -2.0),
      Vectors.sparse(0, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(1, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(2, Array(1), Array(-2.0)))
    vectors.foreach { v =>
      val bytes = SerDe.serializeDoubleVector(v)
      val u = SerDe.deserializeDoubleVector(bytes)
      assert(u.getClass === v.getClass)
      assert(u === v)
    }
  }

  test("labeled point serialization") {
    val points = Seq(
      LabeledPoint(0.0, Vectors.dense(Array.empty[Double])),
      LabeledPoint(1.0, Vectors.dense(0.0)),
      LabeledPoint(-0.5, Vectors.dense(0.0, -2.0)),
      LabeledPoint(0.0, Vectors.sparse(0, Array.empty[Int], Array.empty[Double])),
      LabeledPoint(1.0, Vectors.sparse(1, Array.empty[Int], Array.empty[Double])),
      LabeledPoint(-0.5, Vectors.sparse(2, Array(1), Array(-2.0))))
    points.foreach { p =>
      val bytes = SerDe.serializeLabeledPoint(p)
      val q = SerDe.deserializeLabeledPoint(bytes)
      assert(q.label === p.label)
      assert(q.features.getClass === p.features.getClass)
      assert(q.features === p.features)
    }
  }

  test("double serialization") {
    for (x <- List(123.0, -10.0, 0.0, Double.MaxValue, Double.MinValue, Double.NaN)) {
      val bytes = SerDe.serializeDouble(x)
      val deser = SerDe.deserializeDouble(bytes)
      // We use `equals` here for comparison because we cannot use `==` for NaN
      assert(x.equals(deser))
    }
  }

  test("matrix to 2D array") {
    val values = Array[Double](0, 1.2, 3, 4.56, 7, 8)
    val matrix = Matrices.dense(2, 3, values)
    val arr = SerDe.to2dArray(matrix)
    val expected = Array(Array[Double](0, 3, 7), Array[Double](1.2, 4.56, 8))
    assert(arr === expected)

    // Test conversion for empty matrix
    val empty = Array[Double]()
    val emptyMatrix = Matrices.dense(0, 0, empty)
    val empty2D = SerDe.to2dArray(emptyMatrix)
    assert(empty2D === Array[Array[Double]]())
  }
}
