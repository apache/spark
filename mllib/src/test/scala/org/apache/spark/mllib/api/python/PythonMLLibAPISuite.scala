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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, SparseMatrix, Vectors}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.regression.LabeledPoint

class PythonMLLibAPISuite extends SparkFunSuite {

  SerDe.initialize()

  test("pickle vector") {
    val vectors = Seq(
      Vectors.dense(Array.empty[Double]),
      Vectors.dense(0.0),
      Vectors.dense(0.0, -2.0),
      Vectors.sparse(0, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(1, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(2, Array(1), Array(-2.0)))
    vectors.foreach { v =>
      val u = SerDe.loads(SerDe.dumps(v))
      assert(u.getClass === v.getClass)
      assert(u === v)
    }
  }

  test("pickle labeled point") {
    val points = Seq(
      LabeledPoint(0.0, Vectors.dense(Array.empty[Double])),
      LabeledPoint(1.0, Vectors.dense(0.0)),
      LabeledPoint(-0.5, Vectors.dense(0.0, -2.0)),
      LabeledPoint(0.0, Vectors.sparse(0, Array.empty[Int], Array.empty[Double])),
      LabeledPoint(1.0, Vectors.sparse(1, Array.empty[Int], Array.empty[Double])),
      LabeledPoint(-0.5, Vectors.sparse(2, Array(1), Array(-2.0))))
    points.foreach { p =>
      val q = SerDe.loads(SerDe.dumps(p)).asInstanceOf[LabeledPoint]
      assert(q.label === p.label)
      assert(q.features.getClass === p.features.getClass)
      assert(q.features === p.features)
    }
  }

  test("pickle double") {
    for (x <- List(123.0, -10.0, 0.0, Double.MaxValue, Double.MinValue, Double.NaN)) {
      val deser = SerDe.loads(SerDe.dumps(x.asInstanceOf[AnyRef])).asInstanceOf[Double]
      // We use `equals` here for comparison because we cannot use `==` for NaN
      assert(x.equals(deser))
    }
  }

  test("pickle matrix") {
    val values = Array[Double](0, 1.2, 3, 4.56, 7, 8)
    val matrix = Matrices.dense(2, 3, values)
    val nm = SerDe.loads(SerDe.dumps(matrix)).asInstanceOf[DenseMatrix]
    assert(matrix === nm)

    // Test conversion for empty matrix
    val empty = Array.empty[Double]
    val emptyMatrix = Matrices.dense(0, 0, empty)
    val ne = SerDe.loads(SerDe.dumps(emptyMatrix)).asInstanceOf[DenseMatrix]
    assert(emptyMatrix == ne)

    val sm = new SparseMatrix(3, 2, Array(0, 1, 3), Array(1, 0, 2), Array(0.9, 1.2, 3.4))
    val nsm = SerDe.loads(SerDe.dumps(sm)).asInstanceOf[SparseMatrix]
    assert(sm.toArray === nsm.toArray)

    val smt = new SparseMatrix(
      3, 3, Array(0, 2, 3, 5), Array(0, 2, 1, 0, 2), Array(0.9, 1.2, 3.4, 5.7, 8.9),
      isTransposed = true)
    val nsmt = SerDe.loads(SerDe.dumps(smt)).asInstanceOf[SparseMatrix]
    assert(smt.toArray === nsmt.toArray)
  }

  test("pickle rating") {
    val rat = new Rating(1, 2, 3.0)
    val rat2 = SerDe.loads(SerDe.dumps(rat)).asInstanceOf[Rating]
    assert(rat == rat2)

    // Test name of class only occur once
    val rats = (1 to 10).map(x => new Rating(x, x + 1, x + 3.0)).toArray
    val bytes = SerDe.dumps(rats)
    assert(bytes.mkString(",").split("Rating").length == 1)
    assert(bytes.length / 10 < 25) //  25 bytes per rating

  }
}
