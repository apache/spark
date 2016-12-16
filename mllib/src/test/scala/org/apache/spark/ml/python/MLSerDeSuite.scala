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

package org.apache.spark.ml.python

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, SparseMatrix, Vectors}

class MLSerDeSuite extends SparkFunSuite {

  MLSerDe.initialize()

  test("pickle vector") {
    val vectors = Seq(
      Vectors.dense(Array.empty[Double]),
      Vectors.dense(0.0),
      Vectors.dense(0.0, -2.0),
      Vectors.sparse(0, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(1, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(2, Array(1), Array(-2.0)))
    vectors.foreach { v =>
      val u = MLSerDe.loads(MLSerDe.dumps(v))
      assert(u.getClass === v.getClass)
      assert(u === v)
    }
  }

  test("pickle double") {
    for (x <- List(123.0, -10.0, 0.0, Double.MaxValue, Double.MinValue, Double.NaN)) {
      val deser = MLSerDe.loads(MLSerDe.dumps(x.asInstanceOf[AnyRef])).asInstanceOf[Double]
      // We use `equals` here for comparison because we cannot use `==` for NaN
      assert(x.equals(deser))
    }
  }

  test("pickle matrix") {
    val values = Array[Double](0, 1.2, 3, 4.56, 7, 8)
    val matrix = Matrices.dense(2, 3, values)
    val nm = MLSerDe.loads(MLSerDe.dumps(matrix)).asInstanceOf[DenseMatrix]
    assert(matrix === nm)

    // Test conversion for empty matrix
    val empty = Array.empty[Double]
    val emptyMatrix = Matrices.dense(0, 0, empty)
    val ne = MLSerDe.loads(MLSerDe.dumps(emptyMatrix)).asInstanceOf[DenseMatrix]
    assert(emptyMatrix == ne)

    val sm = new SparseMatrix(3, 2, Array(0, 1, 3), Array(1, 0, 2), Array(0.9, 1.2, 3.4))
    val nsm = MLSerDe.loads(MLSerDe.dumps(sm)).asInstanceOf[SparseMatrix]
    assert(sm.toArray === nsm.toArray)

    val smt = new SparseMatrix(
      3, 3, Array(0, 2, 3, 5), Array(0, 2, 1, 0, 2), Array(0.9, 1.2, 3.4, 5.7, 8.9),
      isTransposed = true)
    val nsmt = MLSerDe.loads(MLSerDe.dumps(smt)).asInstanceOf[SparseMatrix]
    assert(smt.toArray === nsmt.toArray)
  }
}
