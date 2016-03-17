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

package org.apache.spark.mllib.linalg

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD

class NonNegativeMatrixFactorizationSuite extends SparkFunSuite with MLlibTestSparkContext {

  val m = 4
  val n = 3
  val data = Seq(
    (0L, Vectors.dense(0.0, 1.0, 2.0)),
    (1L, Vectors.dense(3.0, 4.0, 5.0)),
    (3L, Vectors.dense(9.0, 0.0, 1.0))
  ).map(x => IndexedRow(x._1, x._2))

  var indexedRows: RDD[IndexedRow] = _

  override def beforeAll() {
    super.beforeAll()
    indexedRows = sc.parallelize(data, 2)
  }

  test("validate matrix sizes of nmf") {
    val A = new IndexedRowMatrix(indexedRows).toCoordinateMatrix()
    val k = 2
    val r = NMF.solve(A, k, 2)
    val rW = r.W.toBlockMatrix().toLocalMatrix().asInstanceOf[DenseMatrix]
    val rH = r.H.toBlockMatrix().toLocalMatrix().asInstanceOf[DenseMatrix]

    assert(rW.numRows === m)
    assert(rW.numCols === k)
    assert(rH.numRows === n)
    assert(rH.numCols === k)
  }

  test("validate result of nmf") {
    val A = new IndexedRowMatrix(indexedRows).toCoordinateMatrix()
    val k = 2
    val r = NMF.solve(A, k, 10)
    val rW = r.W.toBlockMatrix().toLocalMatrix().asInstanceOf[DenseMatrix]
    val rH = r.H.toBlockMatrix().toLocalMatrix().asInstanceOf[DenseMatrix]

    val R = rW.multiply(rH.transpose)
    val AD = A.toBlockMatrix().toLocalMatrix()

    assert(R.numRows === AD.numRows)
    assert(R.numCols === AD.numCols)

    var loss = 0.0
    for(i <- 0 until AD.numRows; j <- 0 until AD.numCols) {
      val diff = AD(i, j) - R(i, j)
      loss += diff * diff
    }

    assert(loss < 2)
  }

}
