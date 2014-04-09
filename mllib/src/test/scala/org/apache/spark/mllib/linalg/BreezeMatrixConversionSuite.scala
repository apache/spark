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

import org.scalatest.FunSuite

import breeze.linalg.{DenseMatrix => BDM}

class BreezeMatrixConversionSuite extends FunSuite {
  test("dense matrix to breeze") {
    val mat = Matrices.dense(3, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val breeze = mat.toBreeze.asInstanceOf[BDM[Double]]
    assert(breeze.rows === mat.numRows)
    assert(breeze.cols === mat.numCols)
    assert(breeze.data.eq(mat.asInstanceOf[DenseMatrix].values), "should not copy data")
  }

  test("dense breeze matrix to matrix") {
    val breeze = new BDM[Double](3, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val mat = Matrices.fromBreeze(breeze).asInstanceOf[DenseMatrix]
    assert(mat.numRows === breeze.rows)
    assert(mat.numCols === breeze.cols)
    assert(mat.values.eq(breeze.data), "should not copy data")
  }
}
