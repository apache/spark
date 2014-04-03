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

package org.apache.spark.mllib.rdd

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.rdd.VectorRDDFunctionsSuite._
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.MLUtils._

/**
 * Test suite for the summary statistics of RDD[Vector]. Both the accuracy and the time consuming
 * between dense and sparse vector are tested.
 */
class VectorRDDFunctionsSuite extends FunSuite with LocalSparkContext {

  val localData = Array(
    Vectors.dense(1.0, 2.0, 3.0),
    Vectors.dense(4.0, 5.0, 6.0),
    Vectors.dense(7.0, 8.0, 9.0)
  )

  val sparseData = ArrayBuffer(Vectors.sparse(3, Seq((0, 1.0))))
  for (i <- 0 until 97) sparseData += Vectors.sparse(3, Seq((2, 0.0)))
  sparseData += Vectors.sparse(3, Seq((0, 5.0)))
  sparseData += Vectors.sparse(3, Seq((1, 5.0)))

  test("dense statistical summary") {
    val data = sc.parallelize(localData, 2)
    val summary = data.computeSummaryStatistics()

    assert(equivVector(summary.mean, Vectors.dense(4.0, 5.0, 6.0)),
      "Dense column mean do not match.")

    assert(equivVector(summary.variance, Vectors.dense(6.0, 6.0, 6.0)),
      "Dense column variance do not match.")

    assert(summary.count === 3, "Dense column cnt do not match.")

    assert(equivVector(summary.numNonZeros, Vectors.dense(3.0, 3.0, 3.0)),
      "Dense column nnz do not match.")

    assert(equivVector(summary.max, Vectors.dense(7.0, 8.0, 9.0)),
      "Dense column max do not match.")

    assert(equivVector(summary.min, Vectors.dense(1.0, 2.0, 3.0)),
      "Dense column min do not match.")
  }

  test("sparse statistical summary") {
    val dataForSparse = sc.parallelize(sparseData.toSeq, 2)
    val summary = dataForSparse.computeSummaryStatistics()

    assert(equivVector(summary.mean, Vectors.dense(0.06, 0.05, 0.0)),
      "Sparse column mean do not match.")

    assert(equivVector(summary.variance, Vectors.dense(0.2564, 0.2475, 0.0)),
      "Sparse column variance do not match.")

    assert(summary.count === 100, "Sparse column cnt do not match.")

    assert(equivVector(summary.numNonZeros, Vectors.dense(2.0, 1.0, 0.0)),
      "Sparse column nnz do not match.")

    assert(equivVector(summary.max, Vectors.dense(5.0, 5.0, 0.0)),
      "Sparse column max do not match.")

    assert(equivVector(summary.min, Vectors.dense(0.0, 0.0, 0.0)),
      "Sparse column min do not match.")
  }
}

object VectorRDDFunctionsSuite {
  def equivVector(lhs: Vector, rhs: Vector): Boolean = {
    (lhs.toBreeze - rhs.toBreeze).norm(2) < 1e-9
  }
}
