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
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.MLUtils._

/**
 * Test suite for the summary statistics of RDD[Vector]. Both the accuracy and the time consuming
 * between dense and sparse vector are tested.
 */
class VectorRDDFunctionsSuite extends FunSuite with LocalSparkContext {
  import VectorRDDFunctionsSuite._

  val localData = Array(
    Vectors.dense(1.0, 2.0, 3.0),
    Vectors.dense(4.0, 5.0, 6.0),
    Vectors.dense(7.0, 8.0, 9.0)
  )

  val sparseData = ArrayBuffer(Vectors.sparse(20, Seq((0, 1.0), (9, 2.0), (10, 7.0))))
  for (i <- 0 until 100) sparseData += Vectors.sparse(20, Seq((9, 0.0)))
  sparseData += Vectors.sparse(20, Seq((0, 5.0), (9, 13.0), (16, 2.0)))
  sparseData += Vectors.sparse(20, Seq((3, 5.0), (9, 13.0), (18, 2.0)))

  test("full-statistics") {
    val data = sc.parallelize(localData, 2)
    val (VectorRDDStatisticalAggregator(mean, variance, cnt, nnz, max, min), denseTime) =
      time(data.summarizeStatistics())

    assert(equivVector(Vectors.fromBreeze(mean), Vectors.dense(4.0, 5.0, 6.0)),
      "Column mean do not match.")
    assert(equivVector(Vectors.fromBreeze(variance), Vectors.dense(6.0, 6.0, 6.0)),
      "Column variance do not match.")
    assert(cnt === 3.0, "Column cnt do not match.")
    assert(equivVector(Vectors.fromBreeze(nnz), Vectors.dense(3.0, 3.0, 3.0)),
      "Column nnz do not match.")
    assert(equivVector(Vectors.fromBreeze(max), Vectors.dense(7.0, 8.0, 9.0)),
      "Column max do not match.")
    assert(equivVector(Vectors.fromBreeze(min), Vectors.dense(1.0, 2.0, 3.0)),
      "Column min do not match.")

    val dataForSparse = sc.parallelize(sparseData.toSeq, 2)
    val (_, sparseTime) = time(dataForSparse.summarizeStatistics())

    println(s"dense time is $denseTime, sparse time is $sparseTime.")
  }
}

object VectorRDDFunctionsSuite {
  def time[R](block: => R): (R, Double) = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    (result, (t1 - t0).toDouble / 1.0e9)
  }

  def equivVector(lhs: Vector, rhs: Vector): Boolean = {
    (lhs.toBreeze - rhs.toBreeze).norm(2) < 1e-9
  }

  def relativeTime(lhs: Double, rhs: Double): Boolean = {
    val denominator = math.max(lhs, rhs)
    math.abs(lhs - rhs) / denominator < 0.3
  }
}