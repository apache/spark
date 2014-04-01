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

import org.scalatest.FunSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.MLUtils._

class VectorRDDFunctionsSuite extends FunSuite with LocalSparkContext {
  import VectorRDDFunctionsSuite._

  val localData = Array(
    Vectors.dense(1.0, 2.0, 3.0),
    Vectors.dense(4.0, 5.0, 6.0),
    Vectors.dense(7.0, 8.0, 9.0)
  )

  test("full-statistics") {
    val data = sc.parallelize(localData, 2)
    val VectorRDDStatisticalSummary(mean, variance, cnt, nnz, max, min) = data.summarizeStatistics(3)
    assert(equivVector(mean, Vectors.dense(4.0, 5.0, 6.0)), "Column mean do not match.")
    assert(equivVector(variance, Vectors.dense(6.0, 6.0, 6.0)), "Column variance do not match.")
    assert(cnt === 3, "Column cnt do not match.")
    assert(equivVector(nnz, Vectors.dense(3.0, 3.0, 3.0)), "Column nnz do not match.")
    assert(equivVector(max, Vectors.dense(7.0, 8.0, 9.0)), "Column max do not match.")
    assert(equivVector(min, Vectors.dense(1.0, 2.0, 3.0)), "Column min do not match.")
  }
}

object VectorRDDFunctionsSuite {
  def equivVector(lhs: Vector, rhs: Vector): Boolean = {
    (lhs.toBreeze - rhs.toBreeze).norm(2) < 1e-9
  }
}

