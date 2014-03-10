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

package org.apache.spark.mllib.util

import org.scalatest.FunSuite

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, norm => breezeNorm,
  squaredDistance => breezeSquaredDistance}

import org.apache.spark.mllib.util.MLUtils._

class MLUtilsSuite extends FunSuite {

  test("fast squared distance") {
    val a = (30 to 0 by -1).map(math.pow(2.0, _)).toArray
    val n = a.length
    val v1 = new BDV[Double](a)
    val norm1: Double = breezeNorm(v1)
    val squaredNorm1 = norm1 * norm1
    val precision = 1e-6
    for (m <- 0 until n) {
      val indices = (0 to m).toArray
      val values = indices.map(i => a(i))
      val v2 = new BSV[Double](indices, values, n)
      val norm2: Double = breezeNorm(v2)
      val squaredNorm2 = norm2 * norm2
      val squaredDist: Double = breezeSquaredDistance(v1, v2)
      val fastSquaredDist = fastSquaredDistance(v1, squaredNorm1, v2, squaredNorm2, precision)
      assert((fastSquaredDist - squaredDist) <= precision * squaredDist, s"failed with m = $m")
    }
  }
}
