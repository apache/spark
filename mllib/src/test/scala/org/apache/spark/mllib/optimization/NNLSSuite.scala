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

package org.apache.spark.mllib.optimization

import scala.util.Random

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import org.apache.spark.mllib.util.LocalSparkContext

import org.jblas.DoubleMatrix
import org.jblas.SimpleBlas

class NNLSSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("NNLSbyPCG: exact solution case") {
    val A = new DoubleMatrix(20, 20)
    val b = new DoubleMatrix(20, 1)
    val rand = new Random(12345)
    for (i <- 0 until 20; j <- 0 until 20) {
      val aij = rand.nextDouble()
      A.put(i, j, aij)
      b.put(i, b.get(i, 0) + aij)
    }

    val ata = new DoubleMatrix(20, 20)
    val atb = new DoubleMatrix(20, 1)
    for (i <- 0 until 20; j <- 0 until 20; k <- 0 until 20) {
      ata.put(i, j, ata.get(i, j) + A.get(k, i) * A.get(k, j))
    }
    for (i <- 0 until 20; j <- 0 until 20) {
      atb.put(i, atb.get(i, 0) + A.get(j, i) * b.get(j))
    }

    val x = NNLSbyPCG.solve(ata, atb, true)
    assert(x.length == 20)
    var error = 0.0
    for (i <- 0 until 20) {
      error = error + (x(i) - 1) * (x(i) - 1)
      assert(Math.abs(x(i) - 1) < 1e-3)
    }
    assert(error < 1e-2)
  }
}
