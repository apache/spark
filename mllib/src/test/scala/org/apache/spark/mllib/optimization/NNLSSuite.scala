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

import org.jblas.{DoubleMatrix, SimpleBlas, NativeBlas}

class NNLSSuite extends FunSuite {
  test("NNLSbyPCG: exact solution case") {
    val n = 20
    val A = new DoubleMatrix(n, n)
    val b = new DoubleMatrix(n, 1)
    val rand = new Random(12345)
    for (i <- 0 until n; j <- 0 until n) {
      val aij = rand.nextDouble()
      A.put(i, j, aij)
      b.put(i, b.get(i, 0) + aij)
    }

    val ata = new DoubleMatrix(n, n)
    val atb = new DoubleMatrix(n, 1)

    NativeBlas.dgemm('T', 'N', n, n, n, 1.0, A.data, 0, n, A.data, 0, n, 0.0, ata.data, 0, n)
    NativeBlas.dgemv('T', n, n, 1.0, A.data, 0, n, b.data, 0, 1, 0.0, atb.data, 0, 1)

    val x = NNLSbyPCG.solve(ata, atb, true)
    assert(x.length == n)
    var error = 0.0
    for (i <- 0 until n) {
      error = error + (x(i) - 1) * (x(i) - 1)
      assert(Math.abs(x(i) - 1) < 1e-3)
    }
    assert(error < 1e-2)
  }

  test("NNLSbyPCG: nonnegativity constraint active") {
    val n = 5
    val M = Array(
      Array( 4.377, -3.531, -1.306, -0.139,  3.418, -1.632),
      Array(-3.531,  4.344,  0.934,  0.305, -2.140,  2.115),
      Array(-1.306,  0.934,  2.644, -0.203, -0.170,  1.094),
      Array(-0.139,  0.305, -0.203,  5.883,  1.428, -1.025),
      Array( 3.418, -2.140, -0.170,  1.428,  4.684, -0.636))
    val ata = new DoubleMatrix(5, 5)
    val atb = new DoubleMatrix(5, 1)
    for (i <- 0 until 5; j <- 0 until 5) ata.put(i, j, M(i)(j))
    for (i <- 0 until 5) atb.put(i, M(i)(5))

    val goodx = Array(0.13025, 0.54506, 0.2874, 0.0, 0.028628)

    val x = NNLSbyPCG.solve(ata, atb, true)
    for (i <- 0 until 5) {
      assert(Math.abs(x(i) - goodx(i)) < 1e-3)
    }
  }
}
