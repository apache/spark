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

import org.jblas.{DoubleMatrix, SimpleBlas}

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.TestingUtils._

class NNLSSuite extends SparkFunSuite {
  /** Generate an NNLS problem whose optimal solution is the all-ones vector. */
  def genOnesData(n: Int, rand: Random): (DoubleMatrix, DoubleMatrix) = {
    val A = new DoubleMatrix(n, n, Array.fill(n*n)(rand.nextDouble()): _*)
    val b = A.mmul(DoubleMatrix.ones(n, 1))

    val ata = A.transpose.mmul(A)
    val atb = A.transpose.mmul(b)

    (ata, atb)
  }

  /** Compute the objective value */
  def computeObjectiveValue(ata: DoubleMatrix, atb: DoubleMatrix, x: DoubleMatrix): Double = {
    val res = (x.transpose().mmul(ata).mmul(x)).mul(0.5).sub(atb.dot(x))
    res.get(0)
  }

  test("NNLS: exact solution cases") {
    val n = 20
    val rand = new Random(12346)
    val ws = NNLS.createWorkspace(n)
    var numSolved = 0

    // About 15% of random 20x20 [-1,1]-matrices have a singular value less than 1e-3.  NNLS
    // can legitimately fail to solve these anywhere close to exactly.  So we grab a considerable
    // sample of these matrices and make sure that we solved a substantial fraction of them.

    for (k <- 0 until 100) {
      val (ata, atb) = genOnesData(n, rand)
      val x = new DoubleMatrix(NNLS.solve(ata.data, atb.data, ws))
      assert(x.length === n)
      val answer = DoubleMatrix.ones(n, 1)
      SimpleBlas.axpy(-1.0, answer, x)
      val solved = (x.norm2 < 1e-2) && (x.normmax < 1e-3)
      if (solved) numSolved = numSolved + 1
    }

    assert(numSolved > 50)
  }

  test("NNLS: nonnegativity constraint active") {
    val n = 5
    // scalastyle:off
    val ata = new DoubleMatrix(Array(
      Array( 4.377, -3.531, -1.306, -0.139,  3.418),
      Array(-3.531,  4.344,  0.934,  0.305, -2.140),
      Array(-1.306,  0.934,  2.644, -0.203, -0.170),
      Array(-0.139,  0.305, -0.203,  5.883,  1.428),
      Array( 3.418, -2.140, -0.170,  1.428,  4.684)))
    // scalastyle:on
    val atb = new DoubleMatrix(Array(-1.632, 2.115, 1.094, -1.025, -0.636))

    val goodx = Array(0.13025, 0.54506, 0.2874, 0.0, 0.028628)

    val ws = NNLS.createWorkspace(n)
    val x = NNLS.solve(ata.data, atb.data, ws)
    for (i <- 0 until n) {
      assert(x(i) ~== goodx(i) absTol 1E-3)
      assert(x(i) >= 0)
    }
  }

  test("NNLS: objective value test") {
    val n = 5
    val ata = new DoubleMatrix(5, 5
      , 517399.13534, 242529.67289, -153644.98976, 130802.84503, -798452.29283
      , 242529.67289, 126017.69765, -75944.21743, 81785.36128, -405290.60884
      , -153644.98976, -75944.21743, 46986.44577, -45401.12659, 247059.51049
      , 130802.84503, 81785.36128, -45401.12659, 67457.31310, -253747.03819
      , -798452.29283, -405290.60884, 247059.51049, -253747.03819, 1310939.40814
    )
    val atb = new DoubleMatrix(5, 1,
      -31755.05710, 13047.14813, -20191.24443, 25993.77580, 11963.55017)

    /** reference solution obtained from matlab function quadprog */
    val refx = new DoubleMatrix(Array(34.90751, 103.96254, 0.00000, 27.82094, 58.79627))
    val refObj = computeObjectiveValue(ata, atb, refx)


    val ws = NNLS.createWorkspace(n)
    val x = new DoubleMatrix(NNLS.solve(ata.data, atb.data, ws))
    val obj = computeObjectiveValue(ata, atb, x)

    assert(obj < refObj + 1E-5)
  }
}
