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
package org.apache.spark.ml.optim.loss

import org.scalactic.{Equality, TolerantNumerics}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{BLAS, Vectors}



class DifferentiableRegularizationSuite extends SparkFunSuite {

  test("L2 regularization") {
    val shouldApply = (_: Int) => true
    val regParam = 0.3
    val coefficients = Vectors.dense(Array(1.0, 3.0, -2.0))
    val numFeatures = coefficients.size

    // check without features standard
    val regFun = new L2Regularization(regParam, shouldApply, None)
    val (loss, grad) = regFun.calculate(coefficients)
    assert(loss === 0.5 * regParam * BLAS.dot(coefficients, coefficients))
    assert(grad === Vectors.dense(coefficients.toArray.map(_ * regParam)))

    // check with features standard
    val featuresStd = Array(0.1, 1.1, 0.5)
    val regFunStd = new L2Regularization(regParam, shouldApply, Some(featuresStd))
    val (lossStd, gradStd) = regFunStd.calculate(coefficients)
    val expectedLossStd = 0.5 * regParam * (0 until numFeatures).map { j =>
      coefficients(j) * coefficients(j) / (featuresStd(j) * featuresStd(j))
    }.sum
    val expectedGradientStd = Vectors.dense((0 until numFeatures).map { j =>
      regParam * coefficients(j) / (featuresStd(j) * featuresStd(j))
    }.toArray)
    assert(lossStd === expectedLossStd)
    assert(gradStd === expectedGradientStd)

    // check should apply
    val shouldApply2 = (i: Int) => i == 1
    val regFunApply = new L2Regularization(regParam, shouldApply2, None)
    val (lossApply, gradApply) = regFunApply.calculate(coefficients)
    assert(lossApply === 0.5 * regParam * coefficients(1) * coefficients(1))
    assert(gradApply ===  Vectors.dense(0.0, coefficients(1) * regParam, 0.0))

    // check with zero features standard
    val featuresStdZero = Array(0.1, 0.0, 0.5)
    val regFunStdZero = new L2Regularization(regParam, shouldApply, Some(featuresStdZero))
    val (_, gradStdZero) = regFunStdZero.calculate(coefficients)
    assert(gradStdZero(1) == 0.0)
  }

  test("Prior regularization") {
    implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-12)
    val shouldApply = (_: Int) => true
    val regParam = 0.3
    val coefficients = Vectors.dense(Array(1.0, 3.0, -2.0))
    val priorCoefficients = Array(0.1, 0.3, -0.5)
    val priorPrecisions = Array(10.0, 1.0, 100.0)
    val numFeatures = coefficients.size

    // check without features standard
    val regFun = new PriorRegularization(priorCoefficients,
      priorPrecisions, regParam, shouldApply, None)
    val (actualLoss, actualGradient) = regFun.calculate(coefficients)

    val expectedSum = (0 until numFeatures).map { i =>
      priorPrecisions(i) * (coefficients(i) - priorCoefficients(i)) *
        (coefficients(i) - priorCoefficients(i))}.toArray.sum
    assert(actualLoss ===  0.5 * regParam * expectedSum)

    val expectedGradient = Vectors.dense((0 until numFeatures).map { i =>
      regParam * priorPrecisions(i) * (coefficients(i) - priorCoefficients(i))}.toArray)
    for (i <- 0 until numFeatures)
      assert(actualGradient(i) === expectedGradient(i))

    // check with features standard
    val featuresStd = Array(0.1, 1.1, 0.5)
    val regFunStd = new PriorRegularization(priorCoefficients,
      priorPrecisions, regParam, shouldApply, Some(featuresStd))
    val (actualLossStd, actualGradientStd) = regFunStd.calculate(coefficients)

    val expectedSumStd = (0 until numFeatures).map { i =>
      priorPrecisions(i) * (coefficients(i) - priorCoefficients(i)) *
        (coefficients(i) - priorCoefficients(i)) / (featuresStd(i) * featuresStd(i))}.toArray.sum
    val expectedLossStd = 0.5 * regParam * expectedSumStd
    assert(actualLossStd === expectedLossStd)

    val expectedGradientStd = Vectors.dense((0 until numFeatures).map { i =>
      regParam * priorPrecisions(i) * (coefficients(i) - priorCoefficients(i)) /
        (featuresStd(i) * featuresStd(i))}.toArray)
    for (i <- 0 until numFeatures)
      assert(actualGradientStd(i) === expectedGradientStd(i))

    // check should apply
    val shouldApply2 = (i: Int) => i == 1
    val regFunApply = new PriorRegularization(priorCoefficients,
      priorPrecisions, regParam, shouldApply2, None)
    val (actualLossApply, actualGradientApply) = regFunApply.calculate(coefficients)

    val expectedSumApply = priorPrecisions(1) * (coefficients(1) - priorCoefficients(1)) *
        (coefficients(1) - priorCoefficients(1))
    assert(actualLossApply ===  0.5 * regParam * expectedSumApply)

    val expectedGradientApply = Vectors.dense(0.0,
      regParam * priorPrecisions(1) * (coefficients(1) - priorCoefficients(1)), 0.0)
    for (i <- 0 until numFeatures)
      assert(actualGradientApply(i) === expectedGradientApply(i))

    // check with zero features standard
    val featuresStdZero = Array(0.1, 0.0, 0.5)
    val regFunStdZero = new PriorRegularization(priorCoefficients,
      priorPrecisions, regParam, shouldApply, Some(featuresStdZero))
    val (_, gradStdZero) = regFunStdZero.calculate(coefficients)
    assert(gradStdZero(1) == 0.0)
  }
}
