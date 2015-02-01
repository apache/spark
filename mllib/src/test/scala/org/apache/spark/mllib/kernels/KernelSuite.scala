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
package org.apache.spark.mllib.kernels

import org.scalatest.FunSuite
import org.apache.spark.mllib.classification.SVMSuite
import org.apache.spark.mllib.prototype.{QuadraticRenyiEntropy, GreedyEntropySelector}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class KernelSuite extends FunSuite with MLlibTestSparkContext {

  test("Testing evaluate function of Polynomial and RBF Functions"){

    val nPoints = 100

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)

    val testRDD = sc.parallelize(testData)

    val rbf = new RBFKernel(1.00)
    val poly = new PolynomialKernel(2, 1.5)

    val mappedData = SVMKernel.indexedRDD(testRDD)

    val kernelMatrix1 = poly.buildKernelMatrixasRDD(mappedData, nPoints)
    val kernelMatrix2 = rbf.buildKernelMatrixasRDD(mappedData, nPoints)

    assert(mappedData.count() == nPoints)
    assert(kernelMatrix1.getKernelMatrix().filter((point) =>
      point._2.isNaN || point._2.isInfinite)
      .count() == 0)
    assert(kernelMatrix2.getKernelMatrix().filter((point) =>
      point._2.isNaN || point._2.isInfinite)
      .count() == 0)

  }

  test("Testing building of feature map from the kernel matrix"){
    val nPoints = 100

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val rbf = new RBFKernel(1.00)
    val poly = new PolynomialKernel(5, 1.5)
    val mappedData = SVMKernel.indexedRDD(testRDD)

    mappedData.cache()
    val kernelMatrixpoly = poly.buildKernelMatrixasRDD(mappedData, nPoints)
    val kernelMatrixRBF = rbf.buildKernelMatrixasRDD(mappedData, nPoints)

    assert(mappedData.count() == nPoints)
    val mappedFeaturespoly = poly.featureMapping(
      kernelMatrixpoly.eigenDecomposition(99)
    )(mappedData)(mappedData)
    val mappedFeaturesrbf = rbf.featureMapping(
      kernelMatrixRBF.eigenDecomposition(99)
    )(mappedData)(mappedData)

    assert(mappedFeaturespoly.filter((point) => point._2.features.size == 99).count() == 100)
    assert(mappedFeaturesrbf.filter((point) => point._2.features.size == 99).count() == 100)
  }

  test("Testing optimal bandwidth calculation on Gaussian Kernel" +
    " and maximum entropy subset selection"){
    val nPoints = 1000
    val subsetSize = 100
    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    val newtestRDD = testRDD.map((p) => p.features)
    newtestRDD.cache()
    val kern = new GaussianDensityKernel()
    kern.optimalBandwidth(newtestRDD)
    assert(kern.eval(newtestRDD.first()) != Double.NaN)

    val newIndexedRDD = SVMKernel.indexedRDD(newtestRDD)
    newIndexedRDD.cache()
    newtestRDD.unpersist()

    val entropy = new QuadraticRenyiEntropy(kern)
    val subsetsel = new GreedyEntropySelector(entropy)

    val subsetRDD = subsetsel.selectPrototypes(
      SVMKernel.indexedRDD(testRDD),
      subsetSize)

    assert(subsetRDD.count() == subsetSize)
  }

  test("Testing rbf kernel with subset selection and feature map extraction") {
    val nPoints = 1000
    val nDimensions = 5
    val subsetSize = 100
    val unZip = SVMKernel.unzipIndexedData _

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(
      A,
      Array[Double](B, C),
      nPoints,
      42)

    val testRDD = sc.parallelize(testData, 2)

    val newtestRDD = testRDD.map(_.features)
    newtestRDD.cache()
    val kern = new GaussianDensityKernel()
    kern.optimalBandwidth(newtestRDD)
    newtestRDD.unpersist()
    val mappedData = SVMKernel.indexedRDD(testRDD)
    mappedData.cache()

    val entropy = new QuadraticRenyiEntropy(kern)
    val subsetsel = new GreedyEntropySelector(entropy)
    val subsetRDD = subsetsel.selectPrototypes(
      mappedData,
      subsetSize)

    val rbf = new RBFKernel(0.8)
    subsetRDD.cache()

    val kernelMatrixRBF = rbf.buildKernelMatrixasRDD(
      SVMKernel.indexedRDD(unZip(subsetRDD)),
      subsetSize)

    val featureMap = rbf.featureMapping(
      kernelMatrixRBF.eigenDecomposition(nDimensions)
    )(subsetRDD) _

    val mappedFeaturesrbf = featureMap(mappedData)

    mappedFeaturesrbf.cache()
    mappedData.unpersist()

    assert(mappedFeaturesrbf.count() == nPoints)
    assert(mappedFeaturesrbf.first()._2.features.size == nDimensions)

  }
}
