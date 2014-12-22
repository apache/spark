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

import org.apache.spark.mllib.classification.SVMSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

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
    val mappedFeaturespoly = kernelMatrixpoly.buildFeatureMap(3)
    val mappedFeaturesrbf = kernelMatrixRBF.buildFeatureMap(5)

    assert(mappedFeaturespoly.filter((point) => point.features.size == 3).count() == 100)
    assert(mappedFeaturesrbf.filter((point) => point.features.size == 5).count() == 100)

  }
}
