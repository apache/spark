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

package org.apache.spark.ml.util

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.ml.SparkMLFunSuite
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.apache.spark.ml.util.TestingUtils._

class TestingUtilsSuite extends SparkMLFunSuite {

  test("Comparing doubles using relative error.") {

    assert(23.1 ~== 23.52 relTol 0.02)
    assert(23.1 ~== 22.74 relTol 0.02)
    assert(23.1 ~= 23.52 relTol 0.02)
    assert(23.1 ~= 22.74 relTol 0.02)
    assert(!(23.1 !~= 23.52 relTol 0.02))
    assert(!(23.1 !~= 22.74 relTol 0.02))

    // Should throw exception with message when test fails.
    intercept[TestFailedException](23.1 !~== 23.52 relTol 0.02)
    intercept[TestFailedException](23.1 !~== 22.74 relTol 0.02)
    intercept[TestFailedException](23.1 ~== 23.63 relTol 0.02)
    intercept[TestFailedException](23.1 ~== 22.34 relTol 0.02)

    assert(23.1 !~== 23.63 relTol 0.02)
    assert(23.1 !~== 22.34 relTol 0.02)
    assert(23.1 !~= 23.63 relTol 0.02)
    assert(23.1 !~= 22.34 relTol 0.02)
    assert(!(23.1 ~= 23.63 relTol 0.02))
    assert(!(23.1 ~= 22.34 relTol 0.02))

    // Comparing against zero should fail the test and throw exception with message
    // saying that the relative error is meaningless in this situation.
    intercept[TestFailedException](0.1 ~== 0.0 relTol 0.032)
    intercept[TestFailedException](0.1 ~= 0.0 relTol 0.032)
    intercept[TestFailedException](0.1 !~== 0.0 relTol 0.032)
    intercept[TestFailedException](0.1 !~= 0.0 relTol 0.032)
    intercept[TestFailedException](0.0 ~== 0.1 relTol 0.032)
    intercept[TestFailedException](0.0 ~= 0.1 relTol 0.032)
    intercept[TestFailedException](0.0 !~== 0.1 relTol 0.032)
    intercept[TestFailedException](0.0 !~= 0.1 relTol 0.032)

    // Comparisons of numbers very close to zero.
    assert(10 * Double.MinPositiveValue ~== 9.5 * Double.MinPositiveValue relTol 0.01)
    assert(10 * Double.MinPositiveValue !~== 11 * Double.MinPositiveValue relTol 0.01)

    assert(-Double.MinPositiveValue ~== 1.18 * -Double.MinPositiveValue relTol 0.012)
    assert(-Double.MinPositiveValue ~== 1.38 * -Double.MinPositiveValue relTol 0.012)
  }

  test("Comparing doubles using absolute error.") {

    assert(17.8 ~== 17.99 absTol 0.2)
    assert(17.8 ~== 17.61 absTol 0.2)
    assert(17.8 ~= 17.99 absTol 0.2)
    assert(17.8 ~= 17.61 absTol 0.2)
    assert(!(17.8 !~= 17.99 absTol 0.2))
    assert(!(17.8 !~= 17.61 absTol 0.2))

    // Should throw exception with message when test fails.
    intercept[TestFailedException](17.8 !~== 17.99 absTol 0.2)
    intercept[TestFailedException](17.8 !~== 17.61 absTol 0.2)
    intercept[TestFailedException](17.8 ~== 18.01 absTol 0.2)
    intercept[TestFailedException](17.8 ~== 17.59 absTol 0.2)

    assert(17.8 !~== 18.01 absTol 0.2)
    assert(17.8 !~== 17.59 absTol 0.2)
    assert(17.8 !~= 18.01 absTol 0.2)
    assert(17.8 !~= 17.59 absTol 0.2)
    assert(!(17.8 ~= 18.01 absTol 0.2))
    assert(!(17.8 ~= 17.59 absTol 0.2))

    // Comparisons of numbers very close to zero, and both side of zeros
    assert(
      Double.MinPositiveValue ~== 4 * Double.MinPositiveValue absTol 5 * Double.MinPositiveValue)
    assert(
      Double.MinPositiveValue !~== 6 * Double.MinPositiveValue absTol 5 * Double.MinPositiveValue)

    assert(
      -Double.MinPositiveValue ~== 3 * Double.MinPositiveValue absTol 5 * Double.MinPositiveValue)
    assert(
      Double.MinPositiveValue !~== -4 * Double.MinPositiveValue absTol 5 * Double.MinPositiveValue)
  }

  test("Comparing vectors using relative error.") {

    // Comparisons of two dense vectors
    assert(Vectors.dense(Array(3.1, 3.5)) ~== Vectors.dense(Array(3.130, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array(3.1, 3.5)) !~== Vectors.dense(Array(3.135, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array(3.1, 3.5)) ~= Vectors.dense(Array(3.130, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array(3.1, 3.5)) !~= Vectors.dense(Array(3.135, 3.534)) relTol 0.01)
    assert(!(Vectors.dense(Array(3.1, 3.5)) !~= Vectors.dense(Array(3.130, 3.534)) relTol 0.01))
    assert(!(Vectors.dense(Array(3.1, 3.5)) ~= Vectors.dense(Array(3.135, 3.534)) relTol 0.01))
    assert(Vectors.dense(Array(3.1)) !~= Vectors.dense(Array(3.130, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array.empty[Double]) !~= Vectors.dense(Array(3.130, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array(3.1)) !~== Vectors.dense(Array(3.130, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array.empty[Double]) !~== Vectors.dense(Array(3.130, 3.534)) relTol 0.01)

    // Should throw exception with message when test fails.
    intercept[TestFailedException](
      Vectors.dense(Array(3.1, 3.5)) !~== Vectors.dense(Array(3.130, 3.534)) relTol 0.01)

    intercept[TestFailedException](
      Vectors.dense(Array(3.1, 3.5)) ~== Vectors.dense(Array(3.135, 3.534)) relTol 0.01)

    intercept[TestFailedException](
      Vectors.dense(Array(3.1)) ~== Vectors.dense(Array(3.535, 3.534)) relTol 0.01)

    intercept[TestFailedException](
      Vectors.dense(Array.empty[Double]) ~== Vectors.dense(Array(3.135)) relTol 0.01)

    // Comparing against zero should fail the test and throw exception with message
    // saying that the relative error is meaningless in this situation.
    intercept[TestFailedException](
      Vectors.dense(Array(3.1, 0.01)) ~== Vectors.dense(Array(3.13, 0.0)) relTol 0.01)

    intercept[TestFailedException](
      Vectors.dense(Array(3.1, 0.01)) ~== Vectors.sparse(2, Array(0), Array(3.13)) relTol 0.01)

    // Comparisons of a sparse vector and a dense vector
    assert(Vectors.dense(Array(3.1, 3.5)) ~==
      Vectors.sparse(2, Array(0, 1), Array(3.130, 3.534)) relTol 0.01)

    assert(Vectors.dense(Array(3.1, 3.5)) !~==
      Vectors.sparse(2, Array(0, 1), Array(3.135, 3.534)) relTol 0.01)

    assert(Vectors.dense(Array(3.1)) !~==
      Vectors.sparse(2, Array(0, 1), Array(3.130, 3.534)) relTol 0.01)

    assert(Vectors.dense(Array.empty[Double]) !~==
      Vectors.sparse(2, Array(0, 1), Array(3.130, 3.534)) relTol 0.01)
  }

  test("Comparing vectors using absolute error.") {

    // Comparisons of two dense vectors
    assert(Vectors.dense(Array(3.1, 3.5, 0.0)) ~==
      Vectors.dense(Array(3.1 + 1E-8, 3.5 + 2E-7, 1E-8)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1, 3.5, 0.0)) !~==
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7, 1 + 1E-3)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1, 3.5, 0.0)) ~=
      Vectors.dense(Array(3.1 + 1E-8, 3.5 + 2E-7, 1E-8)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1, 3.5, 0.0)) !~=
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7, 1 + 1E-3)) absTol 1E-6)

    assert(!(Vectors.dense(Array(3.1, 3.5, 0.0)) !~=
      Vectors.dense(Array(3.1 + 1E-8, 3.5 + 2E-7, 1E-8)) absTol 1E-6))

    assert(!(Vectors.dense(Array(3.1, 3.5, 0.0)) ~=
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7, 1 + 1E-3)) absTol 1E-6))

    assert(Vectors.dense(Array(3.1)) !~=
      Vectors.dense(Array(3.1 + 1E-6, 3.5 + 2E-7)) absTol 1E-5)

    assert(!(Vectors.dense(Array(3.1)) ~=
      Vectors.dense(Array(3.1 + 1E-6, 3.5 + 2E-7)) absTol 1E-5))

    assert(Vectors.dense(Array.empty[Double]) !~=
      Vectors.dense(Array(3.1 + 1E-6, 3.5 + 2E-7)) absTol 1E-5)

    assert(!(Vectors.dense(Array.empty[Double]) ~=
      Vectors.dense(Array(3.1 + 1E-6, 3.5 + 2E-7)) absTol 1E-5))

    assert(Vectors.dense(Array.empty[Double]) ~=
      Vectors.dense(Array.empty[Double]) absTol 1E-5)

    // Should throw exception with message when test fails.
    intercept[TestFailedException](Vectors.dense(Array(3.1, 3.5, 0.0)) !~==
      Vectors.dense(Array(3.1 + 1E-8, 3.5 + 2E-7, 1E-8)) absTol 1E-6)

    intercept[TestFailedException](Vectors.dense(Array(3.1, 3.5, 0.0)) ~==
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7, 1 + 1E-3)) absTol 1E-6)

    intercept[TestFailedException](Vectors.dense(Array(3.1)) ~==
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7)) absTol 1E-6)

    intercept[TestFailedException](Vectors.dense(Array.empty[Double]) ~==
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7)) absTol 1E-6)

    // Comparisons of two sparse vectors
    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) ~==
      Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-8, 2.4 + 1E-7)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-8, 2.4 + 1E-7)) ~==
      Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) !~==
      Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-3, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-3, 2.4)) !~==
      Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-6, 2.4)) !~==
      Vectors.sparse(1, Array(0), Array(3.1)) absTol 1E-3)

    assert(Vectors.sparse(0, Array.empty[Int], Array.empty[Double]) !~==
      Vectors.sparse(1, Array(0), Array(3.1)) absTol 1E-3)

    // Comparisons of a dense vector and a sparse vector
    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) ~==
      Vectors.dense(Array(3.1 + 1E-8, 0, 2.4 + 1E-7)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1 + 1E-8, 0, 2.4 + 1E-7)) ~==
      Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) !~==
      Vectors.dense(Array(3.1, 1E-3, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) !~==
      Vectors.dense(Array(3.1)) absTol 1E-6)

    assert(Vectors.dense(Array.empty[Double]) !~==
      Vectors.sparse(3, Array(0, 2), Array(0, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(1, Array(0), Array(3.1)) !~==
      Vectors.dense(Array(3.1, 3.2)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1)) !~==
      Vectors.sparse(0, Array.empty[Int], Array.empty[Double]) absTol 1E-6)
  }

  test("Comparing Matrices using absolute error.") {

    // Comparisons of two dense Matrices
    assert(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) ~==
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 3.5 + 2E-7, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) ~=
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 3.5 + 2E-7, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.1 + 1E-5, 3.5 + 2E-6, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.1 + 1E-5, 3.5 + 2E-6, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(!(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) ~=
      Matrices.dense(2, 2, Array(3.1 + 1E-5, 3.5 + 2E-6, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6))

    assert(!(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.1 + 1E-7, 3.5 + 2E-8, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6))

    assert(Matrices.dense(2, 1, Array(3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.1 + 1E-7, 3.5 + 2E-8, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.dense(2, 1, Array(3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.1 + 1E-7, 3.5 + 2E-8, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.dense(0, 0, Array()) !~=
      Matrices.dense(2, 2, Array(3.1 + 1E-7, 3.5 + 2E-8, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.dense(0, 0, Array()) !~==
      Matrices.dense(2, 2, Array(3.1 + 1E-7, 3.5 + 2E-8, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    // Should throw exception with message when test fails.
    intercept[TestFailedException](Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 3.5 + 2E-7, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    intercept[TestFailedException](Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) ~==
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 3.5 + 2E-7, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-9)

    intercept[TestFailedException](Matrices.dense(2, 1, Array(3.1, 3.5)) ~==
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 3.5 + 2E-7, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-5)

    intercept[TestFailedException](Matrices.dense(0, 0, Array()) ~==
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 3.5 + 2E-7, 3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-5)

    // Comparisons of two sparse Matrices
    assert(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) ~==
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) ~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) !~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-9)

    assert(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) !~==
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-9)

    assert(!(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) ~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5)) absTol 1E-9))

    assert(!(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) !~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5)) absTol 1E-6))

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-9)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.sparse(0, 0, Array(1), Array(0), Array(0)) !~==
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.sparse(0, 0, Array(1), Array(0), Array(0)) !~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1 + 1E-8, 3.5 + 1E-7)) absTol 1E-6)

    // Comparisons of a dense Matrix and a sparse Matrix
    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) ~=
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 0, 0, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) ~==
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 0, 0, 3.5 + 1E-7)) absTol 1E-6)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 0, 0, 3.5 + 1E-7)) absTol 1E-9)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 0, 0, 3.5 + 1E-7)) absTol 1E-9)

    assert(!(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) ~=
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 0, 0, 3.5 + 1E-7)) absTol 1E-9))

    assert(!(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.1 + 1E-8, 0, 0, 3.5 + 1E-7)) absTol 1E-6))

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.dense(2, 1, Array(3.1 + 1E-8, 0)) absTol 1E-6)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.dense(2, 1, Array(3.1 + 1E-8, 0)) absTol 1E-6)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.dense(0, 0, Array()) absTol 1E-6)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.dense(0, 0, Array()) absTol 1E-6)
  }

  test("Comparing Matrices using relative error.") {

    // Comparisons of two dense Matrices
    assert(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) ~==
      Matrices.dense(2, 2, Array(3.130, 3.534, 3.130, 3.534)) relTol 0.01)

    assert(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) ~=
      Matrices.dense(2, 2, Array(3.130, 3.534, 3.130, 3.534)) relTol 0.01)

    assert(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.135, 3.534, 3.135, 3.534)) relTol 0.01)

    assert(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.135, 3.534, 3.135, 3.534)) relTol 0.01)

    assert(!(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) ~=
      Matrices.dense(2, 2, Array(3.134, 3.535, 3.134, 3.535)) relTol 0.01))

    assert(!(Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.130, 3.534, 3.130, 3.534)) relTol 0.01))

    assert(Matrices.dense(2, 1, Array(3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) relTol 0.01)

    assert(Matrices.dense(2, 1, Array(3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) relTol 0.01)

    assert(Matrices.dense(0, 0, Array()) !~=
      Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) relTol 0.01)

    assert(Matrices.dense(0, 0, Array()) !~==
      Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) relTol 0.01)

    // Should throw exception with message when test fails.
    intercept[TestFailedException](Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.130, 3.534, 3.130, 3.534)) relTol 0.01)

    intercept[TestFailedException](Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) ~==
      Matrices.dense(2, 2, Array(3.135, 3.534, 3.135, 3.534)) relTol 0.01)

    intercept[TestFailedException](Matrices.dense(2, 1, Array(3.1, 3.5)) ~==
      Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) relTol 0.01)

    intercept[TestFailedException](Matrices.dense(0, 0, Array()) ~==
      Matrices.dense(2, 2, Array(3.1, 3.5, 3.1, 3.5)) relTol 0.01)

    // Comparisons of two sparse Matrices
    assert(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) ~==
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.130, 3.534)) relTol 0.01)

    assert(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) ~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.130, 3.534)) relTol 0.01)

    assert(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) !~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.135, 3.534)) relTol 0.01)

    assert(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) !~==
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.135, 3.534)) relTol 0.01)

    assert(!(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) ~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.135, 3.534)) relTol 0.01))

    assert(!(Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) !~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.130, 3.534)) relTol 0.01))

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) relTol 0.01)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) relTol 0.01)

    assert(Matrices.sparse(0, 0, Array(1), Array(0), Array(0)) !~==
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) relTol 0.01)

    assert(Matrices.sparse(0, 0, Array(1), Array(0), Array(0)) !~=
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5)) relTol 0.01)

    // Comparisons of a dense Matrix and a sparse Matrix
    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) ~=
      Matrices.dense(2, 2, Array(3.130, 0, 0, 3.534)) relTol 0.01)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) ~==
      Matrices.dense(2, 2, Array(3.130, 0, 0, 3.534)) relTol 0.01)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.135, 0, 0, 3.534)) relTol 0.01)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.dense(2, 2, Array(3.135, 0, 0, 3.534)) relTol 0.01)

    assert(!(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) ~=
      Matrices.dense(2, 2, Array(3.135, 0, 0, 3.534)) relTol 0.01))

    assert(!(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.dense(2, 2, Array(3.130, 0, 0, 3.534)) relTol 0.01))

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.dense(2, 1, Array(3.1, 0)) relTol 0.01)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.dense(2, 1, Array(3.1, 0)) relTol 0.01)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~==
      Matrices.dense(0, 0, Array()) relTol 0.01)

    assert(Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(3.1, 3.5)) !~=
      Matrices.dense(0, 0, Array()) relTol 0.01)
  }
}
