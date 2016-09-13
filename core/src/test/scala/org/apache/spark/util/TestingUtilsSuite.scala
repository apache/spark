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

package org.apache.spark.util

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.TestingUtils._

class TestingUtilsSuite extends SparkFunSuite {

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
}
