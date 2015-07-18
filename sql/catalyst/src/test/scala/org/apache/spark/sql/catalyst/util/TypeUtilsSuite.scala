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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite

class TypeUtilsSuite extends SparkFunSuite {

  import TypeUtils._

  test("compareDoubles") {
    assert(compareDoubles(0, 0) === 0)
    assert(compareDoubles(1, 0) === -1)
    assert(compareDoubles(0, 1) === 1)
    assert(compareDoubles(Double.MinValue, Double.MaxValue) === 1)
    assert(compareDoubles(Double.NaN, Double.NaN) === 0)
    assert(compareDoubles(Double.NaN, Double.PositiveInfinity) === 1)
    assert(compareDoubles(Double.NaN, Double.NegativeInfinity) === 1)
    assert(compareDoubles(Double.PositiveInfinity, Double.NaN) === -1)
    assert(compareDoubles(Double.NegativeInfinity, Double.NaN) === -1)
  }

  test("compareFloats") {
    assert(compareFloats(0, 0) === 0)
    assert(compareFloats(1, 0) === -1)
    assert(compareFloats(0, 1) === 1)
    assert(compareFloats(Float.MinValue, Float.MaxValue) === 1)
    assert(compareFloats(Float.NaN, Float.NaN) === 0)
    assert(compareFloats(Float.NaN, Float.PositiveInfinity) === 1)
    assert(compareFloats(Float.NaN, Float.NegativeInfinity) === 1)
    assert(compareFloats(Float.PositiveInfinity, Float.NaN) === -1)
    assert(compareFloats(Float.NegativeInfinity, Float.NaN) === -1)
  }
}
