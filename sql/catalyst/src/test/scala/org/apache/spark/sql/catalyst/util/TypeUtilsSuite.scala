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

import java.lang.{Double => JDouble, Float => JFloat}

import org.apache.spark.SparkFunSuite

class TypeUtilsSuite extends SparkFunSuite {

  import TypeUtils._

  test("compareDoubles") {
    def shouldMatchDefaultOrder(a: Double, b: Double): Unit = {
      assert(compareDoubles(a, b) === JDouble.compare(a, b))
      assert(compareDoubles(b, a) === JDouble.compare(b, a))
    }
    shouldMatchDefaultOrder(0d, 0d)
    shouldMatchDefaultOrder(0d, 1d)
    shouldMatchDefaultOrder(Double.MinValue, Double.MaxValue)
    assert(compareDoubles(Double.NaN, Double.NaN) === 0)
    assert(compareDoubles(Double.NaN, Double.PositiveInfinity) === 1)
    assert(compareDoubles(Double.NaN, Double.NegativeInfinity) === 1)
    assert(compareDoubles(Double.PositiveInfinity, Double.NaN) === -1)
    assert(compareDoubles(Double.NegativeInfinity, Double.NaN) === -1)
  }

  test("compareFloats") {
    def shouldMatchDefaultOrder(a: Float, b: Float): Unit = {
      assert(compareFloats(a, b) === JFloat.compare(a, b))
      assert(compareFloats(b, a) === JFloat.compare(b, a))
    }
    shouldMatchDefaultOrder(0f, 0f)
    shouldMatchDefaultOrder(1f, 1f)
    shouldMatchDefaultOrder(Float.MinValue, Float.MaxValue)
    assert(compareFloats(Float.NaN, Float.NaN) === 0)
    assert(compareFloats(Float.NaN, Float.PositiveInfinity) === 1)
    assert(compareFloats(Float.NaN, Float.NegativeInfinity) === 1)
    assert(compareFloats(Float.PositiveInfinity, Float.NaN) === -1)
    assert(compareFloats(Float.NegativeInfinity, Float.NaN) === -1)
  }
}
