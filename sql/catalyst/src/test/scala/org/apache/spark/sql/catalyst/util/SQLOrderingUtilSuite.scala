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

class SQLOrderingUtilSuite extends SparkFunSuite {

  test("compareDoublesSQL") {
    def shouldMatchDefaultOrder(a: Double, b: Double): Unit = {
      assert(SQLOrderingUtil.compareDoubles(a, b) === JDouble.compare(a, b))
      assert(SQLOrderingUtil.compareDoubles(b, a) === JDouble.compare(b, a))
    }
    shouldMatchDefaultOrder(0d, 0d)
    shouldMatchDefaultOrder(0d, 1d)
    shouldMatchDefaultOrder(-1d, 1d)
    shouldMatchDefaultOrder(Double.MinValue, Double.MaxValue)

    val specialNaN = JDouble.longBitsToDouble(0x7ff1234512345678L)
    assert(JDouble.isNaN(specialNaN))
    assert(JDouble.doubleToRawLongBits(Double.NaN) != JDouble.doubleToRawLongBits(specialNaN))

    assert(SQLOrderingUtil.compareDoubles(Double.NaN, Double.NaN) === 0)
    assert(SQLOrderingUtil.compareDoubles(Double.NaN, specialNaN) === 0)
    assert(SQLOrderingUtil.compareDoubles(Double.NaN, Double.PositiveInfinity) > 0)
    assert(SQLOrderingUtil.compareDoubles(specialNaN, Double.PositiveInfinity) > 0)
    assert(SQLOrderingUtil.compareDoubles(Double.NaN, Double.NegativeInfinity) > 0)
    assert(SQLOrderingUtil.compareDoubles(Double.PositiveInfinity, Double.NaN) < 0)
    assert(SQLOrderingUtil.compareDoubles(Double.NegativeInfinity, Double.NaN) < 0)
    assert(SQLOrderingUtil.compareDoubles(0.0d, -0.0d) === 0)
    assert(SQLOrderingUtil.compareDoubles(-0.0d, 0.0d) === 0)
  }

  test("compareFloatsSQL") {
    def shouldMatchDefaultOrder(a: Float, b: Float): Unit = {
      assert(SQLOrderingUtil.compareFloats(a, b) === JFloat.compare(a, b))
      assert(SQLOrderingUtil.compareFloats(b, a) === JFloat.compare(b, a))
    }
    shouldMatchDefaultOrder(0f, 0f)
    shouldMatchDefaultOrder(0f, 1f)
    shouldMatchDefaultOrder(-1f, 1f)
    shouldMatchDefaultOrder(Float.MinValue, Float.MaxValue)

    val specialNaN = JFloat.intBitsToFloat(-6966608)
    assert(JFloat.isNaN(specialNaN))
    assert(JFloat.floatToRawIntBits(Float.NaN) != JFloat.floatToRawIntBits(specialNaN))

    assert(SQLOrderingUtil.compareDoubles(Float.NaN, Float.NaN) === 0)
    assert(SQLOrderingUtil.compareDoubles(Float.NaN, specialNaN) === 0)
    assert(SQLOrderingUtil.compareDoubles(Float.NaN, Float.PositiveInfinity) > 0)
    assert(SQLOrderingUtil.compareDoubles(specialNaN, Float.PositiveInfinity) > 0)
    assert(SQLOrderingUtil.compareDoubles(Float.NaN, Float.NegativeInfinity) > 0)
    assert(SQLOrderingUtil.compareDoubles(Float.PositiveInfinity, Float.NaN) < 0)
    assert(SQLOrderingUtil.compareDoubles(Float.NegativeInfinity, Float.NaN) < 0)
    assert(SQLOrderingUtil.compareDoubles(0.0f, -0.0f) === 0)
    assert(SQLOrderingUtil.compareDoubles(-0.0f, 0.0f) === 0)
  }
}
