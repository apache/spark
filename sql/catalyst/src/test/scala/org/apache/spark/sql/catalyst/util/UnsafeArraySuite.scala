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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.unsafe.Platform

class UnsafeArraySuite extends SparkFunSuite {

  val booleanArray = Array(false, true)
  val shortArray = Array(1.toShort, 10.toShort, 100.toShort)
  val intArray = Array(1, 10, 100)
  val longArray = Array(1.toLong, 10.toLong, 100.toLong)
  val floatArray = Array(1.1.toFloat, 2.2.toFloat, 3.3.toFloat)
  val doubleArray = Array(1.1, 2.2, 3.3)

  test("read array") {
    val unsafeBoolean = ExpressionEncoder[Array[Boolean]].resolveAndBind().
      toRow(booleanArray).getArray(0)
    assert(unsafeBoolean.isInstanceOf[UnsafeArrayData])
    assert(unsafeBoolean.numElements == 2)
    booleanArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeBoolean.getBoolean(i) == e)
    }

    val unsafeShort = ExpressionEncoder[Array[Short]].resolveAndBind().
      toRow(shortArray).getArray(0)
    assert(unsafeShort.isInstanceOf[UnsafeArrayData])
    assert(unsafeShort.numElements == 3)
    shortArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeShort.getShort(i) == e)
    }

    val unsafeInt = ExpressionEncoder[Array[Int]].resolveAndBind().
      toRow(intArray).getArray(0)
    assert(unsafeInt.isInstanceOf[UnsafeArrayData])
    assert(unsafeInt.numElements == 3)
    intArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeInt.getInt(i) == e)
    }

    val unsafeLong = ExpressionEncoder[Array[Long]].resolveAndBind().
      toRow(longArray).getArray(0)
    assert(unsafeLong.isInstanceOf[UnsafeArrayData])
    assert(unsafeLong.numElements == 3)
    longArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeLong.getLong(i) == e)
    }

    val unsafeFloat = ExpressionEncoder[Array[Float]].resolveAndBind().
      toRow(floatArray).getArray(0)
    assert(unsafeFloat.isInstanceOf[UnsafeArrayData])
    assert(unsafeFloat.numElements == 3)
    floatArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeFloat.getFloat(i) == e)
    }

    val unsafeDouble = ExpressionEncoder[Array[Double]].resolveAndBind().
      toRow(doubleArray).getArray(0)
    assert(unsafeDouble.isInstanceOf[UnsafeArrayData])
    assert(unsafeDouble.numElements == 3)
    doubleArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDouble.getDouble(i) == e)
    }
  }

  test("from primitive array") {
    val unsafeInt = UnsafeArrayData.fromPrimitiveArray(intArray)
    assert(unsafeInt.numElements == 3)
    assert(unsafeInt.getSizeInBytes ==
      ((4 + scala.math.ceil(3/64.toDouble) * 8 + 4 * 3 + 7).toInt / 8) * 8)
    intArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeInt.getInt(i) == e)
    }

    val unsafeDouble = UnsafeArrayData.fromPrimitiveArray(doubleArray)
    assert(unsafeDouble.numElements == 3)
    assert(unsafeDouble.getSizeInBytes ==
      ((4 + scala.math.ceil(3/64.toDouble) * 8 + 8 * 3 + 7).toInt / 8) * 8)
    doubleArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDouble.getDouble(i) == e)
    }
  }

  test("to primitive array") {
    val intEncoder = ExpressionEncoder[Array[Int]].resolveAndBind()
    val intInternalRow = intEncoder.toRow(intArray)
    val intUnsafeArray = intInternalRow.getArray(0)
    assert(intUnsafeArray.toIntArray.sameElements(intArray))

    val doubleEncoder = ExpressionEncoder[Array[Double]].resolveAndBind()
    val doubleInternalRow = doubleEncoder.toRow(doubleArray)
    val doubleUnsafeArray = doubleInternalRow.getArray(0)
    assert(doubleUnsafeArray.toDoubleArray.sameElements(doubleArray))
  }
}
