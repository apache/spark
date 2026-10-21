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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.unsafe.types.UTF8String

class InternalRowSuite extends SparkFunSuite {

  test("copyValue detaches a binary value from a buffer the caller mutates afterward") {
    val buffer = Array[Byte](1, 2, 3, 4)
    val copy = InternalRow.copyValue(buffer).asInstanceOf[Array[Byte]]
    assert(copy sameElements Array[Byte](1, 2, 3, 4))
    java.util.Arrays.fill(buffer, 0.toByte)
    assert(copy sameElements Array[Byte](1, 2, 3, 4),
      "copyValue must clone a byte[] instead of aliasing the caller's buffer")
  }

  test("copyValue detaches a UTF8String from a buffer the caller mutates afterward") {
    val buffer = "hello".getBytes("UTF-8")
    val str = UTF8String.fromBytes(buffer)
    val copy = InternalRow.copyValue(str).asInstanceOf[UTF8String]
    java.util.Arrays.fill(buffer, 'X'.toByte)
    assert(copy.toString == "hello")
  }

  test("copyValue detaches array/row values from a buffer the caller mutates afterward") {
    val row = new GenericInternalRow(Array[Any]("a"))
    val rowCopy = InternalRow.copyValue(row).asInstanceOf[InternalRow]
    row.update(0, "mutated")
    assert(rowCopy.get(0, null) == "a")

    val array = new GenericArrayData(Array[Any](1, 2, 3))
    val arrayCopy = InternalRow.copyValue(array).asInstanceOf[ArrayData]
    assert(arrayCopy ne array)
    assert(arrayCopy.array.toSeq == Seq(1, 2, 3))
  }

  test("copyValue returns primitive values unchanged") {
    assert(InternalRow.copyValue(1) == 1)
    assert(InternalRow.copyValue(1L) == 1L)
    assert(InternalRow.copyValue(null) == null)
  }
}
