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
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class ArrayDataIndexedSeqSuite extends SparkFunSuite {
  def utf8(str: String): UTF8String = UTF8String.fromString(str)
  val stringArray = Array("1", "10", "100", null)

  private def testArrayData(arrayData: ArrayData): Unit = {
    assert(arrayData.numElements == stringArray.length)
    stringArray.zipWithIndex.map { case (e, i) =>
      if (e != null) {
        assert(arrayData.getUTF8String(i).toString().equals(e))
      } else {
        assert(arrayData.isNullAt(i))
      }
    }

    val seq = arrayData.toSeq[UTF8String](StringType)
    stringArray.zipWithIndex.map { case (e, i) =>
      if (e != null) {
        assert(seq(i).toString().equals(e))
      } else {
        assert(seq(i) == null)
      }
    }

    intercept[IndexOutOfBoundsException] {
      seq(-1)
    }.getMessage().contains("must be between 0 and the length of the ArrayData.")

    intercept[IndexOutOfBoundsException] {
      seq(seq.length)
    }.getMessage().contains("must be between 0 and the length of the ArrayData.")
  }

  test("ArrayDataIndexedSeq can work on GenericArrayData") {
    val arrayData = new GenericArrayData(stringArray.map(utf8(_)))
    testArrayData(arrayData)
  }

  test("ArrayDataIndexedSeq can work on UnsafeArrayData") {
    val unsafeArrayData = ExpressionEncoder[Array[String]].resolveAndBind().
      toRow(stringArray).getArray(0)
    assert(unsafeArrayData.isInstanceOf[UnsafeArrayData])
    testArrayData(unsafeArrayData)
  }
}
