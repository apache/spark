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
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData

class UnsafeArraySuite extends SparkFunSuite {

  test("from primitive int array") {
    val array = Array(1, 10, 100)
    val unsafe = UnsafeArrayData.fromPrimitiveArray(array)
    assert(unsafe.numElements == 3)
    assert(unsafe.getSizeInBytes == 4 + 4 * 3 + 4 * 3)
    assert(unsafe.getInt(0) == 1)
    assert(unsafe.getInt(1) == 10)
    assert(unsafe.getInt(2) == 100)
  }

  test("from primitive double array") {
    val array = Array(1.1, 2.2, 3.3)
    val unsafe = UnsafeArrayData.fromPrimitiveArray(array)
    assert(unsafe.numElements == 3)
    assert(unsafe.getSizeInBytes == 4 + 4 * 3 + 8 * 3)
    assert(unsafe.getDouble(0) == 1.1)
    assert(unsafe.getDouble(1) == 2.2)
    assert(unsafe.getDouble(2) == 3.3)
  }
}
