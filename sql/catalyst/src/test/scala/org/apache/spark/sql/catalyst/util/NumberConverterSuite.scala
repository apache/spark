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
import org.apache.spark.sql.catalyst.util.NumberConverter.convert
import org.apache.spark.unsafe.types.UTF8String

class NumberConverterSuite extends SparkFunSuite {

  private[this] def checkConv(n: String, fromBase: Int, toBase: Int, expected: String): Unit = {
    assert(convert(UTF8String.fromString(n).getBytes, fromBase, toBase) ===
      UTF8String.fromString(expected))
  }

  test("convert") {
    checkConv("3", 10, 2, "11")
    checkConv("-15", 10, -16, "-F")
    checkConv("-15", 10, 16, "FFFFFFFFFFFFFFF1")
    checkConv("big", 36, 16, "3A48")
    checkConv("9223372036854775807", 36, 16, "FFFFFFFFFFFFFFFF")
    checkConv("11abc", 10, 16, "B")
  }

}
