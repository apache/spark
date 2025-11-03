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

import org.apache.spark.{SparkException, SparkFunSuite}

class DirectByteBufferOutputStreamSuite extends SparkFunSuite {
  test("use after close") {
    val o = new DirectByteBufferOutputStream()
    val size = 1000
    o.write(new Array[Byte](size), 0, size)
    val b = o.toByteBuffer
    o.close()

    // Using `o` after close should throw an exception rather than crashing.
    assertThrows[SparkException] { o.write(123) }
    assertThrows[SparkException] { o.write(new Array[Byte](size), 0, size) }
    assertThrows[SparkException] { o.reset() }
    assertThrows[SparkException] { o.size() }
    assertThrows[SparkException] { o.toByteBuffer }

    // Using `b` after `o` is closed may crash.
    // val arr = new Array[Byte](size)
    // b.get(arr)
  }
}
