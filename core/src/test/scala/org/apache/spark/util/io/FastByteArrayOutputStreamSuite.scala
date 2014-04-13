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

package org.apache.spark.util.io

import org.scalatest.FunSuite


class FastByteArrayOutputStreamSuite extends FunSuite {

  test("write single byte") {
    val out = new FastByteArrayOutputStream(initialCapacity = 4)
    out.write(0)
    out.write(1)
    assert(out.toArray._1(0) === 0)
    assert(out.toArray._1(1) === 1)
    assert(out.toArray._2 === 2)
    assert(out.length === 2)

    out.write(2)
    out.write(3)
    assert(out.toArray._1(2) === 2)
    assert(out.toArray._1(3) === 3)
    assert(out.length === 4)

    out.write(4)
    assert(out.toArray._1(4) === 4)
    assert(out.toArray._2 === 5)
    assert(out.length === 5)

    for (i <- 5 to 100) {
      out.write(i)
    }

    for (i <- 5 to 100) {
      assert(out.toArray._1(i) === i)
    }
  }

  test("write multiple bytes") {
    val out = new FastByteArrayOutputStream(initialCapacity = 4)
    out.write(Array[Byte](0.toByte, 1.toByte))
    assert(out.length === 2)
    assert(out.toArray._1(0) === 0)
    assert(out.toArray._1(1) === 1)

    out.write(Array[Byte](2.toByte, 3.toByte, 4.toByte))
    assert(out.length === 5)
    assert(out.toArray._1(2) === 2)
    assert(out.toArray._1(3) === 3)
    assert(out.toArray._1(4) === 4)

    // Write more than double the size of the current array
    out.write((1 to 100).map(_.toByte).toArray)
    assert(out.length === 105)
    assert(out.toArray._1(104) === 100)
  }

  test("test large writes") {
    val out = new FastByteArrayOutputStream(initialCapacity = 4096)
    out.write(Array.tabulate[Byte](4096 * 1000)(_.toByte))
    assert(out.length === 4096 * 1000)
    assert(out.toArray._1(0) === 0)
    assert(out.toArray._1(4096 * 1000 - 1) === (4096 * 1000 - 1).toByte)
    assert(out.toArray._2 === 4096 * 1000)

    out.write(Array.tabulate[Byte](4096 * 1000)(_.toByte))
    assert(out.length === 2 * 4096 * 1000)
    assert(out.toArray._1(0) === 0)
    assert(out.toArray._1(4096 * 1000) === 0)
    assert(out.toArray._1(2 * 4096 * 1000 - 1) === (4096 * 1000 - 1).toByte)
    assert(out.toArray._2 === 2 * 4096 * 1000)
  }

  test("trim") {
    val out = new FastByteArrayOutputStream(initialCapacity = 4096)
    out.write(1)
    assert(out.trim().toArray._2 === 1)

    val out1 = new FastByteArrayOutputStream(initialCapacity = 1)
    out1.write(1)
    assert(out1.trim().toArray._2 === 1)
  }
}
