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

import java.io.InputStream
import org.scalatest.FunSuite


class LengthBoundedInputStreamSuite extends FunSuite {

  /** A test InputStream that returns 0, 1, 2, ..., (n - 1) and then end of stream (-1) */
  class SequenceInputStream(n: Int) extends InputStream {
    var pos = 0
    var closed = false
    override def read(): Int = {
      if (pos < n) {
        val ret = pos
        pos += 1
        ret
      } else {
        -1
      }
    }

    override def close(): Unit = {
      closed = true
    }
  }

  test("limit == 0") {
    var is = new LengthBoundedInputStream(new SequenceInputStream(3), 0)
    assert(is.read() === -1)
    is = new LengthBoundedInputStream(new SequenceInputStream(3), 0)
    assert(is.read(new Array[Byte](1)) === -1)
  }

  test("limit < underlying implementation") {
    var is = new LengthBoundedInputStream(new SequenceInputStream(3), 2)
    assert(is.read() === 0)
    assert(is.read() === 1)
    assert(is.read() === -1)
    assert(is.read() === -1)

    is = new LengthBoundedInputStream(new SequenceInputStream(3), 2)
    val bytes = new Array[Byte](2)
    assert(is.read(bytes) === 2)
    assert(bytes.toSeq === (0 until 2).map(_.toByte))

    val bytes4 = new Array[Byte](3)
    assert(is.read(bytes) === -1)
  }

  test("limit == underlying implementation") {
    var is = new LengthBoundedInputStream(new SequenceInputStream(3), 3)
    assert(is.read() === 0)
    assert(is.read() === 1)
    assert(is.read() === 2)
    assert(is.read() === -1)
    assert(is.read() === -1)

    is = new LengthBoundedInputStream(new SequenceInputStream(3), 3)
    val bytes = new Array[Byte](3)
    assert(is.read(bytes) === 3)
    assert(bytes.toSeq === (0 until 3).map(_.toByte))

    val bytes4 = new Array[Byte](4)
    assert(is.read(bytes) === -1)
  }

  test("limit > underlying implementation") {
    var is = new LengthBoundedInputStream(new SequenceInputStream(3), 5)
    assert(is.read() === 0)
    assert(is.read() === 1)
    assert(is.read() === 2)
    assert(is.read() === -1)
    assert(is.read() === -1)

    is = new LengthBoundedInputStream(new SequenceInputStream(3), 5)
    val bytes = new Array[Byte](3)
    assert(is.read(bytes) === 3)
    assert(bytes.toSeq === (0 until 3).map(_.toByte))

    val bytes4 = new Array[Byte](4)
    assert(is.read(bytes) === -1)
  }

  test("close() is propogated") {
    val underlying = new SequenceInputStream(3)
    assert(underlying.closed === false)
    val is = new LengthBoundedInputStream(underlying, 5)
    is.close()
    assert(underlying.closed === true)
  }
}
