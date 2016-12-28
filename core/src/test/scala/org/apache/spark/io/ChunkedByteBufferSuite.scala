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

package org.apache.spark.io

import java.nio.ByteBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.util.ByteArrayWritableChannel
import org.apache.spark.util.io.ChunkedByteBuffer

class ChunkedByteBufferSuite extends SparkFunSuite {

  test("no chunks") {
    val emptyChunkedByteBuffer = new ChunkedByteBuffer(Array.empty[ByteBuffer])
    assert(emptyChunkedByteBuffer.size === 0)
    assert(emptyChunkedByteBuffer.getChunks().isEmpty)
    assert(emptyChunkedByteBuffer.toArray === Array.empty)
    assert(emptyChunkedByteBuffer.toByteBuffer.capacity() === 0)
    assert(emptyChunkedByteBuffer.toNetty.capacity() === 0)
    emptyChunkedByteBuffer.toInputStream(dispose = false).close()
    emptyChunkedByteBuffer.toInputStream(dispose = true).close()
  }

  test("getChunks() duplicates chunks") {
    val chunkedByteBuffer = new ChunkedByteBuffer(Array(ByteBuffer.allocate(8)))
    chunkedByteBuffer.getChunks().head.position(4)
    assert(chunkedByteBuffer.getChunks().head.position() === 0)
  }

  test("copy() does not affect original buffer's position") {
    val chunkedByteBuffer = new ChunkedByteBuffer(Array(ByteBuffer.allocate(8)))
    chunkedByteBuffer.copy(ByteBuffer.allocate)
    assert(chunkedByteBuffer.getChunks().head.position() === 0)
  }

  test("writeFully() does not affect original buffer's position") {
    val chunkedByteBuffer = new ChunkedByteBuffer(Array(ByteBuffer.allocate(8)))
    chunkedByteBuffer.writeFully(new ByteArrayWritableChannel(chunkedByteBuffer.size.toInt))
    assert(chunkedByteBuffer.getChunks().head.position() === 0)
  }

  test("toArray()") {
    val empty = ByteBuffer.wrap(Array.empty[Byte])
    val bytes = ByteBuffer.wrap(Array.tabulate(8)(_.toByte))
    val chunkedByteBuffer = new ChunkedByteBuffer(Array(bytes, bytes, empty))
    assert(chunkedByteBuffer.toArray === bytes.array() ++ bytes.array())
  }

  test("toArray() throws UnsupportedOperationException if size exceeds 2GB") {
    val fourMegabyteBuffer = ByteBuffer.allocate(1024 * 1024 * 4)
    fourMegabyteBuffer.limit(fourMegabyteBuffer.capacity())
    val chunkedByteBuffer = new ChunkedByteBuffer(Array.fill(1024)(fourMegabyteBuffer))
    assert(chunkedByteBuffer.size === (1024L * 1024L * 1024L * 4L))
    intercept[UnsupportedOperationException] {
      chunkedByteBuffer.toArray
    }
  }

  test("toInputStream()") {
    val empty = ByteBuffer.wrap(Array.empty[Byte])
    val bytes1 = ByteBuffer.wrap(Array.tabulate(256)(_.toByte))
    val bytes2 = ByteBuffer.wrap(Array.tabulate(128)(_.toByte))
    val chunkedByteBuffer = new ChunkedByteBuffer(Array(empty, bytes1, bytes2))
    assert(chunkedByteBuffer.size === bytes1.limit() + bytes2.limit())

    val inputStream = chunkedByteBuffer.toInputStream(dispose = false)
    val bytesFromStream = new Array[Byte](chunkedByteBuffer.size.toInt)
    ByteStreams.readFully(inputStream, bytesFromStream)
    assert(bytesFromStream === bytes1.array() ++ bytes2.array())
    assert(chunkedByteBuffer.getChunks().head.position() === 0)
  }
}
