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

import java.nio.ByteBuffer

import scala.util.Random

import org.apache.spark.SparkFunSuite


class ChunkedByteBufferOutputStreamSuite extends SparkFunSuite {

  test("empty output") {
    val o = new ChunkedByteBufferOutputStream(1024, ByteBuffer.allocate)
    o.close()
    assert(o.toChunkedByteBuffer.size === 0)
  }

  test("write a single byte") {
    val o = new ChunkedByteBufferOutputStream(1024, ByteBuffer.allocate)
    o.write(10)
    o.close()
    val chunkedByteBuffer = o.toChunkedByteBuffer
    assert(chunkedByteBuffer.getChunks().length === 1)
    assert(chunkedByteBuffer.getChunks().head.array().toSeq === Seq(10.toByte))
  }

  test("write a single near boundary") {
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(new Array[Byte](9))
    o.write(99)
    o.close()
    val chunkedByteBuffer = o.toChunkedByteBuffer
    assert(chunkedByteBuffer.getChunks().length === 1)
    assert(chunkedByteBuffer.getChunks().head.array()(9) === 99.toByte)
  }

  test("write a single at boundary") {
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(new Array[Byte](10))
    o.write(99)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 2)
    assert(arrays(1).length === 1)
    assert(arrays(1)(0) === 99.toByte)
  }

  test("single chunk output") {
    val ref = new Array[Byte](8)
    Random.nextBytes(ref)
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(ref)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 1)
    assert(arrays.head.length === ref.length)
    assert(arrays.head.toSeq === ref.toSeq)
  }

  test("single chunk output at boundary size") {
    val ref = new Array[Byte](10)
    Random.nextBytes(ref)
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(ref)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 1)
    assert(arrays.head.length === ref.length)
    assert(arrays.head.toSeq === ref.toSeq)
  }

  test("multiple chunk output") {
    val ref = new Array[Byte](26)
    Random.nextBytes(ref)
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(ref)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 3)
    assert(arrays(0).length === 10)
    assert(arrays(1).length === 10)
    assert(arrays(2).length === 6)

    assert(arrays(0).toSeq === ref.slice(0, 10))
    assert(arrays(1).toSeq === ref.slice(10, 20))
    assert(arrays(2).toSeq === ref.slice(20, 26))
  }

  test("multiple chunk output at boundary size") {
    val ref = new Array[Byte](30)
    Random.nextBytes(ref)
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(ref)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 3)
    assert(arrays(0).length === 10)
    assert(arrays(1).length === 10)
    assert(arrays(2).length === 10)

    assert(arrays(0).toSeq === ref.slice(0, 10))
    assert(arrays(1).toSeq === ref.slice(10, 20))
    assert(arrays(2).toSeq === ref.slice(20, 30))
  }

  test("SPARK-36464: size returns correct positive number even with over 2GB data") {
    val data4M = 1024 * 1024 * 4
    val writeTimes = 513
    val ref = new Array[Byte](data4M)
    val o = new ChunkedByteBufferOutputStream(data4M, ByteBuffer.allocate)
    (0 until writeTimes).foreach(_ => o.write(ref))
    o.close()
    assert(o.size > 0L) // make sure it is not overflowing
    assert(o.size == ref.length.toLong * writeTimes)
  }
}
