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

import scala.util.Random

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.network.buffer.WrappedLargeByteBuffer

class LargeByteBufferOutputStreamSuite extends FunSuite with Matchers {

  test("merged buffers for < 2GB") {
    val out = new LargeByteBufferOutputStream(10)
    val bytes = new Array[Byte](100)
    Random.nextBytes(bytes)
    out.write(bytes)

    val buffer = out.largeBuffer
    buffer.position() should be (0)
    buffer.size() should be (100)
    val nioBuffer = buffer.asByteBuffer()
    nioBuffer.position() should be (0)
    nioBuffer.capacity() should be (100)
    nioBuffer.limit() should be (100)

    val read = new Array[Byte](100)
    buffer.get(read, 0, 100)
    read should be (bytes)

    buffer.rewind()
    nioBuffer.get(read)
    read should be (bytes)
  }

  test("chunking") {
    val out = new LargeByteBufferOutputStream(10)
    val bytes = new Array[Byte](100)
    Random.nextBytes(bytes)
    out.write(bytes)

    (10 to 100 by 10).foreach { chunkSize =>
      val buffer = out.largeBuffer(chunkSize).asInstanceOf[WrappedLargeByteBuffer]
      buffer.position() should be (0)
      buffer.size() should be (100)
      val read = new Array[Byte](100)
      buffer.get(read, 0, 100)
      read should be (bytes)
    }

  }

}
