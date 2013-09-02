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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest.FunSuite


class CompressionCodecSuite extends FunSuite {

  def testCodec(codec: CompressionCodec) {
    // Write 1000 integers to the output stream, compressed.
    val outputStream = new ByteArrayOutputStream()
    val out = codec.compressedOutputStream(outputStream)
    for (i <- 1 until 1000) {
      out.write(i % 256)
    }
    out.close()

    // Read the 1000 integers back.
    val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
    val in = codec.compressedInputStream(inputStream)
    for (i <- 1 until 1000) {
      assert(in.read() === i % 256)
    }
    in.close()
  }

  test("default compression codec") {
    val codec = CompressionCodec.createCodec()
    assert(codec.getClass === classOf[LZFCompressionCodec])
    testCodec(codec)
  }

  test("lzf compression codec") {
    val codec = CompressionCodec.createCodec(classOf[LZFCompressionCodec].getName)
    assert(codec.getClass === classOf[LZFCompressionCodec])
    testCodec(codec)
  }

  test("snappy compression codec") {
    val codec = CompressionCodec.createCodec(classOf[SnappyCompressionCodec].getName)
    assert(codec.getClass === classOf[SnappyCompressionCodec])
    testCodec(codec)
  }
}
