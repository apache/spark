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

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkFunSuite}

class CompressionCodecSuite extends SparkFunSuite {
  val conf = new SparkConf(false)

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
    val codec = CompressionCodec.createCodec(conf)
    assert(codec.getClass === classOf[LZ4CompressionCodec])
    testCodec(codec)
  }

  test("lz4 compression codec") {
    val codec = CompressionCodec.createCodec(conf, classOf[LZ4CompressionCodec].getName)
    assert(codec.getClass === classOf[LZ4CompressionCodec])
    testCodec(codec)
  }

  test("lz4 compression codec short form") {
    val codec = CompressionCodec.createCodec(conf, "lz4")
    assert(codec.getClass === classOf[LZ4CompressionCodec])
    testCodec(codec)
  }

  test("lz4 supports concatenation of serialized streams") {
    val codec = CompressionCodec.createCodec(conf, classOf[LZ4CompressionCodec].getName)
    assert(codec.getClass === classOf[LZ4CompressionCodec])
    testConcatenationOfSerializedStreams(codec)
  }

  test("lzf compression codec") {
    val codec = CompressionCodec.createCodec(conf, classOf[LZFCompressionCodec].getName)
    assert(codec.getClass === classOf[LZFCompressionCodec])
    testCodec(codec)
  }

  test("lzf compression codec short form") {
    val codec = CompressionCodec.createCodec(conf, "lzf")
    assert(codec.getClass === classOf[LZFCompressionCodec])
    testCodec(codec)
  }

  test("lzf supports concatenation of serialized streams") {
    val codec = CompressionCodec.createCodec(conf, classOf[LZFCompressionCodec].getName)
    assert(codec.getClass === classOf[LZFCompressionCodec])
    testConcatenationOfSerializedStreams(codec)
  }

  test("snappy compression codec") {
    val codec = CompressionCodec.createCodec(conf, classOf[SnappyCompressionCodec].getName)
    assert(codec.getClass === classOf[SnappyCompressionCodec])
    testCodec(codec)
  }

  test("snappy compression codec short form") {
    val codec = CompressionCodec.createCodec(conf, "snappy")
    assert(codec.getClass === classOf[SnappyCompressionCodec])
    testCodec(codec)
  }

  test("snappy supports concatenation of serialized streams") {
    val codec = CompressionCodec.createCodec(conf, classOf[SnappyCompressionCodec].getName)
    assert(codec.getClass === classOf[SnappyCompressionCodec])
    testConcatenationOfSerializedStreams(codec)
  }

  test("zstd compression codec") {
    val codec = CompressionCodec.createCodec(conf, classOf[ZStdCompressionCodec].getName)
    assert(codec.getClass === classOf[ZStdCompressionCodec])
    testCodec(codec)
  }

  test("zstd compression codec short form") {
    val codec = CompressionCodec.createCodec(conf, "zstd")
    assert(codec.getClass === classOf[ZStdCompressionCodec])
    testCodec(codec)
  }

  test("zstd supports concatenation of serialized zstd") {
    val codec = CompressionCodec.createCodec(conf, classOf[ZStdCompressionCodec].getName)
    assert(codec.getClass === classOf[ZStdCompressionCodec])
    testConcatenationOfSerializedStreams(codec)
  }

  test("bad compression codec") {
    intercept[IllegalArgumentException] {
      CompressionCodec.createCodec(conf, "foobar")
    }
  }

  private def testConcatenationOfSerializedStreams(codec: CompressionCodec): Unit = {
    val bytes1: Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val out = codec.compressedOutputStream(baos)
      (0 to 64).foreach(out.write)
      out.close()
      baos.toByteArray
    }
    val bytes2: Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val out = codec.compressedOutputStream(baos)
      (65 to 127).foreach(out.write)
      out.close()
      baos.toByteArray
    }
    val concatenatedBytes = codec.compressedInputStream(new ByteArrayInputStream(bytes1 ++ bytes2))
    val decompressed: Array[Byte] = new Array[Byte](128)
    ByteStreams.readFully(concatenatedBytes, decompressed)
    assert(decompressed.toSeq === (0 to 127))
  }
}
