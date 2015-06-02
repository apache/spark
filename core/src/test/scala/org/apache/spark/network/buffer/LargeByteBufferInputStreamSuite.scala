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
package org.apache.spark.network.buffer

import java.io.{File, FileInputStream, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel.MapMode

import org.junit.Assert._
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class LargeByteBufferInputStreamSuite extends SparkFunSuite with Matchers {

  test("read from large mapped file") {
    val testFile = File.createTempFile("large-buffer-input-stream-test", ".bin")

    try {
      val out: OutputStream = new FileOutputStream(testFile)
      val buffer: Array[Byte] = new Array[Byte](1 << 16)
      val len: Long = buffer.length.toLong + Integer.MAX_VALUE + 1
      (0 until buffer.length).foreach { idx =>
        buffer(idx) = idx.toByte
      }
      (0 until (len / buffer.length).toInt).foreach { idx =>
        out.write(buffer)
      }
      out.close

      val channel = new FileInputStream(testFile).getChannel
      val buf = LargeByteBufferHelper.mapFile(channel, MapMode.READ_ONLY, 0, len)
      val in = new LargeByteBufferInputStream(buf, true)

      val read = new Array[Byte](buffer.length)
      (0 until (len / buffer.length).toInt).foreach { idx =>
        in.disposed should be(false)
        in.read(read) should be(read.length)
        (0 until buffer.length).foreach { arrIdx =>
          assertEquals(buffer(arrIdx), read(arrIdx))
        }
      }
      in.disposed should be(false)
      in.read(read) should be(-1)
      in.disposed should be(false)
      in.close()
      in.disposed should be(true)
    } finally {
      testFile.delete()
    }
  }

  test("dispose on close") {
    // don't need to read to the end -- dispose anytime we close
    val data = new Array[Byte](10)
    val in = new LargeByteBufferInputStream(LargeByteBufferHelper.asLargeByteBuffer(data), true)
    in.disposed should be (false)
    in.close()
    in.disposed should be (true)
  }

  test("io stream roundtrip") {
    val out = new LargeByteBufferOutputStream(128)
    (0 until 200).foreach{idx => out.write(idx)}
    out.close()

    val lb = out.largeBuffer(128)
    // just make sure that we test reading from multiple chunks
    lb.asInstanceOf[WrappedLargeByteBuffer].underlying.size should be > 1

    val rawIn = new LargeByteBufferInputStream(lb)
    val arr = new Array[Byte](500)
    val nRead = rawIn.read(arr, 0, 500)
    nRead should be (200)
    (0 until 200).foreach{idx =>
      arr(idx) should be (idx.toByte)
    }
  }

}
