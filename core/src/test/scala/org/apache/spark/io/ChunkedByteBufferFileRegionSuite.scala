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
import java.nio.channels.WritableByteChannel

import scala.util.Random

import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.util.io.ChunkedByteBuffer

class ChunkedByteBufferFileRegionSuite extends SparkFunSuite with MockitoSugar
    with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf()
    val env = mock[SparkEnv]
    SparkEnv.set(env)
    when(env.conf).thenReturn(conf)
  }

  override protected def afterEach(): Unit = {
    SparkEnv.set(null)
  }

  private def generateChunkedByteBuffer(nChunks: Int, perChunk: Int): ChunkedByteBuffer = {
    val bytes = (0 until nChunks).map { chunkIdx =>
      val bb = ByteBuffer.allocate(perChunk)
      (0 until perChunk).foreach { idx =>
        bb.put((chunkIdx * perChunk + idx).toByte)
      }
      bb.position(0)
      bb
    }.toArray
    new ChunkedByteBuffer(bytes)
  }

  test("transferTo can stop and resume correctly") {
    SparkEnv.get.conf.set(config.BUFFER_WRITE_CHUNK_SIZE, 9L)
    val cbb = generateChunkedByteBuffer(4, 10)
    val fileRegion = cbb.toNetty

    val targetChannel = new LimitedWritableByteChannel(40)

    var pos = 0L
    // write the fileregion to the channel, but with the transfer limited at various spots along
    // the way.

    // limit to within the first chunk
    targetChannel.acceptNBytes = 5
    pos = fileRegion.transferTo(targetChannel, pos)
    assert(targetChannel.pos === 5)

    // a little bit further within the first chunk
    targetChannel.acceptNBytes = 2
    pos += fileRegion.transferTo(targetChannel, pos)
    assert(targetChannel.pos === 7)

    // past the first chunk, into the 2nd
    targetChannel.acceptNBytes = 6
    pos += fileRegion.transferTo(targetChannel, pos)
    assert(targetChannel.pos === 13)

    // right to the end of the 2nd chunk
    targetChannel.acceptNBytes = 7
    pos += fileRegion.transferTo(targetChannel, pos)
    assert(targetChannel.pos === 20)

    // rest of 2nd chunk, all of 3rd, some of 4th
    targetChannel.acceptNBytes = 15
    pos += fileRegion.transferTo(targetChannel, pos)
    assert(targetChannel.pos === 35)

    // now till the end
    targetChannel.acceptNBytes = 5
    pos += fileRegion.transferTo(targetChannel, pos)
    assert(targetChannel.pos === 40)

    // calling again at the end should be OK
    targetChannel.acceptNBytes = 20
    fileRegion.transferTo(targetChannel, pos)
    assert(targetChannel.pos === 40)
  }

  test(s"transfer to with random limits") {
    val rng = new Random()
    val seed = System.currentTimeMillis()
    logInfo(s"seed = $seed")
    rng.setSeed(seed)
    val chunkSize = 1e4.toInt
    SparkEnv.get.conf.set(config.BUFFER_WRITE_CHUNK_SIZE, rng.nextInt(chunkSize).toLong)

    val cbb = generateChunkedByteBuffer(50, chunkSize)
    val fileRegion = cbb.toNetty
    val transferLimit = 1e5.toInt
    val targetChannel = new LimitedWritableByteChannel(transferLimit)
    while (targetChannel.pos < cbb.size) {
      val nextTransferSize = rng.nextInt(transferLimit)
      targetChannel.acceptNBytes = nextTransferSize
      fileRegion.transferTo(targetChannel, targetChannel.pos)
    }
    assert(0 === fileRegion.transferTo(targetChannel, targetChannel.pos))
  }

  /**
   * This mocks a channel which only accepts a limited number of bytes at a time.  It also verifies
   * the written data matches our expectations as the data is received.
   */
  private class LimitedWritableByteChannel(maxWriteSize: Int) extends WritableByteChannel {
    val bytes = new Array[Byte](maxWriteSize)
    var acceptNBytes = 0
    var pos = 0

    override def write(src: ByteBuffer): Int = {
      val length = math.min(acceptNBytes, src.remaining())
      src.get(bytes, 0, length)
      acceptNBytes -= length
      // verify we got the right data
      (0 until length).foreach { idx =>
        assert(bytes(idx) === (pos + idx).toByte, s"; wrong data at ${pos + idx}")
      }
      pos += length
      length
    }

    override def isOpen: Boolean = true

    override def close(): Unit = {}
  }

}
