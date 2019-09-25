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

package org.apache.spark.network

import java.io.InputStream
import java.nio.ByteBuffer

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

import org.scalatest.concurrent._

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.storage.{BlockId, StorageLevel}

class BlockTransferServiceSuite extends SparkFunSuite with TimeLimits {

  implicit val defaultSignaler: Signaler = ThreadSignaler

  test("fetchBlockSync should not hang when BlockFetchingListener.onBlockFetchSuccess fails") {
    // Create a mocked `BlockTransferService` to call `BlockFetchingListener.onBlockFetchSuccess`
    // with a bad `ManagedBuffer` which will trigger an exception in `onBlockFetchSuccess`.
    val blockTransferService = new BlockTransferService {
      override def init(blockDataManager: BlockDataManager): Unit = {}

      override def close(): Unit = {}

      override def port: Int = 0

      override def hostName: String = "localhost-unused"

      override def fetchBlocks(
          host: String,
          port: Int,
          execId: String,
          blockIds: Array[String],
          listener: BlockFetchingListener,
          tempFileManager: DownloadFileManager): Unit = {
        // Notify BlockFetchingListener with a bad ManagedBuffer asynchronously
        new Thread() {
          override def run(): Unit = {
            // This is a bad buffer to trigger `IllegalArgumentException` in
            // `BlockFetchingListener.onBlockFetchSuccess`. The real issue we hit is
            // `ByteBuffer.allocate` throws `OutOfMemoryError`, but we cannot make it happen in
            // a test. Instead, we use a negative size value to make `ByteBuffer.allocate` fail,
            // and this should trigger the same code path as `OutOfMemoryError`.
            val badBuffer = new ManagedBuffer {
              override def size(): Long = -1

              override def nioByteBuffer(): ByteBuffer = null

              override def createInputStream(): InputStream = null

              override def retain(): ManagedBuffer = this

              override def release(): ManagedBuffer = this

              override def convertToNetty(): AnyRef = null
            }
            listener.onBlockFetchSuccess("block-id-unused", badBuffer)
          }
        }.start()
      }

      override def uploadBlock(
          hostname: String,
          port: Int,
          execId: String,
          blockId: BlockId,
          blockData: ManagedBuffer,
          level: StorageLevel,
          classTag: ClassTag[_]): Future[Unit] = {
        // This method is unused in this test
        throw new UnsupportedOperationException("uploadBlock")
      }
    }

    val e = intercept[SparkException] {
      failAfter(10.seconds) {
        blockTransferService.fetchBlockSync(
          "localhost-unused", 0, "exec-id-unused", "block-id-unused", null)
      }
    }
    assert(e.getCause.isInstanceOf[IllegalArgumentException])
  }
}
