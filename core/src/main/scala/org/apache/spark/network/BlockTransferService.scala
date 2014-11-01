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

import java.io.Closeable
import java.nio.ByteBuffer

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.Logging
import org.apache.spark.network.buffer.{NioManagedBuffer, ManagedBuffer}
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

private[spark]
abstract class BlockTransferService extends Closeable with Logging {

  /**
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks.
   */
  def init(blockDataManager: BlockDataManager)

  /**
   * Tear down the transfer service.
   */
  def close(): Unit

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  def port: Int

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  def hostName: String

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   * available only after [[init]] is invoked.
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   */
  def fetchBlocks(
      hostName: String,
      port: Int,
      blockIds: Seq[String],
      listener: BlockFetchingListener): Unit

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel): Future[Unit]

  /**
   * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
   *
   * It is also only available after [[init]] is invoked.
   */
  def fetchBlockSync(hostName: String, port: Int, blockId: String): ManagedBuffer = {
    // A monitor for the thread to wait on.
    val lock = new Object
    @volatile var result: Either[ManagedBuffer, Throwable] = null
    fetchBlocks(hostName, port, Seq(blockId), new BlockFetchingListener {
      override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
        lock.synchronized {
          result = Right(exception)
          lock.notify()
        }
      }
      override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
        lock.synchronized {
          val ret = ByteBuffer.allocate(data.size.toInt)
          ret.put(data.nioByteBuffer())
          ret.flip()
          result = Left(new NioManagedBuffer(ret))
          lock.notify()
        }
      }
    })

    // Sleep until result is no longer null
    lock.synchronized {
      while (result == null) {
        try {
          lock.wait()
        } catch {
          case e: InterruptedException =>
        }
      }
    }

    result match {
      case Left(data) => data
      case Right(e) => throw e
    }
  }

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   *
   * This method is similar to [[uploadBlock]], except this one blocks the thread
   * until the upload finishes.
   */
  def uploadBlockSync(
      hostname: String,
      port: Int,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel): Unit = {
    Await.result(uploadBlock(hostname, port, blockId, blockData, level), Duration.Inf)
  }
}
