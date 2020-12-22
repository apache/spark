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

import java.nio.ByteBuffer

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.storage.{BlockId, EncryptedManagedBuffer, StorageLevel}
import org.apache.spark.util.ThreadUtils

/**
 * The BlockTransferService that used for fetching a set of blocks at time. Each instance of
 * BlockTransferService contains both client and server inside.
 */
private[spark]
abstract class BlockTransferService extends BlockStoreClient {

  /**
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks. The fetchBlocks method in [[BlockStoreClient]] also
   * available only after this is invoked.
   */
  def init(blockDataManager: BlockDataManager): Unit

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  def port: Int

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  def hostName: String

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit]

  /**
   * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
   *
   * It is also only available after [[init]] is invoked.
   */
  def fetchBlockSync(
      host: String,
      port: Int,
      execId: String,
      blockId: String,
      tempFileManager: DownloadFileManager): ManagedBuffer = {
    // A monitor for the thread to wait on.
    val result = Promise[ManagedBuffer]()
    fetchBlocks(host, port, execId, Array(blockId),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          data match {
            case f: FileSegmentManagedBuffer =>
              result.success(f)
            case e: EncryptedManagedBuffer =>
              result.success(e)
            case _ =>
              try {
                val ret = ByteBuffer.allocate(data.size.toInt)
                ret.put(data.nioByteBuffer())
                ret.flip()
                result.success(new NioManagedBuffer(ret))
              } catch {
                case e: Throwable => result.failure(e)
              }
          }
        }
      }, tempFileManager)
    ThreadUtils.awaitResult(result.future, Duration.Inf)
  }

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   *
   * This method is similar to [[uploadBlock]], except this one blocks the thread
   * until the upload finishes.
   */
  @throws[java.io.IOException]
  def uploadBlockSync(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Unit = {
    val future = uploadBlock(hostname, port, execId, blockId, blockData, level, classTag)
    ThreadUtils.awaitResult(future, Duration.Inf)
  }
}
