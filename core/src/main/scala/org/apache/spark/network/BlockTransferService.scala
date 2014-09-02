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

import org.apache.spark.storage.StorageLevel


abstract class BlockTransferService {

  /**
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks.
   */
  def init(blockDataManager: BlockDataManager)

  /**
   * Tear down the transfer service.
   */
  def stop(): Unit

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  def port: Int

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  def hostName: String

  /**
   * Fetch a sequence of blocks from a remote node, available only after [[init]] is invoked.
   *
   * This takes a sequence so the implementation can batch requests.
   */
  def fetchBlocks(
      hostName: String,
      port: Int,
      blockIds: Seq[String],
      listener: BlockFetchingListener): Unit

  /**
   * Fetch a single block from a remote node, available only after [[init]] is invoked.
   *
   * This is functionally equivalent to
   * {{{
   *   fetchBlocks(hostName, port, Seq(blockId)).iterator().next()._2
   * }}}
   */
  def fetchBlock(hostName: String, port: Int, blockId: String): ManagedBuffer = {
    // TODO(rxin): Add timeout?
    val lock = new Object
    @volatile var result: Either[ManagedBuffer, Exception] = null
    fetchBlocks(hostName, port, Seq(blockId), new BlockFetchingListener {
      override def onBlockFetchFailure(exception: Exception): Unit = {
        lock.synchronized {
          result = Right(exception)
          lock.notify()
        }
      }
      override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
        lock.synchronized {
          result = Left(data)
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
      case Left(data: ManagedBuffer) => data
      case Right(e: Exception) => throw e
    }
  }

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   *
   * This call blocks until the upload completes, or throws an exception upon failures.
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      blockId: String,
      blockData: ManagedBuffer,
      level: StorageLevel): Unit
}
