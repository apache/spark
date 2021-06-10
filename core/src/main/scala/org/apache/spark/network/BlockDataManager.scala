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

import scala.reflect.ClassTag

import org.apache.spark.TaskContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.storage.{BlockId, StorageLevel}

private[spark]
trait BlockDataManager {

  /**
   * Get the local directories that used by BlockManager to save the blocks to disk
   */
  def getLocalDiskDirs: Array[String]

  /**
   * Interface to get host-local shuffle block data. Throws an exception if the block cannot be
   * found or cannot be read successfully.
   */
  def getHostLocalShuffleData(blockId: BlockId, dirs: Array[String]): ManagedBuffer

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  def getLocalBlockData(blockId: BlockId): ManagedBuffer

  /**
   * Put the block locally, using the given storage level.
   *
   * Returns true if the block was stored and false if the put operation failed or the block
   * already existed.
   */
  def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean

  /**
   * Put the given block that will be received as a stream.
   *
   * When this method is called, the block data itself is not available -- it will be passed to the
   * returned StreamCallbackWithID.
   */
  def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]): StreamCallbackWithID

  /**
   * Release locks acquired by [[putBlockData()]] and [[getLocalBlockData()]].
   */
  def releaseLock(blockId: BlockId, taskContext: Option[TaskContext]): Unit
}
