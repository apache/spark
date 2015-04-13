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

package org.apache.spark.storage

import java.nio.ByteBuffer
import org.apache.spark.Logging

import scala.util.control.NonFatal


private[spark] abstract class ExtBlockManager {

  /**
   * desc for the implementation.
   *
   */
  def desc(): String = {"External Block Store"}

  /**
   * initialize a concrete block manager implementation.
   *
   * @throws java.io.IOException when FS init failure.
   */
  def init(blockManager: BlockManager, executorId: String): Unit

  /**
   * remove the cache from ExtBlkStore
   *
   * @throws java.io.IOException when FS failure in removing file.
   */
  def removeFile(blockId: BlockId): Boolean

  /**
   * check the existence of the block cache
   *
   * @throws java.io.IOException when FS failure in checking the block existence.
   */
  def fileExists(blockId: BlockId): Boolean

  /**
   * save the cache to the ExtBlkStore.
   *
   * @throws java.io.IOException when FS failure in put blocks.
   */
  def putBytes(blockId: BlockId, bytes: ByteBuffer)

  /**
   * retrieve the cache from ExtBlkStore
   *
   * @throws java.io.IOException when FS failure in get blocks.
   */
  def getBytes(blockId: BlockId): Option[ByteBuffer]

  /**
   * retrieve the size of the cache
   *
   * @throws java.io.IOException when FS failure in get block size.
   */
  def getSize(blockId: BlockId): Long

  /**
   * cleanup when shutdown
   *
   */
  def addShutdownHook()
}
