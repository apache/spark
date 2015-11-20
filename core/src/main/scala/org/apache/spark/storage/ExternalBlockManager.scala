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

/**
 * An abstract class that the concrete external block manager has to inherit.
 * The class has to have a no-argument constructor, and will be initialized by init,
 * which is invoked by ExternalBlockStore. The main input parameter is blockId for all
 * the methods, which is the unique identifier for Block in one Spark application.
 *
 * The underlying external block manager should avoid any name space conflicts  among multiple
 * Spark applications. For example, creating different directory for different applications
 * by randomUUID
 *
 */
private[spark] abstract class ExternalBlockManager {

  protected var blockManager: BlockManager = _

  override def toString: String = {"External Block Store"}

  /**
   * Initialize a concrete block manager implementation. Subclass should initialize its internal
   * data structure, e.g, file system, in this function, which is invoked by ExternalBlockStore
   * right after the class is constructed. The function should throw IOException on failure
   *
   * @throws java.io.IOException if there is any file system failure during the initialization.
   */
  def init(blockManager: BlockManager, executorId: String): Unit = {
    this.blockManager = blockManager
  }

  /**
   * Drop the block from underlying external block store, if it exists..
   * @return true on successfully removing the block
   *         false if the block could not be removed as it was not found
   *
   * @throws java.io.IOException if there is any file system failure in removing the block.
   */
  def removeBlock(blockId: BlockId): Boolean

  /**
   * Used by BlockManager to check the existence of the block in the underlying external
   * block store.
   * @return true if the block exists.
   *         false if the block does not exists.
   *
   * @throws java.io.IOException if there is any file system failure in checking
   *                             the block existence.
   */
  def blockExists(blockId: BlockId): Boolean

  /**
   * Put the given block to the underlying external block store. Note that in normal case,
   * putting a block should never fail unless something wrong happens to the underlying
   * external block store, e.g., file system failure, etc. In this case, IOException
   * should be thrown.
   *
   * @throws java.io.IOException if there is any file system failure in putting the block.
   */
  def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit

  def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val bytes = blockManager.dataSerialize(blockId, values)
    putBytes(blockId, bytes)
  }

  /**
   * Retrieve the block bytes.
   * @return Some(ByteBuffer) if the block bytes is successfully retrieved
   *         None if the block does not exist in the external block store.
   *
   * @throws java.io.IOException if there is any file system failure in getting the block.
   */
  def getBytes(blockId: BlockId): Option[ByteBuffer]

  /**
   * Retrieve the block data.
   * @return Some(Iterator[Any]) if the block data is successfully retrieved
   *         None if the block does not exist in the external block store.
   *
   * @throws java.io.IOException if there is any file system failure in getting the block.
   */
  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  /**
   * Get the size of the block saved in the underlying external block store,
   * which is saved before by putBytes.
   * @return size of the block
   *         0 if the block does not exist
   *
   * @throws java.io.IOException if there is any file system failure in getting the block size.
   */
  def getSize(blockId: BlockId): Long

  /**
   * Clean up any information persisted in the underlying external block store,
   * e.g., the directory, files, etc,which is invoked by the shutdown hook of ExternalBlockStore
   * during system shutdown.
   *
   */
  def shutdown()
}
