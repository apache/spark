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

import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.util.Utils


/**
 * Stores BlockManager blocks on ExternalBlockStore.
 * We capture any potential exception from underlying implementation
 * and return with the expected failure value
 */
private[spark] class ExternalBlockStore(blockManager: BlockManager, executorId: String)
  extends BlockStore(blockManager: BlockManager) with Logging {

  lazy val externalBlockManager: Option[ExternalBlockManager] = createBlkManager()

  logInfo("ExternalBlockStore started")

  override def getSize(blockId: BlockId): Long = {
    try {
      externalBlockManager.map(_.getSize(blockId)).getOrElse(0)
    } catch {
      case NonFatal(t) =>
        logError(s"Error in getSize($blockId)", t)
        0L
    }
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel): PutResult = {
    putIntoExternalBlockStore(blockId, bytes, returnValues = true)
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIntoExternalBlockStore(blockId, values.toIterator, returnValues)
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIntoExternalBlockStore(blockId, values, returnValues)
  }

  private def putIntoExternalBlockStore(
      blockId: BlockId,
      values: Iterator[_],
      returnValues: Boolean): PutResult = {
    logTrace(s"Attempting to put block $blockId into ExternalBlockStore")
    // we should never hit here if externalBlockManager is None. Handle it anyway for safety.
    try {
      val startTime = System.currentTimeMillis
      if (externalBlockManager.isDefined) {
        externalBlockManager.get.putValues(blockId, values)
        val size = getSize(blockId)
        val data = if (returnValues) {
          Left(getValues(blockId).get)
        } else {
          null
        }
        val finishTime = System.currentTimeMillis
        logDebug("Block %s stored as %s file in ExternalBlockStore in %d ms".format(
          blockId, Utils.bytesToString(size), finishTime - startTime))
        PutResult(size, data)
      } else {
        logError(s"Error in putValues($blockId): no ExternalBlockManager has been configured")
        PutResult(-1, null, Seq((blockId, BlockStatus.empty)))
      }
    } catch {
      case NonFatal(t) =>
        logError(s"Error in putValues($blockId)", t)
        PutResult(-1, null, Seq((blockId, BlockStatus.empty)))
    }
  }

  private def putIntoExternalBlockStore(
      blockId: BlockId,
      bytes: ByteBuffer,
      returnValues: Boolean): PutResult = {
    logTrace(s"Attempting to put block $blockId into ExternalBlockStore")
    // we should never hit here if externalBlockManager is None. Handle it anyway for safety.
    try {
      val startTime = System.currentTimeMillis
      if (externalBlockManager.isDefined) {
        val byteBuffer = bytes.duplicate()
        byteBuffer.rewind()
        externalBlockManager.get.putBytes(blockId, byteBuffer)
        val size = bytes.limit()
        val data = if (returnValues) {
          Right(bytes)
        } else {
          null
        }
        val finishTime = System.currentTimeMillis
        logDebug("Block %s stored as %s file in ExternalBlockStore in %d ms".format(
          blockId, Utils.bytesToString(size), finishTime - startTime))
        PutResult(size, data)
      } else {
        logError(s"Error in putBytes($blockId): no ExternalBlockManager has been configured")
        PutResult(-1, null, Seq((blockId, BlockStatus.empty)))
      }
    } catch {
      case NonFatal(t) =>
        logError(s"Error in putBytes($blockId)", t)
        PutResult(-1, null, Seq((blockId, BlockStatus.empty)))
    }
  }

  // We assume the block is removed even if exception thrown
  override def remove(blockId: BlockId): Boolean = {
    try {
      externalBlockManager.map(_.removeBlock(blockId)).getOrElse(true)
    } catch {
      case NonFatal(t) =>
        logError(s"Error in removeBlock($blockId)", t)
        true
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    try {
      externalBlockManager.flatMap(_.getValues(blockId))
    } catch {
      case NonFatal(t) =>
        logError(s"Error in getValues($blockId)", t)
        None
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    try {
      externalBlockManager.flatMap(_.getBytes(blockId))
    } catch {
      case NonFatal(t) =>
        logError(s"Error in getBytes($blockId)", t)
        None
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    try {
      val ret = externalBlockManager.map(_.blockExists(blockId)).getOrElse(false)
      if (!ret) {
        logInfo(s"Remove block $blockId")
        blockManager.removeBlock(blockId, true)
      }
      ret
    } catch {
      case NonFatal(t) =>
        logError(s"Error in getBytes($blockId)", t)
        false
    }
  }

  private def addShutdownHook() {
    Runtime.getRuntime.addShutdownHook(new Thread("ExternalBlockStore shutdown hook") {
      override def run(): Unit = Utils.logUncaughtExceptions {
        logDebug("Shutdown hook called")
        externalBlockManager.map(_.shutdown())
      }
    })
  }

  // Create concrete block manager and fall back to Tachyon by default for backward compatibility.
  private def createBlkManager(): Option[ExternalBlockManager] = {
    val clsName = blockManager.conf.getOption(ExternalBlockStore.BLOCK_MANAGER_NAME)
      .getOrElse(ExternalBlockStore.DEFAULT_BLOCK_MANAGER_NAME)

    try {
      val instance = Utils.classForName(clsName)
        .newInstance()
        .asInstanceOf[ExternalBlockManager]
      instance.init(blockManager, executorId)
      addShutdownHook();
      Some(instance)
    } catch {
      case NonFatal(t) =>
        logError("Cannot initialize external block store", t)
        None
    }
  }
}

private[spark] object ExternalBlockStore extends Logging {
  val MAX_DIR_CREATION_ATTEMPTS = 10
  val SUB_DIRS_PER_DIR = "64"
  val BASE_DIR = "spark.externalBlockStore.baseDir"
  val FOLD_NAME = "spark.externalBlockStore.folderName"
  val MASTER_URL = "spark.externalBlockStore.url"
  val BLOCK_MANAGER_NAME = "spark.externalBlockStore.blockManager"
  val DEFAULT_BLOCK_MANAGER_NAME = "org.apache.spark.storage.TachyonBlockManager"
}
