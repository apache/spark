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

import java.io.DataInputStream
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_ENABLED, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_RELIABLE}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.BlockManagerMessages.RemoveShuffle
import org.apache.spark.storage.FallbackStorage.asyncCopyExecutionContext
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A fallback storage used by storage decommissioners.
 */
private[storage] class FallbackStorage(
    conf: SparkConf,
    asyncCopies: ConcurrentMap[ShuffleBlockInfo, Future[Unit]]) extends Logging {
  require(conf.contains("spark.app.id"))
  require(FallbackStorage.isConfigured(conf))

  private val fallbackPath = FallbackStorage.getPath(conf)
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
  private val appId = conf.getAppId

  // Visible for testing
  def copy(
      shuffleBlockInfo: ShuffleBlockInfo,
      bm: BlockManager,
      isAsyncCopy: Boolean = false,
      reportBlockStatus: Boolean = true): Unit = {
    val shuffleId = shuffleBlockInfo.shuffleId
    val mapId = shuffleBlockInfo.mapId

    // wait for the ongoing async copy to finish
    if (!isAsyncCopy) {
      Option(asyncCopies.get(shuffleBlockInfo)).foreach { asyncCopy =>
        logInfo("Waiting for the ongoing async copy to finish: " +
          log"${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}")
        ThreadUtils.awaitResult(asyncCopy, Duration.Inf)
      }
    }

    // only copy the files if they don't yet exist on the fallback fs
    // they might have been pro-actively copied
    bm.migratableResolver match {
      case r: IndexShuffleBlockResolver =>
        val indexFile = r.getIndexFile(shuffleId, mapId)
        val fallbackIndexFilePath = getFallbackFilePath(shuffleId, indexFile.getName)
        val fallbackIndexFileExists = fallbackFileSystem.exists(fallbackIndexFilePath)
        if (fallbackIndexFileExists || indexFile.exists()) {
          if (!fallbackIndexFileExists) {
            fallbackFileSystem.copyFromLocalFile(
              new Path(Utils.resolveURI(indexFile.getAbsolutePath)),
              fallbackIndexFilePath)
          }

          val dataFile = r.getDataFile(shuffleId, mapId)
          val fallbackDataFilePath = getFallbackFilePath(shuffleId, dataFile.getName)
          val fallbackDataFileExist = fallbackFileSystem.exists(fallbackDataFilePath)
          if (!fallbackDataFileExist && dataFile.exists()) {
            fallbackFileSystem.copyFromLocalFile(
              new Path(Utils.resolveURI(dataFile.getAbsolutePath)),
              fallbackDataFilePath)
          }

          // Report block statuses
          if (reportBlockStatus) {
            val reduceId = NOOP_REDUCE_ID
            val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, reduceId)
            FallbackStorage.reportBlockStatus(bm, indexBlockId, indexFile.length)
            if (fallbackDataFileExist || dataFile.exists) {
              val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, reduceId)
              FallbackStorage.reportBlockStatus(bm, dataBlockId, dataFile.length)
            }
          }
        }
      case r =>
        logWarning(log"Unsupported Resolver: ${MDC(CLASS_NAME, r.getClass.getName)}")
    }
  }

  def copyAsync(
      shuffleBlockInfo: ShuffleBlockInfo,
      bm: BlockManager): Unit = {
    asyncCopies.computeIfAbsent(shuffleBlockInfo, _ => Future {
        logInfo(log"Starting copying shuffle block ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}")
        copy(shuffleBlockInfo, bm, isAsyncCopy = true, reportBlockStatus = false)
        logInfo(log"Finished copying shuffle block ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}")
      }(asyncCopyExecutionContext)
    ).andThen {
      case _ => asyncCopies.remove(shuffleBlockInfo)
    }(asyncCopyExecutionContext)
  }

  def getFallbackFilePath(shuffleId: Int, filename: String): Path =
    FallbackStorage.getFallbackFilePath(fallbackPath, appId, shuffleId, filename)

  private[storage] def exists(shuffleId: Int, mapId: Long): Boolean = {
    val indexName = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val indexFile = getFallbackFilePath(shuffleId, indexName)
    val dataName = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val dataFile = getFallbackFilePath(shuffleId, dataName)
    fallbackFileSystem.exists(indexFile) && fallbackFileSystem.exists(dataFile)
  }
}

private[storage] class FallbackStorageRpcEndpointRef(conf: SparkConf, hadoopConf: Configuration)
    extends RpcEndpointRef(conf) {
  // scalastyle:off executioncontextglobal
  import scala.concurrent.ExecutionContext.Implicits.global
  // scalastyle:on executioncontextglobal
  override def address: RpcAddress = null
  override def name: String = "fallback"
  override def send(message: Any): Unit = {}
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    message match {
      case RemoveShuffle(shuffleId) =>
        FallbackStorage.cleanUp(conf, hadoopConf, Some(shuffleId))
      case _ => // no-op
    }
    Future{true.asInstanceOf[T]}
  }
}

private[spark] object FallbackStorage extends Logging {
  /** We use one block manager id as a place holder. */
  val FALLBACK_BLOCK_MANAGER_ID: BlockManagerId = BlockManagerId("fallback", "remote", 7337)
  /** Holds a future for each async copy in progress. Removed by the future on completion. */
  val FALLBACK_ASYNC_COPIES: ConcurrentMap[ShuffleBlockInfo, Future[Unit]] =
    new ConcurrentHashMap[ShuffleBlockInfo, Future[Unit]]()

  def isConfigured(conf: SparkConf): Boolean =
    conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined

  def isProactive(conf: SparkConf): Boolean = {
    isConfigured(conf) &&
      conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_ENABLED)
  }

  def isReliable(conf: SparkConf): Boolean =
    isProactive(conf) &&
      conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_RELIABLE)

  def getPath(conf: SparkConf): Path =
    new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)

  private val asyncCopyExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("fallback-storage-async-copy", 16))

  def getFallbackStorage(conf: SparkConf): Option[FallbackStorage] = {
    if (isConfigured(conf)) {
      Some(new FallbackStorage(conf, FALLBACK_ASYNC_COPIES))
    } else {
      None
    }
  }

  /** Register the fallback block manager and its RPC endpoint. */
  def registerBlockManagerIfNeeded(
      master: BlockManagerMaster,
      conf: SparkConf,
      hadoopConf: Configuration): Unit = {
    if (isConfigured(conf)) {
      master.registerBlockManager(
        FALLBACK_BLOCK_MANAGER_ID, Array.empty[String], 0, 0,
        new FallbackStorageRpcEndpointRef(conf, hadoopConf))
    }
  }

  /** Clean up the generated fallback location for this app (and shuffle id if given). */
  def cleanUp(conf: SparkConf, hadoopConf: Configuration, shuffleId: Option[Int] = None): Unit = {
    if (isConfigured(conf) &&
        conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP) &&
        conf.contains("spark.app.id")) {
      val fallbackPath = shuffleId.foldLeft(
        new Path(FallbackStorage.getPath(conf), conf.getAppId)
      ) { case (path, shuffleId) => new Path(path, shuffleId.toString) }
      val fallbackUri = fallbackPath.toUri
      val fallbackFileSystem = FileSystem.get(fallbackUri, hadoopConf)
      // The fallback directory for this app may not be created yet.
      if (fallbackFileSystem.exists(fallbackPath)) {
        if (fallbackFileSystem.delete(fallbackPath, true)) {
          logInfo(log"Succeed to clean up: ${MDC(URI, fallbackUri)}")
        } else {
          // Clean-up can fail due to the permission issues.
          logWarning(log"Failed to clean up: ${MDC(URI, fallbackUri)}")
        }
      }
    }
  }

  /** Report block status to block manager master and map output tracker master. */
  private def reportBlockStatus(blockManager: BlockManager, blockId: BlockId, dataLength: Long) = {
    assert(blockManager.master != null)
    blockManager.master.updateBlockInfo(
      FALLBACK_BLOCK_MANAGER_ID, blockId, StorageLevel.DISK_ONLY, memSize = 0, dataLength)
  }

  private def getFallbackFilePath(
      fallbackPath: Path,
      appId: String,
      shuffleId: Int,
      filename: String): Path = {
    val hash = JavaUtils.nonNegativeHash(filename)
    new Path(fallbackPath, s"$appId/$shuffleId/$hash/$filename")
  }

  def getShuffleFiles(conf: SparkConf, blockId: BlockId): (FileSystem, Path, Path, Long, Long) = {
    val fallbackPath = FallbackStorage.getPath(conf)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
    val appId = conf.getAppId

    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw SparkException.internalError(
          s"unexpected shuffle block id format: $blockId", category = "STORAGE")
    }

    val indexName = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val indexFile = getFallbackFilePath(fallbackPath, appId, shuffleId, indexName)
    val dataName = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val dataFile = getFallbackFilePath(fallbackPath, appId, shuffleId, dataName)
    val start = startReduceId * 8L
    val end = endReduceId * 8L
    (fallbackFileSystem, indexFile, dataFile, start, end)
  }

  /**
   * Read a ManagedBuffer.
   */
  def read(conf: SparkConf, blockId: BlockId): ManagedBuffer = {
    logInfo(log"Read ${MDC(BLOCK_ID, blockId)}")
    val (fallbackFileSystem, indexFile, dataFile, start, end) = getShuffleFiles(conf, blockId)
    Utils.tryWithResource(fallbackFileSystem.open(indexFile)) { inputStream =>
      Utils.tryWithResource(new DataInputStream(inputStream)) { index =>
        index.skip(start)
        val offset = index.readLong()
        index.skip(end - (start + 8L))
        val nextOffset = index.readLong()
        val size = nextOffset - offset
        logDebug(s"To byte array $size")
        val array = new Array[Byte](size.toInt)
        val startTimeNs = System.nanoTime()
        Utils.tryWithResource(fallbackFileSystem.open(dataFile)) { f =>
          f.seek(offset)
          f.readFully(array)
          logDebug(s"Took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms")
        }
        new NioManagedBuffer(ByteBuffer.wrap(array))
      }
    }
  }
}
