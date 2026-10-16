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

package org.apache.spark.shuffle

import java.io.File
import java.util.Optional

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.commons.io.FileExistsException

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_DRIVER_REUSE_PVC
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config.{SHUFFLE_CHECKSUM_ALGORITHM, SHUFFLE_CHECKSUM_ENABLED}
import org.apache.spark.shuffle.ShuffleChecksumUtils.{compareChecksums, getChecksumFileName}
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, SingleSpillShuffleMapOutputWriter}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleDataBlockId, StorageLevel, UnrecognizedBlockId}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

class KubernetesLocalDiskShuffleExecutorComponents(sparkConf: SparkConf)
  extends ShuffleExecutorComponents with Logging {

  private val delegate = new LocalDiskShuffleExecutorComponents(sparkConf)
  private var blockManager: BlockManager = _

  override def initializeExecutor(
      appId: String, execId: String, extraConfigs: java.util.Map[String, String]): Unit = {
    delegate.initializeExecutor(appId, execId, extraConfigs)
    blockManager = SparkEnv.get.blockManager
    if (sparkConf.getBoolean(KUBERNETES_DRIVER_REUSE_PVC.key, false)) {
      logInfo("Try to recover shuffle data.")
      // Turn off the deletion of the shuffle data in order to reuse
      blockManager.diskBlockManager.deleteFilesOnStop = false
      Utils.tryLogNonFatalError {
        KubernetesLocalDiskShuffleExecutorComponents.recoverDiskStore(sparkConf, blockManager)
      }
    } else {
      logInfo(log"Skip recovery because ${MDC(LogKeys.CONFIG, KUBERNETES_DRIVER_REUSE_PVC.key)} " +
        log"is disabled.")
    }
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int)
    : ShuffleMapOutputWriter = {
    delegate.createMapOutputWriter(shuffleId, mapTaskId, numPartitions)
  }

  override def createSingleFileMapOutputWriter(shuffleId: Int, mapId: Long)
    : Optional[SingleSpillShuffleMapOutputWriter] = {
    delegate.createSingleFileMapOutputWriter(shuffleId, mapId)
  }
}

object KubernetesLocalDiskShuffleExecutorComponents extends Logging {
  /**
   * `File.listFiles` returns null when the directory cannot be read (an IO error, a permission
   * denial, or the directory being removed during the walk). Skip such a directory with a warning
   * instead of failing the whole recovery, so one unreadable entry cannot cost us every other
   * recoverable file. This is the single-level counterpart of `SparkFileUtils.recursiveList`, and
   * unlike `JavaUtils.listFilesSafely` it does not throw on an unlistable directory.
   */
  private def listEntriesOrEmpty(dir: File): Array[File] = {
    val entries = dir.listFiles()
    if (entries != null) {
      entries
    } else {
      logWarning(log"Failed to list ${MDC(LogKeys.FILE_ABSOLUTE_PATH, dir.getAbsolutePath)}; " +
        log"skipping it during shuffle data recovery.")
      Array.empty[File]
    }
  }

  /**
   * Walks two levels up from a local directory to reach the volume root shared by all executor
   * generations. Returns None for a path with fewer than three components, where `getParent`
   * yields null.
   */
  private def volumeRootOf(localDir: String): Option[File] = {
    val root = Option(new File(localDir).getParentFile).flatMap(p => Option(p.getParentFile))
    if (root.isEmpty) {
      logWarning(log"Cannot locate the volume root of local directory " +
        log"${MDC(LogKeys.PATH, localDir)}; skipping it during shuffle data recovery. Recovery " +
        log"requires a local directory nested at least two levels below the volume root, such as " +
        log"/data/spark-x/executor-y. Set the mount path of the spark-local-dir-* volume, or " +
        log"${MDC(LogKeys.CONFIG, "spark.local.dir")} when no such volume is mounted, " +
        log"accordingly.")
    }
    root
  }

  /**
   * This tries to recover shuffle data of dead executors' local dirs if exists.
   * Since the executors are already dead, we cannot use `getHostLocalDirs`.
   * This is enabled only when spark.kubernetes.driver.reusePersistentVolumeClaim is true.
   */
  def recoverDiskStore(conf: SparkConf, bm: BlockManager): Unit = {
    // Find All files
    val (checksumFiles, files) = Utils.getConfiguredLocalDirs(conf)
      .filter(s => s != null && s.nonEmpty)
      .flatMap(volumeRootOf)
      .flatMap { dir =>
        val oldDirs = listEntriesOrEmpty(dir).filter { f =>
          f.isDirectory && f.getName.startsWith("spark-")
        }
        oldDirs
          .flatMap(listEntriesOrEmpty).filter(_.isDirectory) // executor-xxx
          .flatMap(listEntriesOrEmpty).filter(_.isDirectory) // blockmgr-xxx
          .flatMap(listEntriesOrEmpty).filter(_.isDirectory) // 00
          .flatMap(listEntriesOrEmpty)
          .toImmutableArraySeq
      }
      .partition(_.getName.contains(".checksum"))
    val (indexFiles, dataFiles) = files.partition(_.getName.endsWith(".index"))

    logInfo(log"Found ${MDC(LogKeys.NUM_DATA_FILE, dataFiles.length)} data files, " +
      log"${MDC(LogKeys.NUM_INDEX_FILE, indexFiles.length)} index files, " +
      log"and ${MDC(LogKeys.NUM_CHECKSUM_FILE, checksumFiles.length)} checksum files.")

    // Build a hashmap with checksum file name as a key
    val checksumFileMap = new mutable.HashMap[String, File]()
    val algorithm = conf.get(SHUFFLE_CHECKSUM_ALGORITHM)
    checksumFiles.foreach { f =>
      logInfo(log"${MDC(LogKeys.FILE_NAME, f.getName)} -> " +
        log"${MDC(LogKeys.FILE_ABSOLUTE_PATH, f.getAbsolutePath)}")
      checksumFileMap.put(f.getName, f)
    }
    // Build a hashmap with shuffle data file name as a key
    val indexFileMap = new mutable.HashMap[String, File]()
    indexFiles.foreach { f =>
      logInfo(log"${MDC(LogKeys.FILE_NAME, f.getName.replace(".index", ".data"))} -> " +
        log"${MDC(LogKeys.FILE_ABSOLUTE_PATH, f.getAbsolutePath)}")
      indexFileMap.put(f.getName.replace(".index", ".data"), f)
    }

    // This is not used.
    val classTag = implicitly[ClassTag[Object]]
    val level = StorageLevel.DISK_ONLY
    val checksumDisabled = !conf.get(SHUFFLE_CHECKSUM_ENABLED)
    (dataFiles ++ indexFiles).foreach { f =>
      logInfo(log"Try to recover ${MDC(LogKeys.FILE_ABSOLUTE_PATH, f.getAbsolutePath)}")
      try {
        val id = BlockId(f.getName)
        // To make it sure to handle only shuffle blocks
        if (id.isShuffle) {
          // For index files, skipVerification is true and checksumFile and indexFile are ignored.
          val skipVerification = checksumDisabled || f.getName.endsWith(".index")
          val checksumFile = checksumFileMap.getOrElse(getChecksumFileName(id, algorithm), null)
          val indexFile = indexFileMap.getOrElse(f.getName, null)
          if (skipVerification || verifyChecksum(algorithm, id, checksumFile, indexFile, f)) {
            val decryptedSize = f.length()
            bm.TempFileBasedBlockStoreUpdater(id, level, classTag, f, decryptedSize).save()
          } else {
            logInfo(log"Ignore ${MDC(LogKeys.FILE_ABSOLUTE_PATH, f.getAbsolutePath)} " +
              log"due to the verification failure.")
          }
        } else {
          logInfo("Ignore a non-shuffle block file.")
        }
      } catch {
        case _: UnrecognizedBlockId =>
          logInfo("Skip due to UnrecognizedBlockId.")
        case _: FileExistsException =>
          // This may happen due to recompute, but we continue to recover next files
          logInfo("Ignore due to FileExistsException.")
      }
    }
  }

  def verifyChecksum(
      algorithm: String,
      blockId: BlockId,
      checksumFile: File,
      indexFile: File,
      dataFile: File): Boolean = {
    blockId match {
      case _: ShuffleDataBlockId =>
        if (dataFile == null || !dataFile.exists()) {
          false // Fail because the data file doesn't exist.
        } else if (checksumFile == null || !checksumFile.exists()) {
          true // Pass if the checksum file doesn't exist.
        } else if (checksumFile.length() == 0 || checksumFile.length() % 8 != 0) {
          false // Fail because the checksum file is corrupted.
        } else if (indexFile == null || !indexFile.exists()) {
          false // Fail because the index file is missing.
        } else if (indexFile.length() == 0) {
          false // Fail because the index file is empty.
        } else {
          val numPartition = (checksumFile.length() / 8).toInt
          compareChecksums(numPartition, algorithm, checksumFile, dataFile, indexFile)
        }
      case _ =>
        true // Ignore if blockId is not a shuffle data block.
    }
  }
}

