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

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 */
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {

  private[spark] val subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  private[spark] val localDirs: Array[File] = StorageUtils.createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }

  private[spark] val localDirsString: Array[String] = localDirs.map(_.toString)

  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  /* Directories persist the temporary files (temp_local, temp_shuffle).
   * We separate the storage directories of temp block from non-temp block since
   * the cleaning process for temp block may be different between deploy modes.
   * For example, these files have no opportunity to be cleaned before application end on YARN.
   * This is a real issue, especially for long-lived Spark application like Spark thrift-server.
   * So for Yarn mode, we persist these files in YARN container directories which could be
   * cleaned by YARN when the container exists. */
  private val tempDirs: Array[File] = createTempDirs(conf)
  if (tempDirs.isEmpty) {
    logError("Failed to create any temp dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }

  // Similar with subDirs, tempSubDirs are used only for temp block.
  private val tempSubDirs = createTempSubDirs(conf)

  private val shutdownHook = addShutdownHook()

  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExecutorDiskUtils#getFile().
  private def getFile(localDirs: Array[File], subDirs: Array[Array[File]],
      subDirsPerLocalDir: Int, filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % localDirs.length
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists()) {
          Files.createDirectory(newDir.toPath)
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  /** Looks up a file by hashing it into one of our local/temp subdirectories. */
  def getFile(blockId: BlockId): File = {
    if (blockId.isTemp) {
      getFile(tempDirs, tempSubDirs, subDirsPerLocalDir, blockId.name)
    } else {
      getFile(localDirs, subDirs, subDirsPerLocalDir, blockId.name)
    }
  }

  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId).exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    // compare their references are same
    val allSubDirs = if (subDirs eq tempSubDirs) subDirs else subDirs ++ tempSubDirs
    allSubDirs.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files.toSeq else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().flatMap { f =>
      try {
        Some(BlockId(f.getName))
      } catch {
        case _: UnrecognizedBlockId =>
          // Skip files which do not correspond to blocks, for example temporary
          // files created by [[SortShuffleWriter]].
          None
      }
    }
  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * Create local temp directories for storing temp block data. These directories are
   * located inside configured local directories. If executors are running in Yarn,
   * these directories will be deleted on the Yarn container exit. Or store them in localDirs,
   * if that they won't be deleted on JVM exit when using the external shuffle service.
   */
  private def createTempDirs(conf: SparkConf): Array[File] = {
    if (Utils.isRunningInYarnContainer(conf)) {
      StorageUtils.createContainerDirs(conf)
    } else {
      // To be compatible with current implementation, store temp block in localDirs
      localDirs
    }
  }

  private def createTempSubDirs(conf: SparkConf): Array[Array[File]] = {
    if (Utils.isRunningInYarnContainer(conf)) {
      Array.fill(tempDirs.length)(new Array[File](subDirsPerLocalDir))
    } else {
      // To be compatible with current implementation, store temp block in subDirsDirs
      subDirs
    }
  }

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop(): Unit = {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    if (deleteFilesOnStop) {
      // compare their references are same
      val toDelete = if (localDirs eq tempDirs) localDirs else localDirs ++ tempDirs
      toDelete.foreach { dir =>
        if (dir.isDirectory() && dir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(dir)) {
              Utils.deleteRecursively(dir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $dir", e)
          }
        }
      }
    }
  }
}
