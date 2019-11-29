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

import java.io.{File, IOException}
import java.util.UUID

import scala.util.control.NonFatal

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
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }

  def containerDirEnabled: Boolean = Utils.isRunningInYarnContainer(conf)

  /* Create container directories on YARN to persist the temporary files.
   * (temp_local, temp_shuffle)
   * These files have no opportunity to be cleaned before application end on YARN.
   * This is a real issue, especially for long-lived Spark application like Spark thrift-server.
   * So we persist these files in YARN container directories which could be cleaned by YARN when
   * the container exists. */
  private[spark] val containerDirs: Array[File] =
    if (containerDirEnabled) createContainerDirs(conf) else Array.empty[File]

  private[spark] val localDirsString: Array[String] = localDirs.map(_.toString)

  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  private val subContainerDirs = if (containerDirEnabled) {
    Array.fill(containerDirs.length)(new Array[File](subDirsPerLocalDir))
  } else {
    Array.empty[Array[File]]
  }

  private val shutdownHook = addShutdownHook()

  /** Looks up a file by hashing it into one of our local/container subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExecutorDiskUtils#getFile().
  def getFile(localDirs: Array[File], subDirs: Array[Array[File]],
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
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  /**
   * Used only for testing.
   */
  private[spark] def getFile(filename: String): File =
    getFile(localDirs, subDirs, subDirsPerLocalDir, filename)

  def getFile(blockId: BlockId): File = {
    if (containerDirEnabled && blockId.isTemp) {
      getFile(containerDirs, subContainerDirs, subDirsPerLocalDir, blockId.name)
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
    (subDirs ++ subContainerDirs).flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
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
    var blockId = TempLocalBlockId(UUID.randomUUID())
    var tempLocalFile = getFile(blockId)
    var count = 0
    while (!canCreateFile(tempLocalFile) && count < Utils.MAX_DIR_CREATION_ATTEMPTS) {
      blockId = TempLocalBlockId(UUID.randomUUID())
      tempLocalFile = getFile(blockId)
      count += 1
    }
    (blockId, tempLocalFile)
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = TempShuffleBlockId(UUID.randomUUID())
    var tempShuffleFile = getFile(blockId)
    var count = 0
    while (!canCreateFile(tempShuffleFile) && count < Utils.MAX_DIR_CREATION_ATTEMPTS) {
      blockId = TempShuffleBlockId(UUID.randomUUID())
      tempShuffleFile = getFile(blockId)
      count += 1
    }
    (blockId, tempShuffleFile)
  }

  private def canCreateFile(file: File): Boolean = {
    try {
      file.createNewFile()
    } catch {
      case NonFatal(_) =>
        logError("Failed to create temporary block file: " + file.getAbsoluteFile)
        false
    }
  }

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  /**
   * Create container directories for storing block data in YARN mode.
   * These directories are located inside configured local directories and
   * will be deleted in the processing of container clean of YARN.
   */
  private def createContainerDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      val containerDirPath = s"$rootDir/${conf.getenv("CONTAINER_ID")}"
      try {
        val containerDir = Utils.createDirectory(containerDirPath, "blockmgr")
        logInfo(s"Created YARN container directory at $containerDir")
        Some(containerDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create YARN container dir in $containerDirPath." +
            s" Ignoring this directory.", e)
          None
      }
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
      (localDirs ++ containerDirs).foreach { dir =>
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
