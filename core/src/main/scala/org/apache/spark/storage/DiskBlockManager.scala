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

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 */
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {

  private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  private val shutdownHook = addShutdownHook()

  private abstract class FileAllocationStrategy {
    def apply(filename: String): File

    protected def getFile(filename: String, storageDirs: Array[File]): File = {
      require(storageDirs.nonEmpty, "could not find file when the directories are empty")

      // Figure out which local directory it hashes to, and which subdirectory in that
      val hash = Utils.nonNegativeHash(filename)
      val dirId = localDirs.indexOf(storageDirs(hash % storageDirs.length))
      val subDirId = (hash / storageDirs.length) % subDirsPerLocalDir

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
  }

  /** Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  private object hashAllocator extends FileAllocationStrategy {
    def apply(filename: String): File = getFile(filename, localDirs)
  }

  /** Looks up a file by hierarchy way in different speed storage devices. */
  private val hierarchyStore = conf.getOption("spark.storage.hierarchyStore")
  private class HierarchyAllocator extends FileAllocationStrategy {
    case class LayerInfo(key: String, threshold: Long, dirs: Array[File])
    val hsSpecs: Array[(String, Long)] =
      // e.g.: hierarchyStore = "ssd 200GB, hdd 100GB"
      hierarchyStore.get.trim.split(",").map {
        s => val x = s.trim.split(" +")
          (x(0).toLowerCase, Utils.byteStringAsGb(x(1)))
      }
    val hsLayers: Array[LayerInfo] = hsSpecs.map(
      s => LayerInfo(s._1, s._2, localDirs.filter(_.getPath.toLowerCase.containsSlice(s._1)))
    )
    val lastLayerDirs = localDirs.filter(dir => !hsLayers.exists(_.dirs.contains(dir)))
    val allLayers: Array[LayerInfo] = hsLayers :+
      LayerInfo("Last Storage", 10.toLong, lastLayerDirs)
    val finalLayers: Array[LayerInfo] = allLayers.filter(_.dirs.nonEmpty)
    logInfo("Hierarchy store info:")
    for (layer <- finalLayers) {
      logInfo("Layer: %s, Threshold: %dGB".format(layer.key, layer.threshold))
      layer.dirs.foreach { dir => logInfo("\t%s".format(dir.getCanonicalPath)) }
    }

    def apply(filename: String): File = {
      var availableFile: File = null
      for (layer <- finalLayers) {
        val file = getFile(filename, layer.dirs)
        if (file.exists()) return file

        if (availableFile == null && file.getParentFile.getUsableSpace>>30 >= layer.threshold) {
          availableFile = file
        }
      }

      if (availableFile == null) {
        throw new IOException(s"No enough disk space.")
      }
      availableFile
    }
  }

  private val fileAllocator: FileAllocationStrategy =
    if (hierarchyStore.isDefined && !conf.getBoolean("spark.shuffle.service.enabled", false)) {
      logInfo(s"Hierarchy allocator for blocks is enabled")
      new HierarchyAllocator
    }
    else {
      logInfo(s"Hash allocator for blocks is enabled")
      hashAllocator
    }

  def getFile(filename: String): File = fileAllocator(filename)

  def getFile(blockId: BlockId): File = getFile(blockId.name)
  
  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
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
    getAllFiles().map(f => BlockId(f.getName))
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

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
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
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
