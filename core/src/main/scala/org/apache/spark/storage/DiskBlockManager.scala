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
import java.text.SimpleDateFormat
import java.util.{Date, Random, UUID}

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.network.netty.PathResolver
import org.apache.spark.util.Utils
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. By default, one block is mapped to one file with a name given by its BlockId.
 * However, it is also possible to have a block map to only a segment of a file, by calling
 * mapBlockToFileSegment().
 *
 * @param rootDirs The directories to use for storing block files. Data will be hashed among these.
 */
private[spark] class DiskBlockManager(shuffleBlockManager: ShuffleBlockManager, rootDirs: String)
  extends PathResolver with Logging {

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  private val subDirsPerLocalDir =
    shuffleBlockManager.conf.getInt("spark.diskStore.subDirectories", 64)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  val localDirs: Array[File] = createLocalDirs()
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  addShutdownHook()

  /**
   * Returns the physical file segment in which the given BlockId is located. If the BlockId has
   * been mapped to a specific FileSegment by the shuffle layer, that will be returned.
   * Otherwise, we assume the Block is mapped to the whole file identified by the BlockId.
   */
  def getBlockLocation(blockId: BlockId): FileSegment = {
    val env = SparkEnv.get  // NOTE: can be null in unit tests
    if (blockId.isShuffle && env != null && env.shuffleManager.isInstanceOf[SortShuffleManager]) {
      // For sort-based shuffle, let it figure out its blocks
      val sortShuffleManager = env.shuffleManager.asInstanceOf[SortShuffleManager]
      sortShuffleManager.getBlockLocation(blockId.asInstanceOf[ShuffleBlockId], this)
    } else if (blockId.isShuffle && shuffleBlockManager.consolidateShuffleFiles) {
      // For hash-based shuffle with consolidated files, ShuffleBlockManager takes care of this
      shuffleBlockManager.getBlockLocation(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      val file = getFile(blockId.name)
      new FileSegment(file, 0, file.length())
    }
  }

  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % localDirs.length
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    var subDir = subDirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subDirs(dirId).synchronized {
        val old = subDirs(dirId)(subDirId)
        if (old != null) {
          old
        } else {
          val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
          newDir.mkdir()
          subDirs(dirId)(subDirId) = newDir
          newDir
        }
      }
    }

    new File(subDir, filename)
  }

  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getBlockLocation(blockId).file.exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatten.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().map(f => BlockId(f.getName))
  }

  /** Produces a unique block id and File suitable for intermediate results. */
  def createTempBlock(): (TempBlockId, File) = {
    var blockId = new TempBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  private def createLocalDirs(): Array[File] = {
    logDebug(s"Creating local directories at root dirs '$rootDirs'")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").flatMap { rootDir =>
      var foundLocalDir = false
      var localDir: File = null
      var localDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          localDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          localDir = new File(rootDir, s"spark-local-$localDirId")
          if (!localDir.exists) {
            foundLocalDir = localDir.mkdirs()
          }
        } catch {
          case e: Exception =>
            logWarning(s"Attempt $tries to create local dir $localDir failed", e)
        }
      }
      if (!foundLocalDir) {
        logError(s"Failed $MAX_DIR_CREATION_ATTEMPTS attempts to create local dir in $rootDir." +
                  " Ignoring this directory.")
        None
      } else {
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      }
    }
  }

  private def addShutdownHook() {
    localDirs.foreach(localDir => Utils.registerShutdownDeleteDir(localDir))
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark local dirs") {
      override def run(): Unit = Utils.logUncaughtExceptions {
        logDebug("Shutdown hook called")
        DiskBlockManager.this.stop()
      }
    })
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
    localDirs.foreach { localDir =>
      if (localDir.isDirectory() && localDir.exists()) {
        try {
          if (!Utils.hasRootAsShutdownDeleteDir(localDir)) Utils.deleteRecursively(localDir)
        } catch {
          case e: Exception =>
            logError(s"Exception while deleting local spark dir: $localDir", e)
        }
      }
    }
  }
}
