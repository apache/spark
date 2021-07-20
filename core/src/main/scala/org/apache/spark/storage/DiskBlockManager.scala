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
import java.nio.file.Files
import java.util.UUID

import scala.collection.mutable.HashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.storage.DiskBlockManager.ATTEMPT_ID_KEY
import org.apache.spark.storage.DiskBlockManager.MERGE_DIR_KEY
import org.apache.spark.storage.DiskBlockManager.MERGE_DIRECTORY
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 *
 * ShuffleDataIO also can change the behavior of deleteFilesOnStop.
 */
private[spark] class DiskBlockManager(conf: SparkConf, var deleteFilesOnStop: Boolean)
  extends Logging {

  private[spark] val subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }

  private[spark] val localDirsString: Array[String] = localDirs.map(_.toString)

  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  // Get merge directory name, append attemptId if there is any
  private val mergeDirName =
    s"$MERGE_DIRECTORY${conf.get(config.APP_ATTEMPT_ID).map(id => s"_$id").getOrElse("")}"

  // Create merge directories
  createLocalDirsForMergedShuffleBlocks()

  private val shutdownHook = addShutdownHook()

  /** Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExecutorDiskUtils#getFile().
  def getFile(filename: String): File = {
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

  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /**
   * This should be in sync with
   * @see [[org.apache.spark.network.shuffle.RemoteBlockPushResolver#getFile(
   *     java.lang.String, java.lang.String)]]
   */
  def getMergedShuffleFile(blockId: BlockId, dirs: Option[Array[String]]): File = {
    blockId match {
      case mergedBlockId: ShuffleMergedDataBlockId =>
        getMergedShuffleFile(mergedBlockId.name, dirs)
      case mergedIndexBlockId: ShuffleMergedIndexBlockId =>
        getMergedShuffleFile(mergedIndexBlockId.name, dirs)
      case mergedMetaBlockId: ShuffleMergedMetaBlockId =>
        getMergedShuffleFile(mergedMetaBlockId.name, dirs)
      case _ =>
        throw new IllegalArgumentException(
          s"Only merged block ID is supported, but got $blockId")
    }
  }

  private def getMergedShuffleFile(filename: String, dirs: Option[Array[String]]): File = {
    if (!dirs.exists(_.nonEmpty)) {
      throw new IllegalArgumentException(
        s"Cannot read $filename because merged shuffle dirs is empty")
    }
    ExecutorDiskUtils.getFile(dirs.get, subDirsPerLocalDir, filename)
  }

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
   * Get the list of configured local dirs storing merged shuffle blocks created by executors
   * if push based shuffle is enabled. Note that the files in this directory will be created
   * by the external shuffle services. We only create the merge_manager directories and
   * subdirectories here because currently the external shuffle service doesn't have
   * permission to create directories under application local directories.
   */
  private def createLocalDirsForMergedShuffleBlocks(): Unit = {
    if (Utils.isPushBasedShuffleEnabled(conf)) {
      // Will create the merge_manager directory only if it doesn't exist under the local dir.
      Utils.getConfiguredLocalDirs(conf).foreach { rootDir =>
        try {
          val mergeDir = new File(rootDir, mergeDirName)
          if (!mergeDir.exists()) {
            // This executor does not find merge_manager directory, it will try to create
            // the merge_manager directory and the sub directories.
            logDebug(s"Try to create $mergeDir and its sub dirs since the " +
              s"$mergeDirName dir does not exist")
            for (dirNum <- 0 until subDirsPerLocalDir) {
              val subDir = new File(mergeDir, "%02x".format(dirNum))
              if (!subDir.exists()) {
                // Only one container will create this directory. The filesystem will handle
                // any race conditions.
                createDirWithPermission770(subDir)
              }
            }
          }
          logInfo(s"Merge directory and its sub dirs get created at $mergeDir")
        } catch {
          case e: IOException =>
            logError(
              s"Failed to create $mergeDirName dir in $rootDir. Ignoring this directory.", e)
        }
      }
    }
  }

  /**
   * Create a directory that is writable by the group.
   * Grant the permission 770 "rwxrwx---" to the directory so the shuffle server can
   * create subdirs/files within the merge folder.
   * TODO: Find out why can't we create a dir using java api with permission 770
   *  Files.createDirectories(mergeDir.toPath, PosixFilePermissions.asFileAttribute(
   *  PosixFilePermissions.fromString("rwxrwx---")))
   */
  def createDirWithPermission770(dirToCreate: File): Unit = {
    var attempts = 0
    val maxAttempts = Utils.MAX_DIR_CREATION_ATTEMPTS
    var created: File = null
    while (created == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException(
          s"Failed to create directory ${dirToCreate.getAbsolutePath} with permission " +
            s"770 after $maxAttempts attempts!")
      }
      try {
        val builder = new ProcessBuilder().command(
          "mkdir", "-p", "-m770", dirToCreate.getAbsolutePath)
        val proc = builder.start()
        val exitCode = proc.waitFor()
        if (dirToCreate.exists()) {
          created = dirToCreate
        }
        logDebug(
          s"Created directory at ${dirToCreate.getAbsolutePath} with permission " +
            s"770 and exitCode $exitCode")
      } catch {
        case e: SecurityException =>
          logWarning(s"Failed to create directory ${dirToCreate.getAbsolutePath} " +
            s"with permission 770", e)
          created = null;
      }
    }
  }

  def getMergeDirectoryAndAttemptIDJsonString(): String = {
    val mergedMetaMap: HashMap[String, String] = new HashMap[String, String]()
    mergedMetaMap.put(MERGE_DIR_KEY, mergeDirName)
    conf.get(config.APP_ATTEMPT_ID).foreach(
      attemptId => mergedMetaMap.put(ATTEMPT_ID_KEY, attemptId))
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val jsonString = mapper.writeValueAsString(mergedMetaMap)
    jsonString
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

private[spark] object DiskBlockManager {
  val MERGE_DIRECTORY = "merge_manager"
  val MERGE_DIR_KEY = "mergeDir"
  val ATTEMPT_ID_KEY = "attemptId"
}
