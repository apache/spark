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
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.util.UUID

import scala.collection.mutable.HashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKeys.{MERGE_DIR_NAME, PATH}
import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.storage.DiskBlockManager.ATTEMPT_ID_KEY
import org.apache.spark.storage.DiskBlockManager.MERGE_DIR_KEY
import org.apache.spark.storage.DiskBlockManager.MERGE_DIRECTORY
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 *
 * ShuffleDataIO also can change the behavior of deleteFilesOnStop.
 */
private[spark] class DiskBlockManager(
    conf: SparkConf,
    var deleteFilesOnStop: Boolean,
    isDriver: Boolean)
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

  // If either of these features are enabled, we must change permissions on block manager
  // directories and files to accommodate the shuffle service deleting files in a secure
  // environment. Parent directories are assumed to be restrictive to prevent unauthorized users
  // from accessing or modifying world readable files.
  private val permissionChangingRequired = conf.get(config.SHUFFLE_SERVICE_ENABLED) && (
    conf.get(config.SHUFFLE_SERVICE_REMOVE_SHUFFLE_ENABLED) ||
    conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)
  )

  /** Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExecutorDiskUtils#getFilePath().
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
          val path = newDir.toPath
          Files.createDirectory(path)
          if (permissionChangingRequired) {
            // SPARK-37618: Create dir as group writable so files within can be deleted by the
            // shuffle service in a secure setup. This will remove the setgid bit so files created
            // within won't be created with the parent folder group.
            val currentPerms = Files.getPosixFilePermissions(path)
            currentPerms.add(PosixFilePermission.GROUP_WRITE)
            Files.setPosixFilePermissions(path, currentPerms)
          }
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
        throw SparkException.internalError(
          s"Only merged block ID is supported, but got $blockId", category = "STORAGE")
    }
  }

  private def getMergedShuffleFile(filename: String, dirs: Option[Array[String]]): File = {
    if (!dirs.exists(_.nonEmpty)) {
      throw SparkException.internalError(
        s"Cannot read $filename because merged shuffle dirs is empty", category = "STORAGE")
    }
    new File(ExecutorDiskUtils.getFilePath(dirs.get, subDirsPerLocalDir, filename))
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
      if (files != null) files.toImmutableArraySeq else Seq.empty
    }.toImmutableArraySeq
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

  /**
   * SPARK-37618: Makes sure that the file is created as world readable. This is to get
   * around the fact that making the block manager sub dirs group writable removes
   * the setgid bit in secure Yarn environments, which prevents the shuffle service
   * from being able to read shuffle files. The outer directories will still not be
   * world executable, so this doesn't allow access to these files except for the
   * running user and shuffle service.
   */
  def createWorldReadableFile(file: File): Unit = {
    val path = file.toPath
    Files.createFile(path)
    val currentPerms = Files.getPosixFilePermissions(path)
    currentPerms.add(PosixFilePermission.OTHERS_READ)
    Files.setPosixFilePermissions(path, currentPerms)
  }

  /**
   * Creates a temporary version of the given file with world readable permissions (if required).
   * Used to create block files that will be renamed to the final version of the file.
   */
  def createTempFileWith(file: File): File = {
    val tmpFile = Utils.tempFileWith(file)
    if (permissionChangingRequired) {
      // SPARK-37618: we need to make the file world readable because the parent will
      // lose the setgid bit when making it group writable. Without this the shuffle
      // service can't read the shuffle files in a secure setup.
      createWorldReadableFile(tmpFile)
    }
    tmpFile
  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = TempShuffleBlockId(UUID.randomUUID())
    }
    val tmpFile = getFile(blockId)
    if (permissionChangingRequired) {
      // SPARK-37618: we need to make the file world readable because the parent will
      // lose the setgid bit when making it group writable. Without this the shuffle
      // service can't read the shuffle files in a secure setup.
      createWorldReadableFile(tmpFile)
    }
    (blockId, tmpFile)
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
          logError(
            log"Failed to create local dir in ${MDC(PATH, rootDir)}. Ignoring this directory.", e)
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
    if (Utils.isPushBasedShuffleEnabled(conf, isDriver = isDriver, checkSerializer = false)) {
      // Will create the merge_manager directory only if it doesn't exist under the local dir.
      Utils.getConfiguredLocalDirs(conf).foreach { rootDir =>
        try {
          val mergeDir = new File(rootDir, mergeDirName)
          if (!mergeDir.exists() || mergeDir.listFiles().length < subDirsPerLocalDir) {
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
              log"Failed to create ${MDC(MERGE_DIR_NAME, mergeDirName)} dir in " +
                log"${MDC(PATH, rootDir)}. Ignoring this directory.", e)
        }
      }
    }
  }

  /**
   * Create a directory that is writable by the group.
   * Grant the permission 770 "rwxrwx---" to the directory so the shuffle server can
   * create subdirs/files within the merge folder.
   */
  def createDirWithPermission770(dirToCreate: File): Unit = {
    var attempts = 0
    val maxAttempts = Utils.MAX_DIR_CREATION_ATTEMPTS
    var created: File = null
    while (created == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw SparkCoreErrors.failToCreateDirectoryError(dirToCreate.getAbsolutePath, maxAttempts)
      }
      try {
        dirToCreate.mkdirs()
        Files.setPosixFilePermissions(
          dirToCreate.toPath, PosixFilePermissions.fromString("rwxrwx---"))
        if (dirToCreate.exists()) {
          created = dirToCreate
        }
        logDebug(s"Created directory at ${dirToCreate.getAbsolutePath} with permission 770")
      } catch {
        case e: SecurityException =>
          logWarning(log"Failed to create directory ${MDC(PATH, dirToCreate.getAbsolutePath)} " +
            log"with permission 770", e)
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
              logError(log"Exception while deleting local spark dir: ${MDC(PATH, localDir)}", e)
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
