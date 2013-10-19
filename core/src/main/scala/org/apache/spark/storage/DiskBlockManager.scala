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
import java.util.{Date, Random}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.Logging
import org.apache.spark.network.netty.{PathResolver, ShuffleSender}
import org.apache.spark.util.Utils

/**
 * Creates an maintains the logical mapping between logical Blocks and physical on-disk
 * locations. By default, one Block is mapped to one file with a name given by its BlockId.
 * However, it is also possible to have a Block map to only a segment of a file, by calling
 * mapBlockToFileSegment().
 *
 * @param rootDirs The directories to use for storing Block files. Data will be hashed among these.
 */
class DiskBlockManager(rootDirs: String) extends PathResolver with Logging {

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  private val subDirsPerLocalDir = System.getProperty("spark.diskStore.subDirectories", "64").toInt

  // Create one local directory for each path mentioned in spark.local.dir; then, inside this
  // directory, create multiple subdirectories that we will hash files into, in order to avoid
  // having really large inodes at the top level.
  private val localDirs: Array[File] = createLocalDirs()
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))
  private var shuffleSender : ShuffleSender = null

  // Stores only Blocks which have been specifically mapped to segments of files
  // (rather than the default, which maps a Block to a whole file).
  // This keeps our bookkeeping down, since the file system itself tracks the standalone Blocks. 
  // ConcurrentHashMap does not take a lock on read operations, which makes it very efficient here.
  private val blockToFileSegmentMap = new ConcurrentHashMap[BlockId, FileSegment]

  addShutdownHook()

  /**
   * Creates a logical mapping from the given BlockId to a segment of a file.
   * This will cause any accesses of the logical BlockId to be directed to the specified
   * physical location.
   */
  def mapBlockToFileSegment(blockId: BlockId, fileSegment: FileSegment) {
    blockToFileSegmentMap.put(blockId, fileSegment)
  }

  /**
   * Returns the phyiscal file segment in which the given BlockId is located.
   * If the BlockId has been mapped to a specific FileSegment, that will be returned.
   * Otherwise, we assume the Block is mapped to a whole file identified by the BlockId directly.
   */
  def getBlockLocation(blockId: BlockId): FileSegment = {
    if (blockToFileSegmentMap.containsKey(blockId)) {
      blockToFileSegmentMap.get(blockId)
    } else {
      val file = getFile(blockId.name)
      new FileSegment(file, 0, file.length())
    }
  }

  /**
   * Simply returns a File to place the given Block into. This does not physically create the file.
   * If filename is given, that file will be used. Otherwise, we will use the BlockId to get
   * a unique filename.
   */
  def createBlockFile(blockId: BlockId, filename: String = "", allowAppending: Boolean): File = {
    val actualFilename = if (filename == "") blockId.name else filename
    val file = getFile(actualFilename)
    if (!allowAppending && file.exists()) {
      throw new IllegalStateException(
        "Attempted to create file that already exists: " + actualFilename)
    }
    file
  }

  private def getFile(filename: String): File = {
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

  private def createLocalDirs(): Array[File] = {
    logDebug("Creating local directories at root dirs '" + rootDirs + "'")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").map { rootDir =>
      var foundLocalDir = false
      var localDir: File = null
      var localDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          localDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          localDir = new File(rootDir, "spark-local-" + localDirId)
          if (!localDir.exists) {
            foundLocalDir = localDir.mkdirs()
          }
        } catch {
          case e: Exception =>
            logWarning("Attempt " + tries + " to create local dir " + localDir + " failed", e)
        }
      }
      if (!foundLocalDir) {
        logError("Failed " + MAX_DIR_CREATION_ATTEMPTS +
          " attempts to create local dir in " + rootDir)
        System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
      }
      logInfo("Created local directory at " + localDir)
      localDir
    }
  }

  private def addShutdownHook() {
    localDirs.foreach(localDir => Utils.registerShutdownDeleteDir(localDir))
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark local dirs") {
      override def run() {
        logDebug("Shutdown hook called")
        localDirs.foreach { localDir =>
          try {
            if (!Utils.hasRootAsShutdownDeleteDir(localDir)) Utils.deleteRecursively(localDir)
          } catch {
            case t: Throwable =>
              logError("Exception while deleting local spark dir: " + localDir, t)
          }
        }

        if (shuffleSender != null) {
          shuffleSender.stop()
        }
      }
    })
  }

  private[storage] def startShuffleBlockSender(port: Int): Int = {
    shuffleSender = new ShuffleSender(port, this)
    logInfo("Created ShuffleSender binding to port : " + shuffleSender.port)
    shuffleSender.port
  }
}
