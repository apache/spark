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

import java.text.SimpleDateFormat
import java.util.{Date, Random}

import tachyon.client.TachyonFS
import tachyon.client.TachyonFile

import org.apache.spark.Logging
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.Utils


/**
 * Creates and maintains the logical mapping between logical blocks and tachyon fs locations. By
 * default, one block is mapped to one file with a name given by its BlockId.
 *
 * @param rootDirs The directories to use for storing block files. Data will be hashed among these.
 */
private[spark] class TachyonBlockManager(
    blockManager: BlockManager,
    rootDirs: String,
    val master: String)
  extends Logging {

  val client = if (master != null && master != "") TachyonFS.get(master) else null

  if (client == null) {
    logError("Failed to connect to the Tachyon as the master address is not configured")
    System.exit(ExecutorExitCode.TACHYON_STORE_FAILED_TO_INITIALIZE)
  }

  private val MAX_DIR_CREATION_ATTEMPTS = 10
  private val subDirsPerTachyonDir =
    blockManager.conf.get("spark.tachyonStore.subDirectories", "64").toInt

  // Create one Tachyon directory for each path mentioned in spark.tachyonStore.folderName;
  // then, inside this directory, create multiple subdirectories that we will hash files into,
  // in order to avoid having really large inodes at the top level in Tachyon.
  private val tachyonDirs: Array[TachyonFile] = createTachyonDirs()
  private val subDirs = Array.fill(tachyonDirs.length)(new Array[TachyonFile](subDirsPerTachyonDir))

  addShutdownHook()

  def removeFile(file: TachyonFile): Boolean = {
    client.delete(file.getPath(), false)
  }

  def fileExists(file: TachyonFile): Boolean = {
    client.exist(file.getPath())
  }

  def getFile(filename: String): TachyonFile = {
    // Figure out which tachyon directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % tachyonDirs.length
    val subDirId = (hash / tachyonDirs.length) % subDirsPerTachyonDir

    // Create the subdirectory if it doesn't already exist
    var subDir = subDirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subDirs(dirId).synchronized {
        val old = subDirs(dirId)(subDirId)
        if (old != null) {
          old
        } else {
          val path = tachyonDirs(dirId) + "/" + "%02x".format(subDirId)
          client.mkdir(path)
          val newDir = client.getFile(path)
          subDirs(dirId)(subDirId) = newDir
          newDir
        }
      }
    }
    val filePath = subDir + "/" + filename
    if(!client.exist(filePath)) {
      client.createFile(filePath)
    }
    val file = client.getFile(filePath)
    file
  }

  def getFile(blockId: BlockId): TachyonFile = getFile(blockId.name)

  // TODO: Some of the logic here could be consolidated/de-duplicated with that in the DiskStore.
  private def createTachyonDirs(): Array[TachyonFile] = {
    logDebug("Creating tachyon directories at root dirs '" + rootDirs + "'")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").map { rootDir =>
      var foundLocalDir = false
      var tachyonDir: TachyonFile = null
      var tachyonDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          tachyonDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          val path = rootDir + "/" + "spark-tachyon-" + tachyonDirId
          if (!client.exist(path)) {
            foundLocalDir = client.mkdir(path)
            tachyonDir = client.getFile(path)
          }
        } catch {
          case e: Exception =>
            logWarning("Attempt " + tries + " to create tachyon dir " + tachyonDir + " failed", e)
        }
      }
      if (!foundLocalDir) {
        logError("Failed " + MAX_DIR_CREATION_ATTEMPTS + " attempts to create tachyon dir in " +
          rootDir)
        System.exit(ExecutorExitCode.TACHYON_STORE_FAILED_TO_CREATE_DIR)
      }
      logInfo("Created tachyon directory at " + tachyonDir)
      tachyonDir
    }
  }

  private def addShutdownHook() {
    tachyonDirs.foreach(tachyonDir => Utils.registerShutdownDeleteDir(tachyonDir))
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark tachyon dirs") {
      override def run(): Unit = Utils.logUncaughtExceptions {
        logDebug("Shutdown hook called")
        tachyonDirs.foreach { tachyonDir =>
          try {
            if (!Utils.hasRootAsShutdownDeleteDir(tachyonDir)) {
              Utils.deleteRecursively(tachyonDir, client)
            }
          } catch {
            case e: Exception =>
              logError("Exception while deleting tachyon spark dir: " + tachyonDir, e)
          }
        }
        client.close()
      }
    })
  }
}
