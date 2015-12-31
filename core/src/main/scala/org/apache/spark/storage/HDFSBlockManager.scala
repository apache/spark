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

import java.net.URI
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import scala.util.control.NonFatal

import com.google.common.io.ByteStreams

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.Logging
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.Utils

private[spark]  class HDFSBlockManager extends ExternalBlockManager with Logging{

  var fs: FileSystem = null
  var rootDirs: String = null
  var master: Option[String] = None
  var storeDir: String = null
  var appFolderName: String = null
  var subDirsPerDir: Int = _
  private var hdfsDirs: Array[String] = _
  private var subDirs: Array[Array[String]] = _

  override def init(blockManager: BlockManager, executorId: String): Unit = {
    super.init(blockManager, executorId)
    val hadoopConf = new Configuration()
    val conf = blockManager.conf
    val storeDir = blockManager.conf.get(ExternalBlockStore.BASE_DIR, "/tmp/spark_external_store")
    val appFolderName = blockManager.conf.get(ExternalBlockStore.FOLD_NAME)
    rootDirs = s"$storeDir/$appFolderName/$executorId"
    master = conf.getOption(ExternalBlockStore.MASTER_URL)
    fs = master.map(m =>
      FileSystem.get(new URI(m), hadoopConf)).getOrElse(FileSystem.get(hadoopConf))
    hdfsDirs = createDirs()
    subDirsPerDir = blockManager.conf.get("spark.externalBlockStore.subDirectories",
      ExternalBlockStore.SUB_DIRS_PER_DIR).toInt
    subDirs = Array.fill(hdfsDirs.length)(new Array[String](subDirsPerDir))
  }

  override def toString(): String = {"ExternalBlockStore-HDFS"}

  override def removeBlock(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    if (fileExists(file)) {
      fs.delete(new Path(file), true)
    } else {
      false
    }
  }

  override def blockExists(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    fileExists(file)
  }

  private def fileExists(filename: String): Boolean = {
    fs.exists(new Path(filename))
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val file = new Path(getFile(blockId))
    if (fs.exists(file)) {
      fs.delete(file, true);
    }
    val os = fs.create(file);
    try {
      os.write(bytes.array())
      fs.deleteOnExit(file)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into HDFS", e)
    } finally {
      os.close()
    }
  }

  override def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val file = getFile(blockId)
    val os = fs.create(new Path(file));
    try {
      blockManager.dataSerializeStream(blockId, os, values)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into HDFS", e)
    } finally {
      os.close()
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val p = new Path(getFile(blockId))
    val is = fs.open(p)
    if (is == null) {
      return None
    }
    try {
      val cSummary = fs.getContentSummary(p);
      val size = cSummary.getLength
      val bs = new Array[Byte](size.asInstanceOf[Int])
      ByteStreams.readFully(is, bs)
      Some(ByteBuffer.wrap(bs))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get bytes of block $blockId from HDFS", e)
        None
    } finally {
      is.close()
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val file = getFile(blockId)
    val is = fs.open(new Path(file));
    Option(is).map { is =>
      blockManager.dataDeserializeStream(blockId, is)
    }
  }

  override def getSize(blockId: BlockId): Long = {
    val p = new Path(getFile(blockId))
    val cSummary = fs.getContentSummary(p);
    cSummary.getLength
  }

  private def getFile(filename: String): String = {
   // Figure out which hdfs directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % hdfsDirs.length
    val subDirId = (hash / hdfsDirs.length) % subDirsPerDir
    // Create the subdirectory if it doesn't already exist
    var subDir = subDirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subDirs(dirId).synchronized {
        val old = subDirs(dirId)(subDirId)
        if (old == null) {
          val pathStr = hdfsDirs(dirId) + "/" + "%02x".format(subDirId)
          val path = new Path(pathStr)
          fs.mkdirs(path)
          subDirs(dirId)(subDirId) = pathStr
          fs.deleteOnExit(path)
          pathStr
        } else {
          old
        }
      }
    }
    val filePath = subDir + "/" + filename
    filePath
  }

  private def createDirs(): Array[String] = {
    logInfo(s"Creating hdfs off heap directories at root dirs: ${master}/${rootDirs}")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").map { rootDir =>
      var foundLocalDir = false
      var hdfsDir: String = null
      var hdfsDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          hdfsDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          hdfsDir = rootDir + "/" + "spark-hdfs-" + hdfsDirId
          val path = new Path(hdfsDir)
          if (!fs.exists(path)) {
            foundLocalDir = fs.mkdirs(path)
          }
          fs.deleteOnExit(path)
        } catch {
          case e: Exception =>
            logWarning("Attempt " + tries + " to create hdfs dir " + hdfsDir + " failed", e)
        }
      }
      if (!foundLocalDir) {
        logError("Failed " + ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS
          + " attempts to create hdfs dir in " +  rootDir)
        System.exit(ExecutorExitCode.EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR)
      }
      hdfsDir
    }
  }

  override def shutdown() {
    try {
      fs.close()
    } catch {
      case NonFatal(t) =>
        logError(s"error in closing file system", t)
    }
  }

  private def getFile(blockId: BlockId): String = getFile(blockId.name)
}
