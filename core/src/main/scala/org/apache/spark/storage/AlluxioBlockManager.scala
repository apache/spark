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

import java.io.{File, FileInputStream, InputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.util.{ByteBufferInputStream, Utils}

private[spark] class AlluxioBlockManager extends ExternalBlockManager with Logging {

  private var chroot: Path = _
  private var subDirs: Array[Array[Path]] = _
  private var hdfsDirs: Array[Array[Path]] = _
  private var hdfsroot: Path = _
  private var usehdfs: Boolean = _

  override def toString: String = "ExternalBlockStore-Alluxio"

  override def init(blockManager: BlockManager): Unit = {
    super.init(blockManager)

    val conf = blockManager.conf

    val masterUrl = conf.get(ExternalBlockStore.MASTER_URL, "alluxio://localhost:19998")
    val hdfsUrl = conf.get("spark.alluxio.hdfs.nameservice", "viewfs://nsX")
    val storeDir = conf.get(ExternalBlockStore.BASE_DIR, "/tmp_spark_alluxio")
    val folderName = conf.get(ExternalBlockStore.FOLD_NAME)
    val subDirsPerAlluxio = conf.getInt(
      "spark.externalBlockStore.subDirectories",
      ExternalBlockStore.SUB_DIRS_PER_DIR.toInt)

    val master = new Path(masterUrl)
    chroot = new Path(master, s"$storeDir/$folderName/" + conf.getAppId )
    fs = master.getFileSystem(new Configuration)

    val hdfsMaster = new Path(hdfsUrl)
    hdfsFs = hdfsMaster.getFileSystem(new Configuration)
    hdfsroot = new Path(hdfsMaster, s"$storeDir/$folderName/" + conf.getAppId)

    subDirs = Array.fill(subDirsPerAlluxio)(new Array[Path](subDirsPerAlluxio))
    hdfsDirs = Array.fill(subDirsPerAlluxio)(new Array[Path](subDirsPerAlluxio))

  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val path = getFile(blockId)
    val output = fs.create(path, true)
    try {
      output.write(bytes.array())
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into Alluxio", e)
    } finally {
      try {
        output.close()
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to close output when put bytes to Alluxion", e)
      }
    }
  }

  override def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val output = fs.create(getFile(blockId), true)
    try {
      blockManager.serializerManager.dataSerializeStream(blockId, output, values)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into Alluxio", e)
    } finally {
      try {
        output.close()
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to close output when put bytes to Alluxion", e)
      }
    }
  }

  override def putFile(shuffleId : Int, blockId: BlockId, file : File): Unit = {

    val in = new FileInputStream(file)
    val bytes = new Array[Byte](file.length().toInt)
    in.read(bytes)
    in.close()
    putBytes(blockId, ByteBuffer.wrap(bytes))
  }


  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val path = getFile(blockId)
    if (!fs.exists(path)) {
      None
    } else {
      val size = fs.getFileStatus(path).getLen
      if (size == 0) {
        None
      } else {
        val input = fs.open(path)
        var flag = true
        try {
          val buffer = new Array[Byte](size.toInt)
          ByteStreams.readFully(input, buffer)
          Some(ByteBuffer.wrap(buffer))
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to get bytes of block $blockId from Alluxio", e)
            flag = false
            getBytesFromHdfs(blockId)
        } finally {
          if (flag) {
            try {
              input.close()
            } catch {
              case NonFatal(e) =>
                logWarning(s"Failed to close input when get bytes to Alluxion", e)
            }
          }
        }
      }
    }
  }

  def getBytesFromHdfs(blockId: BlockId): Option[ByteBuffer] = {
    usehdfs = true
    val path = changePathToHDFS(getFile(blockId))
    if (!hdfsFs.exists(path)) {
      None
    } else {
      val size = hdfsFs.getFileStatus(path).getLen
      if (size == 0) {
        logInfo("Test-log path size is " + size  )
        None
      } else {
        val input = hdfsFs.open(path)
        try {
          val buffer = new Array[Byte](size.toInt)
          ByteStreams.readFully(input, buffer)
          Some(ByteBuffer.wrap(buffer))
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to get bytes of block $blockId from HDFS", e)
            None
        } finally {
          try {
            input.close()
          } catch {
            case NonFatal(e) =>
              logWarning(s"Failed to close input when get bytes to Alluxion", e)
          }
        }
      }
    }
  }

  override def createInputStream(path: Path): Option[InputStream] = {
    if (usehdfs) {
      createInputStreamFromHDFS(path)
    } else {
      createInputStreamFromAlluxio(path)
    }
  }

  def createInputStreamFromAlluxio(path: Path): Option[InputStream] = {

    if (!fs.exists(path)) {
      None
    } else {
      val size = fs.getFileStatus(path).getLen
      if (size == 0) {
        None
      } else {
        try {
          val input = fs.open(path)
          Some(input)
        } catch {
          case NonFatal(e) =>
            logInfo(s"Failed to createInputStream $path from Alluxio", e)
            None
        }
      }
    }
  }

  def createInputStreamFromHDFS(path: Path): Option[InputStream] = {
    val hdfsPath = changePathToHDFS(path)
    if (!hdfsFs.exists(hdfsPath)) {
      None
    } else {
      val size = hdfsFs.getFileStatus(hdfsPath).getLen
      if (size == 0) {
        None
      } else {
        try {
          val input = hdfsFs.open(hdfsPath)
          Some(input)
        } catch {
          case NonFatal(e) =>
            logInfo(s"Failed to createInputStream $hdfsPath from HDFS", e)
            None
        }
      }
    }
  }

  def changePathToHDFS(path: Path): Path = {
    val pathName = path.getName
    getFileFromHDFS(BlockId(pathName))
  }

  override def getSize(path: Path): Long = {
    val size = if (!fs.exists(path)) {
      fs.getFileStatus(path).getLen
    } else 0L
    size
  }

  override def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val ct = implicitly[ClassTag[Iterator[_]]]
    val bytes: Option[ByteBuffer] = getBytes(blockId)
    bytes.map(bs =>
      // alluxio.hadoop.HdfsFileInputStream#available unsupport!
      // blockManager.dataDeserialize(blockId, input)
      blockManager.serializerManager.dataDeserializeStream(blockId,
        inputStream = new ByteBufferInputStream(bs))(ct)
    )
  }

  override def getSize(blockId: BlockId): Long =
    fs.getFileStatus(getFile(blockId)).getLen

  override def blockExists(blockId: BlockId): Boolean =
    fs.exists(getFile(blockId))

  override def removeBlock(blockId: BlockId): Boolean =
    fs.delete(getFile(blockId), false)

  override def shutdown(): Unit = {
    hdfsFs.close()
  }

  override def getFile(blockId: BlockId): Path = getFile(blockId.name)

  def getFile(filename: String): Path = {
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % subDirs.length
    val subDirId = hash % subDirs.length

    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
          old
      } else {
        val path = new Path(chroot, s"$dirId/" + subDirId.toString)
        subDirs(dirId)(subDirId) = path
        hdfsDirs(dirId)(subDirId) = new Path(hdfsroot, s"$dirId/" + subDirId.toString)
        path
      }
    }
    new Path(subDir, filename)
  }

  def getFileFromHDFS(blockId: BlockId): Path = {
    val hash = Utils.nonNegativeHash(blockId.name)
    val dirId = hash % subDirs.length
    val subDirId = hash % subDirs.length

    val subDir = hdfsDirs(dirId)(subDirId)

    new Path(subDir, blockId.name)
  }

}
