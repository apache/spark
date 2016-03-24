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

package org.apache.spark.sql.execution.streaming

import java.io.{FileNotFoundException, IOException}
import java.nio.ByteBuffer
import java.util.{ConcurrentModificationException, EnumSet}

import scala.reflect.ClassTag

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.SQLContext


/**
 * A [[MetadataLog]] implementation based on HDFS. [[HDFSMetadataLog]] uses the specified `path`
 * as the metadata storage.
 *
 * When writing a new batch, [[HDFSMetadataLog]] will firstly write to a temp file and then rename
 * it to the final batch file. If the rename step fails, there must be multiple writers and only
 * one of them will succeed and the others will fail.
 *
 * Note: [[HDFSMetadataLog]] doesn't support S3-like file systems as they don't guarantee listing
 * files in a directory always shows the latest files.
 */
class HDFSMetadataLog[T: ClassTag](sqlContext: SQLContext, path: String)
  extends MetadataLog[T]
  with Logging {

  import HDFSMetadataLog._

  private val metadataPath = new Path(path)
  private val fileManager = createFileManager()

  if (!fileManager.exists(metadataPath)) {
    fileManager.mkdirs(metadataPath)
  }

  /**
   * A `PathFilter` to filter only batch files
   */
  private val batchFilesFilter = new PathFilter {
    override def accept(path: Path): Boolean = try {
      path.getName.toLong
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  private val serializer = new JavaSerializer(sqlContext.sparkContext.conf).newInstance()

  private def batchFile(batchId: Long): Path = {
    new Path(metadataPath, batchId.toString)
  }

  override def add(batchId: Long, metadata: T): Boolean = {
    get(batchId).map(_ => false).getOrElse {
      // Only write metadata when the batch has not yet been written.
      val buffer = serializer.serialize(metadata)
      try {
        writeBatch(batchId, JavaUtils.bufferToArray(buffer))
        true
      } catch {
        case e: IOException if "java.lang.InterruptedException" == e.getMessage =>
          // create may convert InterruptedException to IOException. Let's convert it back to
          // InterruptedException so that this failure won't crash StreamExecution
          throw new InterruptedException("Creating file is interrupted")
      }
    }
  }

  /**
   * Write a batch to a temp file then rename it to the batch file.
   *
   * There may be multiple [[HDFSMetadataLog]] using the same metadata path. Although it is not a
   * valid behavior, we still need to prevent it from destroying the files.
   */
  private def writeBatch(batchId: Long, bytes: Array[Byte]): Unit = {
    // Use nextId to create a temp file
    var nextId = 0
    while (true) {
      val tempPath = new Path(metadataPath, s".${batchId}_$nextId.tmp")
      fileManager.deleteOnExit(tempPath)
      try {
        val output = fileManager.create(tempPath)
        try {
          output.write(bytes)
        } finally {
          output.close()
        }
        try {
          // Try to commit the batch
          // It will fail if there is an existing file (someone has committed the batch)
          logDebug(s"Attempting to write log #${batchFile(batchId)}")
          fileManager.rename(tempPath, batchFile(batchId))
          return
        } catch {
          case e: IOException if isFileAlreadyExistsException(e) =>
            // If "rename" fails, it means some other "HDFSMetadataLog" has committed the batch.
            // So throw an exception to tell the user this is not a valid behavior.
            throw new ConcurrentModificationException(
              s"Multiple HDFSMetadataLog are using $path", e)
          case e: FileNotFoundException =>
            // Sometimes, "create" will succeed when multiple writers are calling it at the same
            // time. However, only one writer can call "rename" successfully, others will get
            // FileNotFoundException because the first writer has removed it.
            throw new ConcurrentModificationException(
              s"Multiple HDFSMetadataLog are using $path", e)
        }
      } catch {
        case e: IOException if isFileAlreadyExistsException(e) =>
          // Failed to create "tempPath". There are two cases:
          // 1. Someone is creating "tempPath" too.
          // 2. This is a restart. "tempPath" has already been created but not moved to the final
          // batch file (not committed).
          //
          // For both cases, the batch has not yet been committed. So we can retry it.
          //
          // Note: there is a potential risk here: if HDFSMetadataLog A is running, people can use
          // the same metadata path to create "HDFSMetadataLog" and fail A. However, this is not a
          // big problem because it requires the attacker must have the permission to write the
          // metadata path. In addition, the old Streaming also have this issue, people can create
          // malicious checkpoint files to crash a Streaming application too.
          nextId += 1
      }
    }
  }

  private def isFileAlreadyExistsException(e: IOException): Boolean = {
    e.isInstanceOf[FileAlreadyExistsException] ||
      // Old Hadoop versions don't throw FileAlreadyExistsException. Although it's fixed in
      // HADOOP-9361, we still need to support old Hadoop versions.
      (e.getMessage != null && e.getMessage.startsWith("File already exists: "))
  }

  override def get(batchId: Long): Option[T] = {
    val batchMetadataFile = batchFile(batchId)
    if (fileManager.exists(batchMetadataFile)) {
      val input = fileManager.open(batchMetadataFile)
      val bytes = IOUtils.toByteArray(input)
      Some(serializer.deserialize[T](ByteBuffer.wrap(bytes)))
    } else {
      logDebug(s"Unable to find batch $batchMetadataFile")
      None
    }
  }

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, T)] = {
    val files = fileManager.list(metadataPath, batchFilesFilter)
    val batchIds = files
      .map(_.getPath.getName.toLong)
      .filter { batchId =>
        (endId.isEmpty || batchId <= endId.get) && (startId.isEmpty || batchId >= startId.get)
    }
    batchIds.sorted.map(batchId => (batchId, get(batchId))).filter(_._2.isDefined).map {
      case (batchId, metadataOption) =>
        (batchId, metadataOption.get)
    }
  }

  override def getLatest(): Option[(Long, T)] = {
    val batchIds = fileManager.list(metadataPath, batchFilesFilter)
      .map(_.getPath.getName.toLong)
      .sorted
      .reverse
    for (batchId <- batchIds) {
      val batch = get(batchId)
      if (batch.isDefined) {
        return Some((batchId, batch.get))
      }
    }
    None
  }

  private def createFileManager(): FileManager = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    try {
      new FileContextManager(metadataPath, hadoopConf)
    } catch {
      case e: UnsupportedFileSystemException =>
        logWarning("Could not use FileContext API for managing metadata log file. The log may be" +
          "inconsistent under failures.", e)
        new FileSystemManager(metadataPath, hadoopConf)
    }
  }
}

object HDFSMetadataLog {

  /** A simple trait to abstract out the file management operations needed by HDFSMetadataLog */
  trait FileManager {
    def list(path: Path, filter: PathFilter): Array[FileStatus]
    def mkdirs(path: Path): Unit
    def exists(path: Path): Boolean
    def open(path: Path): FSDataInputStream
    def create(path: Path): FSDataOutputStream
    def rename(srcPath: Path, destPath: Path): Unit
    def deleteOnExit(path: Path): Unit
  }

  /** Implementation of FileManager using newer FileContext API */
  class FileContextManager(path: Path, hadoopConf: Configuration) extends FileManager {
    private val fc = if (path.toUri.getScheme == null) {
      FileContext.getFileContext(hadoopConf)
    } else {
      FileContext.getFileContext(path.toUri, hadoopConf)
    }

    override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
      fc.util.listStatus(path, filter)
    }

    override def rename(srcPath: Path, destPath: Path): Unit = {
      fc.rename(srcPath, destPath)
    }

    override def mkdirs(path: Path): Unit = {
      fc.mkdir(path, FsPermission.getDirDefault, true)
    }

    override def open(path: Path): FSDataInputStream = {
      fc.open(path)
    }

    override def create(path: Path): FSDataOutputStream = {
      fc.create(path, EnumSet.of(CreateFlag.CREATE))
    }

    override def exists(path: Path): Boolean = {
      fc.util().exists(path)
    }

    override def deleteOnExit(path: Path): Unit = {
      fc.deleteOnExit(path)
    }
  }

  /** Implementation of FileManager using older FileSystem API */
  class FileSystemManager(path: Path, hadoopConf: Configuration) extends FileManager {
    private val fs = if (path.toUri.getScheme == null) {
      FileSystem.get(hadoopConf)
    } else {
      FileSystem.get(path.toUri, hadoopConf)
    }

    override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
      fs.listStatus(path, filter)
    }

    override def rename(srcPath: Path, destPath: Path): Unit = {
      if (fs.exists(destPath)) {
        throw new FileAlreadyExistsException(s"File already exists: $destPath")
      }
      fs.rename(srcPath, destPath)
    }

    override def mkdirs(path: Path): Unit = {
      fs.mkdirs(path, FsPermission.getDirDefault)
    }

    override def open(path: Path): FSDataInputStream = {
      fs.open(path)
    }

    override def create(path: Path): FSDataOutputStream = {
      fs.create(path, false)
    }

    override def exists(path: Path): Boolean = {
      fs.exists(path)
    }

    override def deleteOnExit(path: Path): Unit = {
      fs.deleteOnExit(path)
    }
  }
}
