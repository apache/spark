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

import java.io.{DataOutput, EOFException, IOException}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.SparkException
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Utils

/**
 * A [[MetadataLog]] implementation based on HDFS. [[HDFSMetadataLog]] uses the specified `path`
 * as the metadata storage.
 *
 * There should only be one [[HDFSMetadataLog]] using `path` at the same time. [[HDFSMetadataLog]]
 * uses a file ".lock" in the directory to make sure there is always only one user using `path`.
 * When [[HDFSMetadataLog]] is created, it will create a ".lock" file. If this step fails,
 * [[HDFSMetadataLog]] will throw an exception saying there is someone using the same directory.
 * When [[HDFSMetadataLog]] is stopped, it will delete the ".lock" file.
 *
 * However, in extreme case, e.g., power outage, [[HDFSMetadataLog]] won't be able to delete ".lock"
 * file. Then [[HDFSMetadataLog]] cannot be created even if no one is using the `path`. In such
 * case, the user has to delete the lock file in the exception message to restart their application.
 *
 * Note: [[HDFSMetadataLog]] doesn't support S3-like file systems as they don't guarantee listing
 * files in a directory always shows the latest files.
 */
class HDFSMetadataLog[T: ClassTag](sqlContext: SQLContext, path: String) extends MetadataLog[T] {

  private val metadataPath = new Path(path)
  private val fs = metadataPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
  if (!fs.exists(metadataPath)) {
    fs.mkdirs(metadataPath)
  }

  tryAcquireLock()

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

  private def tryAcquireLock(): Unit = {
    val lockFile = new Path(metadataPath, ".lock")
    fs.deleteOnExit(lockFile)
    try {
      // Must set overwrite to false otherwise HDFS allows multiple clients to create the same file.
      // When overwrite is false, the latter one will receive FileAlreadyExistsException
      fs.create(lockFile, false).close()
    } catch {
      case e: IOException =>
        throw new SparkException(s"There are another user using $path. Please check that. " +
          s"If nobody is using it, please delete ${lockFile}", e)
    }
  }

  private def batchFile(batchId: Long): Path = {
    new Path(metadataPath, batchId.toString)
  }

  override def add(batchId: Long, metadata: T): Unit = {
    get(batchId).getOrElse {
      // Only write metadata when the batch has not yet been written.
      val buffer = serializer.serialize(metadata)
      val output = try {
        fs.create(batchFile(batchId))
      } catch {
        case e: IOException if "java.lang.InterruptedException" == e.getMessage =>
          // create may convert InterruptedException to IOException. Let's convert it back to
          // InterruptedException so that this failure won't crash StreamExecution
          throw new InterruptedException("Creating file is interrupted")
      }
      try {
        output.writeInt(buffer.remaining())
        Utils.writeByteBuffer(buffer, output: DataOutput)
      } finally {
        output.close()
      }
    }
  }

  override def get(batchId: Long): Option[T] = {
    val batchMetadataFile = batchFile(batchId)
    if (fs.exists(batchMetadataFile)) {
      val input = fs.open(batchMetadataFile)
      try {
        val size = input.readInt()
        val bytes = new Array[Byte](size)
        val readSize = input.read(bytes)
        if (readSize == size) {
          Some(serializer.deserialize[T](ByteBuffer.wrap(bytes)))
        } else {
          None
        }
      } catch {
        case _: EOFException =>
          // The file is corrupted, so return None
          // For other exceptions, we still throw them
          None
      }
    } else {
      None
    }
  }

  override def get(startId: Option[Long], endId: Long): Array[(Long, T)] = {
    val batchIds = fs.listStatus(metadataPath, batchFilesFilter)
      .map(_.getPath.getName.toLong)
      .filter { batchId =>
      batchId <= endId && (startId.isEmpty || batchId >= startId.get)
    }
    batchIds.sorted.map(batchId => (batchId, get(batchId))).filter(_._2.isDefined).map {
      case (batchId, metadataOption) =>
        (batchId, metadataOption.get)
    }
  }

  override def getLatest(): Option[(Long, T)] = {
    val batchIds = fs.listStatus(metadataPath, batchFilesFilter)
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

  override def stop(): Unit = {
    val lockFile = new Path(metadataPath, ".lock")
    if (fs.exists(lockFile)) {
      fs.delete(lockFile, false)
    }
  }
}
