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

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.hadoop.fs.{Path, PathFilter}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf

/**
 * @param path the file path
 * @param size the file size
 * @param action the file action. Must be either "add" or "delete".
 */
case class FileLog(path: String, size: Long, action: String)

/**
 * A special log for [[FileStreamSink]]. It will write one log file for each batch. The first line
 * of the log file is the version number, and there are multiple JSON lines following. Each JSON
 * line is a JSON format of [[FileLog]].
 *
 * As reading from many small files is usually pretty slow, [[FileStreamSinkLog]] will compact log
 * files every "spark.sql.sink.file.log.compactLen" batches into a big file. When doing a compact,
 * it will read all history logs and merge them with the new batch. During the compaction, it will
 * also delete the files that are deleted (marked by [[FileLog.action]]). When the reader uses
 * `allLogs` to list all files, this method only returns the visible files (drops the deleted
 * files).
 */
class FileStreamSinkLog(sqlContext: SQLContext, path: String)
  extends HDFSMetadataLog[Seq[FileLog]](sqlContext, path) {

  import FileStreamSinkLog._

  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * If we delete the old files after compaction at once, there is a race condition in S3: other
   * processes may see the old files are deleted but still cannot see the compaction file. The user
   * should set a reasonable `fileExpiredTimeMS`. We will wait until then so that the compaction
   * file is guaranteed to be visible for all readers
   */
  private val fileExpiredTimeMS = sqlContext.getConf(SQLConf.FILE_STREAM_SINK_LOG_EXPIRED_TIME)

  private val isDeletingExpiredLog = sqlContext.getConf(SQLConf.FILE_STREAM_SINK_LOG_DELETE)

  private val compactLength = sqlContext.getConf(SQLConf.FILE_STREAM_SINK_LOG_COMPACT_LEN)

  override def batchIdToPath(batchId: Long): Path = {
    if (isCompactionBatch(batchId, compactLength)) {
      new Path(metadataPath, s"$batchId$COMPACT_FILE_SUFFIX")
    } else {
      new Path(metadataPath, batchId.toString)
    }
  }

  override def pathToBatchId(path: Path): Long = {
    getBatchIdFromFileName(path.getName)
  }

  override def isBatchFile(path: Path): Boolean = {
    try {
      getBatchIdFromFileName(path.getName)
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  override def serialize(logs: Seq[FileLog]): Array[Byte] = {
    (VERSION +: logs.map(write(_))).mkString("\n").getBytes(UTF_8)
  }

  override def deserialize(bytes: Array[Byte]): Seq[FileLog] = {
    val lines = new String(bytes, UTF_8).split("\n")
    if (lines.length == 0) {
      throw new IllegalStateException("Incomplete log file")
    }
    val version = lines(0)
    if (version != VERSION) {
      throw new IllegalStateException(s"Unknown log version: ${version}")
    }
    lines.toSeq.slice(1, lines.length).map(read[FileLog](_))
  }

  override def add(batchId: Long, logs: Seq[FileLog]): Boolean = {
    if (isCompactionBatch(batchId, compactLength)) {
      compact(batchId, logs)
    } else {
      super.add(batchId, logs)
    }
  }

  /**
   * Compacts all logs before `batchId` plus the provided `logs`, and writes them into the
   * corresponding `batchId` file.
   */
  private def compact(batchId: Long, logs: Seq[FileLog]): Boolean = {
    val validBatches = getValidBatchesBeforeCompactionBatch(batchId, compactLength)
    val allLogs = validBatches.flatMap(batchId => get(batchId)).flatten ++ logs
    if (super.add(batchId, compactLogs(allLogs))) {
      if (isDeletingExpiredLog) {
        deleteExpiredLog(batchId)
      }
      true
    } else {
      // Return false as there is another writer.
      false
    }
  }

  /**
   * Returns all file logs except the deleted files.
   */
  def allLogs(): Array[FileLog] = {
    var latestId = getLatest().map(_._1).getOrElse(-1L)
    while (true) {
      if (latestId >= 0) {
        val startId = getAllValidBatches(latestId, compactLength)(0)
        try {
          val logs = get(Some(startId), Some(latestId)).flatMap(_._2)
          return compactLogs(logs).toArray
        } catch {
          case e: IOException =>
            // Another process may delete the batch files when we are reading. However, it only
            // happens when there is a compaction done. If so, we should retry to read the batches.
            // Otherwise, this is a real IO issue and we should throw it.
            val preLatestId = latestId
            latestId = getLatest().map(_._1).getOrElse(-1L)
            if (preLatestId == latestId) {
              throw e
            }
        }
      } else {
        return Array.empty
      }
    }
    Array.empty
  }

  /**
   * Since all logs before `compactionBatchId` are compacted and written into the
   * `compactionBatchId` log file, they can be removed. However, due to the eventual consistency of
   * S3, the compaction file may not be seen by other processes at once. So we only delete files
   * created `fileExpiredTimeMS` milliseconds ago.
   */
  private def deleteExpiredLog(compactionBatchId: Long): Unit = {
    val expiredTime = System.currentTimeMillis() - fileExpiredTimeMS
    fileManager.list(metadataPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        try {
          val batchId = getBatchIdFromFileName(path.getName)
          batchId < compactionBatchId
        } catch {
          case _: NumberFormatException =>
            false
        }
      }
    }).foreach { f =>
      if (f.getModificationTime <= expiredTime) {
        fileManager.delete(f.getPath)
      }
    }
  }
}

object FileStreamSinkLog {
  val VERSION = "v1"
  val COMPACT_FILE_SUFFIX = ".compact"
  val DELETE_ACTION = "delete"
  val ADD_ACTION = "add"

  def getBatchIdFromFileName(fileName: String): Long = {
    fileName.stripSuffix(COMPACT_FILE_SUFFIX).toLong
  }

  /**
   * Returns if this is a compaction batch. FileStreamSinkLog will compact old logs every
   * `compactLength` commits.
   *
   * E.g., if `compactLength` is 3, then 2, 5, 8, ... are all compaction batches.
   */
  def isCompactionBatch(batchId: Long, compactLength: Int): Boolean = {
    (batchId + 1) % compactLength == 0
  }

  /**
   * Returns all valid batches before the specified `compactionBatchId`. They contain all logs we
   * need to do a new compaction.
   */
  def getValidBatchesBeforeCompactionBatch(
      compactionBatchId: Long, compactLength: Int): Seq[Long] = {
    assert(isCompactionBatch(compactionBatchId, compactLength),
      s"$compactionBatchId is not a compaction batch")
    (math.max(0, compactionBatchId - compactLength)) until compactionBatchId
  }

  /**
   * Returns all necessary logs before `batchId` (inclusive). If `batchId` is a compaction, just
   * return itself. Otherwise, it will find the previous compaction batch and return all batches
   * between it and `batchId`.
   */
  def getAllValidBatches(batchId: Long, compactLength: Long): Seq[Long] = {
    assert(batchId >= 0)
    val start = math.max(0, (batchId + 1) / compactLength * compactLength - 1)
    start to batchId
  }

  /**
   * Removes all deleted files from logs. It assumes once one file is deleted, it won't be added to
   * the log in future.
   */
  def compactLogs(logs: Seq[FileLog]): Seq[FileLog] = {
    val deletedFiles = logs.filter(_.action == DELETE_ACTION).map(_.path).toSet
    if (deletedFiles.isEmpty) {
      logs
    } else {
      logs.filter(f => !deletedFiles.contains(f.path))
    }
  }
}
