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

import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * The status of a file outputted by [[FileStreamSink]]. A file is visible only if it appears in
 * the sink log and its action is not "delete".
 *
 * @param path the file path.
 * @param size the file size.
 * @param isDir whether this file is a directory.
 * @param modificationTime the file last modification time.
 * @param blockReplication the block replication.
 * @param blockSize the block size.
 * @param action the file action. Must be either "add" or "delete".
 */
case class SinkFileStatus(
    path: String,
    size: Long,
    isDir: Boolean,
    modificationTime: Long,
    blockReplication: Int,
    blockSize: Long,
    action: String) {

  def toFileStatus: FileStatus = {
    new FileStatus(size, isDir, blockReplication, blockSize, modificationTime, new Path(path))
  }
}

object SinkFileStatus {
  def apply(f: FileStatus): SinkFileStatus = {
    SinkFileStatus(
      path = f.getPath.toUri.toString,
      size = f.getLen,
      isDir = f.isDirectory,
      modificationTime = f.getModificationTime,
      blockReplication = f.getReplication,
      blockSize = f.getBlockSize,
      action = FileStreamSinkLog.ADD_ACTION)
  }
}

/**
 * A special log for [[FileStreamSink]]. It will write one log file for each batch. The first line
 * of the log file is the version number, and there are multiple JSON lines following. Each JSON
 * line is a JSON format of [[SinkFileStatus]].
 *
 * As reading from many small files is usually pretty slow, [[FileStreamSinkLog]] will compact log
 * files every "spark.sql.sink.file.log.compactLen" batches into a big file. When doing a
 * compaction, it will read all old log files and merge them with the new batch. During the
 * compaction, it will also delete the files that are deleted (marked by [[SinkFileStatus.action]]).
 * When the reader uses `allFiles` to list all files, this method only returns the visible files
 * (drops the deleted files).
 */
class FileStreamSinkLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[Array[SinkFileStatus]](sparkSession, path) {

  import FileStreamSinkLog._

  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * If we delete the old files after compaction at once, there is a race condition in S3: other
   * processes may see the old files are deleted but still cannot see the compaction file using
   * "list". The `allFiles` handles this by looking for the next compaction file directly, however,
   * a live lock may happen if the compaction happens too frequently: one processing keeps deleting
   * old files while another one keeps retrying. Setting a reasonable cleanup delay could avoid it.
   */
  private val fileCleanupDelayMs = sparkSession.sessionState.conf.fileSinkLogCleanupDelay

  private val isDeletingExpiredLog = sparkSession.sessionState.conf.fileSinkLogDeletion

  private val compactInterval = sparkSession.sessionState.conf.fileSinkLogCompatInterval
  require(compactInterval > 0,
    s"Please set ${SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key} (was $compactInterval) " +
      "to a positive value.")

  override def batchIdToPath(batchId: Long): Path = {
    if (isCompactionBatch(batchId, compactInterval)) {
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

  override def serialize(logData: Array[SinkFileStatus]): Array[Byte] = {
    (VERSION +: logData.map(write(_))).mkString("\n").getBytes(UTF_8)
  }

  override def deserialize(bytes: Array[Byte]): Array[SinkFileStatus] = {
    val lines = new String(bytes, UTF_8).split("\n")
    if (lines.length == 0) {
      throw new IllegalStateException("Incomplete log file")
    }
    val version = lines(0)
    if (version != VERSION) {
      throw new IllegalStateException(s"Unknown log version: ${version}")
    }
    lines.slice(1, lines.length).map(read[SinkFileStatus](_))
  }

  override def add(batchId: Long, logs: Array[SinkFileStatus]): Boolean = {
    if (isCompactionBatch(batchId, compactInterval)) {
      compact(batchId, logs)
    } else {
      super.add(batchId, logs)
    }
  }

  /**
   * Returns all files except the deleted ones.
   */
  def allFiles(): Array[SinkFileStatus] = {
    var latestId = getLatest().map(_._1).getOrElse(-1L)
    // There is a race condition when `FileStreamSink` is deleting old files and `StreamFileCatalog`
    // is calling this method. This loop will retry the reading to deal with the
    // race condition.
    while (true) {
      if (latestId >= 0) {
        val startId = getAllValidBatches(latestId, compactInterval)(0)
        try {
          val logs = get(Some(startId), Some(latestId)).flatMap(_._2)
          return compactLogs(logs).toArray
        } catch {
          case e: IOException =>
            // Another process using `FileStreamSink` may delete the batch files when
            // `StreamFileCatalog` are reading. However, it only happens when a compaction is
            // deleting old files. If so, let's try the next compaction batch and we should find it.
            // Otherwise, this is a real IO issue and we should throw it.
            latestId = nextCompactionBatchId(latestId, compactInterval)
            get(latestId).getOrElse {
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
   * Compacts all logs before `batchId` plus the provided `logs`, and writes them into the
   * corresponding `batchId` file. It will delete expired files as well if enabled.
   */
  private def compact(batchId: Long, logs: Seq[SinkFileStatus]): Boolean = {
    val validBatches = getValidBatchesBeforeCompactionBatch(batchId, compactInterval)
    val allLogs = validBatches.flatMap(batchId => get(batchId)).flatten ++ logs
    if (super.add(batchId, compactLogs(allLogs).toArray)) {
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
   * Since all logs before `compactionBatchId` are compacted and written into the
   * `compactionBatchId` log file, they can be removed. However, due to the eventual consistency of
   * S3, the compaction file may not be seen by other processes at once. So we only delete files
   * created `fileCleanupDelayMs` milliseconds ago.
   */
  private def deleteExpiredLog(compactionBatchId: Long): Unit = {
    val expiredTime = System.currentTimeMillis() - fileCleanupDelayMs
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
   * `compactInterval` commits.
   *
   * E.g., if `compactInterval` is 3, then 2, 5, 8, ... are all compaction batches.
   */
  def isCompactionBatch(batchId: Long, compactInterval: Int): Boolean = {
    (batchId + 1) % compactInterval == 0
  }

  /**
   * Returns all valid batches before the specified `compactionBatchId`. They contain all logs we
   * need to do a new compaction.
   *
   * E.g., if `compactInterval` is 3 and `compactionBatchId` is 5, this method should returns
   * `Seq(2, 3, 4)` (Note: it includes the previous compaction batch 2).
   */
  def getValidBatchesBeforeCompactionBatch(
      compactionBatchId: Long,
      compactInterval: Int): Seq[Long] = {
    assert(isCompactionBatch(compactionBatchId, compactInterval),
      s"$compactionBatchId is not a compaction batch")
    (math.max(0, compactionBatchId - compactInterval)) until compactionBatchId
  }

  /**
   * Returns all necessary logs before `batchId` (inclusive). If `batchId` is a compaction, just
   * return itself. Otherwise, it will find the previous compaction batch and return all batches
   * between it and `batchId`.
   */
  def getAllValidBatches(batchId: Long, compactInterval: Long): Seq[Long] = {
    assert(batchId >= 0)
    val start = math.max(0, (batchId + 1) / compactInterval * compactInterval - 1)
    start to batchId
  }

  /**
   * Removes all deleted files from logs. It assumes once one file is deleted, it won't be added to
   * the log in future.
   */
  def compactLogs(logs: Seq[SinkFileStatus]): Seq[SinkFileStatus] = {
    val deletedFiles = logs.filter(_.action == DELETE_ACTION).map(_.path).toSet
    if (deletedFiles.isEmpty) {
      logs
    } else {
      logs.filter(f => !deletedFiles.contains(f.path))
    }
  }

  /**
   * Returns the next compaction batch id after `batchId`.
   */
  def nextCompactionBatchId(batchId: Long, compactInterval: Long): Long = {
    (batchId + compactInterval + 1) / compactInterval * compactInterval - 1
  }
}
