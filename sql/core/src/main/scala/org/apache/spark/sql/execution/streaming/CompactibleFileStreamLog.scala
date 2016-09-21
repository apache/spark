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

import scala.reflect.ClassTag

import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.sql.SparkSession

/**
 * An abstract class for compactible metadata logs. It will write one log file for each batch.
 * The first line of the log file is the version number, and there are multiple serialized
 * metadata lines following.
 *
 * As reading from many small files is usually pretty slow, also too many
 * small files in one folder will mess the FS, [[CompactibleFileStreamLog]] will
 * compact log files every 10 batches by default into a big file. When
 * doing a compaction, it will read all old log files and merge them with the new batch.
 */
abstract class CompactibleFileStreamLog[T: ClassTag](
    metadataLogVersion: String,
    sparkSession: SparkSession,
    path: String)
  extends HDFSMetadataLog[Array[T]](sparkSession, path) {

  import CompactibleFileStreamLog._

  /**
   * If we delete the old files after compaction at once, there is a race condition in S3: other
   * processes may see the old files are deleted but still cannot see the compaction file using
   * "list". The `allFiles` handles this by looking for the next compaction file directly, however,
   * a live lock may happen if the compaction happens too frequently: one processing keeps deleting
   * old files while another one keeps retrying. Setting a reasonable cleanup delay could avoid it.
   */
  protected def fileCleanupDelayMs: Long

  protected def isDeletingExpiredLog: Boolean

  protected def compactInterval: Int

  /**
   * Serialize the data into encoded string.
   */
  protected def serializeData(t: T): String

  /**
   * Deserialize the string into data object.
   */
  protected def deserializeData(encodedString: String): T

  /**
   * Filter out the obsolete logs.
   */
  def compactLogs(logs: Seq[T]): Seq[T]

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

  override def serialize(logData: Array[T]): Array[Byte] = {
    (metadataLogVersion +: logData.map(serializeData)).mkString("\n").getBytes(UTF_8)
  }

  override def deserialize(bytes: Array[Byte]): Array[T] = {
    val lines = new String(bytes, UTF_8).split("\n")
    if (lines.length == 0) {
      throw new IllegalStateException("Incomplete log file")
    }
    val version = lines(0)
    if (version != metadataLogVersion) {
      throw new IllegalStateException(s"Unknown log version: ${version}")
    }
    lines.slice(1, lines.length).map(deserializeData)
  }

  override def add(batchId: Long, logs: Array[T]): Boolean = {
    if (isCompactionBatch(batchId, compactInterval)) {
      compact(batchId, logs)
    } else {
      super.add(batchId, logs)
    }
  }

  /**
   * Compacts all logs before `batchId` plus the provided `logs`, and writes them into the
   * corresponding `batchId` file. It will delete expired files as well if enabled.
   */
  private def compact(batchId: Long, logs: Array[T]): Boolean = {
    val validBatches = getValidBatchesBeforeCompactionBatch(batchId, compactInterval)
    val allLogs = validBatches.flatMap(batchId => super.get(batchId)).flatten ++ logs
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
   * Returns all files except the deleted ones.
   */
  def allFiles(): Array[T] = {
    var latestId = getLatest().map(_._1).getOrElse(-1L)
    // There is a race condition when `FileStreamSink` is deleting old files and `StreamFileCatalog`
    // is calling this method. This loop will retry the reading to deal with the
    // race condition.
    while (true) {
      if (latestId >= 0) {
        try {
          val logs =
            getAllValidBatches(latestId, compactInterval).flatMap(id => super.get(id)).flatten
          return compactLogs(logs).toArray
        } catch {
          case e: IOException =>
            // Another process using `CompactibleFileStreamLog` may delete the batch files when
            // `StreamFileCatalog` are reading. However, it only happens when a compaction is
            // deleting old files. If so, let's try the next compaction batch and we should find it.
            // Otherwise, this is a real IO issue and we should throw it.
            latestId = nextCompactionBatchId(latestId, compactInterval)
            super.get(latestId).getOrElse {
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

object CompactibleFileStreamLog {
  val COMPACT_FILE_SUFFIX = ".compact"

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
   * Returns the next compaction batch id after `batchId`.
   */
  def nextCompactionBatchId(batchId: Long, compactInterval: Long): Long = {
    (batchId + compactInterval + 1) / compactInterval * compactInterval - 1
  }
}
