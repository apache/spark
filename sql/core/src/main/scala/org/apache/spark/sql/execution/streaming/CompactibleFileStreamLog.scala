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

import java.io.{InputStream, IOException, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util

import scala.io.{Source => IOSource}
import scala.reflect.ClassTag

import org.apache.hadoop.fs.{Path, PathFilter}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

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
abstract class CompactibleFileStreamLog[T <: AnyRef : ClassTag](
    metadataLogVersion: String,
    sparkSession: SparkSession,
    path: String)
  extends HDFSMetadataLog[Array[T]](sparkSession, path) {

  import CompactibleFileStreamLog._

  private implicit val formats = Serialization.formats(NoTypeHints)

  /** Needed to serialize type T into JSON when using Jackson */
  private implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

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
   * Filter out the obsolete logs.
   */
  def compactLogs(logs: Seq[T]): Seq[T]

  /**
   * Upon restart, we should pick up any previous batches including compaction batches. This is
   * not simple since `compactInterval` could vary from each run; to support the following
   * situations:
   *
   *  (1) a fresh run
   *  (2) the previous run with `compactInterval` = 2
   *      0
   *  (3) the previous run with `compactInterval` = 2
   *      0   1.compact
   *  (4) previous run with `compactInterval` = 2 and `compactInterval` = 5
   *      0   1.compact    2   3.compact   4.compact
   *  (5)last run with `compactInterval` = 2 and `compactInterval` = 5
   *      0   1.compact    2   3.compact   4.compact   5   6   7   8
   *
   * We introduce `knownCompactionBatches` which holds the existing compaction batches before
   * this run, and `zeroBatch` which holds the first batch this run should write to. Thus we can
   * support the above situations with:
   *
   *  (1) `knownCompactionBatches` = (), `zeroBatch` = 0
   *  (2) `knownCompactionBatches` = (), `zeroBatch` = 1
   *  (3) `knownCompactionBatches` = (1), `zeroBatch` = 2
   *  (4) `knownCompactionBatches` = (1, 3, 4), `zeroBatch` = 5
   *  (5) `knownCompactionBatches` = (1, 3, 4), `zeroBatch` = 9
   */
  private[sql] val (knownCompactionBatches: Array[Long], zeroBatch: Long) = {
    val fileNames: Array[String] =
      listExistingFiles()
        .filter(isBatchFile)
        .map(path => (getBatchIdFromFileName(path.getName), path))
        .sortBy(_._1)
        .reverse
        .dropWhile(idAndPath => super.get(idAndPath._2).isEmpty)
        .reverse
        .map(idAndPath => idAndPath._2.getName)

    val knownCompactionBatches =
      fileNames
        .filter(isCompactionBatchFromFileName)
        .map(getBatchIdFromFileName)
    val zeroBatch = fileNames.map(getBatchIdFromFileName).lastOption

    (knownCompactionBatches, zeroBatch.map(_ + 1).getOrElse(0L))
  }

  override def batchIdToPath(batchId: Long): Path = {
    if (isCompactionBatch(knownCompactionBatches, zeroBatch, batchId, compactInterval)) {
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

  override def serialize(logData: Array[T], out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(metadataLogVersion.getBytes(UTF_8))
    logData.foreach { data =>
      out.write('\n')
      out.write(Serialization.write(data).getBytes(UTF_8))
    }
  }

  override def deserialize(in: InputStream): Array[T] = {
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }
    val version = lines.next()
    if (version != metadataLogVersion) {
      throw new IllegalStateException(s"Unknown log version: ${version}")
    }
    lines.map(Serialization.read[T]).toArray
  }

  override def add(batchId: Long, logs: Array[T]): Boolean = {
    if (isCompactionBatch(knownCompactionBatches, zeroBatch, batchId, compactInterval)) {
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
    val validBatches = getValidBatchesBeforeCompactionBatch(
      knownCompactionBatches, zeroBatch, batchId, compactInterval)
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
    // There is a race condition when `FileStreamSink` is deleting old files and `StreamFileIndex`
    // is calling this method. This loop will retry the reading to deal with the
    // race condition.
    while (true) {
      if (latestId >= 0) {
        try {
          val logs =
            getAllValidBatches(knownCompactionBatches, zeroBatch, latestId, compactInterval)
              .flatMap(id => super.get(id))
              .flatten
          return compactLogs(logs).toArray
        } catch {
          case e: IOException =>
            // Another process using `CompactibleFileStreamLog` may delete the batch files when
            // `StreamFileIndex` are reading. However, it only happens when a compaction is
            // deleting old files. If so, let's try the next compaction batch and we should find it.
            // Otherwise, this is a real IO issue and we should throw it.
            latestId = nextCompactionBatchId(zeroBatch, latestId, compactInterval)
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

  def isCompactionBatchFromFileName(fileName: String): Boolean = {
    fileName.endsWith(COMPACT_FILE_SUFFIX)
  }

  /**
   * Returns if this is a compaction batch. FileStreamSinkLog will compact old logs every
   * `compactInterval` commits.
   *
   * E.g., given `zeroBatch` equals 10 and `compactInterval` equals 3, then 12, 15, 18, ... are all
   * compaction batches.
   */
  def isCompactionBatch(
      knownCompactionBatches: Array[Long],
      zeroBatch: Long,
      batchId: Long,
      compactInterval: Int): Boolean = {
    if (batchId < zeroBatch) {
      knownCompactionBatches.nonEmpty &&
        util.Arrays.binarySearch(knownCompactionBatches, batchId) >= 0
    }
    else {
      (batchId - zeroBatch + 1) % compactInterval == 0
    }
  }

  /**
   * Returns all valid batches before the specified `compactionBatchId`. They contain all logs we
   * need to do a new compaction.
   *
   * E.g., given `zeroBatch` equals 10 and `compactInterval` equals 3, this method should return
   * `Seq(12, 13, 14)` for `compactionBatchId` 15 (Note: it includes the previous compaction batch
   * 12).
   */
  def getValidBatchesBeforeCompactionBatch(
      knownCompactionBatches: Array[Long],
      zeroBatch: Long,
      compactionBatchId: Long,
      compactInterval: Int): Seq[Long] = {
    assert(
      isCompactionBatch(knownCompactionBatches, zeroBatch, compactionBatchId, compactInterval),
      s"$compactionBatchId is not a compaction batch")
    assert(compactionBatchId >= zeroBatch, s"start at least with zeroBatch = $zeroBatch!")

    if (compactionBatchId - compactInterval >= zeroBatch) {
      // we have at least one compaction batch since zeroBatch
      (compactionBatchId - compactInterval) until compactionBatchId
    } else {
      // we have no compaction batch yet since zeroBatch
      // so pick the latest compaction batch (if exist) from previous runs, or just pick 0
      knownCompactionBatches.lastOption.getOrElse(0L) until compactionBatchId
    }
  }

  /**
   * Returns all necessary logs before `batchId` (inclusive). If `batchId` is a compaction, just
   * return itself. Otherwise, it will find the previous compaction batch and return all batches
   * between it and `batchId`.
   */
  def getAllValidBatches(
      knownCompactionBatches: Array[Long],
      zeroBatch: Long,
      batchId: Long,
      compactInterval: Long): Seq[Long] = {
    assert(batchId >= 0)
    if (batchId >= zeroBatch) {
      val _nextCompactionBatchId = nextCompactionBatchId(zeroBatch, batchId, compactInterval)
      if (_nextCompactionBatchId - compactInterval >= zeroBatch) {
        // we have at least one compaction batch since zeroBatch
        // so we pick the latest compaction batch id in this run
        return (_nextCompactionBatchId - compactInterval) to batchId
      }
    }
    // we have no compaction batch yet since zeroBatch
    // so pick the latest compaction batch less than or equal to `batchId` (if exist) from
    // previous runs, or just pick 0
    return knownCompactionBatches.reverse.find(_ <= batchId).getOrElse(0L) to batchId
  }

  /**
   * Returns the next compaction batch id after `batchId`.
   *
   * E.g., given `zeroBatch` equals 10, `compactInterval` equals 3, this method should return 12 for
   * `batchId` 10, 11, should return 15 for `batchId` 12, 13, 14.
   */
  def nextCompactionBatchId(
      zeroBatch: Long,
      batchId: Long,
      compactInterval: Long): Long = {
    assert(batchId >= zeroBatch, s"start at least with zeroBatch = $zeroBatch!")
    (batchId - zeroBatch + compactInterval + 1) / compactInterval * compactInterval + zeroBatch - 1
  }
}
