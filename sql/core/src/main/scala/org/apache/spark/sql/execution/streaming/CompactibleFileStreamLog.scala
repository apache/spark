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

import java.io.{FileNotFoundException, InputStream, IOException, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import scala.io.{Source => IOSource}
import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

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
    metadataLogVersion: Int,
    sparkSession: SparkSession,
    path: String)
  extends HDFSMetadataLog[Array[T]](sparkSession, path) {

  import CompactibleFileStreamLog._

  private implicit val formats = Serialization.formats(NoTypeHints)

  /** Needed to serialize type T into JSON when using Jackson */
  private implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

  protected val minBatchesToRetain = sparkSession.sessionState.conf.minBatchesToRetain

  /**
   * If we delete the old files after compaction at once, there is a race condition in S3: other
   * processes may see the old files are deleted but still cannot see the compaction file using
   * "list". The `allFiles` handles this by looking for the next compaction file directly, however,
   * a live lock may happen if the compaction happens too frequently: one processing keeps deleting
   * old files while another one keeps retrying. Setting a reasonable cleanup delay could avoid it.
   */
  protected def fileCleanupDelayMs: Long

  protected def isDeletingExpiredLog: Boolean

  protected def defaultCompactInterval: Int

  protected final lazy val compactInterval: Int = {
    // SPARK-18187: "compactInterval" can be set by user via defaultCompactInterval.
    // If there are existing log entries, then we should ensure a compatible compactInterval
    // is used, irrespective of the defaultCompactInterval. There are three cases:
    //
    // 1. If there is no '.compact' file, we can use the default setting directly.
    // 2. If there are two or more '.compact' files, we use the interval of patch id suffix with
    // '.compact' as compactInterval. This case could arise if isDeletingExpiredLog == false.
    // 3. If there is only one '.compact' file, then we must find a compact interval
    // that is compatible with (i.e., a divisor of) the previous compact file, and that
    // faithfully tries to represent the revised default compact interval i.e., is at least
    // is large if possible.
    // e.g., if defaultCompactInterval is 5 (and previous compact interval could have
    // been any 2,3,4,6,12), then a log could be: 11.compact, 12, 13, in which case
    // will ensure that the new compactInterval = 6 > 5 and (11 + 1) % 6 == 0
    val compactibleBatchIds = fileManager.list(metadataPath, batchFilesFilter)
      .filter(f => f.getPath.toString.endsWith(CompactibleFileStreamLog.COMPACT_FILE_SUFFIX))
      .map(f => pathToBatchId(f.getPath))
      .sorted(Ordering.Long.reverse)

    // Case 1
    var interval = defaultCompactInterval
    if (compactibleBatchIds.length >= 2) {
      // Case 2
      val latestCompactBatchId = compactibleBatchIds(0)
      val previousCompactBatchId = compactibleBatchIds(1)
      interval = (latestCompactBatchId - previousCompactBatchId).toInt
    } else if (compactibleBatchIds.length == 1) {
      // Case 3
      interval = CompactibleFileStreamLog.deriveCompactInterval(
        defaultCompactInterval, compactibleBatchIds(0).toInt)
    }
    assert(interval > 0, s"intervalValue = $interval not positive value.")
    logInfo(s"Set the compact interval to $interval " +
      s"[defaultCompactInterval: $defaultCompactInterval]")
    interval
  }

  /**
   * Determine whether the log should be retained or not.
   *
   * Default implementation retains all log entries. Implementations should override the method
   * to change the behavior.
   */
  def shouldRetain(log: T, currentTime: Long): Boolean = true

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

  private def serializeEntry(entry: T, out: OutputStream): Unit = {
    out.write(Serialization.write(entry).getBytes(UTF_8))
  }

  private def deserializeEntry(line: String): T = Serialization.read[T](line)

  override def serialize(logData: Array[T], out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(("v" + metadataLogVersion).getBytes(UTF_8))
    logData.foreach { data =>
      out.write('\n')
      serializeEntry(data, out)
    }
  }

  override def deserialize(in: InputStream): Array[T] = {
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }
    validateVersion(lines.next(), metadataLogVersion)
    lines.map(deserializeEntry).toArray
  }

  override def add(batchId: Long, logs: Array[T]): Boolean = {
    val batchAdded =
      if (isCompactionBatch(batchId, compactInterval)) {
        compact(batchId, logs)
      } else {
        super.add(batchId, logs)
      }
    if (batchAdded && isDeletingExpiredLog) {
      deleteExpiredLog(batchId)
    }
    batchAdded
  }

  /**
   * CompactibleFileStreamLog maintains logs by itself, and manual purging might break internal
   * state, specifically which latest compaction batch is purged.
   *
   * To simplify the situation, this method just throws UnsupportedOperationException regardless
   * of given parameter, and let CompactibleFileStreamLog handles purging by itself.
   */
  override def purge(thresholdBatchId: Long): Unit = throw new UnsupportedOperationException(
    s"Cannot purge as it might break internal state.")

  /**
   * Apply function on all entries in the specific batch. The method will throw
   * FileNotFoundException if the metadata log file doesn't exist.
   *
   * NOTE: This doesn't fail early on corruption. The caller should handle the exception
   * properly and make sure the logic is not affected by failing in the middle.
   */
  def foreachInBatch(batchId: Long)(fn: T => Unit): Unit = applyFnInBatch(batchId)(_.foreach(fn))

  /**
   * Apply filter on all entries in the specific batch.
   */
  def filterInBatch(batchId: Long)(predicate: T => Boolean): Option[Array[T]] = {
    try {
      Some(applyFnInBatch(batchId)(_.filter(predicate).toArray))
    } catch {
      case _: FileNotFoundException => None
    }
  }

  private def applyFnInBatch[RET](batchId: Long)(fn: Iterator[T] => RET): RET = {
    applyFnToBatchByStream(batchId) { input =>
      val lines = IOSource.fromInputStream(input, UTF_8.name()).getLines()
      if (!lines.hasNext) {
        throw new IllegalStateException("Incomplete log file")
      }
      validateVersion(lines.next(), metadataLogVersion)
      fn(lines.map(deserializeEntry))
    }
  }

  /**
   * Compacts all logs before `batchId` plus the provided `logs`, and writes them into the
   * corresponding `batchId` file. It will delete expired files as well if enabled.
   */
  private def compact(batchId: Long, logs: Array[T]): Boolean = {
    val curTime = System.currentTimeMillis()
    def writeEntry(entry: T, output: OutputStream): Unit = {
      if (shouldRetain(entry, curTime)) {
        output.write('\n')
        serializeEntry(entry, output)
      }
    }

    val (writeSucceed, elapsedMs) = Utils.timeTakenMs {
      addNewBatchByStream(batchId) { output =>
        output.write(("v" + metadataLogVersion).getBytes(UTF_8))
        val validBatches = getValidBatchesBeforeCompactionBatch(batchId, compactInterval)
        validBatches.foreach { id =>
          foreachInBatch(id) { entry => writeEntry(entry, output) }
        }
        logs.foreach { entry => writeEntry(entry, output) }
      }
    }

    if (elapsedMs >= COMPACT_LATENCY_WARN_THRESHOLD_MS) {
      logWarning(s"Compacting took $elapsedMs ms for compact batch $batchId")
    } else {
      logDebug(s"Compacting took $elapsedMs ms for compact batch $batchId")
    }

    writeSucceed
  }

  /**
   * Returns all files except the deleted ones.
   */
  def allFiles(): Array[T] = {
    val curTime = System.currentTimeMillis()
    var latestId = getLatestBatchId().getOrElse(-1L)
    // There is a race condition when `FileStreamSink` is deleting old files and `StreamFileIndex`
    // is calling this method. This loop will retry the reading to deal with the
    // race condition.
    while (true) {
      if (latestId >= 0) {
        try {
          val logs =
            getAllValidBatches(latestId, compactInterval).flatMap { id =>
              filterInBatch(id)(shouldRetain(_, curTime)).getOrElse {
                throw new IllegalStateException(
                  s"${batchIdToPath(id)} doesn't exist " +
                    s"(latestId: $latestId, compactInterval: $compactInterval)")
              }
            }
          return logs.toArray
        } catch {
          case e: IOException =>
            // Another process using `CompactibleFileStreamLog` may delete the batch files when
            // `StreamFileIndex` are reading. However, it only happens when a compaction is
            // deleting old files. If so, let's try the next compaction batch and we should find it.
            // Otherwise, this is a real IO issue and we should throw it.
            val expectedMinLatestId = nextCompactionBatchId(latestId, compactInterval)
            latestId = super.getLatestBatchId().getOrElse(-1)
            if (latestId < expectedMinLatestId) {
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
   * Delete expired log entries that proceed the currentBatchId and retain
   * sufficient minimum number of batches (given by minBatchesToRetain). This
   * equates to retaining the earliest compaction log that proceeds
   * batch id position currentBatchId + 1 - minBatchesToRetain. All log entries
   * prior to the earliest compaction log proceeding that position will be removed.
   * However, due to the eventual consistency of S3, the compaction file may not
   * be seen by other processes at once. So we only delete files created
   * `fileCleanupDelayMs` milliseconds ago.
   */
  private def deleteExpiredLog(currentBatchId: Long): Unit = {
    if (compactInterval <= currentBatchId + 1 - minBatchesToRetain) {
      // Find the first compaction batch id that maintains minBatchesToRetain
      val minBatchId = currentBatchId + 1 - minBatchesToRetain
      val minCompactionBatchId = minBatchId - (minBatchId % compactInterval) - 1
      assert(isCompactionBatch(minCompactionBatchId, compactInterval),
        s"$minCompactionBatchId is not a compaction batch")

      logInfo(s"Current compact batch id = $currentBatchId " +
        s"min compaction batch id to delete = $minCompactionBatchId")

      val expiredTime = System.currentTimeMillis() - fileCleanupDelayMs
      fileManager.list(metadataPath, (path: Path) => {
        try {
          val batchId = getBatchIdFromFileName(path.getName)
          batchId < minCompactionBatchId
        } catch {
          case _: NumberFormatException =>
            false
        }
      }).foreach { f =>
        if (f.getModificationTime <= expiredTime) {
          fileManager.delete(f.getPath)
        }
      }
    }
  }
}

object CompactibleFileStreamLog {
  val COMPACT_FILE_SUFFIX = ".compact"
  val COMPACT_LATENCY_WARN_THRESHOLD_MS = 2000

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

  /**
   * Derives a compact interval from the latest compact batch id and
   * a default compact interval.
   */
  def deriveCompactInterval(defaultInterval: Int, latestCompactBatchId: Int) : Int = {
    if (latestCompactBatchId + 1 <= defaultInterval) {
      latestCompactBatchId + 1
    } else if (defaultInterval < (latestCompactBatchId + 1) / 2) {
      // Find the first divisor >= default compact interval
      def properDivisors(min: Int, n: Int) =
        (min to n/2).view.filter(i => n % i == 0).toSeq :+ n

      properDivisors(defaultInterval, latestCompactBatchId + 1).head
    } else {
      // default compact interval > than any divisor other than latest compact id
      latestCompactBatchId + 1
    }
  }
}

