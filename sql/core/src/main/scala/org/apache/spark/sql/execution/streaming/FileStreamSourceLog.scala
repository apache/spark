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

import java.util.{LinkedHashMap => JLinkedHashMap}
import java.util.Map.Entry

import scala.collection.mutable

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.FileEntry
import org.apache.spark.sql.internal.SQLConf

class FileStreamSourceLog(
    metadataLogVersion: Int,
    sparkSession: SparkSession,
    path: String)
  extends CompactibleFileStreamLog[FileEntry](metadataLogVersion, sparkSession, path) {

  import CompactibleFileStreamLog._
  import FileStreamSourceLog._

  // Configurations about metadata compaction
  protected override val defaultCompactInterval: Int =
    sparkSession.sessionState.conf.fileSourceLogCompactInterval

  require(defaultCompactInterval > 0,
    s"Please set ${SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key} " +
      s"(was $defaultCompactInterval) to a positive value.")

  protected override val fileCleanupDelayMs =
    sparkSession.sessionState.conf.fileSourceLogCleanupDelay

  protected override val isDeletingExpiredLog = sparkSession.sessionState.conf.fileSourceLogDeletion

  private implicit val formats = Serialization.formats(NoTypeHints)

  // A fixed size log entry cache to cache the file entries belong to the compaction batch. It is
  // used to avoid scanning the compacted log file to retrieve it's own batch data.
  private val cacheSize = compactInterval
  private val fileEntryCache = new JLinkedHashMap[Long, Array[FileEntry]] {
    override def removeEldestEntry(eldest: Entry[Long, Array[FileEntry]]): Boolean = {
      size() > cacheSize
    }
  }

  override def add(batchId: Long, logs: Array[FileEntry]): Boolean = {
    if (super.add(batchId, logs)) {
      if (isCompactionBatch(batchId, compactInterval)) {
        fileEntryCache.put(batchId, logs)
      }
      true
    } else {
      false
    }
  }

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, Array[FileEntry])] = {
    val startBatchId = startId.getOrElse(0L)
    val endBatchId = endId.orElse(getLatest().map(_._1)).getOrElse(0L)

    val (existedBatches, removedBatches) = (startBatchId to endBatchId).map { id =>
      if (isCompactionBatch(id, compactInterval) && fileEntryCache.containsKey(id)) {
        (id, Some(fileEntryCache.get(id)))
      } else {
        val logs = filterInBatch(id)(_.batchId == id)
        (id, logs)
      }
    }.partition(_._2.isDefined)

    // The below code may only be happened when original metadata log file has been removed, so we
    // have to get the batch from latest compacted log file. This is quite time-consuming and may
    // not be happened in the current FileStreamSource code path, since we only fetch the
    // latest metadata log file.
    val searchKeys = removedBatches.map(_._1)
    val retrievedBatches = if (searchKeys.nonEmpty) {
      logWarning(s"Get batches from removed files, this is unexpected in the current code path!!!")
      val latestBatchId = getLatestBatchId().getOrElse(-1L)
      if (latestBatchId < 0) {
        Map.empty[Long, Option[Array[FileEntry]]]
      } else {
        val latestCompactedBatchId = getAllValidBatches(latestBatchId, compactInterval)(0)
        val allLogs = new mutable.HashMap[Long, mutable.ArrayBuffer[FileEntry]]

        super.get(latestCompactedBatchId).foreach { entries =>
          entries.foreach { e =>
            allLogs.put(e.batchId, allLogs.getOrElse(e.batchId, mutable.ArrayBuffer()) += e)
          }
        }

        searchKeys.map(id => id -> allLogs.get(id).map(_.toArray)).filter(_._2.isDefined).toMap
      }
    } else {
      Map.empty[Long, Option[Array[FileEntry]]]
    }

    val batches =
      (existedBatches ++ retrievedBatches).map(i => i._1 -> i._2.get).toArray.sortBy(_._1)
    if (startBatchId <= endBatchId) {
      HDFSMetadataLog.verifyBatchIds(batches.map(_._1), startId, endId)
    }
    batches
  }

  def restore(): Array[FileEntry] = {
    val files = allFiles()

    // When restarting the query, there is a case which the query starts from compaction batch,
    // and the batch has source metadata file to read. One case is that the previous query
    // succeeded to read from inputs, but not finalized the batch for various reasons.
    // The below code finds the latest compaction batch, and put entries for the batch into the
    // file entry cache which would avoid reading compact batch file twice.
    // It doesn't know about offset / commit metadata in checkpoint so doesn't know which exactly
    // batch to start from, but in practice, only couple of latest batches are candidates to
    // be started. We leverage the fact to skip calculation if possible.
    files.lastOption.foreach { lastEntry =>
      val latestBatchId = lastEntry.batchId
      val latestCompactedBatchId = getAllValidBatches(latestBatchId, compactInterval)(0)
      if ((latestBatchId - latestCompactedBatchId) < PREV_NUM_BATCHES_TO_READ_IN_RESTORE) {
        val logsForLatestCompactedBatch = files.filter { entry =>
          entry.batchId == latestCompactedBatchId
        }
        fileEntryCache.put(latestCompactedBatchId, logsForLatestCompactedBatch)
      }
    }

    files
  }
}

object FileStreamSourceLog {
  val VERSION = 1
  val PREV_NUM_BATCHES_TO_READ_IN_RESTORE = 2
}
