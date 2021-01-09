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

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

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
    new FileStatus(
      size, isDir, blockReplication, blockSize, modificationTime, new Path(new URI(path)))
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
class FileStreamSinkLog(
    metadataLogVersion: Int,
    sparkSession: SparkSession,
    path: String,
    _retentionMs: Option[Long] = None)
  extends CompactibleFileStreamLog[SinkFileStatus](metadataLogVersion, sparkSession, path) {

  private implicit val formats = Serialization.formats(NoTypeHints)

  protected override val fileCleanupDelayMs = sparkSession.sessionState.conf.fileSinkLogCleanupDelay

  protected override val isDeletingExpiredLog = sparkSession.sessionState.conf.fileSinkLogDeletion

  protected override val defaultCompactInterval =
    sparkSession.sessionState.conf.fileSinkLogCompactInterval

  require(defaultCompactInterval > 0,
    s"Please set ${SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key} (was $defaultCompactInterval) " +
      "to a positive value.")

  val retentionMs: Long = _retentionMs match {
    case Some(retention) =>
      logInfo(s"Retention is set to $retention ms")
      retention

    case _ => Long.MaxValue
  }

  override def shouldRetain(log: SinkFileStatus, currentTime: Long): Boolean = {
    if (retentionMs < Long.MaxValue) {
      if (currentTime - log.modificationTime > retentionMs) {
        logDebug(s"${log.path} excluded by retention - current time: $currentTime / " +
          s"modification time: ${log.modificationTime} / retention: $retentionMs ms.")
        false
      } else {
        true
      }
    } else {
      true
    }
  }
}

object FileStreamSinkLog {
  val VERSION = 1
  val ADD_ACTION = "add"
}
