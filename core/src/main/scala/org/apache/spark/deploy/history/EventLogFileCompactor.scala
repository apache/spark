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

package org.apache.spark.deploy.history

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN
import org.apache.spark.scheduler._

/**
 * This class compacts the old event log files into one compact file, via two phases reading:
 *
 * 1) Initialize available [[EventFilterBuilder]] instances, and replay the old event log files with
 * builders, so that these builders can gather the information to create [[EventFilter]] instances.
 * 2) Initialize [[EventFilter]] instances from [[EventFilterBuilder]] instances, and replay the
 * old event log files with filters. Rewrite the content to the compact file if the filters decide
 * to filter in.
 *
 * This class assumes caller will provide the sorted list of files which are sorted by the index of
 * event log file - caller should keep in mind that this class doesn't care about the semantic of
 * ordering.
 *
 * When compacting the files, the range of compaction for given file list is determined as:
 * (rightmost compact file ~ the file where there're `maxFilesToRetain` files on the right side)
 *
 * If there's no compact file in the list, it starts from the first file. If there're not enough
 * files after rightmost compact file, compaction will be skipped.
 */
class EventLogFileCompactor(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    fs: FileSystem) extends Logging {

  private val maxFilesToRetain: Int = sparkConf.get(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN)

  def compact(eventLogFiles: Seq[FileStatus]): Seq[FileStatus] = {
    if (eventLogFiles.length <= maxFilesToRetain) {
      return eventLogFiles
    }

    if (EventLogFileWriter.isCompacted(eventLogFiles.last.getPath)) {
      return Seq(eventLogFiles.last)
    }

    val (filesToCompact, filesToRetain) = findFilesToCompact(eventLogFiles)
    if (filesToCompact.isEmpty) {
      filesToRetain
    } else {
      val builders = EventFilterBuilder.initializeBuilders(fs, filesToCompact.map(_.getPath))

      val rewriter = new FilteredEventLogFileRewriter(sparkConf, hadoopConf, fs,
        builders.map(_.createFilter()))
      val compactedPath = rewriter.rewrite(filesToCompact)

      cleanupCompactedFiles(filesToCompact)

      fs.getFileStatus(new Path(compactedPath)) :: filesToRetain.toList
    }
  }

  private def cleanupCompactedFiles(files: Seq[FileStatus]): Unit = {
    files.foreach { file =>
      var deleted = false
      try {
        deleted = fs.delete(file.getPath, true)
      } catch {
        case _: IOException =>
      }
      if (!deleted) {
        logWarning(s"Failed to remove ${file.getPath} / skip removing.")
      }
    }
  }

  private def findFilesToCompact(
      eventLogFiles: Seq[FileStatus]): (Seq[FileStatus], Seq[FileStatus]) = {
    val files = RollingEventLogFilesFileReader.dropBeforeLastCompactFile(eventLogFiles)
    if (files.length > maxFilesToRetain) {
      (files.dropRight(maxFilesToRetain), files.takeRight(maxFilesToRetain))
    } else {
      (Seq.empty, files)
    }
  }
}

/**
 * This class rewrites the event log files into one compact file: the compact file will only
 * contain the events which pass the filters. Events will be filtered out only when all filters
 * decide to filter out the event or don't mind about the event. Otherwise, the original line for
 * the event is written to the compact file as it is.
 */
class FilteredEventLogFileRewriter(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    override val fs: FileSystem,
    override val filters: Seq[EventFilter]) extends EventFilterApplier {

  private var logWriter: Option[CompactedEventLogFileWriter] = None

  def rewrite(eventLogFiles: Seq[FileStatus]): String = {
    require(eventLogFiles.nonEmpty)

    val targetEventLogFilePath = eventLogFiles.last.getPath
    logWriter = Some(new CompactedEventLogFileWriter(targetEventLogFilePath, "dummy", None,
      targetEventLogFilePath.getParent.toUri, sparkConf, hadoopConf))

    val writer = logWriter.get
    writer.start()
    eventLogFiles.foreach { file => applyFilter(file.getPath) }
    writer.stop()

    writer.logPath
  }

  override protected def handleFilteredInEvent(line: String, event: SparkListenerEvent): Unit = {
    logWriter.foreach { writer => writer.writeEvent(line, flushLogger = true) }
  }

  override protected def handleFilteredOutEvent(line: String, event: SparkListenerEvent): Unit = {}

  override protected def handleUnidentifiedLine(line: String): Unit = {
    logWriter.foreach { writer => writer.writeEvent(line, flushLogger = true) }
  }
}

/**
 * This class helps to write compact file; to avoid reimplement everything, it extends
 * [[SingleEventLogFileWriter]], but only `originalFilePath` is used to determine the
 * path of compact file.
 */
class CompactedEventLogFileWriter(
    originalFilePath: Path,
    appId: String,
    appAttemptId: Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  override val logPath: String = originalFilePath.toUri.toString + EventLogFileWriter.COMPACTED
}
