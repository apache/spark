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
import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

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
      val bus = new ReplayListenerBus()

      val builders = ServiceLoader.load(classOf[EventFilterBuilder],
        Utils.getContextOrSparkClassLoader).asScala.toSeq
      builders.foreach(bus.addListener)

      filesToCompact.foreach { log =>
        Utils.tryWithResource(EventLogFileReader.openEventLog(log.getPath, fs)) { in =>
          bus.replay(in, log.getPath.getName)
        }
      }

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
    fs: FileSystem,
    filters: Seq[EventFilter]) extends Logging {

  def rewrite(eventLogFiles: Seq[FileStatus]): String = {
    require(eventLogFiles.nonEmpty)

    val targetEventLogFilePath = eventLogFiles.last.getPath
    val logWriter: CompactedEventLogFileWriter = new CompactedEventLogFileWriter(
      targetEventLogFilePath, "dummy", None, targetEventLogFilePath.getParent.toUri,
      sparkConf, hadoopConf)

    logWriter.start()
    eventLogFiles.foreach { file => rewriteFile(logWriter, file) }
    logWriter.stop()

    logWriter.logPath
  }

  private def rewriteFile(logWriter: CompactedEventLogFileWriter, fileStatus: FileStatus): Unit = {
    Utils.tryWithResource(EventLogFileReader.openEventLog(fileStatus.getPath, fs)) { in =>
      val lines = Source.fromInputStream(in)(Codec.UTF8).getLines()

      var currentLine: String = null
      var lineNumber: Int = 0

      try {
        val lineEntries = lines.zipWithIndex
        while (lineEntries.hasNext) {
          val entry = lineEntries.next()

          currentLine = entry._1
          lineNumber = entry._2 + 1

          val event = try {
            Some(JsonProtocol.sparkEventFromJson(parse(currentLine)))
          } catch {
            // ignore any exception occurred from unidentified json
            // just skip handling and write the line
            case NonFatal(_) =>
              logWriter.writeLine(currentLine)
              None
          }

          event.foreach { e =>
            if (checkFilters(e)) {
              logWriter.writeLine(currentLine)
            }
          }
        }
      } catch {
        case e: Exception =>
          logError(s"Exception parsing Spark event log: ${fileStatus.getPath.getName}", e)
          logError(s"Malformed line #$lineNumber: $currentLine\n")
          throw e
      }
    }
  }

  private def checkFilters(event: SparkListenerEvent): Boolean = {
    val results = filters.flatMap(filter => applyFilter(filter, event))
    results.isEmpty || results.forall(_ == true)
  }

  private def applyFilter(filter: EventFilter, event: SparkListenerEvent): Option[Boolean] = {
    // This pattern match should have same list of event types, but it would be safe even if
    // it's out of sync, once filter doesn't mark events to filter out for unknown event types.
    event match {
      case event: SparkListenerStageSubmitted => filter.filterStageSubmitted(event)
      case event: SparkListenerStageCompleted => filter.filterStageCompleted(event)
      case event: SparkListenerJobStart => filter.filterJobStart(event)
      case event: SparkListenerJobEnd => filter.filterJobEnd(event)
      case event: SparkListenerTaskStart => filter.filterTaskStart(event)
      case event: SparkListenerTaskGettingResult => filter.filterTaskGettingResult(event)
      case event: SparkListenerTaskEnd => filter.filterTaskEnd(event)
      case event: SparkListenerEnvironmentUpdate => filter.filterEnvironmentUpdate(event)
      case event: SparkListenerBlockManagerAdded => filter.filterBlockManagerAdded(event)
      case event: SparkListenerBlockManagerRemoved => filter.filterBlockManagerRemoved(event)
      case event: SparkListenerUnpersistRDD => filter.filterUnpersistRDD(event)
      case event: SparkListenerApplicationStart => filter.filterApplicationStart(event)
      case event: SparkListenerApplicationEnd => filter.filterApplicationEnd(event)
      case event: SparkListenerExecutorMetricsUpdate => filter.filterExecutorMetricsUpdate(event)
      case event: SparkListenerStageExecutorMetrics => filter.filterStageExecutorMetrics(event)
      case event: SparkListenerExecutorAdded => filter.filterExecutorAdded(event)
      case event: SparkListenerExecutorRemoved => filter.filterExecutorRemoved(event)
      case event: SparkListenerExecutorBlacklistedForStage =>
        filter.filterExecutorBlacklistedForStage(event)
      case event: SparkListenerNodeBlacklistedForStage =>
        filter.filterNodeBlacklistedForStage(event)
      case event: SparkListenerExecutorBlacklisted => filter.filterExecutorBlacklisted(event)
      case event: SparkListenerExecutorUnblacklisted => filter.filterExecutorUnblacklisted(event)
      case event: SparkListenerNodeBlacklisted => filter.filterNodeBlacklisted(event)
      case event: SparkListenerNodeUnblacklisted => filter.filterNodeUnblacklisted(event)
      case event: SparkListenerBlockUpdated => filter.filterBlockUpdated(event)
      case event: SparkListenerSpeculativeTaskSubmitted =>
        filter.filterSpeculativeTaskSubmitted(event)
      case _ => filter.filterOtherEvent(event)
    }
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

  // override to make writeLine method be 'public' only for this class
  override def writeLine(line: String, flushLogger: Boolean): Unit = {
    super.writeLine(line, flushLogger)
  }
}
