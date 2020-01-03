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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.EventFilter.FilterStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{EVENT_LOG_COMPACTION_SCORE_THRESHOLD, EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN}
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.util.Utils

/**
 * This class compacts the old event log files into one compact file, via two phases reading:
 *
 * 1) Initialize available [[EventFilterBuilder]] instances, and replay the old event log files with
 * builders, so that these builders can gather the information to create [[EventFilter]] instances.
 * 2) Initialize [[EventFilter]] instances from [[EventFilterBuilder]] instances, and replay the
 * old event log files with filters. Rewrite the events to the compact file which the filters decide
 * to accept.
 *
 * This class will calculate the score based on statistic from [[EventFilter]] instances, which
 * represents approximate rate of filtered-out events. Score is being calculated via applying
 * heuristic; task events tend to take most size in event log.
 *
 * This class assumes caller will provide the sorted list of files which are sorted by the index of
 * event log file, with "at most" one compact file placed first if it exists. Caller should keep in
 * mind that this class doesn't care about the semantic of ordering.
 *
 * When compacting the files, the range of compaction for given file list is determined as:
 * (first ~ the file where there're `maxFilesToRetain` files on the right side)
 *
 * If there're not enough files on the range of compaction, compaction will be skipped.
 */
class EventLogFileCompactor(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    fs: FileSystem) extends Logging {
  private val maxFilesToRetain: Int = sparkConf.get(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN)
  private val compactionThresholdScore: Double = sparkConf.get(EVENT_LOG_COMPACTION_SCORE_THRESHOLD)

  def compact(eventLogFiles: Seq[FileStatus]): (CompactionResult.Value, Option[Long]) = {
    assertPrecondition(eventLogFiles)

    if (eventLogFiles.length < maxFilesToRetain) {
      return (CompactionResult.NOT_ENOUGH_FILES, None)
    }

    val filesToCompact = findFilesToCompact(eventLogFiles)
    if (filesToCompact.isEmpty) {
      (CompactionResult.NOT_ENOUGH_FILES, None)
    } else {
      val builders = initializeBuilders(fs, filesToCompact.map(_.getPath))

      val filters = builders.map(_.createFilter())
      val minScore = filters.flatMap(_.statistics()).map(calculateScore).min

      if (minScore < compactionThresholdScore) {
        (CompactionResult.LOW_SCORE_FOR_COMPACTION, None)
      } else {
        val rewriter = new FilteredEventLogFileRewriter(sparkConf, hadoopConf, fs, filters)
        rewriter.rewrite(filesToCompact)
        cleanupCompactedFiles(filesToCompact)
        (CompactionResult.SUCCESS, Some(RollingEventLogFilesWriter.getEventLogFileIndex(
          filesToCompact.last.getPath.getName)))
      }
    }
  }

  private def assertPrecondition(eventLogFiles: Seq[FileStatus]): Unit = {
    val idxCompactedFiles = eventLogFiles.zipWithIndex.filter { case (file, _) =>
      EventLogFileWriter.isCompacted(file.getPath)
    }
    require(idxCompactedFiles.size < 2 && idxCompactedFiles.headOption.forall(_._2 == 0),
      "The number of compact files should be at most 1, and should be placed first if exists.")
  }

  /**
   * Loads all available EventFilterBuilders in classloader via ServiceLoader, and initializes
   * them via replaying events in given files.
   */
  private def initializeBuilders(fs: FileSystem, files: Seq[Path]): Seq[EventFilterBuilder] = {
    val bus = new ReplayListenerBus()

    val builders = ServiceLoader.load(classOf[EventFilterBuilder],
      Utils.getContextOrSparkClassLoader).asScala.toSeq
    builders.foreach(bus.addListener)

    files.foreach { log =>
      Utils.tryWithResource(EventLogFileReader.openEventLog(log, fs)) { in =>
        bus.replay(in, log.getName)
      }
    }

    builders
  }

  private def calculateScore(stats: FilterStatistics): Double = {
    // For now it's simply measuring how many task events will be filtered out (rejected)
    // but it can be sophisticated later once we get more heuristic information and found
    // the case where this simple calculation doesn't work.
    (stats.totalTasks - stats.liveTasks) * 1.0 / stats.totalTasks
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

  private def findFilesToCompact(eventLogFiles: Seq[FileStatus]): Seq[FileStatus] = {
    val numNormalEventLogFiles = {
      if (EventLogFileWriter.isCompacted(eventLogFiles.head.getPath)) {
        eventLogFiles.length - 1
      } else {
        eventLogFiles.length
      }
    }

    // This avoids compacting only compact file.
    if (numNormalEventLogFiles > maxFilesToRetain) {
      eventLogFiles.dropRight(maxFilesToRetain)
    } else {
      Seq.empty
    }
  }
}

object CompactionResult extends Enumeration {
  val SUCCESS, NOT_ENOUGH_FILES, LOW_SCORE_FOR_COMPACTION = Value
}

/**
 * This class rewrites the event log files into one compact file: the compact file will only
 * contain the events which pass the filters. Events will be dropped only when all filters
 * decide to reject the event or don't mind about the event. Otherwise, the original line for
 * the event is written to the compact file as it is.
 */
class FilteredEventLogFileRewriter(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    fs: FileSystem,
    filters: Seq[EventFilter]) {

  def rewrite(eventLogFiles: Seq[FileStatus]): String = {
    require(eventLogFiles.nonEmpty)

    val lastIndexEventLogPath = eventLogFiles.last.getPath
    val logWriter = new CompactedEventLogFileWriter(lastIndexEventLogPath, "dummy", None,
      lastIndexEventLogPath.getParent.toUri, sparkConf, hadoopConf)

    logWriter.start()
    eventLogFiles.foreach { file =>
      EventFilter.applyFilterToFile(fs, filters, file.getPath,
        onAccepted = (line, _) => logWriter.writeEvent(line, flushLogger = true),
        onRejected = (_, _) => {},
        onUnidentified = line => logWriter.writeEvent(line, flushLogger = true)
      )
    }
    logWriter.stop()

    logWriter.logPath
  }
}

/**
 * This class helps to write compact file; to avoid reimplementing everything, it extends
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
