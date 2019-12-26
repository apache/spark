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
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.EventFilter.FilterStatistic
import org.apache.spark.deploy.history.EventFilterBuildersLoader.LowerIndexLoadRequested
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
 * When compacting the files, the range of compaction for given file list is determined as:
 * (first ~ the file where there're `maxFilesToRetain` files on the right side)
 *
 * If there're not enough files on the range of compaction, compaction will be skipped.
 */
class EventLogFileCompactor(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    fs: FileSystem) extends Logging {
  import EventFilterBuildersLoader._

  private val maxFilesToRetain: Int = sparkConf.get(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN)
  private val compactionThresholdScore: Double = sparkConf.get(EVENT_LOG_COMPACTION_SCORE_THRESHOLD)

  private var filterBuildersLoader = new EventFilterBuildersLoader(fs)
  private var loadedLogPath: Path = _

  def compact(reader: EventLogFileReader): (CompactionResult.Value, Option[Long]) = {
    if (loadedLogPath == null) {
      loadedLogPath = reader.rootPath
    } else {
      require(loadedLogPath == null || reader.rootPath == loadedLogPath,
        "An instance of compactor should deal with same path of event log.")
    }

    if (reader.lastIndex.isEmpty) {
      return (CompactionResult.NOT_ENOUGH_FILES, None)
    }

    val eventLogFiles = reader.listEventLogFiles
    if (eventLogFiles.length < maxFilesToRetain) {
      return (CompactionResult.NOT_ENOUGH_FILES, None)
    }

    val filesToCompact = findFilesToCompact(eventLogFiles)
    if (filesToCompact.isEmpty) {
      return (CompactionResult.NOT_ENOUGH_FILES, None)
    }

    val builders = loadFilesToFilterBuilder(filesToCompact)
    val filters = builders.map(_.createFilter())
    val minScore = filters.flatMap(_.statistic()).map(calculateScore).min

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

  private def loadFilesToFilterBuilder(files: Seq[FileStatus]): Seq[EventFilterBuilder] = {
    try {
      filterBuildersLoader.loadNewFiles(files)
    } catch {
      case _: LowerIndexLoadRequested =>
        // reset loader and load again
        filterBuildersLoader = new EventFilterBuildersLoader(fs)
        loadFilesToFilterBuilder(files)

      case NonFatal(e) =>
        // reset loader before throwing exception, as filter builders aren't properly loaded
        filterBuildersLoader = new EventFilterBuildersLoader(fs)
        throw e
    }
  }

  private def calculateScore(stats: FilterStatistic): Double = {
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

  private def findFilesToCompact(
      eventLogFiles: Seq[FileStatus]): Seq[FileStatus] = {
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

class EventFilterBuildersLoader(fs: FileSystem) {
  // the implementation of this bus is expected to be stateless
  private val bus = new ReplayListenerBus()

  /** Loads all available EventFilterBuilders in classloader via ServiceLoader */
  private val filterBuilders: Seq[EventFilterBuilder] = ServiceLoader.load(
    classOf[EventFilterBuilder], Utils.getContextOrSparkClassLoader).asScala.toSeq

  filterBuilders.foreach(bus.addListener)

  private var latestIndexLoaded: Long = -1L

  /** only exposed for testing; simple metric to help testing */
  private[history] var numFilesToLoad: Long = 0L

  /**
   * Initializes EventFilterBuilders via replaying events in given files. Loading files are done
   * incrementally, via dropping indices which are already loaded and replaying remaining files.
   * For example, If the last index of requested files is same as the last index being loaded,
   * this will not replay any files.
   *
   * If the last index of requested files is smaller than the last index being loaded, it will
   * throw [[LowerIndexLoadRequested]], which caller can decide whether ignoring it or
   * invalidating loader and retrying.
   */
  def loadNewFiles(eventLogFiles: Seq[FileStatus]): Seq[EventFilterBuilder] = {
    require(eventLogFiles.nonEmpty)

    val idxToStatuses = eventLogFiles.map { status =>
      val idx = RollingEventLogFilesWriter.getEventLogFileIndex(status.getPath.getName)
      idx -> status
    }

    val newLatestIdx = idxToStatuses.last._1
    if (newLatestIdx < latestIndexLoaded) {
      throw new LowerIndexLoadRequested("Loader already loads higher index of event log than" +
        " requested.")
    }

    val filesToLoad = idxToStatuses
      .filter { case (idx, _) => idx > latestIndexLoaded }
      .map { case (_, status) => status.getPath }

    if (filesToLoad.nonEmpty) {
      filesToLoad.foreach { log =>
        Utils.tryWithResource(EventLogFileReader.openEventLog(log, fs)) { in =>
          bus.replay(in, log.getName)
        }
        numFilesToLoad += 1
      }

      latestIndexLoaded = newLatestIdx
    }

    filterBuilders
  }
}

object EventFilterBuildersLoader {
  class LowerIndexLoadRequested(_msg: String) extends Exception(_msg)
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
