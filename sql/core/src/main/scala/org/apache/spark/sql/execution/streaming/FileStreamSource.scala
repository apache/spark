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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.{DataSource, ListingFileCatalog, LogicalRelation}
import org.apache.spark.sql.types.StructType

/**
 * A very simple source that reads files from the given directory as they appear.
 *
 * TODO: Clean up the metadata log files periodically.
 */
class FileStreamSource(
    sparkSession: SparkSession,
    path: String,
    fileFormatClassName: String,
    override val schema: StructType,
    metadataPath: String,
    options: Map[String, String]) extends Source with Logging {

  import FileStreamSource._

  private val sourceOptions = new FileStreamOptions(options)

  private val qualifiedBasePath: Path = {
    val fs = new Path(path).getFileSystem(sparkSession.sessionState.newHadoopConf())
    fs.makeQualified(new Path(path))  // can contains glob patterns
  }

  private val metadataLog = new HDFSMetadataLog[Seq[FileEntry]](sparkSession, metadataPath)

  private var maxBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  /** Maximum number of new files to be considered in each batch */
  private val maxFilesPerBatch = sourceOptions.maxFilesPerTrigger

  /** A mapping from a file that we have processed to some timestamp it was last modified. */
  // Visible for testing and debugging in production.
  val seenFiles = new SeenFilesMap(sourceOptions.maxFileAgeMs)

  metadataLog.get(None, Some(maxBatchId)).foreach { case (batchId, entry) =>
    entry.foreach(seenFiles.add)
    // TODO: move purge call out of the loop once we truncate logs.
    seenFiles.purge()
  }

  logInfo(s"maxFilesPerBatch = $maxFilesPerBatch, maxFileAge = ${sourceOptions.maxFileAgeMs}")

  /**
   * Returns the maximum offset that can be retrieved from the source.
   *
   * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
   * there is no race here, so the cost of `synchronized` should be rare.
   */
  private def fetchMaxOffset(): LongOffset = synchronized {
    // All the new files found - ignore aged files and files that we have seen.
    val newFiles = fetchAllFiles().filter(seenFiles.isNewFile)

    // Obey user's setting to limit the number of files in this batch trigger.
    val batchFiles =
      if (maxFilesPerBatch.nonEmpty) newFiles.take(maxFilesPerBatch.get) else newFiles

    batchFiles.foreach { file =>
      seenFiles.add(file)
      logDebug(s"New file: $file")
    }
    val numPurged = seenFiles.purge()

    logTrace(
      s"""
         |Number of new files = ${newFiles.size}
         |Number of files selected for batch = ${batchFiles.size}
         |Number of seen files = ${seenFiles.size}
         |Number of files purged from tracking map = $numPurged
       """.stripMargin)

    if (batchFiles.nonEmpty) {
      maxBatchId += 1
      metadataLog.add(maxBatchId, batchFiles)
      logInfo(s"Max batch id increased to $maxBatchId with ${batchFiles.size} new files")
    }

    new LongOffset(maxBatchId)
  }

  /**
   * For test only. Run `func` with the internal lock to make sure when `func` is running,
   * the current offset won't be changed and no new batch will be emitted.
   */
  def withBatchingLocked[T](func: => T): T = synchronized {
    func
  }

  /** Return the latest offset in the source */
  def currentOffset: LongOffset = synchronized {
    new LongOffset(maxBatchId)
  }

  /**
   * Returns the data that is between the offsets (`start`, `end`].
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startId = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(-1L)
    val endId = end.asInstanceOf[LongOffset].offset

    assert(startId <= endId)
    val files = metadataLog.get(Some(startId + 1), Some(endId)).flatMap(_._2)
    logInfo(s"Processing ${files.length} files from ${startId + 1}:$endId")
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    val newDataSource =
      DataSource(
        sparkSession,
        paths = files.map(_.path),
        userSpecifiedSchema = Some(schema),
        className = fileFormatClassName,
        options = sourceOptions.optionMapWithoutPath)
    Dataset.ofRows(sparkSession, LogicalRelation(newDataSource.resolveRelation()))
  }

  /**
   * Returns a list of files found, sorted by their timestamp.
   */
  private def fetchAllFiles(): Seq[FileEntry] = {
    val startTime = System.nanoTime
    val globbedPaths = SparkHadoopUtil.get.globPathIfNecessary(qualifiedBasePath)
    val catalog = new ListingFileCatalog(sparkSession, globbedPaths, options, Some(new StructType))
    val files = catalog.allFiles().sortBy(_.getModificationTime).map { status =>
      FileEntry(status.getPath.toUri.toString, status.getModificationTime)
    }
    val endTime = System.nanoTime
    val listingTimeMs = (endTime.toDouble - startTime) / 1000000
    if (listingTimeMs > 2000) {
      // Output a warning when listing files uses more than 2 seconds.
      logWarning(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    } else {
      logTrace(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    }
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    files
  }

  override def getOffset: Option[Offset] = Some(fetchMaxOffset()).filterNot(_.offset == -1)

  override def toString: String = s"FileStreamSource[$qualifiedBasePath]"

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  override def commit(end: Offset): Unit = {
    // No-op for now; FileStreamSource currently garbage-collects files based on timestamp
    // and the value of the maxFileAge parameter.
  }

  override def stop() {}
}


object FileStreamSource {

  /** Timestamp for file modification time, in ms since January 1, 1970 UTC. */
  type Timestamp = Long

  case class FileEntry(path: String, timestamp: Timestamp) extends Serializable

  /**
   * A custom hash map used to track the list of files seen. This map is not thread-safe.
   *
   * To prevent the hash map from growing indefinitely, a purge function is available to
   * remove files "maxAgeMs" older than the latest file.
   */
  class SeenFilesMap(maxAgeMs: Long) {
    require(maxAgeMs >= 0)

    /** Mapping from file to its timestamp. */
    private val map = new java.util.HashMap[String, Timestamp]

    /** Timestamp of the latest file. */
    private var latestTimestamp: Timestamp = 0L

    /** Timestamp for the last purge operation. */
    private var lastPurgeTimestamp: Timestamp = 0L

    /** Add a new file to the map. */
    def add(file: FileEntry): Unit = {
      map.put(file.path, file.timestamp)
      if (file.timestamp > latestTimestamp) {
        latestTimestamp = file.timestamp
      }
    }

    /**
     * Returns true if we should consider this file a new file. The file is only considered "new"
     * if it is new enough that we are still tracking, and we have not seen it before.
     */
    def isNewFile(file: FileEntry): Boolean = {
      // Note that we are testing against lastPurgeTimestamp here so we'd never miss a file that
      // is older than (latestTimestamp - maxAgeMs) but has not been purged yet.
      file.timestamp >= lastPurgeTimestamp && !map.containsKey(file.path)
    }

    /** Removes aged entries and returns the number of files removed. */
    def purge(): Int = {
      lastPurgeTimestamp = latestTimestamp - maxAgeMs
      val iter = map.entrySet().iterator()
      var count = 0
      while (iter.hasNext) {
        val entry = iter.next()
        if (entry.getValue < lastPurgeTimestamp) {
          count += 1
          iter.remove()
        }
      }
      count
    }

    def size: Int = map.size()

    def allEntries: Seq[FileEntry] = {
      map.entrySet().asScala.map(entry => FileEntry(entry.getKey, entry.getValue)).toSeq
    }
  }
}
