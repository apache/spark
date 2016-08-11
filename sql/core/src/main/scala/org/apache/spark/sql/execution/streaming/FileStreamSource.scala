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

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.{CaseInsensitiveMap, DataSource, ListingFileCatalog, LogicalRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.OpenHashSet

/**
 * A very simple source that reads text files from the given directory as they appear.
 *
 * Special options that this source can take (via `options`):
 * <ul>
 *   <li><b>maxFilesPerTrigger</b>: The maximum number of files to include in a
 *        microbatch. If more than this number of files appear at once, an arbitrary
 *        subset of the files of this size will be used for the next batch.
 *        Default: No limit.
 *   <li><b>deleteCommittedFiles</b>: If true, the source will delete old files and
 *        clean up associated internal metadata when Spark has completed processing
 *        the data in those files.
 *        Default: False.
 * </ul>
 */
class FileStreamSource(
    sparkSession: SparkSession,
    path: String,
    fileFormatClassName: String,
    override val schema: StructType,
    metadataPath: String,
    options: Map[String, String]) extends Source with Logging {

  private val fs = new Path(path).getFileSystem(sparkSession.sessionState.newHadoopConf())
  private val qualifiedBasePath = fs.makeQualified(new Path(path)) // can contains glob patterns
  private val metadataLog = new HDFSMetadataLog[Seq[String]](sparkSession, metadataPath)

  /**
   * ID of the last batch committed and cleaned up, or -1 if no files have been removed.
   */
  private var minBatchId = -1L

  /**
   * ID of the most recent batch that has been entered into the metadata log, or -1 if
   * no files have been processed at all.
   */
  private var maxBatchId = -1L

  /** Maximum number of new files to be considered in each batch */
  private val maxFilesPerBatch = getMaxFilesPerBatch()

  /**
   * Should files whose data has been committed be removed from the directory, along with
   * their metadata entries?
   */
  private val deleteCommittedFiles = getDeleteCommittedFiles()

  private val seenFiles = new OpenHashSet[String]

  /**
   * Initialize the state of this source from the on-disk checkpoint, if present.
   */
  private def initialize() = {
    maxBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)

    metadataLog.get(None, Some(maxBatchId)).foreach {
      case (batchId, files) =>
        files.foreach(seenFiles.add)
        minBatchId = math.max(minBatchId, batchId)
    }
  }

  initialize()

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

  override def toString: String = s"FileStreamSource[$qualifiedBasePath]"

  //////////////////////////////////
  // BEGIN methods from Source trait

  /**
   * Returns the highest offset that this source has <b>removed</b> from its internal buffer
   * in response to a call to `commit`.
   * Returns `None` if this source has not removed any data.
   */
  override def getMinOffset: Option[Offset] = {
    if (-1L == minBatchId) {
      None
    } else {
      Some(new LongOffset(minBatchId))
    }
  }

  override def getMaxOffset: Option[Offset] = Some(fetchMaxOffset()).filterNot(_.offset == -1)

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startId = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(-1L)
    val endId = end.asInstanceOf[LongOffset].offset

    assert(startId <= endId)
    val files = metadataLog.get(Some(startId + 1), Some(endId)).flatMap(_._2)
    logInfo(s"Processing ${files.length} files from ${startId + 1}:$endId")
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    val newOptions = new CaseInsensitiveMap(options).filterKeys(_ != "path")
    val newDataSource =
      DataSource(
        sparkSession,
        paths = files,
        userSpecifiedSchema = Some(schema),
        className = fileFormatClassName,
        options = newOptions)
    Dataset.ofRows(sparkSession, LogicalRelation(newDataSource.resolveRelation()))
  }

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  override def commit(end: Offset): Unit = {
    if (end.isInstanceOf[LongOffset]) {
      val lastCommittedBatch = end.asInstanceOf[LongOffset].offset

      // Build up a list of batches, then process them one by one
      val firstCommittedBatch = math.max(minBatchId, 0)

      if (deleteCommittedFiles) {
        var batchId = 0L;
        for (batchId <- firstCommittedBatch to lastCommittedBatch) {
          val files = metadataLog.get(batchId)
          if (files.isDefined) {

            // Files may actually be directories, so use the recursive version
            // of the delete method.
            // TODO: This delete() should be wrapped in more error handling.
            // Examples of problems to catch: Spark does not have permission to delete
            // the file; or the filesystem metadata is corrupt.
            files.get.foreach(f => fs.delete(new Path(f), true))

            // TODO: Add a "remove" method to HDFSMetadataLog, then add code here to
            // remove the metadata for each completed batch. It's important that we
            // remove the metadata only after the files are deleted; otherwise we
            // may not be able to tell what files to delete after a crash.
          }

        }
      }

      minBatchId = lastCommittedBatch

      val offsetDiff = (newOffset.offset - lastCommittedOffset.offset).toInt

      if (offsetDiff < 0) {
        sys.error(s"Offsets committed out of order: $lastCommittedOffset followed by $end")
      }

      batches.trimStart(offsetDiff)
      lastCommittedOffset = newOffset
    } else {
      sys.error(s"FileStreamSource.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    }


  }

  override def stop() {}

  // END methods from Source trait
  ////////////////////////////////

  // All methods that follow are internal to this class.

  /**
   * Scans the directory and creates batches from any new files found.
   *
   * Returns the maximum offset that can be retrieved from the source.
   *
   * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
   * there is no race here, so the cost of `synchronized` should be rare.
   */
  private def fetchMaxOffset(): LongOffset = synchronized {
    val newFiles = fetchAllFiles().filter(!seenFiles.contains(_))
    logTrace(s"Number of new files = ${newFiles.size})")

    // To make the source's behavior less nondeterministic, we process files in
    // alphabetical order and ensure that every new file goes into a batch.
    val remainingFiles = mutable.Queue[String]()
    remainingFiles ++= newFiles.sorted

    val batches = ListBuffer[Seq[String]]()
    if (maxFilesPerBatch.nonEmpty) {
      while (remainingFiles.size > 0) {
        batches += remainingFiles.take(maxFilesPerBatch.get)
      }
    } else {
      batches += remainingFiles
    }

    newFiles.foreach { file =>
      seenFiles.add(file)
      logDebug(s"New file: $file")
    }

    batches.foreach {
      case batchFiles =>
        maxBatchId += 1
        metadataLog.add(maxBatchId, batchFiles)
        logInfo(s"Max batch id increased to $maxBatchId with ${batchFiles.size} new files")
    }

    val batchFiles =
      if (maxFilesPerBatch.nonEmpty) newFiles.take(maxFilesPerBatch.get) else newFiles
    batchFiles.foreach { file =>
      seenFiles.add(file)
      logDebug(s"New file: $file")
    }

    new LongOffset(maxBatchId)
  }


  private def fetchAllFiles(): Seq[String] = {
    val startTime = System.nanoTime
    val globbedPaths = SparkHadoopUtil.get.globPathIfNecessary(qualifiedBasePath)
    val catalog = new ListingFileCatalog(sparkSession, globbedPaths, options, Some(new StructType))
    val files = catalog.allFiles().sortBy(_.getModificationTime).map(_.getPath.toUri.toString)
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

  private def getDeleteCommittedFiles(): Boolean = {
    val str = options.getOrElse("deleteCommittedFiles", "false")
    try {
      str.toBoolean
    } catch {
      case _ => throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'deleteCommittedFiles', must be true or false")
    }
  }

  private def getMaxFilesPerBatch(): Option[Int] = {
    new CaseInsensitiveMap(options)
      .get("maxFilesPerTrigger")
      .map { str =>
        Try(str.toInt).toOption.filter(_ > 0).getOrElse {
          throw new IllegalArgumentException(
            s"Invalid value '$str' for option 'maxFilesPerTrigger', must be a positive integer")
        }
      }
  }



}
