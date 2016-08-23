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
 * TODO Clean up the metadata files periodically
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
  private var maxBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  /** Maximum number of new files to be considered in each batch */
  private val maxFilesPerBatch = getMaxFilesPerBatch()

  private val seenFiles = new OpenHashSet[String]
  metadataLog.get(None, Some(maxBatchId)).foreach { case (batchId, files) =>
    files.foreach(seenFiles.add)
  }

  /**
   * Returns the maximum offset that can be retrieved from the source.
   *
   * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
   * there is no race here, so the cost of `synchronized` should be rare.
   */
  private def fetchMaxOffset(): LongOffset = synchronized {
    val newFiles = fetchAllFiles().filter(!seenFiles.contains(_))
    val batchFiles =
      if (maxFilesPerBatch.nonEmpty) newFiles.take(maxFilesPerBatch.get) else newFiles
    batchFiles.foreach { file =>
      seenFiles.add(file)
      logDebug(s"New file: $file")
    }
    logTrace(s"Number of new files = ${newFiles.size})")
    logTrace(s"Number of files selected for batch = ${batchFiles.size}")
    logTrace(s"Number of seen files = ${seenFiles.size}")
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
    val newOptions = options.filterKeys(_ != "path")
    val newDataSource =
      DataSource(
        sparkSession,
        paths = files,
        userSpecifiedSchema = Some(schema),
        className = fileFormatClassName,
        options = newOptions)
    Dataset.ofRows(sparkSession, LogicalRelation(newDataSource.resolveRelation()))
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

  private def getMaxFilesPerBatch(): Option[Int] = {
    options.get("maxFilesPerTrigger")
      .map { str =>
        Try(str.toInt).toOption.filter(_ > 0).getOrElse {
          throw new IllegalArgumentException(
            s"Invalid value '$str' for option 'maxFilesPerTrigger', must be a positive integer")
        }
      }
  }

  override def getOffset: Option[Offset] = Some(fetchMaxOffset()).filterNot(_.offset == -1)

  override def toString: String = s"FileStreamSource[$qualifiedBasePath]"

  override def stop() {}
}
