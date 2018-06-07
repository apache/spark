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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.{ConcurrentModificationException, EnumSet, UUID}

import scala.reflect.ClassTag

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


/**
 * A [[MetadataLog]] implementation based on HDFS. [[HDFSMetadataLog]] uses the specified `path`
 * as the metadata storage.
 *
 * When writing a new batch, [[HDFSMetadataLog]] will firstly write to a temp file and then rename
 * it to the final batch file. If the rename step fails, there must be multiple writers and only
 * one of them will succeed and the others will fail.
 *
 * Note: [[HDFSMetadataLog]] doesn't support S3-like file systems as they don't guarantee listing
 * files in a directory always shows the latest files.
 */
class HDFSMetadataLog[T <: AnyRef : ClassTag](sparkSession: SparkSession, path: String)
  extends MetadataLog[T] with Logging {

  private implicit val formats = Serialization.formats(NoTypeHints)

  /** Needed to serialize type T into JSON when using Jackson */
  private implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

  // Avoid serializing generic sequences, see SPARK-17372
  require(implicitly[ClassTag[T]].runtimeClass != classOf[Seq[_]],
    "Should not create a log with type Seq, use Arrays instead - see SPARK-17372")

  val metadataPath = new Path(path)

  protected val fileManager =
    CheckpointFileManager.create(metadataPath, sparkSession.sessionState.newHadoopConf)

  if (!fileManager.exists(metadataPath)) {
    fileManager.mkdirs(metadataPath)
  }

  /**
   * A `PathFilter` to filter only batch files
   */
  protected val batchFilesFilter = new PathFilter {
    override def accept(path: Path): Boolean = isBatchFile(path)
  }

  protected def batchIdToPath(batchId: Long): Path = {
    new Path(metadataPath, batchId.toString)
  }

  protected def pathToBatchId(path: Path) = {
    path.getName.toLong
  }

  protected def isBatchFile(path: Path) = {
    try {
      path.getName.toLong
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  protected def serialize(metadata: T, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    Serialization.write(metadata, out)
  }

  protected def deserialize(in: InputStream): T = {
    // called inside a try-finally where the underlying stream is closed in the caller
    val reader = new InputStreamReader(in, StandardCharsets.UTF_8)
    Serialization.read[T](reader)
  }

  /**
   * Store the metadata for the specified batchId and return `true` if successful. If the batchId's
   * metadata has already been stored, this method will return `false`.
   */
  override def add(batchId: Long, metadata: T): Boolean = {
    require(metadata != null, "'null' metadata cannot written to a metadata log")
    get(batchId).map(_ => false).getOrElse {
      // Only write metadata when the batch has not yet been written
      writeBatchToFile(metadata, batchIdToPath(batchId))
      true
    }
  }

  /** Write a batch to a temp file then rename it to the batch file.
   *
   * There may be multiple [[HDFSMetadataLog]] using the same metadata path. Although it is not a
   * valid behavior, we still need to prevent it from destroying the files.
   */
  private def writeBatchToFile(metadata: T, path: Path): Unit = {
    val output = fileManager.createAtomic(path, overwriteIfPossible = false)
    try {
      serialize(metadata, output)
      output.close()
    } catch {
      case e: FileAlreadyExistsException =>
        output.cancel()
        // If next batch file already exists, then another concurrently running query has
        // written it.
        throw new ConcurrentModificationException(
          s"Multiple streaming queries are concurrently using $path", e)
      case e: Throwable =>
        output.cancel()
        throw e
    }
  }

  override def get(batchId: Long): Option[T] = {
    val batchMetadataFile = batchIdToPath(batchId)
    if (fileManager.exists(batchMetadataFile)) {
      val input = fileManager.open(batchMetadataFile)
      try {
        Some(deserialize(input))
      } catch {
        case ise: IllegalStateException =>
          // re-throw the exception with the log file path added
          throw new IllegalStateException(
            s"Failed to read log file $batchMetadataFile. ${ise.getMessage}", ise)
      } finally {
        IOUtils.closeQuietly(input)
      }
    } else {
      logDebug(s"Unable to find batch $batchMetadataFile")
      None
    }
  }

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, T)] = {
    assert(startId.isEmpty || endId.isEmpty || startId.get <= endId.get)
    val files = fileManager.list(metadataPath, batchFilesFilter)
    val batchIds = files
      .map(f => pathToBatchId(f.getPath))
      .filter { batchId =>
        (endId.isEmpty || batchId <= endId.get) && (startId.isEmpty || batchId >= startId.get)
    }.sorted

    HDFSMetadataLog.verifyBatchIds(batchIds, startId, endId)

    batchIds.map(batchId => (batchId, get(batchId))).filter(_._2.isDefined).map {
      case (batchId, metadataOption) =>
        (batchId, metadataOption.get)
    }
  }

  override def getLatest(): Option[(Long, T)] = {
    val batchIds = fileManager.list(metadataPath, batchFilesFilter)
      .map(f => pathToBatchId(f.getPath))
      .sorted
      .reverse
    for (batchId <- batchIds) {
      val batch = get(batchId)
      if (batch.isDefined) {
        return Some((batchId, batch.get))
      }
    }
    None
  }

  /**
   * Get an array of [FileStatus] referencing batch files.
   * The array is sorted by most recent batch file first to
   * oldest batch file.
   */
  def getOrderedBatchFiles(): Array[FileStatus] = {
    fileManager.list(metadataPath, batchFilesFilter)
      .sortBy(f => pathToBatchId(f.getPath))
      .reverse
  }

  /**
   * Removes all the log entry earlier than thresholdBatchId (exclusive).
   */
  override def purge(thresholdBatchId: Long): Unit = {
    val batchIds = fileManager.list(metadataPath, batchFilesFilter)
      .map(f => pathToBatchId(f.getPath))

    for (batchId <- batchIds if batchId < thresholdBatchId) {
      val path = batchIdToPath(batchId)
      fileManager.delete(path)
      logTrace(s"Removed metadata log file: $path")
    }
  }

  /**
   * Removes all log entries later than thresholdBatchId (exclusive).
   */
  def purgeAfter(thresholdBatchId: Long): Unit = {
    val batchIds = fileManager.list(metadataPath, batchFilesFilter)
      .map(f => pathToBatchId(f.getPath))

    for (batchId <- batchIds if batchId > thresholdBatchId) {
      val path = batchIdToPath(batchId)
      fileManager.delete(path)
      logTrace(s"Removed metadata log file: $path")
    }
  }

  /**
   * Parse the log version from the given `text` -- will throw exception when the parsed version
   * exceeds `maxSupportedVersion`, or when `text` is malformed (such as "xyz", "v", "v-1",
   * "v123xyz" etc.)
   */
  private[sql] def parseVersion(text: String, maxSupportedVersion: Int): Int = {
    if (text.length > 0 && text(0) == 'v') {
      val version =
        try {
          text.substring(1, text.length).toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalStateException(s"Log file was malformed: failed to read correct log " +
              s"version from $text.")
        }
      if (version > 0) {
        if (version > maxSupportedVersion) {
          throw new IllegalStateException(s"UnsupportedLogVersion: maximum supported log version " +
            s"is v${maxSupportedVersion}, but encountered v$version. The log file was produced " +
            s"by a newer version of Spark and cannot be read by this version. Please upgrade.")
        } else {
          return version
        }
      }
    }

    // reaching here means we failed to read the correct log version
    throw new IllegalStateException(s"Log file was malformed: failed to read correct log " +
      s"version from $text.")
  }
}

object HDFSMetadataLog {

  /**
   * Verify if batchIds are continuous and between `startId` and `endId`.
   *
   * @param batchIds the sorted ids to verify.
   * @param startId the start id. If it's set, batchIds should start with this id.
   * @param endId the start id. If it's set, batchIds should end with this id.
   */
  def verifyBatchIds(batchIds: Seq[Long], startId: Option[Long], endId: Option[Long]): Unit = {
    // Verify that we can get all batches between `startId` and `endId`.
    if (startId.isDefined || endId.isDefined) {
      if (batchIds.isEmpty) {
        throw new IllegalStateException(s"batch ${startId.orElse(endId).get} doesn't exist")
      }
      if (startId.isDefined) {
        val minBatchId = batchIds.head
        assert(minBatchId >= startId.get)
        if (minBatchId != startId.get) {
          val missingBatchIds = startId.get to minBatchId
          throw new IllegalStateException(
            s"batches (${missingBatchIds.mkString(", ")}) don't exist " +
              s"(startId: $startId, endId: $endId)")
        }
      }

      if (endId.isDefined) {
        val maxBatchId = batchIds.last
        assert(maxBatchId <= endId.get)
        if (maxBatchId != endId.get) {
          val missingBatchIds = maxBatchId to endId.get
          throw new IllegalStateException(
            s"batches (${missingBatchIds.mkString(", ")}) don't  exist " +
              s"(startId: $startId, endId: $endId)")
        }
      }
    }

    if (batchIds.nonEmpty) {
      val minBatchId = batchIds.head
      val maxBatchId = batchIds.last
      val missingBatchIds = (minBatchId to maxBatchId).toSet -- batchIds
      if (missingBatchIds.nonEmpty) {
        throw new IllegalStateException(s"batches (${missingBatchIds.mkString(", ")}) " +
          s"don't exist (startId: $startId, endId: $endId)")
      }
    }
  }
}
