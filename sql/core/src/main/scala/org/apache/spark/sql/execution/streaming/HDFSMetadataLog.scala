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
import java.util.{Collections, LinkedHashMap => JLinkedHashMap}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs._
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf


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

  protected val metadataCacheEnabled: Boolean
    = sparkSession.sessionState.conf.getConf(SQLConf.STREAMING_METADATA_CACHE_ENABLED)

  /**
   * Cache the latest two batches. [[StreamExecution]] usually just accesses the latest two batches
   * when committing offsets, this cache will save some file system operations.
   */
  protected[sql] val batchCache = Collections.synchronizedMap(new JLinkedHashMap[Long, T](2) {
    override def removeEldestEntry(e: java.util.Map.Entry[Long, T]): Boolean = size > 2
  })

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

  /**
   * Serialize the metadata and write to the output stream. If this method is overridden in a
   * subclass, the overriding method should not close the given output stream, as it will be closed
   * in the caller.
   */
  protected def serialize(metadata: T, out: OutputStream): Unit = {
    Serialization.write(metadata, out)
  }

  /**
   * Read and deserialize the metadata from input stream. If this method is overridden in a
   * subclass, the overriding method should not close the given input stream, as it will be closed
   * in the caller.
   */
  protected def deserialize(in: InputStream): T = {
    val reader = new InputStreamReader(in, StandardCharsets.UTF_8)
    Serialization.read[T](reader)
  }

  /**
   * Store the metadata for the specified batchId and return `true` if successful. If the batchId's
   * metadata has already been stored, this method will return `false`.
   */
  override def add(batchId: Long, metadata: T): Boolean = {
    require(metadata != null, "'null' metadata cannot written to a metadata log")
    val res = addNewBatchByStream(batchId) { output => serialize(metadata, output) }
    if (metadataCacheEnabled && res) batchCache.put(batchId, metadata)
    res
  }

  override def get(batchId: Long): Option[T] = {
    if (metadataCacheEnabled && batchCache.containsKey(batchId)) {
      val metadata = batchCache.get(batchId)
      assert(metadata != null)
      return Some(metadata)
    }

    try {
      applyFnToBatchByStream(batchId) { input => Some(deserialize(input)) }
    } catch {
      case fne: FileNotFoundException =>
        logDebug(fne.getMessage)
        None
    }
  }

  /**
   * Get the id of the previous batch from storage
   * @param batchId get the previous batch id of this batch with batchId
   * @return
   */
  def getPrevBatchFromStorage(batchId: Long): Option[Long] = {
    val batchFiles = listBatchesOnDisk

    var prev: Option[Long] = None
    for (file <- batchFiles.sorted) {
      if (file >= batchId) {
        return prev
      }
      prev = Some(file)
    }
    None
  }

  /**
   * Apply provided function to each entry in the specific batch metadata log.
   *
   * Unlike get which will materialize all entries into memory, this method streamlines the process
   * via READ-AND-PROCESS. This helps to avoid the memory issue on huge metadata log file.
   *
   * NOTE: This no longer fails early on corruption. The caller should handle the exception
   * properly and make sure the logic is not affected by failing in the middle.
   */
  def applyFnToBatchByStream[RET](
      batchId: Long, skipExistingCheck: Boolean = false)(fn: InputStream => RET): RET = {
    val batchMetadataFile = batchIdToPath(batchId)
    if (skipExistingCheck || fileManager.exists(batchMetadataFile)) {
      val input = fileManager.open(batchMetadataFile)
      try {
        fn(input)
      } catch {
        case ise: IllegalStateException =>
          // re-throw the exception with the log file path added
          throw new IllegalStateException(
            s"Failed to read log file $batchMetadataFile. ${ise.getMessage}", ise)
      } finally {
        IOUtils.closeQuietly(input)
      }
    } else {
      throw QueryExecutionErrors.batchMetadataFileNotFoundError(batchMetadataFile)
    }
  }

  protected def write(batchMetadataFile: Path,
                      fn: OutputStream => Unit): Unit = {
    // Only write metadata when the batch has not yet been written
    val output = fileManager.createAtomic(batchMetadataFile, overwriteIfPossible = false)
    try {
      fn(output)
      output.close()
    } catch {
      case e: FileAlreadyExistsException =>
        output.cancel()
        // If next batch file already exists, then another concurrently running query has
        // written it.
        throw QueryExecutionErrors.multiStreamingQueriesUsingPathConcurrentlyError(path, e)
      case e: Throwable =>
        output.cancel()
        throw e
    }
  }

  /**
   * Store the metadata for the specified batchId and return `true` if successful. This method
   * fills the content of metadata via executing function. If the function throws an exception,
   * writing will be automatically cancelled and this method will propagate the exception.
   *
   * If the batchId's metadata has already been stored, this method will return `false`.
   *
   * Writing the metadata is done by writing a batch to a temp file then rename it to the batch
   * file.
   *
   * There may be multiple [[HDFSMetadataLog]] using the same metadata path. Although it is not a
   * valid behavior, we still need to prevent it from destroying the files.
   */
  def addNewBatchByStream(batchId: Long)(fn: OutputStream => Unit): Boolean = {
    val batchMetadataFile = batchIdToPath(batchId)

    if ((metadataCacheEnabled && batchCache.containsKey(batchId))
      || fileManager.exists(batchMetadataFile)) {
      false
    } else {
      write(batchMetadataFile, fn)
      true
    }
  }

  private def getExistingBatch(batchId: Long): T = {
    val metadata = batchCache.get(batchId)
    if (metadata == null) {
      applyFnToBatchByStream(batchId, skipExistingCheck = true) { input => deserialize(input) }
    } else {
      metadata
    }
  }

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, T)] = {
    assert(startId.isEmpty || endId.isEmpty || startId.get <= endId.get)
    val batchIds = listBatches.filter { batchId =>
      (endId.isEmpty || batchId <= endId.get) && (startId.isEmpty || batchId >= startId.get)
    }.sorted

    HDFSMetadataLog.verifyBatchIds(batchIds, startId, endId)
    batchIds.map(batchId => (batchId, getExistingBatch(batchId)))
  }

  /** Return the latest batch id without reading the file. */
  def getLatestBatchId(): Option[Long] = listBatches.sorted.lastOption

  override def getLatest(): Option[(Long, T)] = {
    listBatches.sorted.lastOption.map { batchId =>
      logInfo(s"Getting latest batch $batchId")
      (batchId, getExistingBatch(batchId))
    }
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

  private var lastPurgedBatchId: Long = -1L

  /**
   * Removes all the log entry earlier than thresholdBatchId (exclusive).
   */
  override def purge(thresholdBatchId: Long): Unit = {
    val possibleTargetBatchIds = (lastPurgedBatchId + 1 until thresholdBatchId)
    if (possibleTargetBatchIds.length <= 3) {
      // avoid using list if we only need to purge at most 3 elements
      possibleTargetBatchIds.foreach { batchId =>
        val path = batchIdToPath(batchId)
        fileManager.delete(path)
        if (metadataCacheEnabled) batchCache.remove(batchId)
        logTrace(s"Removed metadata log file: $path")
      }
    } else {
      // using list to retrieve all elements
      for (batchId <- listBatches if batchId < thresholdBatchId) {
        val path = batchIdToPath(batchId)
        fileManager.delete(path)
        if (metadataCacheEnabled) batchCache.remove(batchId)
        logTrace(s"Removed metadata log file: $path")
      }
    }

    lastPurgedBatchId = thresholdBatchId - 1
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
      if (metadataCacheEnabled) batchCache.remove(batchId)
      logTrace(s"Removed metadata log file: $path")
    }
  }

  /** List the available batches on file system. */
  protected def listBatches: Array[Long] = {
    val batchIds = fileManager.list(metadataPath, batchFilesFilter)
      .map(f => pathToBatchId(f.getPath)) ++
      // Iterate over keySet is not thread safe. We call `toArray` to make a copy in the lock to
      // elimiate the race condition.
      batchCache.synchronized {
        batchCache.keySet.asScala.toArray
      }
    logInfo("BatchIds found from listing: " + batchIds.sorted.mkString(", "))

    if (batchIds.isEmpty) {
      Array.empty
    } else {
      // Assume batch ids are continuous
      (batchIds.min to batchIds.max).toArray
    }
  }

  /**
   * List the batches persisted to storage
   * @return array of batches ids
   */
  def listBatchesOnDisk: Array[Long] = {
    fileManager.list(metadataPath, batchFilesFilter)
      .map(f => pathToBatchId(f.getPath)).sorted
  }

  private[sql] def validateVersion(text: String, maxSupportedVersion: Int): Int =
    MetadataVersionUtil.validateVersion(text, maxSupportedVersion)
}

object HDFSMetadataLog {

  /**
   * Verify if batchIds are continuous and between `startId` and `endId` (both inclusive and
   * startId assumed to be <= endId).
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
