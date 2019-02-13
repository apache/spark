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
import java.util.concurrent.TimeUnit._

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.{DataSource, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.types.StructType

/**
 * A very simple source that reads files from the given directory as they appear.
 */
class FileStreamSource(
    sparkSession: SparkSession,
    path: String,
    fileFormatClassName: String,
    override val schema: StructType,
    partitionColumns: Seq[String],
    metadataPath: String,
    options: Map[String, String]) extends Source with Logging {

  import FileStreamSource._

  private val sourceOptions = new FileStreamOptions(options)

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  @transient private val fs = new Path(path).getFileSystem(hadoopConf)

  private val qualifiedBasePath: Path = {
    fs.makeQualified(new Path(path))  // can contain glob patterns
  }

  private val sourceCleaner = new FileStreamSourceCleaner(fs, qualifiedBasePath,
    sourceOptions.sourceArchiveDir)

  private val optionsWithPartitionBasePath = sourceOptions.optionMapWithoutPath ++ {
    if (!SparkHadoopUtil.get.isGlobPath(new Path(path)) && options.contains("path")) {
      Map("basePath" -> path)
    } else {
      Map()
    }}

  private val metadataLog =
    new FileStreamSourceLog(FileStreamSourceLog.VERSION, sparkSession, metadataPath)
  private var metadataLogCurrentOffset = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  /** Maximum number of new files to be considered in each batch */
  private val maxFilesPerBatch = sourceOptions.maxFilesPerTrigger

  private val fileSortOrder = if (sourceOptions.latestFirst) {
      logWarning(
        """'latestFirst' is true. New files will be processed first, which may affect the watermark
          |value. In addition, 'maxFileAge' will be ignored.""".stripMargin)
      implicitly[Ordering[Long]].reverse
    } else {
      implicitly[Ordering[Long]]
    }

  private val maxFileAgeMs: Long = if (sourceOptions.latestFirst && maxFilesPerBatch.isDefined) {
    Long.MaxValue
  } else {
    sourceOptions.maxFileAgeMs
  }

  private val fileNameOnly = sourceOptions.fileNameOnly
  if (fileNameOnly) {
    logWarning("'fileNameOnly' is enabled. Make sure your file names are unique (e.g. using " +
      "UUID), otherwise, files with the same name but under different paths will be considered " +
      "the same and causes data lost.")
  }

  /** A mapping from a file that we have processed to some timestamp it was last modified. */
  // Visible for testing and debugging in production.
  val seenFiles = new SeenFilesMap(maxFileAgeMs, fileNameOnly)

  metadataLog.allFiles().foreach { entry =>
    seenFiles.add(entry.path, entry.timestamp)
  }
  seenFiles.purge()

  logInfo(s"maxFilesPerBatch = $maxFilesPerBatch, maxFileAgeMs = $maxFileAgeMs")

  /**
   * Returns the maximum offset that can be retrieved from the source.
   *
   * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
   * there is no race here, so the cost of `synchronized` should be rare.
   */
  private def fetchMaxOffset(): FileStreamSourceOffset = synchronized {
    // All the new files found - ignore aged files and files that we have seen.
    val newFiles = fetchAllFiles().filter {
      case (path, timestamp) => seenFiles.isNewFile(path, timestamp)
    }

    // Obey user's setting to limit the number of files in this batch trigger.
    val batchFiles =
      if (maxFilesPerBatch.nonEmpty) newFiles.take(maxFilesPerBatch.get) else newFiles

    batchFiles.foreach { file =>
      seenFiles.add(file._1, file._2)
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
      metadataLogCurrentOffset += 1
      metadataLog.add(metadataLogCurrentOffset, batchFiles.map { case (p, timestamp) =>
        FileEntry(path = p, timestamp = timestamp, batchId = metadataLogCurrentOffset)
      }.toArray)
      logInfo(s"Log offset set to $metadataLogCurrentOffset with ${batchFiles.size} new files")
    }

    FileStreamSourceOffset(metadataLogCurrentOffset)
  }

  /**
   * For test only. Run `func` with the internal lock to make sure when `func` is running,
   * the current offset won't be changed and no new batch will be emitted.
   */
  def withBatchingLocked[T](func: => T): T = synchronized {
    func
  }

  /** Return the latest offset in the [[FileStreamSourceLog]] */
  def currentLogOffset: Long = synchronized { metadataLogCurrentOffset }

  /**
   * Returns the data that is between the offsets (`start`, `end`].
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset = start.map(FileStreamSourceOffset(_).logOffset).getOrElse(-1L)
    val endOffset = FileStreamSourceOffset(end).logOffset

    assert(startOffset <= endOffset)
    val files = metadataLog.get(Some(startOffset + 1), Some(endOffset)).flatMap(_._2)
    logInfo(s"Processing ${files.length} files from ${startOffset + 1}:$endOffset")
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    val newDataSource =
      DataSource(
        sparkSession,
        paths = files.map(f => new Path(new URI(f.path)).toString),
        userSpecifiedSchema = Some(schema),
        partitionColumns = partitionColumns,
        className = fileFormatClassName,
        options = optionsWithPartitionBasePath)
    Dataset.ofRows(sparkSession, LogicalRelation(newDataSource.resolveRelation(
      checkFilesExist = false), isStreaming = true))
  }

  /**
   * If the source has a metadata log indicating which files should be read, then we should use it.
   * Only when user gives a non-glob path that will we figure out whether the source has some
   * metadata log
   *
   * None        means we don't know at the moment
   * Some(true)  means we know for sure the source DOES have metadata
   * Some(false) means we know for sure the source DOSE NOT have metadata
   */
  @volatile private[sql] var sourceHasMetadata: Option[Boolean] =
    if (SparkHadoopUtil.get.isGlobPath(new Path(path))) Some(false) else None

  private def allFilesUsingInMemoryFileIndex() = {
    val globbedPaths = SparkHadoopUtil.get.globPathIfNecessary(fs, qualifiedBasePath)
    val fileIndex = new InMemoryFileIndex(sparkSession, globbedPaths, options, Some(new StructType))
    fileIndex.allFiles()
  }

  private def allFilesUsingMetadataLogFileIndex() = {
    // Note if `sourceHasMetadata` holds, then `qualifiedBasePath` is guaranteed to be a
    // non-glob path
    new MetadataLogFileIndex(sparkSession, qualifiedBasePath,
      CaseInsensitiveMap(options), None).allFiles()
  }

  /**
   * Returns a list of files found, sorted by their timestamp.
   */
  private def fetchAllFiles(): Seq[(String, Long)] = {
    val startTime = System.nanoTime

    var allFiles: Seq[FileStatus] = null
    sourceHasMetadata match {
      case None =>
        if (FileStreamSink.hasMetadata(Seq(path), hadoopConf, sparkSession.sessionState.conf)) {
          sourceHasMetadata = Some(true)
          allFiles = allFilesUsingMetadataLogFileIndex()
        } else {
          allFiles = allFilesUsingInMemoryFileIndex()
          if (allFiles.isEmpty) {
            // we still cannot decide
          } else {
            // decide what to use for future rounds
            // double check whether source has metadata, preventing the extreme corner case that
            // metadata log and data files are only generated after the previous
            // `FileStreamSink.hasMetadata` check
            if (FileStreamSink.hasMetadata(Seq(path), hadoopConf, sparkSession.sessionState.conf)) {
              sourceHasMetadata = Some(true)
              allFiles = allFilesUsingMetadataLogFileIndex()
            } else {
              sourceHasMetadata = Some(false)
              // `allFiles` have already been fetched using InMemoryFileIndex in this round
            }
          }
        }
      case Some(true) => allFiles = allFilesUsingMetadataLogFileIndex()
      case Some(false) => allFiles = allFilesUsingInMemoryFileIndex()
    }

    val files = allFiles.sortBy(_.getModificationTime)(fileSortOrder).map { status =>
      (status.getPath.toUri.toString, status.getModificationTime)
    }

    val endTime = System.nanoTime
    val listingTimeMs = NANOSECONDS.toMillis(endTime - startTime)
    if (listingTimeMs > 2000) {
      // Output a warning when listing files uses more than 2 seconds.
      logWarning(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    } else {
      logTrace(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    }
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    files
  }

  override def getOffset: Option[Offset] = Some(fetchMaxOffset()).filterNot(_.logOffset == -1)

  override def toString: String = s"FileStreamSource[$qualifiedBasePath]"

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  override def commit(end: Offset): Unit = {
    val logOffset = FileStreamSourceOffset(end).logOffset

    if (sourceOptions.cleanSource != CleanSourceMode.NO_OP) {
      val files = metadataLog.get(Some(logOffset), Some(logOffset)).flatMap(_._2)
      val validFileEntities = files.filter(_.batchId == logOffset)
      logDebug(s"completed file entries: ${validFileEntities.mkString(",")}")
      sourceOptions.cleanSource match {
        case CleanSourceMode.ARCHIVE =>
          validFileEntities.foreach(sourceCleaner.archive)

        case CleanSourceMode.DELETE =>
          validFileEntities.foreach(sourceCleaner.delete)

        case _ =>
      }
    } else {
      // No-op for now; FileStreamSource currently garbage-collects files based on timestamp
      // and the value of the maxFileAge parameter.
    }

  }

  override def stop(): Unit = {}
}


object FileStreamSource {
  /** Timestamp for file modification time, in ms since January 1, 1970 UTC. */
  type Timestamp = Long

  case class FileEntry(path: String, timestamp: Timestamp, batchId: Long) extends Serializable

  /**
   * A custom hash map used to track the list of files seen. This map is not thread-safe.
   *
   * To prevent the hash map from growing indefinitely, a purge function is available to
   * remove files "maxAgeMs" older than the latest file.
   */
  class SeenFilesMap(maxAgeMs: Long, fileNameOnly: Boolean) {
    require(maxAgeMs >= 0)

    /** Mapping from file to its timestamp. */
    private val map = new java.util.HashMap[String, Timestamp]

    /** Timestamp of the latest file. */
    private var latestTimestamp: Timestamp = 0L

    /** Timestamp for the last purge operation. */
    private var lastPurgeTimestamp: Timestamp = 0L

    @inline private def stripPathIfNecessary(path: String) = {
      if (fileNameOnly) new Path(new URI(path)).getName else path
    }

    /** Add a new file to the map. */
    def add(path: String, timestamp: Timestamp): Unit = {
      map.put(stripPathIfNecessary(path), timestamp)
      if (timestamp > latestTimestamp) {
        latestTimestamp = timestamp
      }
    }

    /**
     * Returns true if we should consider this file a new file. The file is only considered "new"
     * if it is new enough that we are still tracking, and we have not seen it before.
     */
    def isNewFile(path: String, timestamp: Timestamp): Boolean = {
      // Note that we are testing against lastPurgeTimestamp here so we'd never miss a file that
      // is older than (latestTimestamp - maxAgeMs) but has not been purged yet.
      timestamp >= lastPurgeTimestamp && !map.containsKey(stripPathIfNecessary(path))
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
  }

  private[sql] class FileStreamSourceCleaner(
      fileSystem: FileSystem,
      sourcePath: Path,
      baseArchivePathString: Option[String]) extends Logging {

    private val sourceGlobFilters: Seq[GlobFilter] = buildSourceGlobFilters(sourcePath)

    private val baseArchivePath: Option[Path] = baseArchivePathString.map(new Path(_))

    def archive(entry: FileEntry): Unit = {
      require(baseArchivePath.isDefined)

      val curPath = new Path(new URI(entry.path))
      val curPathUri = curPath.toUri

      val newPath = buildArchiveFilePath(curPathUri)

      if (isArchiveFileMatchedAgainstSourcePattern(newPath)) {
        logWarning(s"Fail to move $curPath to $newPath - destination matches " +
          s"to source path/pattern. Skip moving file.")
      } else {
        doArchive(curPath, newPath)
      }
    }

    def delete(entry: FileEntry): Unit = {
      val curPath = new Path(new URI(entry.path))
      try {
        logDebug(s"Removing completed file $curPath")

        if (!fileSystem.delete(curPath, false)) {
          logWarning(s"Fail to remove $curPath / skip removing file.")
        }
      } catch {
        case NonFatal(e) =>
          // Log to error but swallow exception to avoid process being stopped
          logWarning(s"Fail to remove $curPath / skip removing file.", e)
      }
    }

    private def buildSourceGlobFilters(sourcePath: Path): Seq[GlobFilter] = {
      val filters = new scala.collection.mutable.MutableList[GlobFilter]()

      var currentPath = sourcePath
      while (!currentPath.isRoot) {
        filters += new GlobFilter(currentPath.getName)
        currentPath = currentPath.getParent
      }

      filters.toList
    }

    private def buildArchiveFilePath(pathUri: URI): Path = {
      require(baseArchivePathString.isDefined)
      val baseArchivePathStr = baseArchivePathString.get
      val normalizedBaseArchiveDirPath = if (baseArchivePathStr.endsWith("/")) {
        baseArchivePathStr.substring(0, baseArchivePathStr.length - 1)
      } else {
        baseArchivePathStr
      }

      new Path(normalizedBaseArchiveDirPath + pathUri.getPath)
    }

    private def isArchiveFileMatchedAgainstSourcePattern(archiveFile: Path): Boolean = {
      if (baseArchivePath.get.depth() > 2) {
        // there's no chance for archive file to be matched against source pattern
        return false
      }

      var matched = true

      // new path will never match against source path when the depth is not a range of
      // the depth of source path ~ (the depth of source path + 1)
      // because the source files are picked when they match against source pattern or
      // their parent directories match against source pattern
      val depthSourcePattern = sourceGlobFilters.length
      val depthArchiveFile = archiveFile.depth()

      // we already checked against the depth of archive path, but rechecking wouldn't hurt
      if (depthArchiveFile < depthSourcePattern || depthArchiveFile > depthSourcePattern + 1) {
        // never matched
        matched = false
      } else {
        var pathToCompare = if (depthArchiveFile == depthSourcePattern + 1) {
          archiveFile.getParent
        } else {
          archiveFile
        }

        // Now pathToCompare should have same depth as sourceGlobFilters.length
        var index = 0
        do {
          // GlobFilter only matches against its name, not full path so it's safe to compare
          if (!sourceGlobFilters(index).accept(pathToCompare)) {
            matched = false
          } else {
            pathToCompare = pathToCompare.getParent
            index += 1
          }
        } while (matched && !pathToCompare.isRoot)
      }

      matched
    }

    private def doArchive(sourcePath: Path, archivePath: Path): Unit = {
      try {
        logDebug(s"Creating directory if it doesn't exist ${archivePath.getParent}")
        if (!fileSystem.exists(archivePath.getParent)) {
          fileSystem.mkdirs(archivePath.getParent)
        }

        logDebug(s"Archiving completed file $sourcePath to $archivePath")
        if (!fileSystem.rename(sourcePath, archivePath)) {
          logWarning(s"Fail to move $sourcePath to $archivePath / skip moving file.")
        }
      } catch {
        case NonFatal(e) =>
          logWarning(s"Fail to move $sourcePath to $archivePath / skip moving file.", e)
      }
    }

  }
}
