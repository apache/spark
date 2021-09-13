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
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit._

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit, ReadMaxFiles, SupportsAdmissionControl, SupportsTriggerAvailableNow}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{DataSource, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

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
    options: Map[String, String])
  extends SupportsAdmissionControl
  with SupportsTriggerAvailableNow
  with Source
  with Logging {

  import FileStreamSource._

  private val sourceOptions = new FileStreamOptions(options)

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  @transient private val fs = new Path(path).getFileSystem(hadoopConf)

  private val qualifiedBasePath: Path = {
    fs.makeQualified(new Path(path))  // can contain glob patterns
  }

  private val sourceCleaner: Option[FileStreamSourceCleaner] = FileStreamSourceCleaner(
    fs, qualifiedBasePath, sourceOptions, hadoopConf)

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

  private var allFilesForTriggerAvailableNow: Seq[(String, Long)] = _

  metadataLog.restore().foreach { entry =>
    seenFiles.add(entry.path, entry.timestamp)
  }
  seenFiles.purge()

  logInfo(s"maxFilesPerBatch = $maxFilesPerBatch, maxFileAgeMs = $maxFileAgeMs")

  private var unreadFiles: Seq[(String, Long)] = _

  /**
   * Returns the maximum offset that can be retrieved from the source.
   *
   * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
   * there is no race here, so the cost of `synchronized` should be rare.
   */
  private def fetchMaxOffset(limit: ReadLimit): FileStreamSourceOffset = synchronized {
    val newFiles = if (unreadFiles != null) {
      logDebug(s"Reading from unread files - ${unreadFiles.size} files are available.")
      unreadFiles
    } else {
      // All the new files found - ignore aged files and files that we have seen.
      // Use the pre-fetched list of files when Trigger.AvailableNow is enabled.
      val allFiles = if (allFilesForTriggerAvailableNow != null) {
        allFilesForTriggerAvailableNow
      } else {
        fetchAllFiles()
      }
      allFiles.filter {
        case (path, timestamp) => seenFiles.isNewFile(path, timestamp)
      }
    }

    // Obey user's setting to limit the number of files in this batch trigger.
    val (batchFiles, unselectedFiles) = limit match {
      case files: ReadMaxFiles if !sourceOptions.latestFirst =>
        // we can cache and reuse remaining fetched list of files in further batches
        val (bFiles, usFiles) = newFiles.splitAt(files.maxFiles())
        if (usFiles.size < files.maxFiles() * DISCARD_UNSEEN_FILES_RATIO) {
          // Discard unselected files if the number of files are smaller than threshold.
          // This is to avoid the case when the next batch would have too few files to read
          // whereas there're new files available.
          logTrace(s"Discarding ${usFiles.length} unread files as it's smaller than threshold.")
          (bFiles, null)
        } else {
          (bFiles, usFiles)
        }

      case files: ReadMaxFiles =>
        // implies "sourceOptions.latestFirst = true" which we want to refresh the list per batch
        (newFiles.take(files.maxFiles()), null)

      case _: ReadAllAvailable => (newFiles, null)
    }

    if (unselectedFiles != null && unselectedFiles.nonEmpty) {
      logTrace(s"Taking first $MAX_CACHED_UNSEEN_FILES unread files.")
      unreadFiles = unselectedFiles.take(MAX_CACHED_UNSEEN_FILES)
      logTrace(s"${unreadFiles.size} unread files are available for further batches.")
    } else {
      unreadFiles = null
      logTrace(s"No unread file is available for further batches.")
    }

    batchFiles.foreach { file =>
      seenFiles.add(file._1, file._2)
      logDebug(s"New file: $file")
    }
    val numPurged = seenFiles.purge()

    logTrace(
      s"""
         |Number of new files = ${newFiles.size}
         |Number of files selected for batch = ${batchFiles.size}
         |Number of unread files = ${Option(unreadFiles).map(_.size).getOrElse(0)}
         |Number of seen files = ${seenFiles.size}
         |Number of files purged from tracking map = $numPurged
       """.stripMargin)

    if (batchFiles.nonEmpty) {
      metadataLogCurrentOffset += 1

      val fileEntries = batchFiles.map { case (p, timestamp) =>
        FileEntry(path = p, timestamp = timestamp, batchId = metadataLogCurrentOffset)
      }.toArray
      if (metadataLog.add(metadataLogCurrentOffset, fileEntries)) {
        logInfo(s"Log offset set to $metadataLogCurrentOffset with ${batchFiles.size} new files")
      } else {
        throw new IllegalStateException("Concurrent update to the log. Multiple streaming jobs " +
          s"detected for $metadataLogCurrentOffset")
      }
    }

    FileStreamSourceOffset(metadataLogCurrentOffset)
  }

  override def prepareForTriggerAvailableNow(): Unit = {
    allFilesForTriggerAvailableNow = fetchAllFiles()
  }

  override def getDefaultReadLimit: ReadLimit = {
    maxFilesPerBatch.map(ReadLimit.maxFiles).getOrElse(super.getDefaultReadLimit)
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

  private def setSourceHasMetadata(newValue: Option[Boolean]): Unit = newValue match {
    case Some(true) =>
      if (sourceCleaner.isDefined) {
        throw QueryExecutionErrors.cleanUpSourceFilesUnsupportedError()
      }
      sourceHasMetadata = Some(true)
    case _ =>
      sourceHasMetadata = newValue
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
          setSourceHasMetadata(Some(true))
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
              setSourceHasMetadata(Some(true))
              allFiles = allFilesUsingMetadataLogFileIndex()
            } else {
              setSourceHasMetadata(Some(false))
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

  override def getOffset: Option[Offset] = {
    throw QueryExecutionErrors.latestOffsetNotCalledError()
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    Some(fetchMaxOffset(limit)).filterNot(_.logOffset == -1).orNull
  }

  override def toString: String = s"FileStreamSource[$qualifiedBasePath]"

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  override def commit(end: Offset): Unit = {
    val logOffset = FileStreamSourceOffset(end).logOffset

    sourceCleaner.foreach { cleaner =>
      val files = metadataLog.get(Some(logOffset), Some(logOffset)).flatMap(_._2)
      val validFileEntities = files.filter(_.batchId == logOffset)
      logDebug(s"completed file entries: ${validFileEntities.mkString(",")}")
      validFileEntities.foreach(cleaner.clean)
    }
  }

  override def stop(): Unit = sourceCleaner.foreach(_.stop())
}


object FileStreamSource {
  /** Timestamp for file modification time, in ms since January 1, 1970 UTC. */
  type Timestamp = Long

  val DISCARD_UNSEEN_FILES_RATIO = 0.2
  val MAX_CACHED_UNSEEN_FILES = 10000

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

  private[sql] abstract class FileStreamSourceCleaner extends Logging {
    private val cleanThreadPool: Option[ThreadPoolExecutor] = {
      val numThreads = SQLConf.get.getConf(SQLConf.FILE_SOURCE_CLEANER_NUM_THREADS)
      if (numThreads > 0) {
        logDebug(s"Cleaning file source on $numThreads separate thread(s)")
        Some(ThreadUtils.newDaemonCachedThreadPool("file-source-cleaner-threadpool", numThreads))
      } else {
        logDebug("Cleaning file source on main thread")
        None
      }
    }

    def stop(): Unit = cleanThreadPool.foreach(ThreadUtils.shutdown(_))

    def clean(entry: FileEntry): Unit = {
      cleanThreadPool match {
        case Some(p) =>
          p.submit(new Runnable {
            override def run(): Unit = {
              cleanTask(entry)
            }
          })

        case None =>
          cleanTask(entry)
      }
    }

    protected def cleanTask(entry: FileEntry): Unit
  }

  private[sql] object FileStreamSourceCleaner {
    def apply(
        fileSystem: FileSystem,
        sourcePath: Path,
        option: FileStreamOptions,
        hadoopConf: Configuration): Option[FileStreamSourceCleaner] = option.cleanSource match {
      case CleanSourceMode.ARCHIVE =>
        require(option.sourceArchiveDir.isDefined)
        val path = new Path(option.sourceArchiveDir.get)
        val archiveFs = path.getFileSystem(hadoopConf)
        val qualifiedArchivePath = archiveFs.makeQualified(path)
        Some(new SourceFileArchiver(fileSystem, sourcePath, archiveFs, qualifiedArchivePath))

      case CleanSourceMode.DELETE =>
        Some(new SourceFileRemover(fileSystem))

      case _ => None
    }
  }

  private[sql] class SourceFileArchiver(
      fileSystem: FileSystem,
      sourcePath: Path,
      baseArchiveFileSystem: FileSystem,
      baseArchivePath: Path) extends FileStreamSourceCleaner with Logging {
    assertParameters()

    private def assertParameters(): Unit = {
      require(fileSystem.getUri == baseArchiveFileSystem.getUri, "Base archive path is located " +
        s"on a different file system than the source files. source path: $sourcePath" +
        s" / base archive path: $baseArchivePath")

      require(!isBaseArchivePathMatchedAgainstSourcePattern, "Base archive path cannot be set to" +
        " the path where archived path can possibly match with source pattern. Ensure the base " +
        "archive path doesn't match with source pattern in depth, where the depth is minimum of" +
        " depth on both paths.")
    }

    private def getAncestorEnsuringDepth(path: Path, depth: Int): Path = {
      var newPath = path
      while (newPath.depth() > depth) {
        newPath = newPath.getParent
      }
      newPath
    }

    private def isBaseArchivePathMatchedAgainstSourcePattern: Boolean = {
      // We should disallow end users to set base archive path which path matches against source
      // pattern to avoid checking each source file. There're couple of cases which allow
      // FileStreamSource to read any depth of subdirectory under the source pattern, so we should
      // consider all three cases 1) both has same depth 2) base archive path is longer than source
      // pattern 3) source pattern is longer than base archive path. To handle all cases, we take
      // min of depth for both paths, and check the match.

      val minDepth = math.min(sourcePath.depth(), baseArchivePath.depth())

      val sourcePathMinDepth = getAncestorEnsuringDepth(sourcePath, minDepth)
      val baseArchivePathMinDepth = getAncestorEnsuringDepth(baseArchivePath, minDepth)

      val sourceGlobFilters: Seq[GlobFilter] = buildSourceGlobFilters(sourcePathMinDepth)

      var matched = true

      // pathToCompare should have same depth as sourceGlobFilters.length
      var pathToCompare = baseArchivePathMinDepth
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

      matched
    }

    private def buildSourceGlobFilters(sourcePath: Path): Seq[GlobFilter] = {
      val filters = new scala.collection.mutable.ArrayBuffer[GlobFilter]()

      var currentPath = sourcePath
      while (!currentPath.isRoot) {
        filters += new GlobFilter(currentPath.getName)
        currentPath = currentPath.getParent
      }

      filters.toSeq
    }

    override protected def cleanTask(entry: FileEntry): Unit = {
      val curPath = new Path(new URI(entry.path))
      val newPath = new Path(baseArchivePath.toString.stripSuffix("/") + curPath.toUri.getPath)

      try {
        logDebug(s"Creating directory if it doesn't exist ${newPath.getParent}")
        if (!fileSystem.exists(newPath.getParent)) {
          fileSystem.mkdirs(newPath.getParent)
        }

        logDebug(s"Archiving completed file $curPath to $newPath")
        if (!fileSystem.rename(curPath, newPath)) {
          logWarning(s"Fail to move $curPath to $newPath / skip moving file.")
        }
      } catch {
        case NonFatal(e) =>
          logWarning(s"Fail to move $curPath to $newPath / skip moving file.", e)
      }
    }
  }

  private[sql] class SourceFileRemover(fileSystem: FileSystem)
    extends FileStreamSourceCleaner with Logging {

    override protected def cleanTask(entry: FileEntry): Unit = {
      val curPath = new Path(new URI(entry.path))
      try {
        logDebug(s"Removing completed file $curPath")

        if (!fileSystem.delete(curPath, false)) {
          logWarning(s"Failed to remove $curPath / skip removing file.")
        }
      } catch {
        case NonFatal(e) =>
          // Log to error but swallow exception to avoid process being stopped
          logWarning(s"Fail to remove $curPath / skip removing file.", e)
      }
    }
  }
}
