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

package org.apache.spark.sql.execution.datasources

import java.io.FileNotFoundException

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration


/**
 * A [[FileIndex]] that generates the list of files to process by recursively listing all the
 * files present in `paths`.
 *
 * @param rootPathsSpecified the list of root table paths to scan (some of which might be
 *                           filtered out later)
 * @param parameters as set of options to control discovery
 * @param userSpecifiedSchema an optional user specified schema that will be use to provide
 *                            types for the discovered partitions
 */
class InMemoryFileIndex(
    sparkSession: SparkSession,
    rootPathsSpecified: Seq[Path],
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    fileStatusCache: FileStatusCache = NoopCache)
  extends PartitioningAwareFileIndex(
    sparkSession, parameters, userSpecifiedSchema, fileStatusCache) {

  // Filter out streaming metadata dirs or files such as "/.../_spark_metadata" (the metadata dir)
  // or "/.../_spark_metadata/0" (a file in the metadata dir). `rootPathsSpecified` might contain
  // such streaming metadata dir or files, e.g. when after globbing "basePath/*" where "basePath"
  // is the output of a streaming query.
  override val rootPaths =
    rootPathsSpecified.filterNot(FileStreamSink.ancestorIsMetadataDirectory(_, hadoopConf))

  @volatile private var cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus] = _
  @volatile private var cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]] = _
  @volatile private var cachedPartitionSpec: PartitionSpec = _

  refresh0()

  override def partitionSpec(): PartitionSpec = {
    if (cachedPartitionSpec == null) {
      cachedPartitionSpec = inferPartitioning()
    }
    logTrace(s"Partition spec: $cachedPartitionSpec")
    cachedPartitionSpec
  }

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    cachedLeafFiles
  }

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    cachedLeafDirToChildrenFiles
  }

  override def refresh(): Unit = {
    fileStatusCache.invalidateAll()
    refresh0()
  }

  private def refresh0(): Unit = {
    val files = listLeafFiles(rootPaths)
    cachedLeafFiles =
      new mutable.LinkedHashMap[Path, FileStatus]() ++= files.map(f => f.getPath -> f)
    cachedLeafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)
    cachedPartitionSpec = null
  }

  override def equals(other: Any): Boolean = other match {
    case hdfs: InMemoryFileIndex => rootPaths.toSet == hdfs.rootPaths.toSet
    case _ => false
  }

  override def hashCode(): Int = rootPaths.toSet.hashCode()

  /**
   * List leaf files of given paths. This method will submit a Spark job to do parallel
   * listing whenever there is a path having more files than the parallel partition discovery
   * discovery threshold.
   *
   * This is publicly visible for testing.
   */
  def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = {
    val output = mutable.LinkedHashSet[FileStatus]()
    val pathsToFetch = mutable.ArrayBuffer[Path]()
    for (path <- paths) {
      fileStatusCache.getLeafFiles(path) match {
        case Some(files) =>
          HiveCatalogMetrics.incrementFileCacheHits(files.length)
          output ++= files
        case None =>
          pathsToFetch += path
      }
      Unit // for some reasons scalac 2.12 needs this; return type doesn't matter
    }
    val filter = FileInputFormat.getInputPathFilter(new JobConf(hadoopConf, this.getClass))
    val discovered = InMemoryFileIndex.bulkListLeafFiles(
      pathsToFetch, hadoopConf, filter, sparkSession, areRootPaths = true)
    discovered.foreach { case (path, leafFiles) =>
      HiveCatalogMetrics.incrementFilesDiscovered(leafFiles.size)
      fileStatusCache.putLeafFiles(path, leafFiles.toArray)
      output ++= leafFiles
    }
    output
  }
}

object InMemoryFileIndex extends Logging {

  /** A serializable variant of HDFS's BlockLocation. */
  private case class SerializableBlockLocation(
      names: Array[String],
      hosts: Array[String],
      offset: Long,
      length: Long)

  /** A serializable variant of HDFS's FileStatus. */
  private case class SerializableFileStatus(
      path: String,
      length: Long,
      isDir: Boolean,
      blockReplication: Short,
      blockSize: Long,
      modificationTime: Long,
      accessTime: Long,
      blockLocations: Array[SerializableBlockLocation])

  /**
   * Lists a collection of paths recursively. Picks the listing strategy adaptively depending
   * on the number of paths to list.
   *
   * This may only be called on the driver.
   *
   * @return for each input path, the set of discovered files for the path
   */
  private[sql] def bulkListLeafFiles(
      paths: Seq[Path],
      hadoopConf: Configuration,
      filter: PathFilter,
      sparkSession: SparkSession,
      areRootPaths: Boolean): Seq[(Path, Seq[FileStatus])] = {

    val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

    // Short-circuits parallel listing when serial listing is likely to be faster.
    if (paths.size <= sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold) {
      return paths.map { path =>
        val leafFiles = listLeafFiles(
          path,
          hadoopConf,
          filter,
          Some(sparkSession),
          ignoreMissingFiles = ignoreMissingFiles,
          isRootPath = areRootPaths)
        (path, leafFiles)
      }
    }

    logInfo(s"Listing leaf files and directories in parallel under: ${paths.mkString(", ")}")
    HiveCatalogMetrics.incrementParallelListingJobCount(1)

    val sparkContext = sparkSession.sparkContext
    val serializableConfiguration = new SerializableConfiguration(hadoopConf)
    val serializedPaths = paths.map(_.toString)
    val parallelPartitionDiscoveryParallelism =
      sparkSession.sessionState.conf.parallelPartitionDiscoveryParallelism

    // Set the number of parallelism to prevent following file listing from generating many tasks
    // in case of large #defaultParallelism.
    val numParallelism = Math.min(paths.size, parallelPartitionDiscoveryParallelism)

    val previousJobDescription = sparkContext.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
    val statusMap = try {
      val description = paths.size match {
        case 0 =>
          s"Listing leaf files and directories 0 paths"
        case 1 =>
          s"Listing leaf files and directories for 1 path:<br/>${paths(0)}"
        case s =>
          s"Listing leaf files and directories for $s paths:<br/>${paths(0)}, ..."
      }
      sparkContext.setJobDescription(description)
      sparkContext
        .parallelize(serializedPaths, numParallelism)
        .mapPartitions { pathStrings =>
          val hadoopConf = serializableConfiguration.value
          pathStrings.map(new Path(_)).toSeq.map { path =>
            val leafFiles = listLeafFiles(
              path,
              hadoopConf,
              filter,
              None,
              ignoreMissingFiles = ignoreMissingFiles,
              isRootPath = areRootPaths)
            (path, leafFiles)
          }.iterator
        }.map { case (path, statuses) =>
        val serializableStatuses = statuses.map { status =>
          // Turn FileStatus into SerializableFileStatus so we can send it back to the driver
          val blockLocations = status match {
            case f: LocatedFileStatus =>
              f.getBlockLocations.map { loc =>
                SerializableBlockLocation(
                  loc.getNames,
                  loc.getHosts,
                  loc.getOffset,
                  loc.getLength)
              }

            case _ =>
              Array.empty[SerializableBlockLocation]
          }

          SerializableFileStatus(
            status.getPath.toString,
            status.getLen,
            status.isDirectory,
            status.getReplication,
            status.getBlockSize,
            status.getModificationTime,
            status.getAccessTime,
            blockLocations)
        }
        (path.toString, serializableStatuses)
      }.collect()
    } finally {
      sparkContext.setJobDescription(previousJobDescription)
    }

    // turn SerializableFileStatus back to Status
    statusMap.map { case (path, serializableStatuses) =>
      val statuses = serializableStatuses.map { f =>
        val blockLocations = f.blockLocations.map { loc =>
          new BlockLocation(loc.names, loc.hosts, loc.offset, loc.length)
        }
        new LocatedFileStatus(
          new FileStatus(
            f.length, f.isDir, f.blockReplication, f.blockSize, f.modificationTime,
            new Path(f.path)),
          blockLocations)
      }
      (new Path(path), statuses)
    }
  }

  /**
   * Lists a single filesystem path recursively. If a SparkSession object is specified, this
   * function may launch Spark jobs to parallelize listing.
   *
   * If sessionOpt is None, this may be called on executors.
   *
   * @return all children of path that match the specified filter.
   */
  private def listLeafFiles(
      path: Path,
      hadoopConf: Configuration,
      filter: PathFilter,
      sessionOpt: Option[SparkSession],
      ignoreMissingFiles: Boolean,
      isRootPath: Boolean): Seq[FileStatus] = {
    logTrace(s"Listing $path")
    val fs = path.getFileSystem(hadoopConf)

    // Note that statuses only include FileStatus for the files and dirs directly under path,
    // and does not include anything else recursively.
    val statuses: Array[FileStatus] = try {
      fs match {
        // DistributedFileSystem overrides listLocatedStatus to make 1 single call to namenode
        // to retrieve the file status with the file block location. The reason to still fallback
        // to listStatus is because the default implementation would potentially throw a
        // FileNotFoundException which is better handled by doing the lookups manually below.
        case _: DistributedFileSystem =>
          val remoteIter = fs.listLocatedStatus(path)
          new Iterator[LocatedFileStatus]() {
            def next(): LocatedFileStatus = remoteIter.next
            def hasNext(): Boolean = remoteIter.hasNext
          }.toArray
        case _ => fs.listStatus(path)
      }
    } catch {
      // If we are listing a root path (e.g. a top level directory of a table), we need to
      // ignore FileNotFoundExceptions during this root level of the listing because
      //
      //  (a) certain code paths might construct an InMemoryFileIndex with root paths that
      //      might not exist (i.e. not all callers are guaranteed to have checked
      //      path existence prior to constructing InMemoryFileIndex) and,
      //  (b) we need to ignore deleted root paths during REFRESH TABLE, otherwise we break
      //      existing behavior and break the ability drop SessionCatalog tables when tables'
      //      root directories have been deleted (which breaks a number of Spark's own tests).
      //
      // If we are NOT listing a root path then a FileNotFoundException here means that the
      // directory was present in a previous level of file listing but is absent in this
      // listing, likely indicating a race condition (e.g. concurrent table overwrite or S3
      // list inconsistency).
      //
      // The trade-off in supporting existing behaviors / use-cases is that we won't be
      // able to detect race conditions involving root paths being deleted during
      // InMemoryFileIndex construction. However, it's still a net improvement to detect and
      // fail-fast on the non-root cases. For more info see the SPARK-27676 review discussion.
      case _: FileNotFoundException if isRootPath || ignoreMissingFiles =>
        logWarning(s"The directory $path was not found. Was it deleted very recently?")
        Array.empty[FileStatus]
    }

    val filteredStatuses = statuses.filterNot(status => shouldFilterOut(status.getPath.getName))

    val allLeafStatuses = {
      val (dirs, topLevelFiles) = filteredStatuses.partition(_.isDirectory)
      val nestedFiles: Seq[FileStatus] = sessionOpt match {
        case Some(session) =>
          bulkListLeafFiles(
            dirs.map(_.getPath),
            hadoopConf,
            filter,
            session,
            areRootPaths = false
          ).flatMap(_._2)
        case _ =>
          dirs.flatMap { dir =>
            listLeafFiles(
              dir.getPath,
              hadoopConf,
              filter,
              sessionOpt,
              ignoreMissingFiles = ignoreMissingFiles,
              isRootPath = false)
          }
      }
      val allFiles = topLevelFiles ++ nestedFiles
      if (filter != null) allFiles.filter(f => filter.accept(f.getPath)) else allFiles
    }

    val missingFiles = mutable.ArrayBuffer.empty[String]
    val filteredLeafStatuses = allLeafStatuses.filterNot(
      status => shouldFilterOut(status.getPath.getName))
    val resolvedLeafStatuses = filteredLeafStatuses.flatMap {
      case f: LocatedFileStatus =>
        Some(f)

      // NOTE:
      //
      // - Although S3/S3A/S3N file system can be quite slow for remote file metadata
      //   operations, calling `getFileBlockLocations` does no harm here since these file system
      //   implementations don't actually issue RPC for this method.
      //
      // - Here we are calling `getFileBlockLocations` in a sequential manner, but it should not
      //   be a big deal since we always use to `bulkListLeafFiles` when the number of
      //   paths exceeds threshold.
      case f =>
        // The other constructor of LocatedFileStatus will call FileStatus.getPermission(),
        // which is very slow on some file system (RawLocalFileSystem, which is launch a
        // subprocess and parse the stdout).
        try {
          val locations = fs.getFileBlockLocations(f, 0, f.getLen).map { loc =>
            // Store BlockLocation objects to consume less memory
            if (loc.getClass == classOf[BlockLocation]) {
              loc
            } else {
              new BlockLocation(loc.getNames, loc.getHosts, loc.getOffset, loc.getLength)
            }
          }
          val lfs = new LocatedFileStatus(f.getLen, f.isDirectory, f.getReplication, f.getBlockSize,
            f.getModificationTime, 0, null, null, null, null, f.getPath, locations)
          if (f.isSymlink) {
            lfs.setSymlink(f.getSymlink)
          }
          Some(lfs)
        } catch {
          case _: FileNotFoundException if ignoreMissingFiles =>
            missingFiles += f.getPath.toString
            None
        }
    }

    if (missingFiles.nonEmpty) {
      logWarning(
        s"the following files were missing during file scan:\n  ${missingFiles.mkString("\n  ")}")
    }

    resolvedLeafStatuses
  }

  /** Checks if we should filter out this path name. */
  def shouldFilterOut(pathName: String): Boolean = {
    // We filter follow paths:
    // 1. everything that starts with _ and ., except _common_metadata and _metadata
    // because Parquet needs to find those metadata files from leaf files returned by this method.
    // We should refactor this logic to not mix metadata files with data files.
    // 2. everything that ends with `._COPYING_`, because this is a intermediate state of file. we
    // should skip this file in case of double reading.
    val exclude = (pathName.startsWith("_") && !pathName.contains("=")) ||
      pathName.startsWith(".") || pathName.endsWith("._COPYING_")
    val include = pathName.startsWith("_common_metadata") || pathName.startsWith("_metadata")
    exclude && !include
  }
}
