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
import org.apache.hadoop.fs.viewfs.ViewFileSystem
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
    fileStatusCache: FileStatusCache = NoopCache,
    userSpecifiedPartitionSpec: Option[PartitionSpec] = None,
    override val metadataOpsTimeNs: Option[Long] = None)
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
      if (userSpecifiedPartitionSpec.isDefined) {
        cachedPartitionSpec = userSpecifiedPartitionSpec.get
      } else {
        cachedPartitionSpec = inferPartitioning()
      }
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
    val startTime = System.nanoTime()
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
      () // for some reasons scalac 2.12 needs this; return type doesn't matter
    }
    val discovered = InMemoryFileIndex.bulkListLeafFiles(pathsToFetch, hadoopConf, areRootPaths = true)
    discovered.foreach { case (path, leafFiles) =>
      HiveCatalogMetrics.incrementFilesDiscovered(leafFiles.size)
      fileStatusCache.putLeafFiles(path, leafFiles.toArray)
      output ++= leafFiles
    }
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to list leaf files" +
      s" for ${paths.length} paths.")
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

  private[sql] def bulkListLeafFiles(
      paths: Seq[Path],
      hadoopConf: Configuration,
      filter: PathFilter,
      sparkSession: SparkSession,
      areRootPaths: Boolean): Seq[(Path, Seq[FileStatus])] = {

    val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles
    val ignoreLocality = sparkSession.sessionState.conf.ignoreDataLocality

    val filter = FileInputFormat.getInputPathFilter(new JobConf(hadoopConf, this.getClass))

    // Short-circuits parallel listing when serial listing is likely to be faster.
    val threshold = sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold
    if (paths.size <= threshold) {
      return paths.map { path =>
        val leafFiles = HadoopFSUtils.listLeafFiles(
          path,
          hadoopConf,
          filter,
          Some(sparkSession),
          ignoreMissingFiles = ignoreMissingFiles,
          ignoreLocality = ignoreLocality,
          isSQLRootPath = areRootPaths,
          filterFun = Some(shouldFilterOut),
          parallelismThreshold = Some(threshold))
        (path, leafFiles)
      }
    }

    val parallelPartitionDiscoveryParallelism =
      sparkSession.sessionState.conf.parallelPartitionDiscoveryParallelism
  }
  private def parallelScanCallBack(): Unit = {
    logInfo(s"Listing leaf files and directories in parallel under ${paths.length} paths." +
      s" The first several paths are: ${paths.take(10).mkString(", ")}.")
    HiveCatalogMetrics.incrementParallelListingJobCount(1)
  }
  HadoopFSUtils.parallelListLeafFiles(sparkSession.sparkContext, paths, hadoopConf, filter,
    areSQLRootPaths = areRootPaths, ignoreMissingFiles = ignorMissingFiles,
    ignoreLocality = ignoreLocality, parallelPartitionDiscoveryParallelism,
    Some(parallelScanCallBack), Some(shouldFilterOut))
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
