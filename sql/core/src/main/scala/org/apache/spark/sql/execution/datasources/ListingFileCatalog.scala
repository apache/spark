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
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration


/**
 * A [[FileCatalog]] that generates the list of files to process by recursively listing all the
 * files present in `paths`.
 *
 * @param parameters as set of options to control discovery
 * @param paths a list of paths to scan
 * @param partitionSchema an optional partition schema that will be use to provide types for the
 *                        discovered partitions
 */
class ListingFileCatalog(
    sparkSession: SparkSession,
    override val paths: Seq[Path],
    parameters: Map[String, String],
    partitionSchema: Option[StructType])
  extends PartitioningAwareFileCatalog(sparkSession, parameters, partitionSchema) {

  @volatile private var cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus] = _
  @volatile private var cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]] = _
  @volatile private var cachedPartitionSpec: PartitionSpec = _

  refresh()

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
    val files = listLeafFiles(paths)
    cachedLeafFiles =
      new mutable.LinkedHashMap[Path, FileStatus]() ++= files.map(f => f.getPath -> f)
    cachedLeafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)
    cachedPartitionSpec = null
  }

  /**
   * List leaf files of given paths. This method will submit a Spark job to do parallel
   * listing whenever there is a path having more files than the parallel partition discovery
   * discovery threshold.
   *
   * This is publicly visible for testing.
   */
  def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = {
    val files =
      if (paths.length >= sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold) {
        ListingFileCatalog.listLeafFilesInParallel(paths, hadoopConf, sparkSession)
      } else {
        ListingFileCatalog.listLeafFilesInSerial(paths, hadoopConf)
      }

    mutable.LinkedHashSet(files: _*)
  }

  override def equals(other: Any): Boolean = other match {
    case hdfs: ListingFileCatalog => paths.toSet == hdfs.paths.toSet
    case _ => false
  }

  override def hashCode(): Int = paths.toSet.hashCode()
}


object ListingFileCatalog extends Logging {

  // `FileStatus` is Writable but not serializable.  What make it worse, somehow it doesn't play
  // well with `SerializableWritable`.  So there seems to be no way to serialize a `FileStatus`.
  // Here we use `SerializableFileStatus` to extract key components of a `FileStatus` to serialize
  // it from executor side and reconstruct it on driver side.
  private case class SerializableBlockLocation(
      names: Array[String],
      hosts: Array[String],
      offset: Long,
      length: Long)

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
   * List a collection of path recursively.
   */
  private def listLeafFilesInSerial(
      paths: Seq[Path],
      hadoopConf: Configuration): Seq[FileStatus] = {
    // Dummy jobconf to get to the pathFilter defined in configuration
    val jobConf = new JobConf(hadoopConf, this.getClass)
    val filter = FileInputFormat.getInputPathFilter(jobConf)

    paths.flatMap { path =>
      logTrace(s"Listing $path")
      val fs = path.getFileSystem(hadoopConf)

      // [SPARK-17599] Prevent ListingFileCatalog from failing if path doesn't exist
      val status: Option[FileStatus] = try Option(fs.getFileStatus(path)) catch {
        case _: FileNotFoundException =>
          logWarning(s"The directory $path was not found. Was it deleted very recently?")
          None
      }

      status.map(listLeafFiles0(fs, _, filter)).getOrElse(Seq.empty)
    }
  }

  /**
   * List a collection of path recursively in parallel (using Spark executors).
   * Each task launched will use [[listLeafFilesInSerial]] to list.
   */
  private def listLeafFilesInParallel(
      paths: Seq[Path],
      hadoopConf: Configuration,
      sparkSession: SparkSession): Seq[FileStatus] = {
    assert(paths.size >= sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold)
    logInfo(s"Listing leaf files and directories in parallel under: ${paths.mkString(", ")}")

    val sparkContext = sparkSession.sparkContext
    val serializableConfiguration = new SerializableConfiguration(hadoopConf)
    val serializedPaths = paths.map(_.toString)

    // Set the number of parallelism to prevent following file listing from generating many tasks
    // in case of large #defaultParallelism.
    val numParallelism = Math.min(paths.size, 10000)

    val statuses = sparkContext
      .parallelize(serializedPaths, numParallelism)
      .mapPartitions { paths =>
        val hadoopConf = serializableConfiguration.value
        listLeafFilesInSerial(paths.map(new Path(_)).toSeq, hadoopConf).iterator
      }.map { status =>
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
      }.collect()

    // Turn SerializableFileStatus back to Status
    statuses.map { f =>
      val blockLocations = f.blockLocations.map { loc =>
        new BlockLocation(loc.names, loc.hosts, loc.offset, loc.length)
      }
      new LocatedFileStatus(
        new FileStatus(
          f.length, f.isDir, f.blockReplication, f.blockSize, f.modificationTime, new Path(f.path)),
        blockLocations)
    }
  }

  /**
   * List a single path, provided as a FileStatus, in serial.
   */
  private def listLeafFiles0(
      fs: FileSystem, status: FileStatus, filter: PathFilter): Seq[FileStatus] = {
    logTrace(s"Listing ${status.getPath}")
    val name = status.getPath.getName.toLowerCase
    if (shouldFilterOut(name)) {
      Seq.empty[FileStatus]
    } else {
      val statuses = {
        val (dirs, files) = fs.listStatus(status.getPath).partition(_.isDirectory)
        val stats = files ++ dirs.flatMap(dir => listLeafFiles0(fs, dir, filter))
        if (filter != null) stats.filter(f => filter.accept(f.getPath)) else stats
      }
      // statuses do not have any dirs.
      statuses.filterNot(status => shouldFilterOut(status.getPath.getName)).map {
        case f: LocatedFileStatus =>
          f

        // NOTE:
        //
        // - Although S3/S3A/S3N file system can be quite slow for remote file metadata
        //   operations, calling `getFileBlockLocations` does no harm here since these file system
        //   implementations don't actually issue RPC for this method.
        //
        // - Here we are calling `getFileBlockLocations` in a sequential manner, but it should not
        //   be a big deal since we always use to `listLeafFilesInParallel` when the number of
        //   paths exceeds threshold.
        case f =>
          // The other constructor of LocatedFileStatus will call FileStatus.getPermission(),
          // which is very slow on some file system (RawLocalFileSystem, which is launch a
          // subprocess and parse the stdout).
          val locations = fs.getFileBlockLocations(f, 0, f.getLen)
          val lfs = new LocatedFileStatus(f.getLen, f.isDirectory, f.getReplication, f.getBlockSize,
            f.getModificationTime, 0, null, null, null, null, f.getPath, locations)
          if (f.isSymlink) {
            lfs.setSymlink(f.getSymlink)
          }
          lfs
      }
    }
  }

  /** Checks if we should filter out this path name. */
  def shouldFilterOut(pathName: String): Boolean = {
    // We filter everything that starts with _ and ., except _common_metadata and _metadata
    // because Parquet needs to find those metadata files from leaf files returned by this method.
    // We should refactor this logic to not mix metadata files with data files.
    ((pathName.startsWith("_") && !pathName.contains("=")) || pathName.startsWith(".")) &&
      !pathName.startsWith("_common_metadata") && !pathName.startsWith("_metadata")
  }
}
