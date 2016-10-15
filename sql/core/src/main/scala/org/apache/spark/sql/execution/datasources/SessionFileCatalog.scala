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
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration


/**
 * A base class for [[BasicFileCatalog]]s that need a [[SparkSession]] and the ability to find leaf
 * files in a list of HDFS paths.
 *
 * @param sparkSession a [[SparkSession]]
 * @param ignoreFileNotFound (see [[ListingFileCatalog]])
 */
abstract class SessionFileCatalog(sparkSession: SparkSession)
    extends BasicFileCatalog with Logging {
  protected val hadoopConf: Configuration

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
        SessionFileCatalog.listLeafFilesInParallel(paths, hadoopConf, sparkSession)
      } else {
        SessionFileCatalog.listLeafFilesInSerial(paths, hadoopConf)
      }

    HiveCatalogMetrics.incrementFilesDiscovered(files.size)
    mutable.LinkedHashSet(files: _*)
  }
}

object SessionFileCatalog extends Logging {

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
   * List a collection of path recursively.
   */
  private def listLeafFilesInSerial(
      paths: Seq[Path],
      hadoopConf: Configuration): Seq[FileStatus] = {
    // Dummy jobconf to get to the pathFilter defined in configuration
    val jobConf = new JobConf(hadoopConf, this.getClass)
    val filter = FileInputFormat.getInputPathFilter(jobConf)

    paths.flatMap { path =>
      val fs = path.getFileSystem(hadoopConf)
      listLeafFiles0(fs, path, filter)
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
      fs: FileSystem, path: Path, filter: PathFilter): Seq[FileStatus] = {
    logTrace(s"Listing $path")
    val name = path.getName.toLowerCase
    if (shouldFilterOut(name)) {
      Seq.empty[FileStatus]
    } else {
      // [SPARK-17599] Prevent ListingFileCatalog from failing if path doesn't exist
      // Note that statuses only include FileStatus for the files and dirs directly under path,
      // and does not include anything else recursively.
      val statuses = try fs.listStatus(path) catch {
        case _: FileNotFoundException =>
          logWarning(s"The directory $path was not found. Was it deleted very recently?")
          Array.empty[FileStatus]
      }

      val allLeafStatuses = {
        val (dirs, files) = statuses.partition(_.isDirectory)
        val stats = files ++ dirs.flatMap(dir => listLeafFiles0(fs, dir.getPath, filter))
        if (filter != null) stats.filter(f => filter.accept(f.getPath)) else stats
      }

      allLeafStatuses.filterNot(status => shouldFilterOut(status.getPath.getName)).map {
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
