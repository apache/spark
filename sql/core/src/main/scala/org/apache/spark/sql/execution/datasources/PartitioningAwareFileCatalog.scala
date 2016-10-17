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
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.SerializableConfiguration


/**
 * An abstract class that represents [[FileCatalog]]s that are aware of partitioned tables.
 * It provides the necessary methods to parse partition data based on a set of files.
 *
 * @param parameters as set of options to control partition discovery
 * @param partitionSchema an optional partition schema that will be use to provide types for the
 *                        discovered partitions
*/
abstract class PartitioningAwareFileCatalog(
    sparkSession: SparkSession,
    parameters: Map[String, String],
    partitionSchema: Option[StructType]) extends FileCatalog with Logging {
  import PartitioningAwareFileCatalog.BASE_PATH_PARAM

  /** Returns the specification of the partitions inferred from the data. */
  def partitionSpec(): PartitionSpec

  protected val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(parameters)

  protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus]

  protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]]

  override def listFiles(filters: Seq[Expression]): Seq[PartitionFiles] = {
    val selectedPartitions = if (partitionSpec().partitionColumns.isEmpty) {
      PartitionFiles(InternalRow.empty, allFiles().filter(f => isDataPath(f.getPath))) :: Nil
    } else {
      prunePartitions(filters, partitionSpec()).map {
        case PartitionDirectory(values, path) =>
          val files: Seq[FileStatus] = leafDirToChildrenFiles.get(path) match {
            case Some(existingDir) =>
              // Directory has children files in it, return them
              existingDir.filter(f => isDataPath(f.getPath))

            case None =>
              // Directory does not exist, or has no children files
              Nil
          }
          PartitionFiles(values, files)
      }
    }
    logTrace("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))
    selectedPartitions
  }

  /** Returns the list of files that will be read when scanning this relation. */
  override def inputFiles: Array[String] =
    allFiles().map(_.getPath.toUri.toString).toArray

  override def sizeInBytes: Long = allFiles().map(_.getLen).sum

  def allFiles(): Seq[FileStatus] = {
    if (partitionSpec().partitionColumns.isEmpty) {
      // For each of the root input paths, get the list of files inside them
      rootPaths.flatMap { path =>
        // Make the path qualified (consistent with listLeafFiles and listLeafFilesInParallel).
        val fs = path.getFileSystem(hadoopConf)
        val qualifiedPathPre = fs.makeQualified(path)
        val qualifiedPath: Path = if (qualifiedPathPre.isRoot && !qualifiedPathPre.isAbsolute) {
          // SPARK-17613: Always append `Path.SEPARATOR` to the end of parent directories,
          // because the `leafFile.getParent` would have returned an absolute path with the
          // separator at the end.
          new Path(qualifiedPathPre, Path.SEPARATOR)
        } else {
          qualifiedPathPre
        }

        // There are three cases possible with each path
        // 1. The path is a directory and has children files in it. Then it must be present in
        //    leafDirToChildrenFiles as those children files will have been found as leaf files.
        //    Find its children files from leafDirToChildrenFiles and include them.
        // 2. The path is a file, then it will be present in leafFiles. Include this path.
        // 3. The path is a directory, but has no children files. Do not include this path.

        leafDirToChildrenFiles.get(qualifiedPath)
          .orElse { leafFiles.get(qualifiedPath).map(Array(_)) }
          .getOrElse(Array.empty)
      }
    } else {
      leafFiles.values.toSeq
    }
  }

  protected def inferPartitioning(): PartitionSpec = {
    // We use leaf dirs containing data files to discover the schema.
    val leafDirs = leafDirToChildrenFiles.filter { case (_, files) =>
      files.exists(f => isDataPath(f.getPath))
    }.keys.toSeq
    partitionSchema match {
      case Some(userProvidedSchema) if userProvidedSchema.nonEmpty =>
        val spec = PartitioningUtils.parsePartitions(
          leafDirs,
          PartitioningUtils.DEFAULT_PARTITION_NAME,
          typeInference = false,
          basePaths = basePaths)

        // Without auto inference, all of value in the `row` should be null or in StringType,
        // we need to cast into the data type that user specified.
        def castPartitionValuesToUserSchema(row: InternalRow) = {
          InternalRow((0 until row.numFields).map { i =>
            Cast(
              Literal.create(row.getUTF8String(i), StringType),
              userProvidedSchema.fields(i).dataType).eval()
          }: _*)
        }

        PartitionSpec(userProvidedSchema, spec.partitions.map { part =>
          part.copy(values = castPartitionValuesToUserSchema(part.values))
        })
      case _ =>
        PartitioningUtils.parsePartitions(
          leafDirs,
          PartitioningUtils.DEFAULT_PARTITION_NAME,
          typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled,
          basePaths = basePaths)
    }
  }

  private def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[PartitionDirectory] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionDirectory(values, _) => boundPredicate(values)
      }
      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, pruned $percentPruned% partitions."
      }

      selected
    } else {
      partitions
    }
  }

  /**
   * Contains a set of paths that are considered as the base dirs of the input datasets.
   * The partitioning discovery logic will make sure it will stop when it reaches any
   * base path.
   *
   * By default, the paths of the dataset provided by users will be base paths.
   * Below are three typical examples,
   * Case 1) `spark.read.parquet("/path/something=true/")`: the base path will be
   * `/path/something=true/`, and the returned DataFrame will not contain a column of `something`.
   * Case 2) `spark.read.parquet("/path/something=true/a.parquet")`: the base path will be
   * still `/path/something=true/`, and the returned DataFrame will also not contain a column of
   * `something`.
   * Case 3) `spark.read.parquet("/path/")`: the base path will be `/path/`, and the returned
   * DataFrame will have the column of `something`.
   *
   * Users also can override the basePath by setting `basePath` in the options to pass the new base
   * path to the data source.
   * For example, `spark.read.option("basePath", "/path/").parquet("/path/something=true/")`,
   * and the returned DataFrame will have the column of `something`.
   */
  private def basePaths: Set[Path] = {
    parameters.get(BASE_PATH_PARAM).map(new Path(_)) match {
      case Some(userDefinedBasePath) =>
        val fs = userDefinedBasePath.getFileSystem(hadoopConf)
        if (!fs.isDirectory(userDefinedBasePath)) {
          throw new IllegalArgumentException(s"Option '$BASE_PATH_PARAM' must be a directory")
        }
        Set(fs.makeQualified(userDefinedBasePath))

      case None =>
        rootPaths.map { path =>
          // Make the path qualified (consistent with listLeafFiles and listLeafFilesInParallel).
          val qualifiedPath = path.getFileSystem(hadoopConf).makeQualified(path)
          if (leafFiles.contains(qualifiedPath)) qualifiedPath.getParent else qualifiedPath }.toSet
    }
  }

  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
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
        PartitioningAwareFileCatalog.listLeafFilesInParallel(paths, hadoopConf, sparkSession)
      } else {
        PartitioningAwareFileCatalog.listLeafFilesInSerial(paths, hadoopConf)
      }

    HiveCatalogMetrics.incrementFilesDiscovered(files.size)
    mutable.LinkedHashSet(files: _*)
  }
}

object PartitioningAwareFileCatalog extends Logging {
  val BASE_PATH_PARAM = "basePath"

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
