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

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{expressions, CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * ::Experimental::
 * A factory that produces [[OutputWriter]]s.  A new [[OutputWriterFactory]] is created on driver
 * side for each write job issued when writing to a [[HadoopFsRelation]], and then gets serialized
 * to executor side to create actual [[OutputWriter]]s on the fly.
 *
 * @since 1.4.0
 */
@Experimental
abstract class OutputWriterFactory extends Serializable {
  /**
   * When writing to a [[HadoopFsRelation]], this method gets called by each task on executor side
   * to instantiate new [[OutputWriter]]s.
   *
   * @param path Path of the file to which this [[OutputWriter]] is supposed to write.  Note that
   *        this may not point to the final output file.  For example, `FileOutputFormat` writes to
   *        temporary directories and then merge written files back to the final destination.  In
   *        this case, `path` points to a temporary output file under the temporary directory.
   * @param dataSchema Schema of the rows to be written. Partition columns are not included in the
   *        schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   * @since 1.4.0
   */
  def newInstance(
      path: String,
      bucketId: Option[Int], // TODO: This doesn't belong here...
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter
}

/**
 * ::Experimental::
 * [[OutputWriter]] is used together with [[HadoopFsRelation]] for persisting rows to the
 * underlying file system.  Subclasses of [[OutputWriter]] must provide a zero-argument constructor.
 * An [[OutputWriter]] instance is created and initialized when a new output file is opened on
 * executor side.  This instance is used to persist rows to this single output file.
 *
 * @since 1.4.0
 */
@Experimental
abstract class OutputWriter {
  /**
   * Persists a single row.  Invoked on the executor side.  When writing to dynamically partitioned
   * tables, dynamic partition columns are not included in rows to be written.
   *
   * @since 1.4.0
   */
  def write(row: Row): Unit

  /**
   * Closes the [[OutputWriter]]. Invoked on the executor side after all rows are persisted, before
   * the task output is committed.
   *
   * @since 1.4.0
   */
  def close(): Unit

  private var converter: InternalRow => Row = _

  protected[sql] def initConverter(dataSchema: StructType) = {
    converter =
      CatalystTypeConverters.createToScalaConverter(dataSchema).asInstanceOf[InternalRow => Row]
  }

  protected[sql] def writeInternal(row: InternalRow): Unit = {
    write(converter(row))
  }
}

/**
 * Acts as a container for all of the metadata required to read from a datasource. All discovery,
 * resolution and merging logic for schemas and partitions has been removed.
 *
 * @param location A [[FileCatalog]] that can enumerate the locations of all the files that comprise
 *                 this relation.
 * @param partitionSchema The schema of the columns (if any) that are used to partition the relation
 * @param dataSchema The schema of any remaining columns.  Note that if any partition columns are
 *                   present in the actual data files as well, they are preserved.
 * @param bucketSpec Describes the bucketing (hash-partitioning of the files by some column values).
 * @param fileFormat A file format that can be used to read and write the data in files.
 * @param options Configuration used when reading / writing data.
 */
case class HadoopFsRelation(
    sparkSession: SparkSession,
    location: FileCatalog,
    partitionSchema: StructType,
    dataSchema: StructType,
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String]) extends BaseRelation with FileRelation {

  override def sqlContext: SQLContext = sparkSession.wrapped

  val schema: StructType = {
    val dataSchemaColumnNames = dataSchema.map(_.name.toLowerCase).toSet
    StructType(dataSchema ++ partitionSchema.filterNot { column =>
      dataSchemaColumnNames.contains(column.name.toLowerCase)
    })
  }

  def partitionSchemaOption: Option[StructType] =
    if (partitionSchema.isEmpty) None else Some(partitionSchema)
  def partitionSpec: PartitionSpec = location.partitionSpec()

  def refresh(): Unit = location.refresh()

  override def toString: String =
    s"HadoopFiles"

  /** Returns the list of files that will be read when scanning this relation. */
  override def inputFiles: Array[String] =
    location.allFiles().map(_.getPath.toUri.toString).toArray

  override def sizeInBytes: Long = location.allFiles().map(_.getLen).sum
}

/**
 * Used to read and write data stored in files to/from the [[InternalRow]] format.
 */
trait FileFormat {
  /**
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType]

  /**
   * Prepares a read job and returns a potentially updated data source option [[Map]]. This method
   * can be useful for collecting necessary global information for scanning input data.
   */
  def prepareRead(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Map[String, String] = options

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory

  /**
   * Returns whether this format support returning columnar batch or not.
   *
   * TODO: we should just have different traits for the different formats.
   */
  def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = {
    false
  }

  /**
   * Returns a function that can be used to read a single file in as an Iterator of InternalRow.
   *
   * @param dataSchema The global data schema. It can be either specified by the user, or
   *                   reconciled/merged from all underlying data files. If any partition columns
   *                   are contained in the files, they are preserved in this schema.
   * @param partitionSchema The schema of the partition column row that will be present in each
   *                        PartitionedFile. These columns should be appended to the rows that
   *                        are produced by the iterator.
   * @param requiredSchema The schema of the data that should be output for each row.  This may be a
   *                       subset of the columns that are present in the file if column pruning has
   *                       occurred.
   * @param filters A set of filters than can optionally be used to reduce the number of rows output
   * @param options A set of string -> string configuration options.
   * @return
   */
  def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // TODO: Remove this default implementation when the other formats have been ported
    // Until then we guard in [[FileSourceStrategy]] to only call this method on supported formats.
    throw new UnsupportedOperationException(s"buildReader is not supported for $this")
  }
}

/**
 * A collection of data files from a partitioned relation, along with the partition values in the
 * form of an [[InternalRow]].
 */
case class Partition(values: InternalRow, files: Seq[FileStatus])

/**
 * An interface for objects capable of enumerating the files that comprise a relation as well
 * as the partitioning characteristics of those files.
 */
trait FileCatalog {
  def paths: Seq[Path]

  def partitionSpec(): PartitionSpec

  /**
   * Returns all valid files grouped into partitions when the data is partitioned. If the data is
   * unpartitioned, this will return a single partition with not partition values.
   *
   * @param filters the filters used to prune which partitions are returned.  These filters must
   *                only refer to partition columns and this method will only return files
   *                where these predicates are guaranteed to evaluate to `true`.  Thus, these
   *                filters will not need to be evaluated again on the returned data.
   */
  def listFiles(filters: Seq[Expression]): Seq[Partition]

  def allFiles(): Seq[FileStatus]

  def getStatus(path: Path): Array[FileStatus]

  def refresh(): Unit
}

/**
 * A file catalog that caches metadata gathered by scanning all the files present in `paths`
 * recursively.
 *
 * @param parameters as set of options to control discovery
 * @param paths a list of paths to scan
 * @param partitionSchema an optional partition schema that will be use to provide types for the
 *                        discovered partitions
 */
class HDFSFileCatalog(
    sparkSession: SparkSession,
    parameters: Map[String, String],
    override val paths: Seq[Path],
    partitionSchema: Option[StructType])
  extends FileCatalog with Logging {

  private val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(parameters)

  var leafFiles = mutable.LinkedHashMap.empty[Path, FileStatus]
  var leafDirToChildrenFiles = mutable.Map.empty[Path, Array[FileStatus]]
  var cachedPartitionSpec: PartitionSpec = _

  def partitionSpec(): PartitionSpec = {
    if (cachedPartitionSpec == null) {
      cachedPartitionSpec = inferPartitioning(partitionSchema)
    }

    cachedPartitionSpec
  }

  refresh()

  override def listFiles(filters: Seq[Expression]): Seq[Partition] = {
    if (partitionSpec().partitionColumns.isEmpty) {
      Partition(InternalRow.empty, allFiles().filterNot(_.getPath.getName startsWith "_")) :: Nil
    } else {
      prunePartitions(filters, partitionSpec()).map {
        case PartitionDirectory(values, path) =>
          Partition(
            values,
            getStatus(path).filterNot(_.getPath.getName startsWith "_"))
      }
    }
  }

  protected def prunePartitions(
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

  def allFiles(): Seq[FileStatus] = leafFiles.values.toSeq

  def getStatus(path: Path): Array[FileStatus] = leafDirToChildrenFiles(path)

  private def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = {
    if (paths.length >= sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold) {
      HadoopFsRelation.listLeafFilesInParallel(paths, hadoopConf, sparkSession.sparkContext)
    } else {
      val statuses: Seq[FileStatus] = paths.flatMap { path =>
        val fs = path.getFileSystem(hadoopConf)
        logInfo(s"Listing $path on driver")
        // Dummy jobconf to get to the pathFilter defined in configuration
        val jobConf = new JobConf(hadoopConf, this.getClass)
        val pathFilter = FileInputFormat.getInputPathFilter(jobConf)

        val statuses = {
          val stats = Try(fs.listStatus(path)).getOrElse(Array.empty[FileStatus])
          if (pathFilter != null) stats.filter(f => pathFilter.accept(f.getPath)) else stats
        }

        statuses.map {
          case f: LocatedFileStatus => f

          // NOTE:
          //
          // - Although S3/S3A/S3N file system can be quite slow for remote file metadata
          //   operations, calling `getFileBlockLocations` does no harm here since these file system
          //   implementations don't actually issue RPC for this method.
          //
          // - Here we are calling `getFileBlockLocations` in a sequential manner, but it should a
          //   a big deal since we always use to `listLeafFilesInParallel` when the number of paths
          //   exceeds threshold.
          case f => new LocatedFileStatus(f, fs.getFileBlockLocations(f, 0, f.getLen))
        }
      }.filterNot { status =>
        val name = status.getPath.getName
        HadoopFsRelation.shouldFilterOut(name)
      }

      val (dirs, files) = statuses.partition(_.isDirectory)

      // It uses [[LinkedHashSet]] since the order of files can affect the results. (SPARK-11500)
      if (dirs.isEmpty) {
        mutable.LinkedHashSet(files: _*)
      } else {
        mutable.LinkedHashSet(files: _*) ++ listLeafFiles(dirs.map(_.getPath))
      }
    }
  }

  def inferPartitioning(schema: Option[StructType]): PartitionSpec = {
    // We use leaf dirs containing data files to discover the schema.
    val leafDirs = leafDirToChildrenFiles.keys.toSeq
    schema match {
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
          typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled(),
          basePaths = basePaths)
    }
  }

  /**
   * Contains a set of paths that are considered as the base dirs of the input datasets.
   * The partitioning discovery logic will make sure it will stop when it reaches any
   * base path. By default, the paths of the dataset provided by users will be base paths.
   * For example, if a user uses `sqlContext.read.parquet("/path/something=true/")`, the base path
   * will be `/path/something=true/`, and the returned DataFrame will not contain a column of
   * `something`. If users want to override the basePath. They can set `basePath` in the options
   * to pass the new base path to the data source.
   * For the above example, if the user-provided base path is `/path/`, the returned
   * DataFrame will have the column of `something`.
   */
  private def basePaths: Set[Path] = {
    val userDefinedBasePath = parameters.get("basePath").map(basePath => Set(new Path(basePath)))
    userDefinedBasePath.getOrElse {
      // If the user does not provide basePath, we will just use paths.
      paths.toSet
    }.map { hdfsPath =>
      // Make the path qualified (consistent with listLeafFiles and listLeafFilesInParallel).
      val fs = hdfsPath.getFileSystem(hadoopConf)
      hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    }
  }

  def refresh(): Unit = {
    val files = listLeafFiles(paths)

    leafFiles.clear()
    leafDirToChildrenFiles.clear()

    leafFiles ++= files.map(f => f.getPath -> f)
    leafDirToChildrenFiles ++= files.toArray.groupBy(_.getPath.getParent)

    cachedPartitionSpec = null
  }

  override def equals(other: Any): Boolean = other match {
    case hdfs: HDFSFileCatalog => paths.toSet == hdfs.paths.toSet
    case _ => false
  }

  override def hashCode(): Int = paths.toSet.hashCode()
}

/**
 * Helper methods for gathering metadata from HDFS.
 */
private[sql] object HadoopFsRelation extends Logging {

  /** Checks if we should filter out this path name. */
  def shouldFilterOut(pathName: String): Boolean = {
    // TODO: We should try to filter out all files/dirs starting with "." or "_".
    // The only reason that we are not doing it now is that Parquet needs to find those
    // metadata files from leaf files returned by this methods. We should refactor
    // this logic to not mix metadata files with data files.
    pathName == "_SUCCESS" || pathName == "_temporary" || pathName.startsWith(".")
  }

  // We don't filter files/directories whose name start with "_" except "_temporary" here, as
  // specific data sources may take advantages over them (e.g. Parquet _metadata and
  // _common_metadata files). "_temporary" directories are explicitly ignored since failed
  // tasks/jobs may leave partial/corrupted data files there.  Files and directories whose name
  // start with "." are also ignored.
  def listLeafFiles(fs: FileSystem, status: FileStatus): Array[FileStatus] = {
    logInfo(s"Listing ${status.getPath}")
    val name = status.getPath.getName.toLowerCase
    if (shouldFilterOut(name)) {
      Array.empty
    } else {
      // Dummy jobconf to get to the pathFilter defined in configuration
      val jobConf = new JobConf(fs.getConf, this.getClass())
      val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
      val statuses = {
        val (dirs, files) = fs.listStatus(status.getPath).partition(_.isDirectory)
        val stats = files ++ dirs.flatMap(dir => listLeafFiles(fs, dir))
        if (pathFilter != null) stats.filter(f => pathFilter.accept(f.getPath)) else stats
      }
      statuses.filterNot(status => shouldFilterOut(status.getPath.getName)).map {
        case f: LocatedFileStatus => f
        case f => new LocatedFileStatus(f, fs.getFileBlockLocations(f, 0, f.getLen))
      }
    }
  }

  // `FileStatus` is Writable but not serializable.  What make it worse, somehow it doesn't play
  // well with `SerializableWritable`.  So there seems to be no way to serialize a `FileStatus`.
  // Here we use `FakeFileStatus` to extract key components of a `FileStatus` to serialize it from
  // executor side and reconstruct it on driver side.
  case class FakeBlockLocation(
      names: Array[String],
      hosts: Array[String],
      offset: Long,
      length: Long)

  case class FakeFileStatus(
      path: String,
      length: Long,
      isDir: Boolean,
      blockReplication: Short,
      blockSize: Long,
      modificationTime: Long,
      accessTime: Long,
      blockLocations: Array[FakeBlockLocation])

  def listLeafFilesInParallel(
      paths: Seq[Path],
      hadoopConf: Configuration,
      sparkContext: SparkContext): mutable.LinkedHashSet[FileStatus] = {
    logInfo(s"Listing leaf files and directories in parallel under: ${paths.mkString(", ")}")

    val serializableConfiguration = new SerializableConfiguration(hadoopConf)
    val serializedPaths = paths.map(_.toString)

    val fakeStatuses = sparkContext.parallelize(serializedPaths).map(new Path(_)).flatMap { path =>
      val fs = path.getFileSystem(serializableConfiguration.value)
      Try(listLeafFiles(fs, fs.getFileStatus(path))).getOrElse(Array.empty)
    }.map { status =>
      val blockLocations = status match {
        case f: LocatedFileStatus =>
          f.getBlockLocations.map { loc =>
            FakeBlockLocation(
              loc.getNames,
              loc.getHosts,
              loc.getOffset,
              loc.getLength)
          }

        case _ =>
          Array.empty[FakeBlockLocation]
      }

      FakeFileStatus(
        status.getPath.toString,
        status.getLen,
        status.isDirectory,
        status.getReplication,
        status.getBlockSize,
        status.getModificationTime,
        status.getAccessTime,
        blockLocations)
    }.collect()

    val hadoopFakeStatuses = fakeStatuses.map { f =>
      val blockLocations = f.blockLocations.map { loc =>
        new BlockLocation(loc.names, loc.hosts, loc.offset, loc.length)
      }
      new LocatedFileStatus(
        new FileStatus(
          f.length, f.isDir, f.blockReplication, f.blockSize, f.modificationTime, new Path(f.path)),
        blockLocations)
    }
    mutable.LinkedHashSet(hadoopFakeStatuses: _*)
  }
}
