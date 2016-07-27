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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
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

  /**
   * Returns a new instance of [[OutputWriter]] that will write data to the given path.
   * This method gets called by each task on executor to write [[InternalRow]]s to
   * format-specific files. Compared to the other `newInstance()`, this is a newer API that
   * passes only the path that the writer must write to. The writer must write to the exact path
   * and not modify it (do not add subdirectories, extensions, etc.). All other
   * file-format-specific information needed to create the writer must be passed
   * through the [[OutputWriterFactory]] implementation.
   * @since 2.0.0
   */
  def newWriter(path: String): OutputWriter = {
    throw new UnsupportedOperationException("newInstance with just path not supported")
  }
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
    location: FileCatalog,
    partitionSchema: StructType,
    dataSchema: StructType,
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String])(val sparkSession: SparkSession)
  extends BaseRelation with FileRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

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

  override def toString: String = {
    fileFormat match {
      case source: DataSourceRegister => source.shortName()
      case _ => "HadoopFiles"
    }
  }

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
   * Returns a [[OutputWriterFactory]] for generating output writers that can write data.
   * This method is current used only by FileStreamSinkWriter to generate output writers that
   * does not use output committers to write data. The OutputWriter generated by the returned
   * [[OutputWriterFactory]] must implement the method `newWriter(path)`..
   */
  def buildWriter(
      sqlContext: SQLContext,
      dataSchema: StructType,
      options: Map[String, String]): OutputWriterFactory = {
    // TODO: Remove this default implementation when the other formats have been ported
    throw new UnsupportedOperationException(s"buildWriter is not supported for $this")
  }

  /**
   * Returns whether this format support returning columnar batch or not.
   *
   * TODO: we should just have different traits for the different formats.
   */
  def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = {
    false
  }

  /**
   * Allow FileFormats to have a pluggable way to utilize pushed filters to eliminate partitions
   * before execution. By default no pruning is performed and the original partitioning is
   * preserved.
   */
  def filterPartitions(
      filters: Seq[Filter],
      schema: StructType,
      conf: Configuration,
      allFiles: Seq[FileStatus],
      root: Path,
      partitions: Seq[Partition]): Seq[Partition] = {
    partitions
  }

  /**
   * Returns whether a file with `path` could be splitted or not.
   */
  def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
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

  /**
   * Exactly the same as [[buildReader]] except that the reader function returned by this method
   * appends partition values to [[InternalRow]]s produced by the reader function [[buildReader]]
   * returns.
   */
  def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val dataReader = buildReader(
      sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)

    new (PartitionedFile => Iterator[InternalRow]) with Serializable {
      private val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes

      private val joinedRow = new JoinedRow()

      // Using lazy val to avoid serialization
      private lazy val appendPartitionColumns =
        GenerateUnsafeProjection.generate(fullSchema, fullSchema)

      override def apply(file: PartitionedFile): Iterator[InternalRow] = {
        // Using local val to avoid per-row lazy val check (pre-mature optimization?...)
        val converter = appendPartitionColumns

        // Note that we have to apply the converter even though `file.partitionValues` is empty.
        // This is because the converter is also responsible for converting safe `InternalRow`s into
        // `UnsafeRow`s.
        dataReader(file).map { dataRow =>
          converter(joinedRow(dataRow, file.partitionValues))
        }
      }
    }
  }

}

/**
 * The base class file format that is based on text file.
 */
abstract class TextBasedFileFormat extends FileFormat {
  private var codecFactory: CompressionCodecFactory = null
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    if (codecFactory == null) {
      codecFactory = new CompressionCodecFactory(
        sparkSession.sessionState.newHadoopConfWithOptions(options))
    }
    val codec = codecFactory.getCodec(path)
    codec == null || codec.isInstanceOf[SplittableCompressionCodec]
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

  /** Returns the list of input paths from which the catalog will get files. */
  def paths: Seq[Path]

  /** Returns the specification of the partitions inferred from the data. */
  def partitionSpec(): PartitionSpec

  /**
   * Returns all valid files grouped into partitions when the data is partitioned. If the data is
   * unpartitioned, this will return a single partition with no partition values.
   *
   * @param filters The filters used to prune which partitions are returned.  These filters must
   *                only refer to partition columns and this method will only return files
   *                where these predicates are guaranteed to evaluate to `true`.  Thus, these
   *                filters will not need to be evaluated again on the returned data.
   */
  def listFiles(filters: Seq[Expression]): Seq[Partition]

  /** Returns all the valid files. */
  def allFiles(): Seq[FileStatus]

  /** Refresh the file listing */
  def refresh(): Unit
}


/**
 * Helper methods for gathering metadata from HDFS.
 */
object HadoopFsRelation extends Logging {

  /** Checks if we should filter out this path name. */
  def shouldFilterOut(pathName: String): Boolean = {
    // We filter everything that starts with _ and ., except _common_metadata and _metadata
    // because Parquet needs to find those metadata files from leaf files returned by this method.
    // We should refactor this logic to not mix metadata files with data files.
    ((pathName.startsWith("_") && !pathName.contains("=")) || pathName.startsWith(".")) &&
      !pathName.startsWith("_common_metadata") && !pathName.startsWith("_metadata")
  }

  /**
   * Create a LocatedFileStatus using FileStatus and block locations.
   */
  def createLocatedFileStatus(f: FileStatus, locations: Array[BlockLocation]): LocatedFileStatus = {
    // The other constructor of LocatedFileStatus will call FileStatus.getPermission(), which is
    // very slow on some file system (RawLocalFileSystem, which is launch a subprocess and parse the
    // stdout).
    val lfs = new LocatedFileStatus(f.getLen, f.isDirectory, f.getReplication, f.getBlockSize,
      f.getModificationTime, 0, null, null, null, null, f.getPath, locations)
    if (f.isSymlink) {
      lfs.setSymlink(f.getSymlink)
    }
    lfs
  }

  // We don't filter files/directories whose name start with "_" except "_temporary" here, as
  // specific data sources may take advantages over them (e.g. Parquet _metadata and
  // _common_metadata files). "_temporary" directories are explicitly ignored since failed
  // tasks/jobs may leave partial/corrupted data files there.  Files and directories whose name
  // start with "." are also ignored.
  def listLeafFiles(fs: FileSystem, status: FileStatus, filter: PathFilter): Array[FileStatus] = {
    logTrace(s"Listing ${status.getPath}")
    val name = status.getPath.getName.toLowerCase
    if (shouldFilterOut(name)) {
      Array.empty[FileStatus]
    } else {
      val statuses = {
        val (dirs, files) = fs.listStatus(status.getPath).partition(_.isDirectory)
        val stats = files ++ dirs.flatMap(dir => listLeafFiles(fs, dir, filter))
        if (filter != null) stats.filter(f => filter.accept(f.getPath)) else stats
      }
      // statuses do not have any dirs.
      statuses.filterNot(status => shouldFilterOut(status.getPath.getName)).map {
        case f: LocatedFileStatus => f

        // NOTE:
        //
        // - Although S3/S3A/S3N file system can be quite slow for remote file metadata
        //   operations, calling `getFileBlockLocations` does no harm here since these file system
        //   implementations don't actually issue RPC for this method.
        //
        // - Here we are calling `getFileBlockLocations` in a sequential manner, but it should not
        //   be a big deal since we always use to `listLeafFilesInParallel` when the number of
        //   paths exceeds threshold.
        case f => createLocatedFileStatus(f, fs.getFileBlockLocations(f, 0, f.getLen))
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
      sparkSession: SparkSession): mutable.LinkedHashSet[FileStatus] = {
    assert(paths.size >= sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold)
    logInfo(s"Listing leaf files and directories in parallel under: ${paths.mkString(", ")}")

    val sparkContext = sparkSession.sparkContext
    val serializableConfiguration = new SerializableConfiguration(hadoopConf)
    val serializedPaths = paths.map(_.toString)

    // Set the number of parallelism to prevent following file listing from generating many tasks
    // in case of large #defaultParallelism.
    val numParallelism = Math.min(paths.size, 10000)

    val fakeStatuses = sparkContext
        .parallelize(serializedPaths, numParallelism)
        .mapPartitions { paths =>
      // Dummy jobconf to get to the pathFilter defined in configuration
      // It's very expensive to create a JobConf(ClassUtil.findContainingJar() is slow)
      val jobConf = new JobConf(serializableConfiguration.value, this.getClass)
      val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
      paths.map(new Path(_)).flatMap { path =>
        val fs = path.getFileSystem(serializableConfiguration.value)
        listLeafFiles(fs, fs.getFileStatus(path), pathFilter)
      }
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
