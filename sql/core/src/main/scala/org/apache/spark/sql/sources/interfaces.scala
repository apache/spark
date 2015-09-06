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

package org.apache.spark.sql.sources

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.execution.{FileRelation, RDDConversions}
import org.apache.spark.sql.execution.datasources.{PartitioningUtils, PartitionSpec, Partition}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.util.SerializableConfiguration

/**
 * ::DeveloperApi::
 * Data sources should implement this trait so that they can register an alias to their data source.
 * This allows users to give the data source alias as the format type over the fully qualified
 * class name.
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 *
 * @since 1.5.0
 */
@DeveloperApi
trait DataSourceRegister {

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def format(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  def shortName(): String
}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data source.  When
 * Spark SQL is given a DDL operation with a USING clause specified (to specify the implemented
 * RelationProvider), this interface is used to pass in the parameters specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 *
 * @since 1.3.0
 */
@DeveloperApi
trait RelationProvider {
  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data source
 * with a given schema.  When Spark SQL is given a DDL operation with a USING clause specified (
 * to specify the implemented SchemaRelationProvider) and a user defined schema, this interface
 * is used to pass in the parameters specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 *
 * The difference between a [[RelationProvider]] and a [[SchemaRelationProvider]] is that
 * users need to provide a schema when using a [[SchemaRelationProvider]].
 * A relation provider can inherits both [[RelationProvider]] and [[SchemaRelationProvider]]
 * if it can support both schema inference and user-specified schemas.
 *
 * @since 1.3.0
 */
@DeveloperApi
trait SchemaRelationProvider {
  /**
   * Returns a new base relation with the given parameters and user defined schema.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation
}

/**
 * ::Experimental::
 * Implemented by objects that produce relations for a specific kind of data source
 * with a given schema and partitioned columns.  When Spark SQL is given a DDL operation with a
 * USING clause specified (to specify the implemented [[HadoopFsRelationProvider]]), a user defined
 * schema, and an optional list of partition columns, this interface is used to pass in the
 * parameters specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 *
 * The difference between a [[RelationProvider]] and a [[HadoopFsRelationProvider]] is
 * that users need to provide a schema and a (possibly empty) list of partition columns when
 * using a [[HadoopFsRelationProvider]]. A relation provider can inherits both [[RelationProvider]],
 * and [[HadoopFsRelationProvider]] if it can support schema inference, user-specified
 * schemas, and accessing partitioned relations.
 *
 * @since 1.4.0
 */
@Experimental
trait HadoopFsRelationProvider {
  /**
   * Returns a new base relation with the given parameters, a user defined schema, and a list of
   * partition columns. Note: the parameters' keywords are case insensitive and this insensitivity
   * is enforced by the Map that is passed to the function.
   *
   * @param dataSchema Schema of data columns (i.e., columns that are not partition columns).
   */
  def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation
}

/**
 * @since 1.3.0
 */
@DeveloperApi
trait CreatableRelationProvider {
  /**
    * Creates a relation with the given parameters based on the contents of the given
    * DataFrame. The mode specifies the expected behavior of createRelation when
    * data already exists.
    * Right now, there are three modes, Append, Overwrite, and ErrorIfExists.
    * Append mode means that when saving a DataFrame to a data source, if data already exists,
    * contents of the DataFrame are expected to be appended to existing data.
    * Overwrite mode means that when saving a DataFrame to a data source, if data already exists,
    * existing data is expected to be overwritten by the contents of the DataFrame.
    * ErrorIfExists mode means that when saving a DataFrame to a data source,
    * if data already exists, an exception is expected to be thrown.
     *
     * @since 1.3.0
    */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation
}

/**
 * ::DeveloperApi::
 * Represents a collection of tuples with a known schema. Classes that extend BaseRelation must
 * be able to produce the schema of their data in the form of a [[StructType]]. Concrete
 * implementation should inherit from one of the descendant `Scan` classes, which define various
 * abstract methods for execution.
 *
 * BaseRelations must also define a equality function that only returns true when the two
 * instances will return the same data. This equality function is used when determining when
 * it is safe to substitute cached results for a given relation.
 *
 * @since 1.3.0
 */
@DeveloperApi
abstract class BaseRelation {
  def sqlContext: SQLContext
  def schema: StructType

  /**
   * Returns an estimated size of this relation in bytes. This information is used by the planner
   * to decided when it is safe to broadcast a relation and can be overridden by sources that
   * know the size ahead of time. By default, the system will assume that tables are too
   * large to broadcast. This method will be called multiple times during query planning
   * and thus should not perform expensive operations for each invocation.
   *
   * Note that it is always better to overestimate size than underestimate, because underestimation
   * could lead to execution plans that are suboptimal (i.e. broadcasting a very large table).
   *
   * @since 1.3.0
   */
  def sizeInBytes: Long = sqlContext.conf.defaultSizeInBytes

  /**
   * Whether does it need to convert the objects in Row to internal representation, for example:
   *  java.lang.String -> UTF8String
   *  java.lang.Decimal -> Decimal
   *
   * If `needConversion` is `false`, buildScan() should return an [[RDD]] of [[InternalRow]]
   *
   * Note: The internal representation is not stable across releases and thus data sources outside
   * of Spark SQL should leave this as true.
   *
   * @since 1.4.0
   */
  def needConversion: Boolean = true
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can produce all of its tuples as an RDD of Row objects.
 *
 * @since 1.3.0
 */
@DeveloperApi
trait TableScan {
  def buildScan(): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can eliminate unneeded columns before producing an RDD
 * containing all of its tuples as Row objects.
 *
 * @since 1.3.0
 */
@DeveloperApi
trait PrunedScan {
  def buildScan(requiredColumns: Array[String]): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can eliminate unneeded columns and filter using selected
 * predicates before producing an RDD containing all matching tuples as Row objects.
 *
 * The actual filter should be the conjunction of all `filters`,
 * i.e. they should be "and" together.
 *
 * The pushed down filters are currently purely an optimization as they will all be evaluated
 * again.  This means it is safe to use them with methods that produce false positives such
 * as filtering partitions based on a bloom filter.
 *
 * @since 1.3.0
 */
@DeveloperApi
trait PrunedFilteredScan {
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can be used to insert data into it through the insert method.
 * If overwrite in insert method is true, the old data in the relation should be overwritten with
 * the new data. If overwrite in insert method is false, the new data should be appended.
 *
 * InsertableRelation has the following three assumptions.
 * 1. It assumes that the data (Rows in the DataFrame) provided to the insert method
 * exactly matches the ordinal of fields in the schema of the BaseRelation.
 * 2. It assumes that the schema of this relation will not be changed.
 * Even if the insert method updates the schema (e.g. a relation of JSON or Parquet data may have a
 * schema update after an insert operation), the new schema will not be used.
 * 3. It assumes that fields of the data provided in the insert method are nullable.
 * If a data source needs to check the actual nullability of a field, it needs to do it in the
 * insert method.
 *
 * @since 1.3.0
 */
@DeveloperApi
trait InsertableRelation {
  def insert(data: DataFrame, overwrite: Boolean): Unit
}

/**
 * ::Experimental::
 * An interface for experimenting with a more direct connection to the query planner.  Compared to
 * [[PrunedFilteredScan]], this operator receives the raw expressions from the
 * [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]].  Unlike the other APIs this
 * interface is NOT designed to be binary compatible across releases and thus should only be used
 * for experimentation.
 *
 * @since 1.3.0
 */
@Experimental
trait CatalystScan {
  def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row]
}

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
   *
   * @since 1.4.0
   */
  def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter
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
 * ::Experimental::
 * A [[BaseRelation]] that provides much of the common code required for formats that store their
 * data to an HDFS compatible filesystem.
 *
 * For the read path, similar to [[PrunedFilteredScan]], it can eliminate unneeded columns and
 * filter using selected predicates before producing an RDD containing all matching tuples as
 * [[Row]] objects. In addition, when reading from Hive style partitioned tables stored in file
 * systems, it's able to discover partitioning information from the paths of input directories, and
 * perform partition pruning before start reading the data. Subclasses of [[HadoopFsRelation()]]
 * must override one of the three `buildScan` methods to implement the read path.
 *
 * For the write path, it provides the ability to write to both non-partitioned and partitioned
 * tables.  Directory layout of the partitioned tables is compatible with Hive.
 *
 * @constructor This constructor is for internal uses only. The [[PartitionSpec]] argument is for
 *              implementing metastore table conversion.
 *
 * @param maybePartitionSpec An [[HadoopFsRelation]] can be created with an optional
 *        [[PartitionSpec]], so that partition discovery can be skipped.
 *
 * @since 1.4.0
 */
@Experimental
abstract class HadoopFsRelation private[sql](maybePartitionSpec: Option[PartitionSpec])
  extends BaseRelation with FileRelation with Logging {

  override def toString: String = getClass.getSimpleName + paths.mkString("[", ",", "]")

  def this() = this(None)

  private val hadoopConf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

  private val codegenEnabled = sqlContext.conf.codegenEnabled

  private var _partitionSpec: PartitionSpec = _

  private class FileStatusCache {
    var leafFiles = mutable.Map.empty[Path, FileStatus]

    var leafDirToChildrenFiles = mutable.Map.empty[Path, Array[FileStatus]]

    private def listLeafFiles(paths: Array[String]): Set[FileStatus] = {
      if (paths.length >= sqlContext.conf.parallelPartitionDiscoveryThreshold) {
        HadoopFsRelation.listLeafFilesInParallel(paths, hadoopConf, sqlContext.sparkContext)
      } else {
        val statuses = paths.flatMap { path =>
          val hdfsPath = new Path(path)
          val fs = hdfsPath.getFileSystem(hadoopConf)
          val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

          logInfo(s"Listing $qualified on driver")
          Try(fs.listStatus(qualified)).getOrElse(Array.empty)
        }.filterNot { status =>
          val name = status.getPath.getName
          name.toLowerCase == "_temporary" || name.startsWith(".")
        }

        val (dirs, files) = statuses.partition(_.isDir)

        if (dirs.isEmpty) {
          files.toSet
        } else {
          files.toSet ++ listLeafFiles(dirs.map(_.getPath.toString))
        }
      }
    }

    def refresh(): Unit = {
      val files = listLeafFiles(paths)

      leafFiles.clear()
      leafDirToChildrenFiles.clear()

      leafFiles ++= files.map(f => f.getPath -> f).toMap
      leafDirToChildrenFiles ++= files.toArray.groupBy(_.getPath.getParent)
    }
  }

  private lazy val fileStatusCache = {
    val cache = new FileStatusCache
    cache.refresh()
    cache
  }

  protected def cachedLeafStatuses(): Set[FileStatus] = {
    fileStatusCache.leafFiles.values.toSet
  }

  final private[sql] def partitionSpec: PartitionSpec = {
    if (_partitionSpec == null) {
      _partitionSpec = maybePartitionSpec
        .flatMap {
          case spec if spec.partitions.nonEmpty =>
            Some(spec.copy(partitionColumns = spec.partitionColumns.asNullable))
          case _ =>
            None
        }
        .orElse {
          // We only know the partition columns and their data types. We need to discover
          // partition values.
          userDefinedPartitionColumns.map { partitionSchema =>
            val spec = discoverPartitions()
            val partitionColumnTypes = spec.partitionColumns.map(_.dataType)
            val castedPartitions = spec.partitions.map { case p @ Partition(values, path) =>
              val literals = partitionColumnTypes.zipWithIndex.map { case (dt, i) =>
                Literal.create(values.get(i, dt), dt)
              }
              val castedValues = partitionSchema.zip(literals).map { case (field, literal) =>
                Cast(literal, field.dataType).eval()
              }
              p.copy(values = InternalRow.fromSeq(castedValues))
            }
            PartitionSpec(partitionSchema, castedPartitions)
          }
        }
        .getOrElse {
          if (sqlContext.conf.partitionDiscoveryEnabled()) {
            discoverPartitions()
          } else {
            PartitionSpec(StructType(Nil), Array.empty[Partition])
          }
        }
    }
    _partitionSpec
  }

  /**
   * Base paths of this relation.  For partitioned relations, it should be either root directories
   * of all partition directories.
   *
   * @since 1.4.0
   */
  def paths: Array[String]

  override def inputFiles: Array[String] = cachedLeafStatuses().map(_.getPath.toString).toArray

  override def sizeInBytes: Long = cachedLeafStatuses().map(_.getLen).sum

  /**
   * Partition columns.  Can be either defined by [[userDefinedPartitionColumns]] or automatically
   * discovered.  Note that they should always be nullable.
   *
   * @since 1.4.0
   */
  final def partitionColumns: StructType =
    userDefinedPartitionColumns.getOrElse(partitionSpec.partitionColumns)

  /**
   * Optional user defined partition columns.
   *
   * @since 1.4.0
   */
  def userDefinedPartitionColumns: Option[StructType] = None

  private[sql] def refresh(): Unit = {
    fileStatusCache.refresh()
    if (sqlContext.conf.partitionDiscoveryEnabled()) {
      _partitionSpec = discoverPartitions()
    }
  }

  private def discoverPartitions(): PartitionSpec = {
    val typeInference = sqlContext.conf.partitionColumnTypeInferenceEnabled()
    // We use leaf dirs containing data files to discover the schema.
    val leafDirs = fileStatusCache.leafDirToChildrenFiles.keys.toSeq
    PartitioningUtils.parsePartitions(leafDirs, PartitioningUtils.DEFAULT_PARTITION_NAME,
      typeInference)
  }

  /**
   * Schema of this relation.  It consists of columns appearing in [[dataSchema]] and all partition
   * columns not appearing in [[dataSchema]].
   *
   * @since 1.4.0
   */
  override lazy val schema: StructType = {
    val dataSchemaColumnNames = dataSchema.map(_.name.toLowerCase).toSet
    StructType(dataSchema ++ partitionColumns.filterNot { column =>
      dataSchemaColumnNames.contains(column.name.toLowerCase)
    })
  }

  final private[sql] def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[Row] = {
    val inputStatuses = inputPaths.flatMap { input =>
      val path = new Path(input)

      // First assumes `input` is a directory path, and tries to get all files contained in it.
      fileStatusCache.leafDirToChildrenFiles.getOrElse(
        path,
        // Otherwise, `input` might be a file path
        fileStatusCache.leafFiles.get(path).toArray
      ).filter { status =>
        val name = status.getPath.getName
        !name.startsWith("_") && !name.startsWith(".")
      }
    }

    buildScan(requiredColumns, filters, inputStatuses, broadcastedConf)
  }

  /**
   * Specifies schema of actual data files.  For partitioned relations, if one or more partitioned
   * columns are contained in the data files, they should also appear in `dataSchema`.
   *
   * @since 1.4.0
   */
  def dataSchema: StructType

  /**
   * For a non-partitioned relation, this method builds an `RDD[Row]` containing all rows within
   * this relation. For partitioned relations, this method is called for each selected partition,
   * and builds an `RDD[Row]` containing all rows within that single partition.
   *
   * @param inputFiles For a non-partitioned relation, it contains paths of all data files in the
   *        relation. For a partitioned relation, it contains paths of all data files in a single
   *        selected partition.
   *
   * @since 1.4.0
   */
  def buildScan(inputFiles: Array[FileStatus]): RDD[Row] = {
    throw new UnsupportedOperationException(
      "At least one buildScan() method should be overridden to read the relation.")
  }

  /**
   * For a non-partitioned relation, this method builds an `RDD[Row]` containing all rows within
   * this relation. For partitioned relations, this method is called for each selected partition,
   * and builds an `RDD[Row]` containing all rows within that single partition.
   *
   * @param requiredColumns Required columns.
   * @param inputFiles For a non-partitioned relation, it contains paths of all data files in the
   *        relation. For a partitioned relation, it contains paths of all data files in a single
   *        selected partition.
   *
   * @since 1.4.0
   */
  // TODO Tries to eliminate the extra Catalyst-to-Scala conversion when `needConversion` is true
  //
  // PR #7626 separated `Row` and `InternalRow` completely.  One of the consequences is that we can
  // no longer treat an `InternalRow` containing Catalyst values as a `Row`.  Thus we have to
  // introduce another row value conversion for data sources whose `needConversion` is true.
  def buildScan(requiredColumns: Array[String], inputFiles: Array[FileStatus]): RDD[Row] = {
    // Yeah, to workaround serialization...
    val dataSchema = this.dataSchema
    val codegenEnabled = this.codegenEnabled
    val needConversion = this.needConversion

    val requiredOutput = requiredColumns.map { col =>
      val field = dataSchema(col)
      BoundReference(dataSchema.fieldIndex(col), field.dataType, field.nullable)
    }.toSeq

    val rdd: RDD[Row] = buildScan(inputFiles)
    val converted: RDD[InternalRow] =
      if (needConversion) {
        RDDConversions.rowToRowRdd(rdd, dataSchema.fields.map(_.dataType))
      } else {
        rdd.asInstanceOf[RDD[InternalRow]]
      }

    converted.mapPartitions { rows =>
      val buildProjection = if (codegenEnabled) {
        GenerateMutableProjection.generate(requiredOutput, dataSchema.toAttributes)
      } else {
        () => new InterpretedMutableProjection(requiredOutput, dataSchema.toAttributes)
      }

      val projectedRows = {
        val mutableProjection = buildProjection()
        rows.map(r => mutableProjection(r))
      }

      if (needConversion) {
        val requiredSchema = StructType(requiredColumns.map(dataSchema(_)))
        val toScala = CatalystTypeConverters.createToScalaConverter(requiredSchema)
        projectedRows.map(toScala(_).asInstanceOf[Row])
      } else {
        projectedRows
      }
    }.asInstanceOf[RDD[Row]]
  }

  /**
   * For a non-partitioned relation, this method builds an `RDD[Row]` containing all rows within
   * this relation. For partitioned relations, this method is called for each selected partition,
   * and builds an `RDD[Row]` containing all rows within that single partition.
   *
   * @param requiredColumns Required columns.
   * @param filters Candidate filters to be pushed down. The actual filter should be the conjunction
   *        of all `filters`.  The pushed down filters are currently purely an optimization as they
   *        will all be evaluated again. This means it is safe to use them with methods that produce
   *        false positives such as filtering partitions based on a bloom filter.
   * @param inputFiles For a non-partitioned relation, it contains paths of all data files in the
   *        relation. For a partitioned relation, it contains paths of all data files in a single
   *        selected partition.
   *
   * @since 1.4.0
   */
  def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus]): RDD[Row] = {
    buildScan(requiredColumns, inputFiles)
  }

  /**
   * For a non-partitioned relation, this method builds an `RDD[Row]` containing all rows within
   * this relation. For partitioned relations, this method is called for each selected partition,
   * and builds an `RDD[Row]` containing all rows within that single partition.
   *
   * Note: This interface is subject to change in future.
   *
   * @param requiredColumns Required columns.
   * @param filters Candidate filters to be pushed down. The actual filter should be the conjunction
   *        of all `filters`.  The pushed down filters are currently purely an optimization as they
   *        will all be evaluated again. This means it is safe to use them with methods that produce
   *        false positives such as filtering partitions based on a bloom filter.
   * @param inputFiles For a non-partitioned relation, it contains paths of all data files in the
   *        relation. For a partitioned relation, it contains paths of all data files in a single
   *        selected partition.
   * @param broadcastedConf A shared broadcast Hadoop Configuration, which can be used to reduce the
   *                        overhead of broadcasting the Configuration for every Hadoop RDD.
   *
   * @since 1.4.0
   */
  private[sql] def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[Row] = {
    buildScan(requiredColumns, filters, inputFiles)
  }

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   *
   * Note that the only side effect expected here is mutating `job` via its setters.  Especially,
   * Spark SQL caches [[BaseRelation]] instances for performance, mutating relation internal states
   * may cause unexpected behaviors.
   *
   * @since 1.4.0
   */
  def prepareJobForWrite(job: Job): OutputWriterFactory
}

private[sql] object HadoopFsRelation extends Logging {
  // We don't filter files/directories whose name start with "_" except "_temporary" here, as
  // specific data sources may take advantages over them (e.g. Parquet _metadata and
  // _common_metadata files). "_temporary" directories are explicitly ignored since failed
  // tasks/jobs may leave partial/corrupted data files there.  Files and directories whose name
  // start with "." are also ignored.
  def listLeafFiles(fs: FileSystem, status: FileStatus): Array[FileStatus] = {
    logInfo(s"Listing ${status.getPath}")
    val name = status.getPath.getName.toLowerCase
    if (name == "_temporary" || name.startsWith(".")) {
      Array.empty
    } else {
      val (dirs, files) = fs.listStatus(status.getPath).partition(_.isDir)
      files ++ dirs.flatMap(dir => listLeafFiles(fs, dir))
    }
  }

  // `FileStatus` is Writable but not serializable.  What make it worse, somehow it doesn't play
  // well with `SerializableWritable`.  So there seems to be no way to serialize a `FileStatus`.
  // Here we use `FakeFileStatus` to extract key components of a `FileStatus` to serialize it from
  // executor side and reconstruct it on driver side.
  case class FakeFileStatus(
      path: String,
      length: Long,
      isDir: Boolean,
      blockReplication: Short,
      blockSize: Long,
      modificationTime: Long,
      accessTime: Long)

  def listLeafFilesInParallel(
      paths: Array[String],
      hadoopConf: Configuration,
      sparkContext: SparkContext): Set[FileStatus] = {
    logInfo(s"Listing leaf files and directories in parallel under: ${paths.mkString(", ")}")

    val serializableConfiguration = new SerializableConfiguration(hadoopConf)
    val fakeStatuses = sparkContext.parallelize(paths).flatMap { path =>
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(serializableConfiguration.value)
      val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      Try(listLeafFiles(fs, fs.getFileStatus(qualified))).getOrElse(Array.empty)
    }.map { status =>
      FakeFileStatus(
        status.getPath.toString,
        status.getLen,
        status.isDir,
        status.getReplication,
        status.getBlockSize,
        status.getModificationTime,
        status.getAccessTime)
    }.collect()

    fakeStatuses.map { f =>
      new FileStatus(
        f.length, f.isDir, f.blockReplication, f.blockSize, f.modificationTime, new Path(f.path))
    }.toSet
  }
}
