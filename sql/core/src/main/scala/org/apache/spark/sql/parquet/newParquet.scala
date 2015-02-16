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
package org.apache.spark.sql.parquet

import java.io.IOException
import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.text.SimpleDateFormat
import java.util.{Date, List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobContext}
import parquet.filter2.predicate.FilterApi
import parquet.format.converter.ParquetMetadataConverter
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.{ParquetInputFormat, _}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.{NewHadoopPartition, NewHadoopRDD, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.ParquetTypesConverter._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SQLConf, SQLContext, SaveMode}
import org.apache.spark.{Logging, Partition => SparkPartition, SerializableWritable, SparkException, TaskContext}

/**
 * Allows creation of Parquet based tables using the syntax:
 * {{{
 *   CREATE TEMPORARY TABLE ... USING org.apache.spark.sql.parquet OPTIONS (...)
 * }}}
 *
 * Supported options include:
 *
 *  - `path`: Required. When reading Parquet files, `path` should point to the location of the
 *    Parquet file(s). It can be either a single raw Parquet file, or a directory of Parquet files.
 *    In the latter case, this data source tries to discover partitioning information if the the
 *    directory is structured in the same style of Hive partitioned tables. When writing Parquet
 *    file, `path` should point to the destination folder.
 *
 *  - `mergeSchema`: Optional. Indicates whether we should merge potentially different (but
 *    compatible) schemas stored in all Parquet part-files.
 *
 *  - `partition.defaultName`: Optional. Partition name used when a value of a partition column is
 *    null or empty string. This is similar to the `hive.exec.default.partition.name` configuration
 *    in Hive.
 */
class DefaultSource
    extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for parquet tables."))
  }

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    ParquetRelation2(Seq(checkPath(parameters)), parameters, None)(sqlContext)
  }

  /** Returns a new base relation with the given parameters and schema. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    ParquetRelation2(Seq(checkPath(parameters)), parameters, Some(schema))(sqlContext)
  }

  /** Returns a new base relation with the given parameters and save given data into it. */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doInsertion = (mode, fs.exists(filesystemPath)) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"path $path already exists.")
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
    }

    val relation = if (doInsertion) {
      val createdRelation =
        createRelation(sqlContext, parameters, data.schema).asInstanceOf[ParquetRelation2]
      createdRelation.insert(data, overwrite = mode == SaveMode.Overwrite)
      createdRelation
    } else {
      // If the save mode is Ignore, we will just create the relation based on existing data.
      createRelation(sqlContext, parameters)
    }

    relation
  }
}

private[sql] case class Partition(values: Row, path: String)

private[sql] case class PartitionSpec(partitionColumns: StructType, partitions: Seq[Partition])

/**
 * An alternative to [[ParquetRelation]] that plugs in using the data sources API.  This class is
 * intended as a full replacement of the Parquet support in Spark SQL.  The old implementation will
 * be deprecated and eventually removed once this version is proved to be stable enough.
 *
 * Compared with the old implementation, this class has the following notable differences:
 *
 *  - Partitioning discovery: Hive style multi-level partitions are auto discovered.
 *  - Metadata discovery: Parquet is a format comes with schema evolving support.  This data source
 *    can detect and merge schemas from all Parquet part-files as long as they are compatible.
 *    Also, metadata and [[FileStatus]]es are cached for better performance.
 *  - Statistics: Statistics for the size of the table are automatically populated during schema
 *    discovery.
 */
@DeveloperApi
case class ParquetRelation2(
    paths: Seq[String],
    parameters: Map[String, String],
    maybeSchema: Option[StructType] = None,
    maybePartitionSpec: Option[PartitionSpec] = None)(
    @transient val sqlContext: SQLContext)
  extends CatalystScan
  with InsertableRelation
  with SparkHadoopMapReduceUtil
  with Logging {

  // Should we merge schemas from all Parquet part-files?
  private val shouldMergeSchemas =
    parameters.getOrElse(ParquetRelation2.MERGE_SCHEMA, "true").toBoolean

  // Optional Metastore schema, used when converting Hive Metastore Parquet table
  private val maybeMetastoreSchema =
    parameters
      .get(ParquetRelation2.METASTORE_SCHEMA)
      .map(s => DataType.fromJson(s).asInstanceOf[StructType])

  // Hive uses this as part of the default partition name when the partition column value is null
  // or empty string
  private val defaultPartitionName = parameters.getOrElse(
    ParquetRelation2.DEFAULT_PARTITION_NAME, "__HIVE_DEFAULT_PARTITION__")

  override def equals(other: Any) = other match {
    case relation: ParquetRelation2 =>
      // If schema merging is required, we don't compare the actual schemas since they may evolve.
      val schemaEquality = if (shouldMergeSchemas) {
        shouldMergeSchemas == relation.shouldMergeSchemas
      } else {
        schema == relation.schema
      }

      paths.toSet == relation.paths.toSet &&
        schemaEquality &&
        maybeMetastoreSchema == relation.maybeMetastoreSchema &&
        maybePartitionSpec == relation.maybePartitionSpec

    case _ => false
  }

  private[sql] def sparkContext = sqlContext.sparkContext

  private class MetadataCache {
    // `FileStatus` objects of all "_metadata" files.
    private var metadataStatuses: Array[FileStatus] = _

    // `FileStatus` objects of all "_common_metadata" files.
    private var commonMetadataStatuses: Array[FileStatus] = _

    // Parquet footer cache.
    private var footers: Map[FileStatus, Footer] = _

    // `FileStatus` objects of all data files (Parquet part-files).
    var dataStatuses: Array[FileStatus] = _

    // Partition spec of this table, including names, data types, and values of each partition
    // column, and paths of each partition.
    var partitionSpec: PartitionSpec = _

    // Schema of the actual Parquet files, without partition columns discovered from partition
    // directory paths.
    var parquetSchema: StructType = _

    // Schema of the whole table, including partition columns.
    var schema: StructType = _

    // Indicates whether partition columns are also included in Parquet data file schema.  If not,
    // we need to fill in partition column values into read rows when scanning the table.
    var partitionKeysIncludedInParquetSchema: Boolean = _

    def prepareMetadata(path: Path, schema: StructType, conf: Configuration): Unit = {
      conf.set(
        ParquetOutputFormat.COMPRESSION,
        ParquetRelation
          .shortParquetCompressionCodecNames
          .getOrElse(
            sqlContext.conf.parquetCompressionCodec.toUpperCase,
            CompressionCodecName.UNCOMPRESSED).name())

      ParquetRelation.enableLogForwarding()
      ParquetTypesConverter.writeMetaData(schema.toAttributes, path, conf)
    }

    /**
     * Refreshes `FileStatus`es, footers, partition spec, and table schema.
     */
    def refresh(): Unit = {
      val fs = FileSystem.get(sparkContext.hadoopConfiguration)

      // Support either reading a collection of raw Parquet part-files, or a collection of folders
      // containing Parquet files (e.g. partitioned Parquet table).
      val baseStatuses = paths.distinct.map { p =>
        val qualified = fs.makeQualified(new Path(p))

        if (!fs.exists(qualified) && maybeSchema.isDefined) {
          fs.mkdirs(qualified)
          prepareMetadata(qualified, maybeSchema.get, sparkContext.hadoopConfiguration)
        }

        fs.getFileStatus(qualified)
      }.toArray
      assert(baseStatuses.forall(!_.isDir) || baseStatuses.forall(_.isDir))

      // Lists `FileStatus`es of all leaf nodes (files) under all base directories.
      val leaves = baseStatuses.flatMap { f =>
        SparkHadoopUtil.get.listLeafStatuses(fs, f.getPath).filter { f =>
          isSummaryFile(f.getPath) ||
            !(f.getPath.getName.startsWith("_") || f.getPath.getName.startsWith("."))
        }
      }

      dataStatuses = leaves.filterNot(f => isSummaryFile(f.getPath))
      metadataStatuses = leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE)
      commonMetadataStatuses =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE)

      footers = (dataStatuses ++ metadataStatuses ++ commonMetadataStatuses).par.map { f =>
        val parquetMetadata = ParquetFileReader.readFooter(
          sparkContext.hadoopConfiguration, f, ParquetMetadataConverter.NO_FILTER)
        f -> new Footer(f.getPath, parquetMetadata)
      }.seq.toMap

      partitionSpec = maybePartitionSpec.getOrElse {
        val partitionDirs = leaves
          .filterNot(baseStatuses.contains)
          .map(_.getPath.getParent)
          .distinct

        if (partitionDirs.nonEmpty) {
          // Parses names and values of partition columns, and infer their data types.
          ParquetRelation2.parsePartitions(partitionDirs, defaultPartitionName)
        } else {
          // No partition directories found, makes an empty specification
          PartitionSpec(StructType(Seq.empty[StructField]), Seq.empty[Partition])
        }
      }

      parquetSchema = maybeSchema.getOrElse(readSchema())

      partitionKeysIncludedInParquetSchema =
        isPartitioned &&
          partitionColumns.forall(f => parquetSchema.fieldNames.contains(f.name))

      schema = {
        val fullRelationSchema = if (partitionKeysIncludedInParquetSchema) {
          parquetSchema
        } else {
          StructType(parquetSchema.fields ++ partitionColumns.fields)
        }

        // If this Parquet relation is converted from a Hive Metastore table, must reconcile case
        // insensitivity issue and possible schema mismatch.
        maybeMetastoreSchema
          .map(ParquetRelation2.mergeMetastoreParquetSchema(_, fullRelationSchema))
          .getOrElse(fullRelationSchema)
      }
    }

    private def readSchema(): StructType = {
      // Sees which file(s) we need to touch in order to figure out the schema.
      val filesToTouch =
      // Always tries the summary files first if users don't require a merged schema.  In this case,
      // "_common_metadata" is more preferable than "_metadata" because it doesn't contain row
      // groups information, and could be much smaller for large Parquet files with lots of row
      // groups.
      //
      // NOTE: Metadata stored in the summary files are merged from all part-files.  However, for
      // user defined key-value metadata (in which we store Spark SQL schema), Parquet doesn't know
      // how to merge them correctly if some key is associated with different values in different
      // part-files.  When this happens, Parquet simply gives up generating the summary file.  This
      // implies that if a summary file presents, then:
      //
      //   1. Either all part-files have exactly the same Spark SQL schema, or
      //   2. Some part-files don't contain Spark SQL schema in the key-value metadata at all (thus
      //      their schemas may differ from each other).
      //
      // Here we tend to be pessimistic and take the second case into account.  Basically this means
      // we can't trust the summary files if users require a merged schema, and must touch all part-
      // files to do the merge.
        if (shouldMergeSchemas) {
          // Also includes summary files, 'cause there might be empty partition directories.
          (metadataStatuses ++ commonMetadataStatuses ++ dataStatuses).toSeq
        } else {
          // Tries any "_common_metadata" first. Parquet files written by old versions or Parquet
          // don't have this.
          commonMetadataStatuses.headOption
            // Falls back to "_metadata"
            .orElse(metadataStatuses.headOption)
            // Summary file(s) not found, the Parquet file is either corrupted, or different part-
            // files contain conflicting user defined metadata (two or more values are associated
            // with a same key in different files).  In either case, we fall back to any of the
            // first part-file, and just assume all schemas are consistent.
            .orElse(dataStatuses.headOption)
            .toSeq
        }

      ParquetRelation2.readSchema(filesToTouch.map(footers.apply), sqlContext)
    }
  }

  @transient private val metadataCache = new MetadataCache
  metadataCache.refresh()

  def partitionSpec = metadataCache.partitionSpec

  def partitionColumns = metadataCache.partitionSpec.partitionColumns

  def partitions = metadataCache.partitionSpec.partitions

  def isPartitioned = partitionColumns.nonEmpty

  private def partitionKeysIncludedInDataSchema = metadataCache.partitionKeysIncludedInParquetSchema

  private def parquetSchema = metadataCache.parquetSchema

  override def schema = metadataCache.schema

  private def isSummaryFile(file: Path): Boolean = {
    file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
      file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
  }

  // TODO Should calculate per scan size
  // It's common that a query only scans a fraction of a large Parquet file.  Returning size of the
  // whole Parquet file disables some optimizations in this case (e.g. broadcast join).
  override val sizeInBytes = metadataCache.dataStatuses.map(_.getLen).sum

  // This is mostly a hack so that we can use the existing parquet filter code.
  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    val job = new Job(sparkContext.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])
    val jobConf: Configuration = ContextUtil.getConfiguration(job)

    val selectedPartitions = prunePartitions(predicates, partitions)
    val selectedFiles = if (isPartitioned) {
      selectedPartitions.flatMap { p =>
        metadataCache.dataStatuses.filter(_.getPath.getParent.toString == p.path)
      }
    } else {
      metadataCache.dataStatuses.toSeq
    }

    // FileInputFormat cannot handle empty lists.
    if (selectedFiles.nonEmpty) {
      FileInputFormat.setInputPaths(job, selectedFiles.map(_.getPath): _*)
    }

    // Push down filters when possible. Notice that not all filters can be converted to Parquet
    // filter predicate. Here we try to convert each individual predicate and only collect those
    // convertible ones.
    predicates
      .flatMap(ParquetFilters.createFilter)
      .reduceOption(FilterApi.and)
      .filter(_ => sqlContext.conf.parquetFilterPushDown)
      .foreach(ParquetInputFormat.setFilterPredicate(jobConf, _))

    if (isPartitioned) {
      def percentRead = selectedPartitions.size.toDouble / partitions.size.toDouble * 100
      logInfo(s"Reading $percentRead% of partitions")
    }

    val requiredColumns = output.map(_.name)
    val requestedSchema = StructType(requiredColumns.map(schema(_)))

    // Store both requested and original schema in `Configuration`
    jobConf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      convertToString(requestedSchema.toAttributes))
    jobConf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      convertToString(schema.toAttributes))

    // Tell FilteringParquetRowInputFormat whether it's okay to cache Parquet and FS metadata
    val useCache = sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA, "true").toBoolean
    jobConf.set(SQLConf.PARQUET_CACHE_METADATA, useCache.toString)

    val baseRDD =
      new NewHadoopRDD(
          sparkContext,
          classOf[FilteringParquetRowInputFormat],
          classOf[Void],
          classOf[Row],
          jobConf) {
        val cacheMetadata = useCache

        @transient
        val cachedStatus = selectedFiles

        // Overridden so we can inject our own cached files statuses.
        override def getPartitions: Array[SparkPartition] = {
          val inputFormat = if (cacheMetadata) {
            new FilteringParquetRowInputFormat {
              override def listStatus(jobContext: JobContext): JList[FileStatus] = cachedStatus
            }
          } else {
            new FilteringParquetRowInputFormat
          }

          val jobContext = newJobContext(getConf, jobId)
          val rawSplits = inputFormat.getSplits(jobContext)

          Array.tabulate[SparkPartition](rawSplits.size) { i =>
            new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
          }
        }
      }

    // The ordinals for partition keys in the result row, if requested.
    val partitionKeyLocations = partitionColumns.fieldNames.zipWithIndex.map {
      case (name, index) => index -> requiredColumns.indexOf(name)
    }.toMap.filter {
      case (_, index) => index >= 0
    }

    // When the data does not include the key and the key is requested then we must fill it in
    // based on information from the input split.
    if (!partitionKeysIncludedInDataSchema && partitionKeyLocations.nonEmpty) {
      baseRDD.mapPartitionsWithInputSplit { case (split: ParquetInputSplit, iterator) =>
        val partValues = selectedPartitions.collectFirst {
          case p if split.getPath.getParent.toString == p.path => p.values
        }.get

        val requiredPartOrdinal = partitionKeyLocations.keys.toSeq

        iterator.map { pair =>
          val row = pair._2.asInstanceOf[SpecificMutableRow]
          var i = 0
          while (i < requiredPartOrdinal.size) {
            // TODO Avoids boxing cost here!
            val partOrdinal = requiredPartOrdinal(i)
            row.update(partitionKeyLocations(partOrdinal), partValues(partOrdinal))
            i += 1
          }
          row
        }
      }
    } else {
      baseRDD.map(_._2)
    }
  }

  private def prunePartitions(
      predicates: Seq[Expression],
      partitions: Seq[Partition]): Seq[Partition] = {
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    val rawPredicate = partitionPruningPredicates.reduceOption(And).getOrElse(Literal(true))
    val boundPredicate = InterpretedPredicate(rawPredicate transform {
      case a: AttributeReference =>
        val index = partitionColumns.indexWhere(a.name == _.name)
        BoundReference(index, partitionColumns(index).dataType, nullable = true)
    })

    if (isPartitioned && partitionPruningPredicates.nonEmpty) {
      partitions.filter(p => boundPredicate(p.values))
    } else {
      partitions
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    assert(paths.size == 1, s"Can't write to multiple destinations: ${paths.mkString(",")}")

    // TODO: currently we do not check whether the "schema"s are compatible
    // That means if one first creates a table and then INSERTs data with
    // and incompatible schema the execution will fail. It would be nice
    // to catch this early one, maybe having the planner validate the schema
    // before calling execute().

    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val writeSupport = if (parquetSchema.map(_.dataType).forall(_.isPrimitive)) {
      log.debug("Initializing MutableRowWriteSupport")
      classOf[MutableRowWriteSupport]
    } else {
      classOf[RowWriteSupport]
    }

    ParquetOutputFormat.setWriteSupportClass(job, writeSupport)

    val conf = ContextUtil.getConfiguration(job)
    RowWriteSupport.setSchema(data.schema.toAttributes, conf)

    val destinationPath = new Path(paths.head)

    if (overwrite) {
      try {
        destinationPath.getFileSystem(conf).delete(destinationPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${destinationPath.toString} prior" +
              s" to writing to Parquet file:\n${e.toString}")
      }
    }

    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[Row])
    FileOutputFormat.setOutputPath(job, destinationPath)

    val wrappedConf = new SerializableWritable(job.getConfiguration)
    val jobTrackerId = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
    val stageId = sqlContext.sparkContext.newRddId()

    val taskIdOffset = if (overwrite) {
      1
    } else {
      FileSystemHelper.findMaxTaskId(
        FileOutputFormat.getOutputPath(job).toString, job.getConfiguration) + 1
    }

    def writeShard(context: TaskContext, iterator: Iterator[Row]): Unit = {
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(
        jobTrackerId, stageId, isMap = false, context.partitionId(), context.attemptNumber())
      val hadoopContext = newTaskAttemptContext(wrappedConf.value, attemptId)
      val format = new AppendingParquetOutputFormat(taskIdOffset)
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)
      val writer = format.getRecordWriter(hadoopContext)
      try {
        while (iterator.hasNext) {
          val row = iterator.next()
          writer.write(null, row)
        }
      } finally {
        writer.close(hadoopContext)
      }
      committer.commitTask(hadoopContext)
    }
    val jobFormat = new AppendingParquetOutputFormat(taskIdOffset)
    /* apparently we need a TaskAttemptID to construct an OutputCommitter;
     * however we're only going to use this local OutputCommitter for
     * setupJob/commitJob, so we just use a dummy "map" task.
     */
    val jobAttemptId = newTaskAttemptID(jobTrackerId, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)

    jobCommitter.setupJob(jobTaskContext)
    sqlContext.sparkContext.runJob(data.queryExecution.executedPlan.execute(), writeShard _)
    jobCommitter.commitJob(jobTaskContext)

    metadataCache.refresh()
  }
}

object ParquetRelation2 {
  // Whether we should merge schemas collected from all Parquet part-files.
  val MERGE_SCHEMA = "mergeSchema"

  // Default partition name to use when the partition column value is null or empty string.
  val DEFAULT_PARTITION_NAME = "partition.defaultName"

  // Hive Metastore schema, used when converting Metastore Parquet tables.  This option is only used
  // internally.
  private[sql] val METASTORE_SCHEMA = "metastoreSchema"

  private[parquet] def readSchema(footers: Seq[Footer], sqlContext: SQLContext): StructType = {
    footers.map { footer =>
      val metadata = footer.getParquetMetadata.getFileMetaData
      val parquetSchema = metadata.getSchema
      val maybeSparkSchema = metadata
        .getKeyValueMetaData
        .toMap
        .get(RowReadSupport.SPARK_METADATA_KEY)
        .map(DataType.fromJson(_).asInstanceOf[StructType])

      maybeSparkSchema.getOrElse {
        // Falls back to Parquet schema if Spark SQL schema is absent.
        StructType.fromAttributes(
          // TODO Really no need to use `Attribute` here, we only need to know the data type.
          convertToAttributes(
            parquetSchema,
            sqlContext.conf.isParquetBinaryAsString,
            sqlContext.conf.isParquetINT96AsTimestamp))
      }
    }.reduce { (left, right) =>
      try left.merge(right) catch { case e: Throwable =>
        throw new SparkException(s"Failed to merge incompatible schemas $left and $right", e)
      }
    }
  }

  /**
   * Reconciles Hive Metastore case insensitivity issue and data type conflicts between Metastore
   * schema and Parquet schema.
   *
   * Hive doesn't retain case information, while Parquet is case sensitive. On the other hand, the
   * schema read from Parquet files may be incomplete (e.g. older versions of Parquet doesn't
   * distinguish binary and string).  This method generates a correct schema by merging Metastore
   * schema data types and Parquet schema field names.
   */
  private[parquet] def mergeMetastoreParquetSchema(
      metastoreSchema: StructType,
      parquetSchema: StructType): StructType = {
    def schemaConflictMessage =
      s"""Converting Hive Metastore Parquet, but detected conflicting schemas. Metastore schema:
         |${metastoreSchema.prettyJson}
         |
         |Parquet schema:
         |${parquetSchema.prettyJson}
       """.stripMargin

    assert(metastoreSchema.size == parquetSchema.size, schemaConflictMessage)

    val ordinalMap = metastoreSchema.zipWithIndex.map {
      case (field, index) => field.name.toLowerCase -> index
    }.toMap
    val reorderedParquetSchema = parquetSchema.sortBy(f => ordinalMap(f.name.toLowerCase))

    StructType(metastoreSchema.zip(reorderedParquetSchema).map {
      // Uses Parquet field names but retains Metastore data types.
      case (mSchema, pSchema) if mSchema.name.toLowerCase == pSchema.name.toLowerCase =>
        mSchema.copy(name = pSchema.name)
      case _ =>
        throw new SparkException(schemaConflictMessage)
    })
  }

  // TODO Data source implementations shouldn't touch Catalyst types (`Literal`).
  // However, we are already using Catalyst expressions for partition pruning and predicate
  // push-down here...
  private[parquet] case class PartitionValues(columnNames: Seq[String], literals: Seq[Literal]) {
    require(columnNames.size == literals.size)
  }

  /**
   * Given a group of qualified paths, tries to parse them and returns a partition specification.
   * For example, given:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14
   *   hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28
   * }}}
   * it returns:
   * {{{
   *   PartitionSpec(
   *     partitionColumns = StructType(
   *       StructField(name = "a", dataType = IntegerType, nullable = true),
   *       StructField(name = "b", dataType = StringType, nullable = true),
   *       StructField(name = "c", dataType = DoubleType, nullable = true)),
   *     partitions = Seq(
   *       Partition(
   *         values = Row(1, "hello", 3.14),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14"),
   *       Partition(
   *         values = Row(2, "world", 6.28),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28")))
   * }}}
   */
  private[parquet] def parsePartitions(
      paths: Seq[Path],
      defaultPartitionName: String): PartitionSpec = {
    val partitionValues = resolvePartitions(paths.map(parsePartition(_, defaultPartitionName)))
    val fields = {
      val (PartitionValues(columnNames, literals)) = partitionValues.head
      columnNames.zip(literals).map { case (name, Literal(_, dataType)) =>
        StructField(name, dataType, nullable = true)
      }
    }

    val partitions = partitionValues.zip(paths).map {
      case (PartitionValues(_, literals), path) =>
        Partition(Row(literals.map(_.value): _*), path.toString)
    }

    PartitionSpec(StructType(fields), partitions)
  }

  /**
   * Parses a single partition, returns column names and values of each partition column.  For
   * example, given:
   * {{{
   *   path = hdfs://<host>:<port>/path/to/partition/a=42/b=hello/c=3.14
   * }}}
   * it returns:
   * {{{
   *   PartitionValues(
   *     Seq("a", "b", "c"),
   *     Seq(
   *       Literal(42, IntegerType),
   *       Literal("hello", StringType),
   *       Literal(3.14, FloatType)))
   * }}}
   */
  private[parquet] def parsePartition(
      path: Path,
      defaultPartitionName: String): PartitionValues = {
    val columns = ArrayBuffer.empty[(String, Literal)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    var chopped = path

    while (!finished) {
      val maybeColumn = parsePartitionColumn(chopped.getName, defaultPartitionName)
      maybeColumn.foreach(columns += _)
      chopped = chopped.getParent
      finished = maybeColumn.isEmpty || chopped.getParent == null
    }

    val (columnNames, values) = columns.reverse.unzip
    PartitionValues(columnNames, values)
  }

  private def parsePartitionColumn(
      columnSpec: String,
      defaultPartitionName: String): Option[(String, Literal)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = columnSpec.take(equalSignIndex)
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val literal = inferPartitionColumnValue(rawColumnValue, defaultPartitionName)
      Some(columnName -> literal)
    }
  }

  /**
   * Resolves possible type conflicts between partitions by up-casting "lower" types.  The up-
   * casting order is:
   * {{{
   *   NullType ->
   *   IntegerType -> LongType ->
   *   FloatType -> DoubleType -> DecimalType.Unlimited ->
   *   StringType
   * }}}
   */
  private[parquet] def resolvePartitions(values: Seq[PartitionValues]): Seq[PartitionValues] = {
    // Column names of all partitions must match
    val distinctPartitionsColNames = values.map(_.columnNames).distinct
    assert(distinctPartitionsColNames.size == 1, {
      val list = distinctPartitionsColNames.mkString("\t", "\n", "")
      s"Conflicting partition column names detected:\n$list"
    })

    // Resolves possible type conflicts for each column
    val columnCount = values.head.columnNames.size
    val resolvedValues = (0 until columnCount).map { i =>
      resolveTypeConflicts(values.map(_.literals(i)))
    }

    // Fills resolved literals back to each partition
    values.zipWithIndex.map { case (d, index) =>
      d.copy(literals = resolvedValues.map(_(index)))
    }
  }

  /**
   * Converts a string to a `Literal` with automatic type inference.  Currently only supports
   * [[IntegerType]], [[LongType]], [[FloatType]], [[DoubleType]], [[DecimalType.Unlimited]], and
   * [[StringType]].
   */
  private[parquet] def inferPartitionColumnValue(
      raw: String,
      defaultPartitionName: String): Literal = {
    // First tries integral types
    Try(Literal(Integer.parseInt(raw), IntegerType))
      .orElse(Try(Literal(JLong.parseLong(raw), LongType)))
      // Then falls back to fractional types
      .orElse(Try(Literal(JFloat.parseFloat(raw), FloatType)))
      .orElse(Try(Literal(JDouble.parseDouble(raw), DoubleType)))
      .orElse(Try(Literal(new JBigDecimal(raw), DecimalType.Unlimited)))
      // Then falls back to string
      .getOrElse {
        if (raw == defaultPartitionName) Literal(null, NullType) else Literal(raw, StringType)
      }
  }

  private val upCastingOrder: Seq[DataType] =
    Seq(NullType, IntegerType, LongType, FloatType, DoubleType, DecimalType.Unlimited, StringType)

  /**
   * Given a collection of [[Literal]]s, resolves possible type conflicts by up-casting "lower"
   * types.
   */
  private def resolveTypeConflicts(literals: Seq[Literal]): Seq[Literal] = {
    val desiredType = {
      val topType = literals.map(_.dataType).maxBy(upCastingOrder.indexOf(_))
      // Falls back to string if all values of this column are null or empty string
      if (topType == NullType) StringType else topType
    }

    literals.map { case l @ Literal(_, dataType) =>
      Literal(Cast(l, desiredType).eval(), desiredType)
    }
  }
}
