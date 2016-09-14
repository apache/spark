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

package org.apache.spark.sql.execution.datasources.parquet

import java.net.URI
import java.util.logging.{Logger => JLogger}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.{Log => ApacheParquetLog}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.schema.MessageType
import org.slf4j.bridge.SLF4JBridgeHandler

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

class ParquetFileFormat
  extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  override def shortName(): String = "parquet"

  override def toString: String = "ParquetFormat"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[ParquetFileFormat]

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)

    val conf = ContextUtil.getConfiguration(job)

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[ParquetOutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo("Using default output committer for Parquet: " +
        classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[ParquetOutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // We want to clear this temporary metadata from saving into Parquet file.
    // This metadata is only useful for detecting optional columns when pushdowning filters.
    val dataSchemaToWrite = StructType.removeMetadata(StructType.metadataKeyForOptionalField,
      dataSchema).asInstanceOf[StructType]
    ParquetWriteSupport.setSchema(dataSchemaToWrite, conf)

    // Sets flags for `CatalystSchemaConverter` (which converts Catalyst schema to Parquet schema)
    // and `CatalystWriteSupport` (writing actual rows to Parquet files).
    conf.set(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString.toString)

    conf.set(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp.toString)

    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sparkSession.sessionState.conf.writeLegacyParquetFormat.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodec)

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, false)
    }

    ParquetFileFormat.redirectParquetLogs()

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new ParquetOutputWriter(path, bucketId, context)
      }
    }
  }

  def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val parquetOptions = new ParquetOptions(parameters, sparkSession.sessionState.conf)

    // Should we merge schemas from all Parquet part-files?
    val shouldMergeSchemas = parquetOptions.mergeSchema

    val mergeRespectSummaries = sparkSession.conf.get(SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES)

    val filesByType = splitFiles(files)

    // Sees which file(s) we need to touch in order to figure out the schema.
    //
    // Always tries the summary files first if users don't require a merged schema.  In this case,
    // "_common_metadata" is more preferable than "_metadata" because it doesn't contain row
    // groups information, and could be much smaller for large Parquet files with lots of row
    // groups.  If no summary file is available, falls back to some random part-file.
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
    val filesToTouch =
      if (shouldMergeSchemas) {
        // Also includes summary files, 'cause there might be empty partition directories.

        // If mergeRespectSummaries config is true, we assume that all part-files are the same for
        // their schema with summary files, so we ignore them when merging schema.
        // If the config is disabled, which is the default setting, we merge all part-files.
        // In this mode, we only need to merge schemas contained in all those summary files.
        // You should enable this configuration only if you are very sure that for the parquet
        // part-files to read there are corresponding summary files containing correct schema.

        // As filed in SPARK-11500, the order of files to touch is a matter, which might affect
        // the ordering of the output columns. There are several things to mention here.
        //
        //  1. If mergeRespectSummaries config is false, then it merges schemas by reducing from
        //     the first part-file so that the columns of the lexicographically first file show
        //     first.
        //
        //  2. If mergeRespectSummaries config is true, then there should be, at least,
        //     "_metadata"s for all given files, so that we can ensure the columns of
        //     the lexicographically first file show first.
        //
        //  3. If shouldMergeSchemas is false, but when multiple files are given, there is
        //     no guarantee of the output order, since there might not be a summary file for the
        //     lexicographically first file, which ends up putting ahead the columns of
        //     the other files. However, this should be okay since not enabling
        //     shouldMergeSchemas means (assumes) all the files have the same schemas.

        val needMerged: Seq[FileStatus] =
          if (mergeRespectSummaries) {
            Seq()
          } else {
            filesByType.data
          }
        needMerged ++ filesByType.metadata ++ filesByType.commonMetadata
      } else {
        // Tries any "_common_metadata" first. Parquet files written by old versions or Parquet
        // don't have this.
        filesByType.commonMetadata.headOption
            // Falls back to "_metadata"
            .orElse(filesByType.metadata.headOption)
            // Summary file(s) not found, the Parquet file is either corrupted, or different part-
            // files contain conflicting user defined metadata (two or more values are associated
            // with a same key in different files).  In either case, we fall back to any of the
            // first part-file, and just assume all schemas are consistent.
            .orElse(filesByType.data.headOption)
            .toSeq
      }
    ParquetFileFormat.mergeSchemasInParallel(filesToTouch, sparkSession)
  }

  case class FileTypes(
      data: Seq[FileStatus],
      metadata: Seq[FileStatus],
      commonMetadata: Seq[FileStatus])

  private def splitFiles(allFiles: Seq[FileStatus]): FileTypes = {
    // Lists `FileStatus`es of all leaf nodes (files) under all base directories.
    val leaves = allFiles.filter { f =>
      isSummaryFile(f.getPath) ||
        !((f.getPath.getName.startsWith("_") && !f.getPath.getName.contains("=")) ||
          f.getPath.getName.startsWith("."))
    }.toArray.sortBy(_.getPath.toString)

    FileTypes(
      data = leaves.filterNot(f => isSummaryFile(f.getPath)),
      metadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE),
      commonMetadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE))
  }

  private def isSummaryFile(file: Path): Boolean = {
    file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
        file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
  }

  /**
   * Returns whether the reader will return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.parquetVectorizedReaderEnabled && conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    true
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    // For Parquet data source, `buildReader` already handles partition values appending. Here we
    // simply delegate to `buildReader`.
    buildReader(
      sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      ParquetSchemaConverter.checkFieldNames(requiredSchema).json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      ParquetSchemaConverter.checkFieldNames(requiredSchema).json)

    // We want to clear this temporary metadata from saving into Parquet file.
    // This metadata is only useful for detecting optional columns when pushdowning filters.
    val dataSchemaToWrite = StructType.removeMetadata(StructType.metadataKeyForOptionalField,
      requiredSchema).asInstanceOf[StructType]
    ParquetWriteSupport.setSchema(dataSchemaToWrite, hadoopConf)

    // Sets flags for `CatalystSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.conf.get(SQLConf.PARQUET_BINARY_AS_STRING))
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.conf.get(SQLConf.PARQUET_INT96_AS_TIMESTAMP))

    // Try to push down filters when filter push-down is enabled.
    val pushed =
      if (sparkSession.conf.get(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key).toBoolean) {
        filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(ParquetFilters.createFilter(requiredSchema, _))
          .reduceOption(FilterApi.and)
      } else {
        None
      }

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val enableVectorizedReader: Boolean =
      sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
      resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
    // Whole stage codegen (PhysicalRDD) is able to deal with batches directly
    val returningBatch = supportBatch(sparkSession, resultSchema)

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val fileSplit =
        new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)

      val split =
        new org.apache.parquet.hadoop.ParquetInputSplit(
          fileSplit.getPath,
          fileSplit.getStart,
          fileSplit.getStart + fileSplit.getLength,
          fileSplit.getLength,
          fileSplit.getLocations,
          null)

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      // Try to push down filters when filter push-down is enabled.
      // Notice: This push-down is RowGroups level, not individual records.
      if (pushed.isDefined) {
        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
      }
      val parquetReader = if (enableVectorizedReader) {
        val vectorizedReader = new VectorizedParquetRecordReader()
        vectorizedReader.initialize(split, hadoopAttemptContext)
        logDebug(s"Appending $partitionSchema ${file.partitionValues}")
        vectorizedReader.initBatch(partitionSchema, file.partitionValues)
        if (returningBatch) {
          vectorizedReader.enableReturningBatches()
        }
        vectorizedReader
      } else {
        logDebug(s"Falling back to parquet-mr")
        // ParquetRecordReader returns UnsafeRow
        val reader = pushed match {
          case Some(filter) =>
            new ParquetRecordReader[UnsafeRow](
              new ParquetReadSupport,
              FilterCompat.get(filter, null))
          case _ =>
            new ParquetRecordReader[UnsafeRow](new ParquetReadSupport)
        }
        reader.initialize(split, hadoopAttemptContext)
        reader
      }

      val iter = new RecordReaderIterator(parquetReader)

      // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
      if (parquetReader.isInstanceOf[VectorizedParquetRecordReader] &&
          enableVectorizedReader) {
        iter.asInstanceOf[Iterator[InternalRow]]
      } else {
        val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
        val joinedRow = new JoinedRow()
        val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

        // This is a horrible erasure hack...  if we type the iterator above, then it actually check
        // the type in next() and we get a class cast exception.  If we make that function return
        // Object, then we can defer the cast until later!
        if (partitionSchema.length == 0) {
          // There is no partition columns
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          iter.asInstanceOf[Iterator[InternalRow]]
            .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
        }
      }
    }
  }

  override def buildWriter(
      sqlContext: SQLContext,
      dataSchema: StructType,
      options: Map[String, String]): OutputWriterFactory = {
    new ParquetOutputWriterFactory(
      sqlContext.conf,
      dataSchema,
      sqlContext.sessionState.newHadoopConf(),
      options)
  }
}

/**
 * A factory for generating OutputWriters for writing parquet files. This implemented is different
 * from the [[ParquetOutputWriter]] as this does not use any [[OutputCommitter]]. It simply
 * writes the data to the path used to generate the output writer. Callers of this factory
 * has to ensure which files are to be considered as committed.
 */
private[parquet] class ParquetOutputWriterFactory(
    sqlConf: SQLConf,
    dataSchema: StructType,
    hadoopConf: Configuration,
    options: Map[String, String]) extends OutputWriterFactory {

  private val serializableConf: SerializableConfiguration = {
    val job = Job.getInstance(hadoopConf)
    val conf = ContextUtil.getConfiguration(job)
    val parquetOptions = new ParquetOptions(options, sqlConf)

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // We want to clear this temporary metadata from saving into Parquet file.
    // This metadata is only useful for detecting optional columns when pushing down filters.
    val dataSchemaToWrite = StructType.removeMetadata(
      StructType.metadataKeyForOptionalField,
      dataSchema).asInstanceOf[StructType]
    ParquetWriteSupport.setSchema(dataSchemaToWrite, conf)

    // Sets flags for `CatalystSchemaConverter` (which converts Catalyst schema to Parquet schema)
    // and `CatalystWriteSupport` (writing actual rows to Parquet files).
    conf.set(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sqlConf.isParquetBinaryAsString.toString)

    conf.set(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sqlConf.isParquetINT96AsTimestamp.toString)

    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sqlConf.writeLegacyParquetFormat.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodec)
    new SerializableConfiguration(conf)
  }

  /**
   * Returns a [[OutputWriter]] that writes data to the give path without using
   * [[OutputCommitter]].
   */
  override def newWriter(path: String): OutputWriter = new OutputWriter {

    // Create TaskAttemptContext that is used to pass on Configuration to the ParquetRecordWriter
    private val hadoopTaskAttemptId = new TaskAttemptID(new TaskID(new JobID, TaskType.MAP, 0), 0)
    private val hadoopAttemptContext = new TaskAttemptContextImpl(
      serializableConf.value, hadoopTaskAttemptId)

    // Instance of ParquetRecordWriter that does not use OutputCommitter
    private val recordWriter = createNoCommitterRecordWriter(path, hadoopAttemptContext)

    override def write(row: Row): Unit = {
      throw new UnsupportedOperationException("call writeInternal")
    }

    protected[sql] override def writeInternal(row: InternalRow): Unit = {
      recordWriter.write(null, row)
    }

    override def close(): Unit = recordWriter.close(hadoopAttemptContext)
  }

  /** Create a [[ParquetRecordWriter]] that writes the given path without using OutputCommitter */
  private def createNoCommitterRecordWriter(
      path: String,
      hadoopAttemptContext: TaskAttemptContext): RecordWriter[Void, InternalRow] = {
    // Custom ParquetOutputFormat that disable use of committer and writes to the given path
    val outputFormat = new ParquetOutputFormat[InternalRow]() {
      override def getOutputCommitter(c: TaskAttemptContext): OutputCommitter = { null }
      override def getDefaultWorkFile(c: TaskAttemptContext, ext: String): Path = { new Path(path) }
    }
    outputFormat.getRecordWriter(hadoopAttemptContext)
  }

  /** Disable the use of the older API. */
  def newInstance(
      path: String,
      bucketId: Option[Int],
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    throw new UnsupportedOperationException(
      "this version of newInstance not supported for " +
        "ParquetOutputWriterFactory")
  }
}


// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[parquet] class ParquetOutputWriter(
    path: String,
    bucketId: Option[Int],
    context: TaskAttemptContext)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, InternalRow] = {
    val outputFormat = {
      new ParquetOutputFormat[InternalRow]() {
        // Here we override `getDefaultWorkFile` for two reasons:
        //
        //  1. To allow appending.  We need to generate unique output file names to avoid
        //     overwriting existing files (either exist before the write job, or are just written
        //     by other tasks within the same write job).
        //
        //  2. To allow dynamic partitioning.  Default `getDefaultWorkFile` uses
        //     `FileOutputCommitter.getWorkPath()`, which points to the base directory of all
        //     partitions in the case of dynamic partitioning.
        override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
          val configuration = context.getConfiguration
          val uniqueWriteJobId = configuration.get(WriterContainer.DATASOURCE_WRITEJOBUUID)
          val taskAttemptId = context.getTaskAttemptID
          val split = taskAttemptId.getTaskID.getId
          val bucketString = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")
          // It has the `.parquet` extension at the end because (de)compression tools
          // such as gunzip would not be able to decompress this as the compression
          // is not applied on this whole file but on each "page" in Parquet format.
          new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$bucketString$extension")
        }
      }
    }

    outputFormat.getRecordWriter(context)
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override def writeInternal(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(context)
}

object ParquetFileFormat extends Logging {
  private[parquet] def readSchema(
      footers: Seq[Footer], sparkSession: SparkSession): Option[StructType] = {

    def parseParquetSchema(schema: MessageType): StructType = {
      val converter = new ParquetSchemaConverter(
        sparkSession.sessionState.conf.isParquetBinaryAsString,
        sparkSession.sessionState.conf.isParquetBinaryAsString,
        sparkSession.sessionState.conf.writeLegacyParquetFormat)

      converter.convert(schema)
    }

    val seen = mutable.HashSet[String]()
    val finalSchemas: Seq[StructType] = footers.flatMap { footer =>
      val metadata = footer.getParquetMetadata.getFileMetaData
      val serializedSchema = metadata
        .getKeyValueMetaData
        .asScala.toMap
        .get(ParquetReadSupport.SPARK_METADATA_KEY)
      if (serializedSchema.isEmpty) {
        // Falls back to Parquet schema if no Spark SQL schema found.
        Some(parseParquetSchema(metadata.getSchema))
      } else if (!seen.contains(serializedSchema.get)) {
        seen += serializedSchema.get

        // Don't throw even if we failed to parse the serialized Spark schema. Just fallback to
        // whatever is available.
        Some(Try(DataType.fromJson(serializedSchema.get))
          .recover { case _: Throwable =>
            logInfo(
              "Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
                "falling back to the deprecated DataType.fromCaseClassString parser.")
            LegacyTypeStringParser.parse(serializedSchema.get)
          }
          .recover { case cause: Throwable =>
            logWarning(
              s"""Failed to parse serialized Spark schema in Parquet key-value metadata:
                 |\t$serializedSchema
               """.stripMargin,
              cause)
          }
          .map(_.asInstanceOf[StructType])
          .getOrElse {
            // Falls back to Parquet schema if Spark SQL schema can't be parsed.
            parseParquetSchema(metadata.getSchema)
          })
      } else {
        None
      }
    }

    finalSchemas.reduceOption { (left, right) =>
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
  def mergeMetastoreParquetSchema(
      metastoreSchema: StructType,
      parquetSchema: StructType): StructType = {
    def schemaConflictMessage: String =
      s"""Converting Hive Metastore Parquet, but detected conflicting schemas. Metastore schema:
         |${metastoreSchema.prettyJson}
         |
         |Parquet schema:
         |${parquetSchema.prettyJson}
       """.stripMargin

    val mergedParquetSchema = mergeMissingNullableFields(metastoreSchema, parquetSchema)

    assert(metastoreSchema.size <= mergedParquetSchema.size, schemaConflictMessage)

    val ordinalMap = metastoreSchema.zipWithIndex.map {
      case (field, index) => field.name.toLowerCase -> index
    }.toMap

    val reorderedParquetSchema = mergedParquetSchema.sortBy(f =>
      ordinalMap.getOrElse(f.name.toLowerCase, metastoreSchema.size + 1))

    StructType(metastoreSchema.zip(reorderedParquetSchema).map {
      // Uses Parquet field names but retains Metastore data types.
      case (mSchema, pSchema) if mSchema.name.toLowerCase == pSchema.name.toLowerCase =>
        mSchema.copy(name = pSchema.name)
      case _ =>
        throw new SparkException(schemaConflictMessage)
    })
  }

  /**
   * Returns the original schema from the Parquet file with any missing nullable fields from the
   * Hive Metastore schema merged in.
   *
   * When constructing a DataFrame from a collection of structured data, the resulting object has
   * a schema corresponding to the union of the fields present in each element of the collection.
   * Spark SQL simply assigns a null value to any field that isn't present for a particular row.
   * In some cases, it is possible that a given table partition stored as a Parquet file doesn't
   * contain a particular nullable field in its schema despite that field being present in the
   * table schema obtained from the Hive Metastore. This method returns a schema representing the
   * Parquet file schema along with any additional nullable fields from the Metastore schema
   * merged in.
   */
  private[parquet] def mergeMissingNullableFields(
      metastoreSchema: StructType,
      parquetSchema: StructType): StructType = {
    val fieldMap = metastoreSchema.map(f => f.name.toLowerCase -> f).toMap
    val missingFields = metastoreSchema
      .map(_.name.toLowerCase)
      .diff(parquetSchema.map(_.name.toLowerCase))
      .map(fieldMap(_))
      .filter(_.nullable)
    StructType(parquetSchema ++ missingFields)
  }

  /**
   * Figures out a merged Parquet schema with a distributed Spark job.
   *
   * Note that locality is not taken into consideration here because:
   *
   *  1. For a single Parquet part-file, in most cases the footer only resides in the last block of
   *     that file.  Thus we only need to retrieve the location of the last block.  However, Hadoop
   *     `FileSystem` only provides API to retrieve locations of all blocks, which can be
   *     potentially expensive.
   *
   *  2. This optimization is mainly useful for S3, where file metadata operations can be pretty
   *     slow.  And basically locality is not available when using S3 (you can't run computation on
   *     S3 nodes).
   */
  def mergeSchemasInParallel(
      filesToTouch: Seq[FileStatus],
      sparkSession: SparkSession): Option[StructType] = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp
    val writeLegacyParquetFormat = sparkSession.sessionState.conf.writeLegacyParquetFormat
    val serializedConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf())
    val mergeSchema: (Seq[FileStatus]) => Option[StructType] = (files: Seq[FileStatus]) => {
      // Skips row group information since we only need the schema
      val skipRowGroups = true

      // Reads footers in multi-threaded manner within each task
      val footers =
        ParquetFileReader.readAllFootersInParallel(
          serializedConf.value, files.asJava, skipRowGroups).asScala

      // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
      val converter =
        new ParquetSchemaConverter(
          assumeBinaryIsString = assumeBinaryIsString,
          assumeInt96IsTimestamp = assumeInt96IsTimestamp,
          writeLegacyParquetFormat = writeLegacyParquetFormat)

      if (footers.isEmpty) {
        None
      } else {
        var mergedSchema = ParquetFileFormat.readSchemaFromFooter(footers.head, converter)
        footers.tail.foreach { footer =>
          val schema = ParquetFileFormat.readSchemaFromFooter(footer, converter)
          try {
            mergedSchema = mergedSchema.merge(schema)
          } catch { case cause: SparkException =>
            throw new SparkException(
              s"Failed merging schema of file ${footer.getFile}:\n${schema.treeString}", cause)
          }
        }
        Some(mergedSchema)
      }
    }

    if (filesToTouch.isEmpty || filesToTouch.tail.isEmpty) {
      mergeSchema(filesToTouch)
    } else {
      // !! HACK ALERT !!
      //
      // Parquet requires `FileStatus`es to read footers.  Here we try to send cached `FileStatus`es
      // to executor side to avoid fetching them again.  However, `FileStatus` is not `Serializable`
      // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
      // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
      // facts virtually prevents us to serialize `FileStatus`es.
      //
      // Since Parquet only relies on path and length information of those `FileStatus`es to read
      // footers, here we just extract them (which can be easily serialized), send them to executor
      // side, and resemble fake `FileStatus`es there.
      val partialFileStatusInfo = filesToTouch.map(f => (f.getPath.toString, f.getLen))

      // Set the number of partitions to prevent following schema reads from generating many tasks
      // in case of a small number of parquet files.
      val numParallelism = Math.min(Math.max(partialFileStatusInfo.size, 1),
        sparkSession.sparkContext.defaultParallelism)

      // Issues a Spark job to read Parquet schema in parallel.
      val partiallyMergedSchemas =
        sparkSession
          .sparkContext
          .parallelize(partialFileStatusInfo, numParallelism)
          .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
          }.toSeq
          mergeSchema(fakeFileStatuses).map(Iterator.single(_)).getOrElse(Iterator.empty)
        }.collect()

      if (partiallyMergedSchemas.isEmpty) {
        None
      } else {
        var finalSchema = partiallyMergedSchemas.head
        partiallyMergedSchemas.tail.foreach { schema =>
          try {
            finalSchema = finalSchema.merge(schema)
          } catch { case cause: SparkException =>
            throw new SparkException(
              s"Failed merging schema:\n${schema.treeString}", cause)
          }
        }
        Some(finalSchema)
      }
    }
  }

  /**
   * Reads Spark SQL schema from a Parquet footer.  If a valid serialized Spark SQL schema string
   * can be found in the file metadata, returns the deserialized [[StructType]], otherwise, returns
   * a [[StructType]] converted from the [[MessageType]] stored in this footer.
   */
  def readSchemaFromFooter(
      footer: Footer, converter: ParquetSchemaConverter): StructType = {
    val fileMetaData = footer.getParquetMetadata.getFileMetaData
    fileMetaData
      .getKeyValueMetaData
      .asScala.toMap
      .get(ParquetReadSupport.SPARK_METADATA_KEY)
      .flatMap(deserializeSchemaString)
      .getOrElse(converter.convert(fileMetaData.getSchema))
  }

  private def deserializeSchemaString(schemaString: String): Option[StructType] = {
    // Tries to deserialize the schema string as JSON first, then falls back to the case class
    // string parser (data generated by older versions of Spark SQL uses this format).
    Try(DataType.fromJson(schemaString).asInstanceOf[StructType]).recover {
      case _: Throwable =>
        logInfo(
          "Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
            "falling back to the deprecated DataType.fromCaseClassString parser.")
        LegacyTypeStringParser.parse(schemaString).asInstanceOf[StructType]
    }.recoverWith {
      case cause: Throwable =>
        logWarning(
          "Failed to parse and ignored serialized Spark schema in " +
            s"Parquet key-value metadata:\n\t$schemaString", cause)
        Failure(cause)
    }.toOption
  }

  // JUL loggers must be held by a strong reference, otherwise they may get destroyed by GC.
  // However, the root JUL logger used by Parquet isn't properly referenced.  Here we keep
  // references to loggers in both parquet-mr <= 1.6 and >= 1.7
  val apacheParquetLogger: JLogger = JLogger.getLogger(classOf[ApacheParquetLog].getPackage.getName)
  val parquetLogger: JLogger = JLogger.getLogger("parquet")

  // Parquet initializes its own JUL logger in a static block which always prints to stdout.  Here
  // we redirect the JUL logger via SLF4J JUL bridge handler.
  val redirectParquetLogsViaSLF4J: Unit = {
    def redirect(logger: JLogger): Unit = {
      logger.getHandlers.foreach(logger.removeHandler)
      logger.setUseParentHandlers(false)
      logger.addHandler(new SLF4JBridgeHandler)
    }

    // For parquet-mr 1.7.0 and above versions, which are under `org.apache.parquet` namespace.
    // scalastyle:off classforname
    Class.forName(classOf[ApacheParquetLog].getName)
    // scalastyle:on classforname
    redirect(JLogger.getLogger(classOf[ApacheParquetLog].getPackage.getName))

    // For parquet-mr 1.6.0 and lower versions bundled with Hive, which are under `parquet`
    // namespace.
    try {
      // scalastyle:off classforname
      Class.forName("parquet.Log")
      // scalastyle:on classforname
      redirect(JLogger.getLogger("parquet"))
    } catch { case _: Throwable =>
      // SPARK-9974: com.twitter:parquet-hadoop-bundle:1.6.0 is not packaged into the assembly
      // when Spark is built with SBT. So `parquet.Log` may not be found.  This try/catch block
      // should be removed after this issue is fixed.
    }
  }

  /**
   * ParquetFileFormat.prepareWrite calls this function to initialize `redirectParquetLogsViaSLF4J`.
   */
  def redirectParquetLogs(): Unit = {}
}
