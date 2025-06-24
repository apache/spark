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

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop._

import org.apache.spark.TaskContext
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{PATH, SCHEMA}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.vectorized.{ConstantColumnVector, OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

class ParquetFileFormat
  extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  override def shortName(): String = "parquet"

  override def toString: String = "Parquet"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[ParquetFileFormat]

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val sqlConf = sparkSession.sessionState.conf
    val parquetOptions = new ParquetOptions(options, sqlConf)
    ParquetUtils.prepareWrite(sqlConf, job, dataSchema, parquetOptions)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    ParquetUtils.inferSchema(sparkSession, parameters, files)
  }

  /**
   * Returns whether the reader can return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    ParquetUtils.isBatchReadSupportedForSchema(conf, schema)
  }

  override def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    Option(Seq.fill(requiredSchema.fields.length)(
      if (!sqlConf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ) ++ Seq.fill(partitionSchema.fields.length)(classOf[ConstantColumnVector].getName))
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    true
  }

  /**
   * Build the reader.
   *
   * @note It is required to pass FileFormat.OPTION_RETURNING_BATCH in options, to indicate whether
   *       the reader should return row or columnar output.
   *       If the caller can handle both, pass
   *       FileFormat.OPTION_RETURNING_BATCH ->
   *         supportBatch(sparkSession,
   *           StructType(requiredSchema.fields ++ partitionSchema.fields))
   *       as the option.
   *       It should be set to "true" only if this reader can support it.
   */
  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key,
      sparkSession.sessionState.conf.parquetInferTimestampNTZEnabled)
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sparkSession.sessionState.conf.legacyParquetNanosAsLong)


    val broadcastedHadoopConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)

    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val enableVectorizedReader: Boolean =
      ParquetUtils.isBatchReadSupportedForSchema(sqlConf, resultSchema)
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead

    // Should always be set by FileSourceScanExec creating this.
    // Check conf before checking option, to allow working around an issue by changing conf.
    val returningBatch = sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
      options.getOrElse(FileFormat.OPTION_RETURNING_BATCH,
        throw new IllegalArgumentException(
          "OPTION_RETURNING_BATCH should always be set for ParquetFileFormat. " +
            "To workaround this issue, set spark.sql.parquet.enableVectorizedReader=false."))
        .equals("true")
    if (returningBatch) {
      // If the passed option said that we are to return batches, we need to also be able to
      // do this based on config and resultSchema.
      assert(supportBatch(sparkSession, resultSchema))
    }

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val filePath = file.toPath
      val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

      val sharedConf = broadcastedHadoopConf.value.value

      val fileFooter = if (enableVectorizedReader) {
        // When there are vectorized reads, we can avoid reading the footer twice by reading
        // all row groups in advance and filter row groups according to filters that require
        // push down (no need to read the footer metadata again).
        ParquetFooterReader.readFooter(sharedConf, file, ParquetFooterReader.WITH_ROW_GROUPS)
      } else {
        ParquetFooterReader.readFooter(sharedConf, file, ParquetFooterReader.SKIP_ROW_GROUPS)
      }

      val footerFileMetaData = fileFooter.getFileMetaData
      val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get,
        datetimeRebaseModeInRead)
      val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)

      // Try to push down filters when filter push-down is enabled.
      val pushed = if (enableParquetFilterPushDown) {
        val parquetSchema = footerFileMetaData.getSchema
        val parquetFilters = new ParquetFilters(
          parquetSchema,
          pushDownDate,
          pushDownTimestamp,
          pushDownDecimal,
          pushDownStringPredicate,
          pushDownInFilterThreshold,
          isCaseSensitive,
          datetimeRebaseSpec)
        filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter(_))
          .reduceOption(FilterApi.and)
      } else {
        None
      }

      // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
      // *only* if the file was created by something other than "parquet-mr", so check the actual
      // writer here for this file.  We have to do this per-file, as each file in the table may
      // have different writers.
      // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
      def isCreatedByParquetMr: Boolean =
        footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

      val convertTz =
        if (timestampConversion && !isCreatedByParquetMr) {
          Some(DateTimeUtils.getZoneId(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
        } else {
          None
        }


      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      // Try to push down filters when filter push-down is enabled.
      // Notice: This push-down is RowGroups level, not individual records.
      if (pushed.isDefined) {
        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
      }
      val taskContext = Option(TaskContext.get())
      if (enableVectorizedReader) {
        val vectorizedReader = new VectorizedParquetRecordReader(
          convertTz.orNull,
          datetimeRebaseSpec.mode.toString,
          datetimeRebaseSpec.timeZone,
          int96RebaseSpec.mode.toString,
          int96RebaseSpec.timeZone,
          enableOffHeapColumnVector && taskContext.isDefined,
          capacity)
        // SPARK-37089: We cannot register a task completion listener to close this iterator here
        // because downstream exec nodes have already registered their listeners. Since listeners
        // are executed in reverse order of registration, a listener registered here would close the
        // iterator while downstream exec nodes are still running. When off-heap column vectors are
        // enabled, this can cause a use-after-free bug leading to a segfault.
        //
        // Instead, we use FileScanRDD's task completion listener to close this iterator.
        val iter = new RecordReaderIterator(vectorizedReader)
        try {
          vectorizedReader.initialize(split, hadoopAttemptContext, Option.apply(fileFooter))
          logDebug(s"Appending $partitionSchema ${file.partitionValues}")
          vectorizedReader.initBatch(partitionSchema, file.partitionValues)
          if (returningBatch) {
            vectorizedReader.enableReturningBatches()
          }

          // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
          iter.asInstanceOf[Iterator[InternalRow]]
        } catch {
          case e: Throwable =>
            // SPARK-23457: In case there is an exception in initialization, close the iterator to
            // avoid leaking resources.
            iter.close()
            throw e
        }
      } else {
        logDebug(s"Falling back to parquet-mr")
        // ParquetRecordReader returns InternalRow
        val readSupport = new ParquetReadSupport(
          convertTz,
          enableVectorizedReader = false,
          datetimeRebaseSpec,
          int96RebaseSpec)
        val reader = if (pushed.isDefined && enableRecordFilter) {
          val parquetFilter = FilterCompat.get(pushed.get, null)
          new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
        } else {
          new ParquetRecordReader[InternalRow](readSupport)
        }
        val readerWithRowIndexes = ParquetRowIndexUtil.addRowIndexToRecordReaderIfNeeded(reader,
            requiredSchema)
        val iter = new RecordReaderIterator[InternalRow](readerWithRowIndexes)
        try {
          readerWithRowIndexes.initialize(split, hadoopAttemptContext)

          val fullSchema = toAttributes(requiredSchema) ++ toAttributes(partitionSchema)
          val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

          if (partitionSchema.length == 0) {
            // There is no partition columns
            iter.map(unsafeProjection)
          } else {
            val joinedRow = new JoinedRow()
            iter.map(d => unsafeProjection(joinedRow(d, file.partitionValues)))
          }
        } catch {
          case e: Throwable =>
            // SPARK-23457: In case there is an exception in initialization, close the iterator to
            // avoid leaking resources.
            iter.close()
            throw e
        }
      }
    }
  }

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }

  override def metadataSchemaFields: Seq[StructField] = {
    super.metadataSchemaFields :+ ParquetFileFormat.ROW_INDEX_FIELD
  }
}

object ParquetFileFormat extends Logging {
  val ROW_INDEX = "row_index"

  // A name for a temporary column that holds row indexes computed by the file format reader
  // until they can be placed in the _metadata struct.
  val ROW_INDEX_TEMPORARY_COLUMN_NAME = s"_tmp_metadata_$ROW_INDEX"

  // The field readers can use to access the generated row index column.
  val ROW_INDEX_FIELD = FileSourceGeneratedMetadataStructField(
    ROW_INDEX, ROW_INDEX_TEMPORARY_COLUMN_NAME, LongType, nullable = false)

  private[parquet] def readSchema(
      footers: Seq[Footer], sparkSession: SparkSession): Option[StructType] = {

    val converter = new ParquetToSparkSchemaConverter(
      sparkSession.sessionState.conf.isParquetBinaryAsString,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp,
      inferTimestampNTZ = sparkSession.sessionState.conf.parquetInferTimestampNTZEnabled,
      nanosAsLong = sparkSession.sessionState.conf.legacyParquetNanosAsLong)

    val seen = mutable.HashSet[String]()
    val finalSchemas: Seq[StructType] = footers.flatMap { footer =>
      val metadata = footer.getParquetMetadata.getFileMetaData
      val serializedSchema = metadata
        .getKeyValueMetaData
        .asScala.toMap
        .get(ParquetReadSupport.SPARK_METADATA_KEY)
      if (serializedSchema.isEmpty) {
        // Falls back to Parquet schema if no Spark SQL schema found.
        Some(converter.convert(metadata.getSchema))
      } else if (!seen.contains(serializedSchema.get)) {
        seen += serializedSchema.get

        // Don't throw even if we failed to parse the serialized Spark schema. Just fallback to
        // whatever is available.
        Some(Try(DataType.fromJson(serializedSchema.get))
          .recover { case _: Throwable =>
            logInfo(
              "Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
                "falling back to the deprecated DataType.fromCaseClassString parser.")
            LegacyTypeStringParser.parseString(serializedSchema.get)
          }
          .recover { case cause: Throwable =>
            logWarning(
              log"""Failed to parse serialized Spark schema in Parquet key-value metadata:
                 |\t${MDC(SCHEMA, serializedSchema)}
               """.stripMargin,
              cause)
          }
          .map(_.asInstanceOf[StructType])
          .getOrElse {
            // Falls back to Parquet schema if Spark SQL schema can't be parsed.
            converter.convert(metadata.getSchema)
          })
      } else {
        None
      }
    }

    finalSchemas.reduceOption { (left, right) =>
      try left.merge(right) catch { case e: Throwable =>
        throw QueryExecutionErrors.failedToMergeIncompatibleSchemasError(left, right, e)
      }
    }
  }

  /**
   * Reads Parquet footers in multi-threaded manner.
   * If the config "spark.sql.files.ignoreCorruptFiles" is set to true, we will ignore the corrupted
   * files when reading footers.
   */
  private[parquet] def readParquetFootersInParallel(
      conf: Configuration,
      partFiles: Seq[FileStatus],
      ignoreCorruptFiles: Boolean): Seq[Footer] = {
    ThreadUtils.parmap(partFiles, "readingParquetFooters", 8) { currentFile =>
      try {
        // Skips row group information since we only need the schema.
        // ParquetFileReader.readFooter throws RuntimeException, instead of IOException,
        // when it can't read the footer.
        Some(new Footer(currentFile.getPath(),
          ParquetFooterReader.readFooter(
            conf, currentFile, SKIP_ROW_GROUPS)))
      } catch { case e: RuntimeException =>
        if (ignoreCorruptFiles) {
          logWarning(log"Skipped the footer in the corrupted file: ${MDC(PATH, currentFile)}", e)
          None
        } else {
          throw QueryExecutionErrors.cannotReadFooterForFileError(currentFile.getPath, e)
        }
      }
    }.flatten
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
      parameters: Map[String, String],
      filesToTouch: Seq[FileStatus],
      sparkSession: SparkSession): Option[StructType] = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp
    val inferTimestampNTZ = sparkSession.sessionState.conf.parquetInferTimestampNTZEnabled
    val nanosAsLong = sparkSession.sessionState.conf.legacyParquetNanosAsLong

    val reader = (files: Seq[FileStatus], conf: Configuration, ignoreCorruptFiles: Boolean) => {
      // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
      val converter = new ParquetToSparkSchemaConverter(
        assumeBinaryIsString = assumeBinaryIsString,
        assumeInt96IsTimestamp = assumeInt96IsTimestamp,
        inferTimestampNTZ = inferTimestampNTZ,
        nanosAsLong = nanosAsLong)

      readParquetFootersInParallel(conf, files, ignoreCorruptFiles)
        .map(ParquetFileFormat.readSchemaFromFooter(_, converter))
    }

    SchemaMergeUtils.mergeSchemasInParallel(sparkSession, parameters, filesToTouch, reader)
  }

  /**
   * Reads Spark SQL schema from a Parquet footer.  If a valid serialized Spark SQL schema string
   * can be found in the file metadata, returns the deserialized [[StructType]], otherwise, returns
   * a [[StructType]] converted from the [[org.apache.parquet.schema.MessageType]] stored in this
   * footer.
   */
  def readSchemaFromFooter(
      footer: Footer, converter: ParquetToSparkSchemaConverter): StructType = {
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
        LegacyTypeStringParser.parseString(schemaString).asInstanceOf[StructType]
    }.recoverWith {
      case cause: Throwable =>
        logWarning(
          log"Failed to parse and ignored serialized Spark schema in " +
            log"Parquet key-value metadata:\n\t${MDC(SCHEMA, schemaString)}", cause)
        Failure(cause)
    }.toOption
  }
}
