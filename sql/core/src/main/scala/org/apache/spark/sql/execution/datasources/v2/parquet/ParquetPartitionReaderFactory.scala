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
package org.apache.spark.sql.execution.datasources.v2.parquet

import java.net.URI
import java.time.ZoneId

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter.{NO_FILTER, SKIP_ROW_GROUPS}
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * A factory used to create Parquet readers.
 *
 * @param sqlConf SQL configuration.
 * @param broadcastedConf Broadcast serializable Hadoop Configuration.
 * @param dataSchema Schema of Parquet files.
 * @param readDataSchema Required schema of Parquet files.
 * @param partitionSchema Schema of partitions.
 * @param filters Filters to be pushed down in the batch scan.
 * @param aggregation Aggregation to be pushed down in the batch scan.
 * @param parquetOptions The options of Parquet datasource that are set for the read.
 */
case class ParquetPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    aggregation: Option[Aggregation],
    parquetOptions: ParquetOptions) extends FilePartitionReaderFactory with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val resultSchema = StructType(partitionSchema.fields ++ readDataSchema.fields)
  private val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
  private val enableVectorizedReader: Boolean = sqlConf.parquetVectorizedReaderEnabled &&
    resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
  private val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
  private val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
  private val capacity = sqlConf.parquetVectorizedReaderBatchSize
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
  private val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead

  private def getFooter(file: PartitionedFile): ParquetMetadata = {
    val conf = broadcastedConf.value.value
    val filePath = new Path(new URI(file.filePath))

    if (aggregation.isEmpty) {
      ParquetFooterReader.readFooter(conf, filePath, SKIP_ROW_GROUPS)
    } else {
      // For aggregate push down, we will get max/min/count from footer statistics.
      // We want to read the footer for the whole file instead of reading multiple
      // footers for every split of the file. Basically if the start (the beginning of)
      // the offset in PartitionedFile is 0, we will read the footer. Otherwise, it means
      // that we have already read footer for that file, so we will skip reading again.
      if (file.start != 0) return null
      ParquetFooterReader.readFooter(conf, filePath, NO_FILTER)
    }
  }

  private def getDatetimeRebaseMode(
      footerFileMetaData: FileMetaData): LegacyBehaviorPolicy.Value = {
    DataSourceUtils.datetimeRebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    sqlConf.parquetVectorizedReaderEnabled && sqlConf.wholeStageEnabled &&
      resultSchema.length <= sqlConf.wholeStageMaxNumFields &&
      resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val fileReader = if (aggregation.isEmpty) {
      val reader = if (enableVectorizedReader) {
        createVectorizedReader(file)
      } else {
        createRowBaseReader(file)
      }

      new PartitionReader[InternalRow] {
        override def next(): Boolean = reader.nextKeyValue()

        override def get(): InternalRow = reader.getCurrentValue.asInstanceOf[InternalRow]

        override def close(): Unit = reader.close()
      }
    } else {
      new PartitionReader[InternalRow] {
        private var hasNext = true
        private lazy val row: InternalRow = {
          val footer = getFooter(file)
          if (footer != null && footer.getBlocks.size > 0) {
            ParquetUtils.createAggInternalRowFromFooter(footer, file.filePath, dataSchema,
              partitionSchema, aggregation.get, readDataSchema,
              getDatetimeRebaseMode(footer.getFileMetaData), isCaseSensitive)
          } else {
            null
          }
        }
        override def next(): Boolean = {
          hasNext && row != null
        }

        override def get(): InternalRow = {
          hasNext = false
          row
        }

        override def close(): Unit = {}
      }
    }

    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val fileReader = if (aggregation.isEmpty) {
      val vectorizedReader = createVectorizedReader(file)
      vectorizedReader.enableReturningBatches()

      new PartitionReader[ColumnarBatch] {
        override def next(): Boolean = vectorizedReader.nextKeyValue()

        override def get(): ColumnarBatch =
          vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]

        override def close(): Unit = vectorizedReader.close()
      }
    } else {
      new PartitionReader[ColumnarBatch] {
        private var hasNext = true
        private val row: ColumnarBatch = {
          val footer = getFooter(file)
          if (footer != null && footer.getBlocks.size > 0) {
            ParquetUtils.createAggColumnarBatchFromFooter(footer, file.filePath, dataSchema,
              partitionSchema, aggregation.get, readDataSchema, enableOffHeapColumnVector,
              getDatetimeRebaseMode(footer.getFileMetaData), isCaseSensitive)
          } else {
            null
          }
        }

        override def next(): Boolean = {
          hasNext && row != null
        }

        override def get(): ColumnarBatch = {
          hasNext = false
          row
        }

        override def close(): Unit = {}
      }
    }
    fileReader
  }

  private def buildReaderBase[T](
      file: PartitionedFile,
      buildReaderFunc: (
        FileSplit, InternalRow, TaskAttemptContextImpl,
          Option[FilterPredicate], Option[ZoneId],
          LegacyBehaviorPolicy.Value,
          LegacyBehaviorPolicy.Value) => RecordReader[Void, T]): RecordReader[Void, T] = {
    val conf = broadcastedConf.value.value

    val filePath = new Path(new URI(file.filePath))
    val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

    lazy val footerFileMetaData = getFooter(file).getFileMetaData
    val datetimeRebaseMode = getDatetimeRebaseMode(footerFileMetaData)
    // Try to push down filters when filter push-down is enabled.
    val pushed = if (enableParquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = new ParquetFilters(
        parquetSchema,
        pushDownDate,
        pushDownTimestamp,
        pushDownDecimal,
        pushDownStringStartWith,
        pushDownInFilterThreshold,
        isCaseSensitive,
        datetimeRebaseMode)
      filters
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        .flatMap(parquetFilters.createFilter)
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
        Some(DateTimeUtils.getZoneId(conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
      } else {
        None
      }

    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val int96RebaseMode = DataSourceUtils.int96RebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      int96RebaseModeInRead)
    val reader = buildReaderFunc(
      split,
      file.partitionValues,
      hadoopAttemptContext,
      pushed,
      convertTz,
      datetimeRebaseMode,
      int96RebaseMode)
    reader.initialize(split, hadoopAttemptContext)
    reader
  }

  private def createRowBaseReader(file: PartitionedFile): RecordReader[Void, InternalRow] = {
    buildReaderBase(file, createRowBaseParquetReader)
  }

  private def createRowBaseParquetReader(
      split: FileSplit,
      partitionValues: InternalRow,
      hadoopAttemptContext: TaskAttemptContextImpl,
      pushed: Option[FilterPredicate],
      convertTz: Option[ZoneId],
      datetimeRebaseMode: LegacyBehaviorPolicy.Value,
      int96RebaseMode: LegacyBehaviorPolicy.Value): RecordReader[Void, InternalRow] = {
    logDebug(s"Falling back to parquet-mr")
    val taskContext = Option(TaskContext.get())
    // ParquetRecordReader returns InternalRow
    val readSupport = new ParquetReadSupport(
      convertTz,
      enableVectorizedReader = false,
      datetimeRebaseMode,
      int96RebaseMode)
    val reader = if (pushed.isDefined && enableRecordFilter) {
      val parquetFilter = FilterCompat.get(pushed.get, null)
      new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
    } else {
      new ParquetRecordReader[InternalRow](readSupport)
    }
    val iter = new RecordReaderIterator(reader)
    // SPARK-23457 Register a task completion listener before `initialization`.
    taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
    reader
  }

  private def createVectorizedReader(file: PartitionedFile): VectorizedParquetRecordReader = {
    val vectorizedReader = buildReaderBase(file, createParquetVectorizedReader)
      .asInstanceOf[VectorizedParquetRecordReader]
    vectorizedReader.initBatch(partitionSchema, file.partitionValues)
    vectorizedReader
  }

  private def createParquetVectorizedReader(
      split: FileSplit,
      partitionValues: InternalRow,
      hadoopAttemptContext: TaskAttemptContextImpl,
      pushed: Option[FilterPredicate],
      convertTz: Option[ZoneId],
      datetimeRebaseMode: LegacyBehaviorPolicy.Value,
      int96RebaseMode: LegacyBehaviorPolicy.Value): VectorizedParquetRecordReader = {
    val taskContext = Option(TaskContext.get())
    val vectorizedReader = new VectorizedParquetRecordReader(
      convertTz.orNull,
      datetimeRebaseMode.toString,
      int96RebaseMode.toString,
      enableOffHeapColumnVector && taskContext.isDefined,
      capacity)
    val iter = new RecordReaderIterator(vectorizedReader)
    // SPARK-23457 Register a task completion listener before `initialization`.
    taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
    logDebug(s"Appending $partitionSchema $partitionValues")
    vectorizedReader
  }
}
