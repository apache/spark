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
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.Aggregation
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
 * @aggSchema Schema of the pushed down aggregation.
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
    aggSchema: StructType,
    filters: Array[Filter],
    aggregation: Aggregation,
    parquetOptions: ParquetOptions) extends FilePartitionReaderFactory with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val newReadDataSchema = if (aggregation.aggregateExpressions.isEmpty) {
    readDataSchema
  } else {
    aggSchema
  }
  private val resultSchema = StructType(partitionSchema.fields ++ newReadDataSchema.fields)
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

    if (aggregation.aggregateExpressions.isEmpty) {
      ParquetFooterReader.readFooter(conf, filePath, SKIP_ROW_GROUPS)
    } else {
      ParquetFooterReader.readFooter(conf, filePath, NO_FILTER)
    }
  }

  // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
  private def isCreatedByParquetMr(file: PartitionedFile): Boolean =
    getFooter(file).getFileMetaData.getCreatedBy().startsWith("parquet-mr")

  private def convertTz(isCreatedByParquetMr: Boolean): Option[ZoneId] =
    if (timestampConversion && !isCreatedByParquetMr) {
      Some(DateTimeUtils
        .getZoneId(broadcastedConf.value.value.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
    } else {
      None
    }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    sqlConf.parquetVectorizedReaderEnabled && sqlConf.wholeStageEnabled &&
      resultSchema.length <= sqlConf.wholeStageMaxNumFields &&
      resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val fileReader = if (aggregation.aggregateExpressions.isEmpty) {

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
        var count = 0

        override def next(): Boolean = if (count == 0) true else false

        override def get(): InternalRow = {
          count += 1
          val footer = getFooter(file)
          ParquetUtils.createInternalRowFromAggResult(footer, dataSchema, aggregation, aggSchema,
            datetimeRebaseModeInRead, int96RebaseModeInRead, convertTz(isCreatedByParquetMr(file)))
        }

        override def close(): Unit = return
      }
    }

    new PartitionReaderWithPartitionValues(fileReader, newReadDataSchema,
      partitionSchema, file.partitionValues)
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val fileReader = if (aggregation.aggregateExpressions.isEmpty) {
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
        var count = 0

        override def next(): Boolean = if (count == 0) true else false

        override def get(): ColumnarBatch = {
          count += 1
          val footer = getFooter(file)
          ParquetUtils.createColumnarBatchFromAggResult(footer, dataSchema, aggregation, aggSchema,
            enableOffHeapColumnVector, datetimeRebaseModeInRead, int96RebaseModeInRead,
            convertTz(isCreatedByParquetMr(file)))
        }

        override def close(): Unit = return
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
    // Try to push down filters when filter push-down is enabled.
    val pushed = if (enableParquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
        pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
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

    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
    val int96RebaseMode = DataSourceUtils.int96RebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      int96RebaseModeInRead)
    val reader = buildReaderFunc(
      split,
      file.partitionValues,
      hadoopAttemptContext,
      pushed,
      convertTz(isCreatedByParquetMr(file)),
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
