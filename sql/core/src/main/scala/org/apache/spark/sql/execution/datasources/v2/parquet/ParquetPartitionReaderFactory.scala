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

import java.time.ZoneId
import java.util.Optional

import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, Utils}

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
 * @param options The options of Parquet datasource that are set for the read.
 */
case class ParquetPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    aggregation: Option[Aggregation],
    options: ParquetOptions) extends FilePartitionReaderFactory with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val resultSchema = StructType(partitionSchema.fields ++ readDataSchema.fields)
  private val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
  private val enableVectorizedReader: Boolean =
    ParquetUtils.isBatchReadSupportedForSchema(sqlConf, resultSchema)
  private val supportsColumnar = enableVectorizedReader && sqlConf.wholeStageEnabled &&
    !WholeStageCodegenExec.isTooManyFields(sqlConf, resultSchema)
  private val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
  private val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
  private val capacity = sqlConf.parquetVectorizedReaderBatchSize
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val datetimeRebaseModeInRead = options.datetimeRebaseModeInRead
  private val int96RebaseModeInRead = options.int96RebaseModeInRead

  private val parquetReaderCallback = new ParquetReaderCallback()

  private def openFileAndReadFooter(file: PartitionedFile): OpenedParquetFooter = {
    val hadoopConf = broadcastedConf.value.value
    if (aggregation.isDefined) {
      val inputFile = HadoopInputFile.fromPath(file.toPath, hadoopConf)
      // When there are aggregates to push down, we get max/min/count from footer statistics.
      val footer = ParquetFooterReader.readFooter(
        inputFile,
        ParquetFooterReader.buildFilter(hadoopConf, file, false))
      new OpenedParquetFooter(footer, inputFile, Optional.empty)
    } else {
      // When there are vectorized reads, we can avoid
      // 1. opening the file twice by transfering the SeekableInputStream
      // 2. reading the footer twice by reading all row groups in advance and filter row groups
      //    according to filters that require push down
      ParquetFooterReader.openFileAndReadFooter(hadoopConf, file, enableVectorizedReader)
    }
  }

  private def getDatetimeRebaseSpec(
      footerFileMetaData: FileMetaData): RebaseSpec = {
    DataSourceUtils.datetimeRebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    supportsColumnar
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
          val openedFooter = openFileAndReadFooter(file)
          assert(openedFooter.inputStreamOpt.isEmpty)

          if (openedFooter.footer != null && openedFooter.footer.getBlocks.size > 0) {
            ParquetUtils.createAggInternalRowFromFooter(openedFooter.footer,
              file.urlEncodedPath, dataSchema, partitionSchema, aggregation.get,
              readDataSchema, file.partitionValues,
              getDatetimeRebaseSpec(openedFooter.footer.getFileMetaData))
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
        private val batch: ColumnarBatch = {
          val openedFooter = openFileAndReadFooter(file)
          assert(openedFooter.inputStreamOpt.isEmpty)

          if (openedFooter.footer != null && openedFooter.footer.getBlocks.size > 0) {
            val row = ParquetUtils.createAggInternalRowFromFooter(openedFooter.footer,
              file.urlEncodedPath, dataSchema, partitionSchema, aggregation.get,
              readDataSchema, file.partitionValues,
              getDatetimeRebaseSpec(openedFooter.footer.getFileMetaData))
            AggregatePushDownUtils.convertAggregatesRowToBatch(
              row, readDataSchema, enableOffHeapColumnVector && Option(TaskContext.get()).isDefined)
          } else {
            null
          }
        }

        override def next(): Boolean = {
          hasNext && batch != null
        }

        override def get(): ColumnarBatch = {
          hasNext = false
          batch
        }

        override def close(): Unit = {}
      }
    }
    fileReader
  }

  private def buildReaderBase[T](
      file: PartitionedFile,
      buildReaderFunc: (
        InternalRow,
          Option[FilterPredicate], Option[ZoneId],
          RebaseSpec,
          RebaseSpec) => RecordReader[Void, T]): RecordReader[Void, T] = {
    val conf = broadcastedConf.value.value

    val split = new FileSplit(file.toPath, file.start, file.length, Array.empty[String])
    val openedFooter = openFileAndReadFooter(file)
    assert {
      openedFooter.inputStreamOpt.isPresent == (aggregation.isEmpty && enableVectorizedReader)
    }

    // Before transferring the ownership of inputStream to the vectorizedReader,
    // we must take responsibility to close the inputStream if something goes wrong
    // to avoid resource leak.
    var shouldCloseInputStream = openedFooter.inputStreamOpt.isPresent
    try {
      val footerFileMetaData = openedFooter.footer.getFileMetaData
      val datetimeRebaseSpec = getDatetimeRebaseSpec(footerFileMetaData)
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
        footerFileMetaData.getCreatedBy.startsWith("parquet-mr")

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
      pushed.foreach {
        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, _)
      }
      val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get,
        int96RebaseModeInRead)
      Utils.createResourceUninterruptiblyIfInTaskThread {
        Utils.tryInitializeResource(
          buildReaderFunc(
            file.partitionValues,
            pushed,
            convertTz,
            datetimeRebaseSpec,
            int96RebaseSpec)
        ) { reader =>
          reader match {
            case vectorizedReader: VectorizedParquetRecordReader =>
              vectorizedReader.initialize(
                split, hadoopAttemptContext, Some(openedFooter.inputFile),
                Some(openedFooter.inputStream), Some(openedFooter.footer))
              // We don't need to take care of the close of inputStream after calling `initialize`
              // because the ownership of inputStream has been transferred to the vectorizedReader
              shouldCloseInputStream = false
            case _ =>
              reader.initialize(split, hadoopAttemptContext)
          }
          reader
        }
      }
    } finally {
      if (shouldCloseInputStream) {
        openedFooter.inputStreamOpt.ifPresent(Utils.closeQuietly)
      }
    }
  }

  private def createRowBaseReader(file: PartitionedFile): RecordReader[Void, InternalRow] = {
    buildReaderBase(file, createRowBaseParquetReader)
  }

  private def createRowBaseParquetReader(
      partitionValues: InternalRow,
      pushed: Option[FilterPredicate],
      convertTz: Option[ZoneId],
      datetimeRebaseSpec: RebaseSpec,
      int96RebaseSpec: RebaseSpec): RecordReader[Void, InternalRow] = {
    logDebug(s"Falling back to parquet-mr")
    val taskContext = Option(TaskContext.get())
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
    val readerWithRowIndexes = ParquetRowIndexUtil.addRowIndexToRecordReaderIfNeeded(
      reader, readDataSchema)
    val iter = new RecordReaderIterator(readerWithRowIndexes)
    // SPARK-23457 Register a task completion listener before `initialization`.
    parquetReaderCallback.advanceFile(iter)
    taskContext.foreach(parquetReaderCallback.initIfNotAlready)
    readerWithRowIndexes
  }

  private def createVectorizedReader(file: PartitionedFile): VectorizedParquetRecordReader = {
    val vectorizedReader = buildReaderBase(file, createParquetVectorizedReader)
      .asInstanceOf[VectorizedParquetRecordReader]
    vectorizedReader.initBatch(partitionSchema, file.partitionValues)
    vectorizedReader
  }

  private def createParquetVectorizedReader(
      partitionValues: InternalRow,
      pushed: Option[FilterPredicate],
      convertTz: Option[ZoneId],
      datetimeRebaseSpec: RebaseSpec,
      int96RebaseSpec: RebaseSpec): VectorizedParquetRecordReader = {
    val taskContext = Option(TaskContext.get())
    val vectorizedReader = new VectorizedParquetRecordReader(
      convertTz.orNull,
      datetimeRebaseSpec.mode.toString,
      datetimeRebaseSpec.timeZone,
      int96RebaseSpec.mode.toString,
      int96RebaseSpec.timeZone,
      enableOffHeapColumnVector && taskContext.isDefined,
      capacity)
    val iter = new RecordReaderIterator(vectorizedReader)
    // SPARK-23457 Register a task completion listener before `initialization`.
    parquetReaderCallback.advanceFile(iter)
    taskContext.foreach(parquetReaderCallback.initIfNotAlready)
    logDebug(s"Appending $partitionSchema $partitionValues")
    vectorizedReader
  }
}

/**
 * A callback class to handle the cleanup of Parquet readers.
 *
 * This class is used to ensure that the Parquet readers are closed properly when the task
 * completes, and it also allows for the initialization of the reader callback only once per task.
 */
private class ParquetReaderCallback extends Serializable {
  private var init: Boolean = false
  private var iter: RecordReaderIterator[_] = null

  def initIfNotAlready(taskContext: TaskContext): Unit = {
    if (!init) {
      taskContext.addTaskCompletionListener[Unit](_ => closeCurrent())
      init = true
    }
  }

  def advanceFile(iter: RecordReaderIterator[_]): Unit = {
    closeCurrent()

    this.iter = iter
  }

  def closeCurrent(): Unit = {
    if (iter != null) {
      iter.close()
    }
  }
}
