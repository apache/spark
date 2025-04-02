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
package org.apache.spark.sql.execution.datasources.v2.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile, Reader, TypeDescription}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitionedFile}
import org.apache.spark.sql.execution.datasources.orc.{OrcColumnarBatchReader, OrcDeserializer, OrcFilters, OrcOptions, OrcUtils}
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * A factory used to create Orc readers.
 *
 * @param sqlConf SQL configuration.
 * @param broadcastedConf Broadcast serializable Hadoop Configuration.
 * @param dataSchema Schema of orc files.
 * @param readDataSchema Required data schema in the batch scan.
 * @param partitionSchema Schema of partitions.
 * @param options Options for parsing ORC files.
 */
case class OrcPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    aggregation: Option[Aggregation],
    options: OrcOptions,
    memoryMode: MemoryMode) extends FilePartitionReaderFactory {
  private val resultSchema = StructType(readDataSchema.fields ++ partitionSchema.fields)
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val capacity = sqlConf.orcVectorizedReaderBatchSize
  private val orcFilterPushDown = sqlConf.orcFilterPushDown

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    sqlConf.orcVectorizedReaderEnabled && sqlConf.wholeStageEnabled &&
      !WholeStageCodegenExec.isTooManyFields(sqlConf, resultSchema) &&
      resultSchema.forall(s => OrcUtils.supportColumnarReads(
        s.dataType, sqlConf.orcVectorizedReaderNestedColumnEnabled))
  }

  private def pushDownPredicates(orcSchema: TypeDescription, conf: Configuration): Unit = {
    if (orcFilterPushDown && filters.nonEmpty) {
      val fileSchema = OrcUtils.toCatalystSchema(orcSchema)
      OrcFilters.createFilter(fileSchema, filters.toImmutableArraySeq).foreach { f =>
        OrcInputFormat.setSearchArgument(conf, f, fileSchema.fieldNames)
      }
    }
  }

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    if (aggregation.nonEmpty) {
      return buildReaderWithAggregates(file, conf)
    }
    val filePath = file.toPath

    val orcSchema = Utils.tryWithResource(createORCReader(filePath, conf)._1)(_.getSchema)
    val resultedColPruneInfo = OrcUtils.requestedColumnIds(
      isCaseSensitive, dataSchema, readDataSchema, orcSchema, conf)

    if (resultedColPruneInfo.isEmpty) {
      new EmptyPartitionReader[InternalRow]
    } else {
      val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
      OrcUtils.orcResultSchemaString(canPruneCols, dataSchema, resultSchema, partitionSchema, conf)
      assert(requestedColIds.length == readDataSchema.length,
        "[BUG] requested column IDs do not match required schema")

      val taskConf = new Configuration(conf)

      val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

      val orcRecordReader = new OrcInputFormat[OrcStruct]
        .createRecordReader(fileSplit, taskAttemptContext)
      val deserializer = new OrcDeserializer(readDataSchema, requestedColIds)
      val fileReader = new PartitionReader[InternalRow] {
        override def next(): Boolean = orcRecordReader.nextKeyValue()

        override def get(): InternalRow = deserializer.deserialize(orcRecordReader.getCurrentValue)

        override def close(): Unit = orcRecordReader.close()
      }

      new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
        partitionSchema, file.partitionValues)
    }
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    if (aggregation.nonEmpty) {
      return buildColumnarReaderWithAggregates(file, conf)
    }
    val filePath = file.toPath
    lazy val (reader, readerOptions) = createORCReader(filePath, conf)
    val orcSchema = Utils.tryWithResource(reader)(_.getSchema)
    val resultedColPruneInfo = OrcUtils.requestedColumnIds(
      isCaseSensitive, dataSchema, readDataSchema, orcSchema, conf)

    if (resultedColPruneInfo.isEmpty) {
      new EmptyPartitionReader
    } else {
      val (requestedDataColIds, canPruneCols) = resultedColPruneInfo.get
      val resultSchemaString = OrcUtils.orcResultSchemaString(canPruneCols,
        dataSchema, resultSchema, partitionSchema, conf)
      val requestedColIds = requestedDataColIds ++ Array.fill(partitionSchema.length)(-1)
      assert(requestedColIds.length == resultSchema.length,
        "[BUG] requested column IDs do not match required schema")
      val taskConf = new Configuration(conf)

      val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

      val batchReader = new OrcColumnarBatchReader(capacity, memoryMode)
      batchReader.initialize(fileSplit, taskAttemptContext, readerOptions.getOrcTail)
      val requestedPartitionColIds =
        Array.fill(readDataSchema.length)(-1) ++ Range(0, partitionSchema.length)

      batchReader.initBatch(
        TypeDescription.fromString(resultSchemaString),
        resultSchema.fields,
        requestedColIds,
        requestedPartitionColIds,
        file.partitionValues)
      new PartitionRecordReader(batchReader)
    }
  }

  private def createORCReader(
      filePath: Path,
      conf: Configuration): (Reader, OrcFile.ReaderOptions) = {
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)

    val fs = filePath.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    val reader = OrcFile.createReader(filePath, readerOptions)

    pushDownPredicates(reader.getSchema, conf)

    (reader, readerOptions)
  }

  /**
   * Build reader with aggregate push down.
   */
  private def buildReaderWithAggregates(
      file: PartitionedFile,
      conf: Configuration): PartitionReader[InternalRow] = {
    val filePath = file.toPath
    new PartitionReader[InternalRow] {
      private var hasNext = true
      private lazy val row: InternalRow = {
        Utils.tryWithResource(createORCReader(filePath, conf)._1) { reader =>
          OrcUtils.createAggInternalRowFromFooter(
            reader, filePath.toString, dataSchema, partitionSchema, aggregation.get,
            readDataSchema, file.partitionValues)
        }
      }

      override def next(): Boolean = hasNext

      override def get(): InternalRow = {
        hasNext = false
        row
      }

      override def close(): Unit = {}
    }
  }

  /**
   * Build columnar reader with aggregate push down.
   */
  private def buildColumnarReaderWithAggregates(
      file: PartitionedFile,
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val filePath = file.toPath
    new PartitionReader[ColumnarBatch] {
      private var hasNext = true
      private lazy val batch: ColumnarBatch = {
        Utils.tryWithResource(createORCReader(filePath, conf)._1) { reader =>
          val row = OrcUtils.createAggInternalRowFromFooter(
            reader, filePath.toString, dataSchema, partitionSchema, aggregation.get,
            readDataSchema, file.partitionValues)
          AggregatePushDownUtils.convertAggregatesRowToBatch(row, readDataSchema, offHeap = false)
        }
      }

      override def next(): Boolean = hasNext

      override def get(): ColumnarBatch = {
        hasNext = false
        batch
      }

      override def close(): Unit = {}
    }
  }
}
