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

import java.net.URI
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.{OrcColumnarBatchReader, OrcDeserializer, OrcFilters, OrcUtils}
import org.apache.spark.sql.execution.datasources.v2.ColumnarBatchFileSourceReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.util.SerializableConfiguration

class OrcDataSourceV2 extends DataSourceV2 with ReadSupport with ReadSupportWithSchema {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new OrcDataSourceReader(options, None)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new OrcDataSourceReader(options, Some(schema))
  }
}

case class OrcDataSourceReader(options: DataSourceOptions, userSpecifiedSchema: Option[StructType])
  extends ColumnarBatchFileSourceReader
  with SupportsPushDownCatalystFilters {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    OrcUtils.readSchema(sparkSession, files)
  }

  private var pushedFiltersArray: Array[Expression] = Array.empty

  override def readFunction: PartitionedFile => Iterator[InternalRow] = {
    val capacity = sqlConf.orcVectorizedReaderBatchSize
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val copyToSpark = sqlConf.getConf(SQLConf.ORC_COPY_BATCH_TO_SPARK)
    val isCaseSensitive = this.isCaseSensitive
    val dataSchema = this.dataSchema
    val readSchema = this.readSchema()
    val partitionSchema = this.partitionSchema
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val enableVectorizedReader = enableBatchRead()
    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value

      val filePath = new Path(new URI(file.filePath))

      val fs = filePath.getFileSystem(conf)
      val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
      val reader = OrcFile.createReader(filePath, readerOptions)

      val requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
        isCaseSensitive, dataSchema, readSchema, reader, conf)

      if (requestedColIdsOrEmptyFile.isEmpty) {
        Iterator.empty
      } else {
        val requestedColIds = requestedColIdsOrEmptyFile.get
        assert(requestedColIds.length == readSchema.length,
          "[BUG] requested column IDs do not match required schema")
        val taskConf = new Configuration(conf)
        taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
          requestedColIds.filter(_ != -1).sorted.mkString(","))

        val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

        val taskContext = Option(TaskContext.get())
        if (enableVectorizedReader) {
          val batchReader = new OrcColumnarBatchReader(
            enableOffHeapColumnVector && taskContext.isDefined, copyToSpark, capacity)
          // SPARK-23399 Register a task completion listener first to call `close()` in all cases.
          // There is a possibility that `initialize` and `initBatch` hit some errors (like OOM)
          // after opening a file.
          val iter = new RecordReaderIterator(batchReader)
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
          batchReader.initialize(fileSplit, taskAttemptContext)
          val partitionColIds = PartitioningUtils.requestedPartitionColumnIds(
            partitionSchema, readSchema, isCaseSensitive)

          batchReader.initBatch(
            reader.getSchema,
            readSchema.fields,
            requestedColIds,
            partitionColIds,
            file.partitionValues)
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          val requiredDataSchema =
            PartitioningUtils.subtractSchema(readSchema, partitionSchema, isCaseSensitive)
          val orcRecordReader = new OrcInputFormat[OrcStruct]
            .createRecordReader(fileSplit, taskAttemptContext)
          val iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
          taskContext.foreach(_.addTaskCompletionListener(_ => iter.close()))

          val fullSchema = requiredDataSchema.toAttributes ++ partitionSchema.toAttributes
          val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
          val deserializer = new OrcDeserializer(dataSchema, requiredDataSchema, requestedColIds)

          if (partitionSchema.length == 0) {
            iter.map(value => unsafeProjection(deserializer.deserialize(value)))
          } else {
            val joinedRow = new JoinedRow()
            iter.map(value =>
              unsafeProjection(joinedRow(deserializer.deserialize(value), file.partitionValues)))
          }
        }
      }
    }
  }

  override def pushCatalystFilters(filters: Array[Expression]): Array[Expression] = {
    val partitionColumnNames = partitionSchema.toAttributes.map(_.name).toSet
    val (partitionKeyFilters, otherFilters) = filters.partition {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    this.partitionFilters = partitionKeyFilters
    pushedFiltersArray = partitionKeyFilters
    if (sqlConf.orcFilterPushDown) {
      val dataFilters = otherFilters.map { f =>
        (DataSourceStrategy.translateFilter(f), f)
      }.collect { case (optionalFilter, catalystFilter) if optionalFilter.isDefined =>
        (optionalFilter.get, catalystFilter)
      }.toMap
      val pushedDataFilters =
        OrcFilters.convertibleFilters(fullSchema, dataFilters.keys.toSeq).map(dataFilters).toArray
      pushedFiltersArray ++= pushedDataFilters
      OrcFilters.createFilter(fullSchema, dataFilters.keys.toSeq).foreach { f =>
        OrcInputFormat.setSearchArgument(hadoopConf, f, fullSchema.fieldNames)
      }
    }
    otherFilters
  }

  override def pushedCatalystFilters(): Array[Expression] = {
    pushedFiltersArray
  }

  override def enableBatchRead(): Boolean = {
    val schema = readSchema()
    sqlConf.orcVectorizedReaderEnabled && sqlConf.wholeStageEnabled &&
      schema.length <= sqlConf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def isSplitable(path: Path): Boolean = true
}

object OrcDataSourceV2 {
  def satisfy(sparkSession: SparkSession, source: String, paths: Seq[String]): Option[String] = {
    val disabledV2Readers = sparkSession.sqlContext.conf.disabledV2DataSourceReader.split(",")
    val isNative = sparkSession.sqlContext.conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "native"
    if (source.toLowerCase(Locale.ROOT) == "orc" && isNative &&
      !disabledV2Readers.contains(source) && paths.length == 1) {
      Some("org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2")
    } else {
      None
    }
  }
}
