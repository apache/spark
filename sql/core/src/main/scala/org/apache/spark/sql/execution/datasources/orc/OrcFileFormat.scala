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

package org.apache.spark.sql.execution.datasources.orc

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcUtils => _, _}
import org.apache.orc.OrcConf.COMPRESS
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce._

import org.apache.spark.TaskContext
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * New ORC File Format based on Apache ORC.
 */
class OrcFileFormat
  extends FileFormat
  with DataSourceRegister
  with Serializable {

  override def shortName(): String = "orc"

  override def toString: String = "ORC"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[OrcFileFormat]

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    OrcUtils.inferSchema(sparkSession, files, options)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)

    val conf = job.getConfiguration

    conf.set(COMPRESS.getAttribute, orcOptions.compressionCodec)

    conf.asInstanceOf[JobConf]
      .setOutputFormat(classOf[org.apache.orc.mapred.OrcOutputFormat[OrcStruct]])

    val batchSize = sparkSession.sessionState.conf.orcVectorizedWriterBatchSize

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context, batchSize)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(COMPRESS.getAttribute)
          OrcUtils.extensionsForCompressionCodecNames.getOrElse(name, "")
        }

        compressionExtension + ".orc"
      }
    }
  }

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.orcVectorizedReaderEnabled &&
      schema.forall(s => OrcUtils.supportColumnarReads(
        s.dataType, sparkSession.sessionState.conf.orcVectorizedReaderNestedColumnEnabled))
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

    val resultSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val capacity = sqlConf.orcVectorizedReaderBatchSize

    // Should always be set by FileSourceScanExec creating this.
    // Check conf before checking option, to allow working around an issue by changing conf.
    val enableVectorizedReader = sqlConf.orcVectorizedReaderEnabled &&
      options.getOrElse(FileFormat.OPTION_RETURNING_BATCH,
        throw new IllegalArgumentException(
          "OPTION_RETURNING_BATCH should always be set for OrcFileFormat. " +
            "To workaround this issue, set spark.sql.orc.enableVectorizedReader=false."))
        .equals("true")
    if (enableVectorizedReader) {
      // If the passed option said that we are to return batches, we need to also be able to
      // do this based on config and resultSchema.
      assert(supportBatch(sparkSession, resultSchema))
    }

    val memoryMode = if (sqlConf.offHeapColumnVectorEnabled) {
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }

    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(hadoopConf, sqlConf.caseSensitiveAnalysis)

    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val orcFilterPushDown = sparkSession.sessionState.conf.orcFilterPushDown

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value

      val filePath = file.toPath

      val fs = filePath.getFileSystem(conf)
      val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
      val orcSchema =
        Utils.tryWithResource(OrcFile.createReader(filePath, readerOptions))(_.getSchema)
      val resultedColPruneInfo = OrcUtils.requestedColumnIds(
        isCaseSensitive, dataSchema, requiredSchema, orcSchema, conf)

      if (resultedColPruneInfo.isEmpty) {
        Iterator.empty
      } else {
        // ORC predicate pushdown
        if (orcFilterPushDown && filters.nonEmpty) {
          val fileSchema = OrcUtils.toCatalystSchema(orcSchema)
          OrcFilters.createFilter(fileSchema, filters).foreach { f =>
            OrcInputFormat.setSearchArgument(conf, f, fileSchema.fieldNames)
          }
        }

        val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
        val resultSchemaString = OrcUtils.orcResultSchemaString(canPruneCols,
          dataSchema, resultSchema, partitionSchema, conf)
        assert(requestedColIds.length == requiredSchema.length,
          "[BUG] requested column IDs do not match required schema")
        val taskConf = new Configuration(conf)

        val includeColumns = requestedColIds.filter(_ != -1).sorted.mkString(",")
        taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, includeColumns)
        val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

        if (enableVectorizedReader) {
          val batchReader = new OrcColumnarBatchReader(capacity, memoryMode)
          // SPARK-23399 Register a task completion listener first to call `close()` in all cases.
          // There is a possibility that `initialize` and `initBatch` hit some errors (like OOM)
          // after opening a file.
          val iter = new RecordReaderIterator(batchReader)
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
          val requestedDataColIds = requestedColIds ++ Array.fill(partitionSchema.length)(-1)
          val requestedPartitionColIds =
            Array.fill(requiredSchema.length)(-1) ++ Range(0, partitionSchema.length)
          batchReader.initialize(fileSplit, taskAttemptContext, readerOptions.getOrcTail)
          batchReader.initBatch(
            TypeDescription.fromString(resultSchemaString),
            resultSchema.fields,
            requestedDataColIds,
            requestedPartitionColIds,
            file.partitionValues)

          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          val orcRecordReader = new OrcInputFormat[OrcStruct]
            .createRecordReader(fileSplit, taskAttemptContext)
          val iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))

          val fullSchema = toAttributes(requiredSchema) ++ toAttributes(partitionSchema)
          val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
          val deserializer = new OrcDeserializer(requiredSchema, requestedColIds)

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

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: VariantType => false

    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }
}
