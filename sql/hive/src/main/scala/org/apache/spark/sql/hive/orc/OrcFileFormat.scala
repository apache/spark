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

package org.apache.spark.sql.hive.orc

import java.net.URI
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.serde2.objectinspector.{SettableStructObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoUtils}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.{JobConf, OutputFormat => MapRedOutputFormat, RecordWriter, Reporter}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc._
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.{OrcInputFormat, OrcOutputFormat}

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.{HiveInspectors, HiveShim}
import org.apache.spark.sql.sources.{Filter, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * `FileFormat` for reading ORC files. If this is moved or renamed, please update
 * `DataSource`'s backwardCompatibilityMap.
 */
class OrcFileFormat extends FileFormat with DataSourceRegister with Serializable {

  override def shortName(): String = "orc"

  override def toString: String = "ORC"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    files.map(_.getPath).flatMap(OrcUtils.readSchema(_, conf)).headOption.map { schema =>
      CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)

    val configuration = job.getConfiguration

    configuration.set(OrcConf.COMPRESS.getAttribute, orcOptions.compressionCodec)
    val outputFormatClass = classOf[org.apache.orc.mapred.OrcOutputFormat[OrcStruct]]
    configuration match {
      case conf: JobConf =>
        conf.setOutputFormat(outputFormatClass)
      case conf =>
        conf.setClass(
          "mapred.output.format.class",
          outputFormatClass,
          classOf[MapRedOutputFormat[_, _]])
    }
    configuration.set(
      OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute, OrcUtils.getSchemaString(dataSchema))

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(OrcConf.COMPRESS.getAttribute)
          OrcOptions.extensionsForCompressionCodecNames.getOrElse(name, "")
        }

        compressionExtension + ".orc"
      }
    }
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
    if (sparkSession.sessionState.conf.orcFilterPushDown) {
      // Sets pushed predicates
      OrcFilters.createFilter(requiredSchema, filters.toArray).foreach { f =>
        OrcInputFormat.setSearchArgument(hadoopConf, f, dataSchema.fieldNames)
      }
    }
    // Column Selection: we need to set at least one column due to ORC-233
    val columns = if (requiredSchema.isEmpty) {
      "0"
    } else {
      requiredSchema.map(f => dataSchema.fieldIndex(f.name)).mkString(",")
    }
    hadoopConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, columns)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value

      // SPARK-8501: Empty ORC files always have an empty schema stored in their footer. In this
      // case, and we can't read the underlying file
      // using the given physical schema. Instead, we simply return an empty iterator.
      val physicalSchema = OrcUtils.getSchema(dataSchema, file.filePath, conf)
      if (physicalSchema.getFieldNames.isEmpty) {
        Iterator.empty
      } else {

        val orcRecordReader = {
          val fileSplit = new FileSplit(
            new Path(new URI(file.filePath)), file.start, file.length, Array.empty
          )
          val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
          val taskAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
          new OrcInputFormat[OrcStruct].createRecordReader(fileSplit, taskAttemptContext)
        }

        val recordsIterator = new RecordReaderIterator[OrcStruct](orcRecordReader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => recordsIterator.close()))
        val resultSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
        val mutableRow = new SpecificInternalRow(resultSchema.map(_.dataType))
        val unsafeProjection = UnsafeProjection.create(resultSchema)
        val partitionValues = file.partitionValues
        for (i <- requiredSchema.length until resultSchema.length) {
          val value = partitionValues.get(i - requiredSchema.length, resultSchema(i).dataType)
          mutableRow.update(i, value)
        }

        // Unwraps `OrcStruct`s to `UnsafeRow`s
        val valueWrappers = requiredSchema.fields.map(f => OrcUtils.getValueWrapper(f.dataType))
        recordsIterator.map { value =>
          unsafeProjection(OrcUtils.convertOrcStructToInternalRow(
            value, requiredSchema, Some(valueWrappers), Some(mutableRow)))
        }
      }
    }
  }
}

private[orc] class OrcOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter {
  private lazy val orcStruct: OrcStruct =
    OrcUtils.createOrcValue(dataSchema).asInstanceOf[OrcStruct]

  private[this] val writableWrappers =
    dataSchema.fields.map(f => OrcUtils.getWritableWrapper(f.dataType))

  // `OrcRecordWriter.close()` creates an empty file if no rows are written at all.  We use this
  // flag to decide whether `OrcRecordWriter.close()` needs to be called.
  private var recordWriterInstantiated = false

  private lazy val recordWriter = {
    recordWriterInstantiated = true
    new OrcOutputFormat[OrcStruct]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context)
  }

  override def write(row: InternalRow): Unit = {
    recordWriter.write(
      NullWritable.get,
      OrcUtils.convertInternalRowToOrcStruct(
        row, dataSchema, Some(writableWrappers), Some(orcStruct)))
  }

  override def close(): Unit = {
    if (recordWriterInstantiated) {
      recordWriter.close(context)
    }
  }
}
