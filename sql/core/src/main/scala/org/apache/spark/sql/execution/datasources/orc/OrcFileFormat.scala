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
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc._
import org.apache.orc.OrcConf.{COMPRESS, MAPRED_OUTPUT_SCHEMA}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce._

import org.apache.spark.TaskContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

private[sql] object OrcFileFormat {
  private def checkFieldName(name: String): Unit = {
    try {
      TypeDescription.fromString(s"struct<$name:int>")
    } catch {
      case _: IllegalArgumentException =>
        throw new AnalysisException(
          s"""Column name "$name" contains invalid character(s).
             |Please use alias to rename it.
           """.stripMargin.split("\n").mkString(" ").trim)
    }
  }

  def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }
}

class DefaultSource extends OrcFileFormat

/**
 * New ORC File Format based on Apache ORC 1.4.1 and above.
 */
class OrcFileFormat
  extends FileFormat
  with DataSourceRegister
  with Serializable {

  override def shortName(): String = "orc"

  override def toString: String = "ORC_1.4"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[OrcFileFormat]

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    OrcUtils.readSchema(sparkSession, files)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)

    val conf = job.getConfiguration

    conf.set(MAPRED_OUTPUT_SCHEMA.getAttribute, OrcUtils.getSchemaString(dataSchema))

    conf.set(COMPRESS.getAttribute, orcOptions.compressionCodec)

    conf.asInstanceOf[JobConf]
      .setOutputFormat(classOf[org.apache.orc.mapred.OrcOutputFormat[OrcStruct]])

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(COMPRESS.getAttribute)
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
      OrcFilters.createFilter(dataSchema, filters).foreach { f =>
        OrcInputFormat.setSearchArgument(hadoopConf, f, dataSchema.fieldNames)
      }
    }

    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val resolver = sparkSession.sessionState.conf.resolver

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value

      val maybeMissingSchema = OrcUtils.getMissingSchema(
        resolver, dataSchema, partitionSchema, new Path(new URI(file.filePath)), conf)
      if (maybeMissingSchema.isEmpty) {
        Iterator.empty
      } else {
        val missingSchema = maybeMissingSchema.get
        val columns = requiredSchema
          .filter(f => missingSchema.getFieldIndex(f.name).isEmpty)
          .map(f => dataSchema.fieldIndex(f.name)).mkString(",")
        conf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, columns)

        val fileSplit =
          new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val taskAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
        val partitionValues = file.partitionValues

        val resultSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)

        val orcRecordReader = new OrcInputFormat[OrcStruct]
          .createRecordReader(fileSplit, taskAttemptContext)
        val iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))

        val mutableRow = new SpecificInternalRow(resultSchema.map(_.dataType))
        val unsafeProjection = UnsafeProjection.create(resultSchema)

        // Initialize the partition column values once.
        for (i <- requiredSchema.length until resultSchema.length) {
          val value = partitionValues.get(i - requiredSchema.length, resultSchema(i).dataType)
          mutableRow.update(i, value)
        }

        val valueWrappers = requiredSchema.fields.map(f => OrcUtils.getValueWrapper(f.dataType))
        iter.map { value =>
          unsafeProjection(OrcUtils.convertOrcStructToInternalRow(value, dataSchema, requiredSchema,
            maybeMissingSchema, Some(valueWrappers), Some(mutableRow)))
        }
      }
    }
  }
}
