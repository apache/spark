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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.hadoop.{ParquetOutputFormat, ParquetRecordWriter}
import org.apache.parquet.hadoop.util.ContextUtil

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration


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
    options: Map[String, String])
  extends OutputWriterFactory {

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
  override def newInstance(
      path: String,
      fileNamePrefix: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    throw new UnsupportedOperationException("this version of newInstance not supported for " +
        "ParquetOutputWriterFactory")
  }
}


// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[parquet] class ParquetOutputWriter(
    stagingDir: String,
    fileNamePrefix: String,
    context: TaskAttemptContext)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, InternalRow] = {
    val outputFormat = {
      new ParquetOutputFormat[InternalRow]() {
        override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
          new Path(stagingDir, fileNamePrefix + extension)
        }
      }
    }

    outputFormat.getRecordWriter(context)
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override def writeInternal(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(context)
}
