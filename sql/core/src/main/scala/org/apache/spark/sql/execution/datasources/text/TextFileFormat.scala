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

package org.apache.spark.sql.execution.datasources.text

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A data source for reading text files. The text files must be encoded as UTF-8.
 */
case class TextFileFormat() extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "text"

  override def toString: String = "Text"

  private def verifySchema(schema: StructType): Unit = {
    if (schema.size != 1) {
      throw QueryCompilationErrors.textDataSourceWithMultiColumnsError(schema)
    }
  }

  private def verifyReadSchema(schema: StructType): Unit = {
    if (schema.size > 1) {
      throw QueryCompilationErrors.textDataSourceWithMultiColumnsError(schema)
    }
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    val textOptions = new TextOptions(options)
    if (textOptions.archiveFormatEnabled && ArchiveReader.isArchivePath(path)) {
      // A tar archive is read as one sequential stream (entry by entry), so it is never split.
      return false
    }
    super.isSplitable(sparkSession, options, path) && !textOptions.wholeText
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(new StructType().add("value", StringType))

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema)

    val textOptions = new TextOptions(options)
    val conf = job.getConfiguration

    textOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new TextOutputWriter(path, dataSchema, textOptions.lineSeparatorInWrite, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".txt" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    verifyReadSchema(requiredSchema)
    val textOptions = new TextOptions(options)
    val broadcastedHadoopConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    val perFileReader = readToUnsafeMem(broadcastedHadoopConf, requiredSchema, textOptions)
    val archiveReader = readArchive(broadcastedHadoopConf, requiredSchema, textOptions)
    // A tar archive (always a single split, see `isSplitable`) is streamed entry by entry when
    // archive reads are enabled; otherwise the file is read directly. Archive scanning is wired
    // into the V1 file source only, so this dispatch lives here rather than in a shared reader.
    (file: PartitionedFile) => {
      if (textOptions.archiveFormatEnabled && ArchiveReader.isArchivePath(file.toPath)) {
        archiveReader(file)
      } else {
        perFileReader(file)
      }
    }
  }

  /**
   * Streams a tar archive (`.tar`/`.tar.gz`/`.tgz`) entry by entry, emitting the same
   * `value`-column rows the per-file reader produces -- each entry is read as if it were a
   * standalone text file (one row per line, or a single row holding the whole entry when
   * `wholeText` is set), without unpacking the archive to disk. The whole archive is a single
   * split (see `isSplitable`).
   */
  private def readArchive(
      conf: Broadcast[SerializableConfiguration],
      requiredSchema: StructType,
      textOptions: TextOptions): PartitionedFile => Iterator[UnsafeRow] = {
    (file: PartitionedFile) => {
      val confValue = conf.value.value
      ArchiveReader(file.toPath).readEntries(confValue) { (_, in) =>
        // Each entry is read as a standalone text file, so it gets its own row writer, exactly as
        // `readToUnsafeMem` builds one per file.
        val emptyUnsafeRow = new UnsafeRow(0)
        val unsafeRowWriter = new UnsafeRowWriter(1)
        // Mirrors `readToUnsafeMem`: an empty required schema (e.g. `count`) yields one empty row
        // per record; otherwise each record is written into the single `value` column.
        def toRow(bytes: Array[Byte], length: Int): UnsafeRow = {
          if (requiredSchema.isEmpty) {
            emptyUnsafeRow
          } else {
            unsafeRowWriter.reset()
            unsafeRowWriter.write(0, bytes, 0, length)
            unsafeRowWriter.getRow()
          }
        }
        if (textOptions.wholeText) {
          val content = in.readAllBytes()
          Iterator.single(toRow(content, content.length))
        } else {
          ArchiveReader.lineIterator(in, textOptions.lineSeparatorInRead).map { line =>
            toRow(line.getBytes, line.getLength)
          }
        }
      }
    }
  }

  private def readToUnsafeMem(
      conf: Broadcast[SerializableConfiguration],
      requiredSchema: StructType,
      textOptions: TextOptions): (PartitionedFile) => Iterator[UnsafeRow] = {

    (file: PartitionedFile) => {
      val confValue = conf.value.value
      val reader = Utils.createResourceUninterruptiblyIfInTaskThread {
        if (!textOptions.wholeText) {
          new HadoopFileLinesReader(file, textOptions.lineSeparatorInRead, confValue)
        } else {
          new HadoopFileWholeTextReader(file, confValue)
        }
      }
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => reader.close()))
      if (requiredSchema.isEmpty) {
        val emptyUnsafeRow = new UnsafeRow(0)
        reader.map(_ => emptyUnsafeRow)
      } else {
        val unsafeRowWriter = new UnsafeRowWriter(1)

        reader.map { line =>
          // Writes to an UnsafeRow directly
          unsafeRowWriter.reset()
          unsafeRowWriter.write(0, line.getBytes, 0, line.getLength)
          unsafeRowWriter.getRow()
        }
      }
    }
  }

  override def supportDataType(dataType: DataType): Boolean =
    dataType.isInstanceOf[StringType]
}

