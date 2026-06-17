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

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    if (ArchiveOptions.isArchiveEnabled(sparkSession) && ArchiveOptions.isArchive(path)) {
      false
    } else {
      super.isSplitable(sparkSession, options, path)
    }
  }

  override def buildReaderWithPartitionValues(
                                               sparkSession: SparkSession,
                                               dataSchema: StructType,
                                               partitionSchema: StructType,
                                               requiredSchema: StructType,
                                               filters: Seq[Filter],
                                               options: Map[String, String],
                                               hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val textOptions = new TextOptions(options)
    val isArchiveEnabled = ArchiveOptions.isArchiveEnabled(sparkSession)
    val wholeText = textOptions.wholeTextFiles
    val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val path = file.toPath

      // INTERCEPT ROUTE: Process files bundled in Tar/Tar.gz/Tgz archives
      if (isArchiveEnabled && ArchiveOptions.isArchive(path)) {
        val fs = path.getFileSystem(conf)
        val inputStream = fs.open(path)
        val archiveReader = new ArchiveReader(path, inputStream)

        new Iterator[InternalRow] {
          private var currentEntryIterator: Iterator[InternalRow] = Iterator.empty

          override def hasNext: Boolean = {
            // Keep pulling fresh file streams from the archive until one yields data or archive hits EOF
            while (!currentEntryIterator.hasNext && archiveReader.hasNext) {
              val entryStream: InputStream = archiveReader.next()

              currentEntryIterator = if (wholeText) {
                // CASE A: Read whole embedded document into one row
                val content = IOUtils.toString(entryStream, textOptions.encoding)
                Iterator.single(InternalRow(UTF8String.fromString(content)))
              } else {
                // CASE B: Standard line-by-line streaming behavior
                val lineIterator = IOUtils.lineIterator(entryStream, textOptions.encoding)
                new Iterator[InternalRow] {
                  override def hasNext: Boolean = lineIterator.hasNext
                  override def next(): InternalRow = InternalRow(UTF8String.fromString(lineIterator.next()))
                }
              }
            }

            // Clean up root input streams if the archive processing loop completes fully
            if (!currentEntryIterator.hasNext) {
              archiveReader.close()
              inputStream.close()
            }
            currentEntryIterator.hasNext
          }

          override def next(): InternalRow = {
            if (!hasNext) throw new NoSuchElementException("No more records in archive.")
            currentEntryIterator.next()
          }
        }
      } else {
        // FALLBACK ROUTE: Standard native uncompressed text processing
        val reader = MapReduceTextReader.buildReader(conf, file, textOptions)
        reader.map(line => InternalRow(UTF8String.fromString(line.toString)))
      }
    }
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
    readToUnsafeMem(broadcastedHadoopConf, requiredSchema, textOptions)
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

