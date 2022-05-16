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
package org.apache.spark.sql.v2.avro

import java.net.URI

import scala.util.control.NonFatal

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.{AvroDeserializer, AvroOptions, AvroUtils}
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{EmptyPartitionReader, FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A factory used to create AVRO readers.
 *
 * @param sqlConf SQL configuration.
 * @param broadcastedConf Broadcast serializable Hadoop Configuration.
 * @param dataSchema Schema of AVRO files.
 * @param readDataSchema Required data schema of AVRO files.
 * @param partitionSchema Schema of partitions.
 * @param options Options for parsing AVRO files.
 */
case class AvroPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: AvroOptions,
    filters: Seq[Filter]) extends FilePartitionReaderFactory with Logging {
  private val datetimeRebaseModeInRead = options.datetimeRebaseModeInRead

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    val userProvidedSchema = options.schema

    if (options.ignoreExtension || partitionedFile.filePath.endsWith(".avro")) {
      val reader = {
        val in = new FsInput(new Path(new URI(partitionedFile.filePath)), conf)
        try {
          val datumReader = userProvidedSchema match {
            case Some(userSchema) => new GenericDatumReader[GenericRecord](userSchema)
            case _ => new GenericDatumReader[GenericRecord]()
          }
          DataFileReader.openReader(in, datumReader)
        } catch {
          case NonFatal(e) =>
            logError("Exception while opening DataFileReader", e)
            in.close()
            throw e
        }
      }

      // Ensure that the reader is closed even if the task fails or doesn't consume the entire
      // iterator of records.
      Option(TaskContext.get()).foreach { taskContext =>
        taskContext.addTaskCompletionListener[Unit] { _ =>
          reader.close()
        }
      }

      reader.sync(partitionedFile.start)

      val datetimeRebaseMode = DataSourceUtils.datetimeRebaseSpec(
        reader.asInstanceOf[DataFileReader[_]].getMetaString,
        datetimeRebaseModeInRead)

      val avroFilters = if (SQLConf.get.avroFilterPushDown) {
        new OrderedFilters(filters, readDataSchema)
      } else {
        new NoopFilters
      }

      val fileReader = new PartitionReader[InternalRow] with AvroUtils.RowReader {
        override val fileReader = reader
        override val deserializer = new AvroDeserializer(
          userProvidedSchema.getOrElse(reader.getSchema),
          readDataSchema,
          options.positionalFieldMatching,
          datetimeRebaseMode,
          avroFilters)
        override val stopPosition = partitionedFile.start + partitionedFile.length

        override def next(): Boolean = hasNextRow
        override def get(): InternalRow = nextRow
        override def close(): Unit = reader.close()
      }
      new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
        partitionSchema, partitionedFile.partitionValues)
    } else {
      new EmptyPartitionReader[InternalRow]
    }
  }
}
