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
package org.apache.spark.sql.v2.proto

import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.Descriptor
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.{EmptyPartitionReader, FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.proto.{ProtoDeserializer, ProtoOptions, ProtoUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.io.FileInputStream
import java.net.URI
import scala.util.control.NonFatal

/**
 * A factory used to create PROTO readers.
 *
 * @param sqlConf SQL configuration.
 * @param broadcastedConf Broadcast serializable Hadoop Configuration.
 * @param dataSchema Schema of Proto files.
 * @param readDataSchema Required data schema of Proto files.
 * @param partitionSchema Schema of partitions.
 * @param options Options for parsing AVRO files.
 */
case class ProtoPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: ProtoOptions,
    filters: Seq[Filter]) extends FilePartitionReaderFactory with Logging {
  private val datetimeRebaseModeInRead = options.datetimeRebaseModeInRead

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    val userProvidedSchema = options.schema

    if (partitionedFile.filePath.endsWith(".pb")) {
      var readerSchema: Descriptor = null
      val reader = {
        // val in = Files.readAllBytes(Paths.get(partitionedFile.filePath))
        val in = new FileInputStream(new Path(new URI(partitionedFile.filePath)).toUri.toString)
        try {
          val fileDescriptorSet = FileDescriptorSet.parseFrom(in)
          readerSchema = fileDescriptorSet.getDescriptorForType
          fileDescriptorSet.getFileList.iterator()
        } catch {
          case NonFatal(e) =>
            logError("Exception while opening DataFileReader", e)
            in.close()
            throw e
        }
      }

      val protoFilters = if (SQLConf.get.avroFilterPushDown) {
        new OrderedFilters(filters, readDataSchema)
      } else {
        new NoopFilters
      }

      val fileReader = new PartitionReader[InternalRow] with ProtoUtils.RowReader {
        override val fileDescriptor = reader
        override val deserializer = new ProtoDeserializer(
          userProvidedSchema.getOrElse(readerSchema),
          readDataSchema,
          options.positionalFieldMatching,
          RebaseSpec(LegacyBehaviorPolicy.withName("Proto")),
          protoFilters)

        override def next(): Boolean = hasNextRow
        override def get(): InternalRow = nextRow

        override def close(): Unit = ???
      }
      new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
        partitionSchema, partitionedFile.partitionValues)
    } else {
      new EmptyPartitionReader[InternalRow]
    }
  }
}
