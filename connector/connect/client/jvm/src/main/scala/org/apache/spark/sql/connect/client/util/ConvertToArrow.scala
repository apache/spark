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
package org.apache.spark.sql.connect.client.util

import java.nio.channels.Channels

import com.google.protobuf.ByteString
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}

import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.util.ArrowUtils

/**
 * Utility for converting common Scala objects into Arrow IPC Stream.
 */
private[sql] object ConvertToArrow {

  /**
   * Convert an iterator of common Scala objects into a sinlge Arrow IPC Stream.
   */
  def apply[T](
      encoder: AgnosticEncoder[T],
      data: Iterator[T],
      timeZoneId: String,
      bufferAllocator: BufferAllocator): ByteString = {
    val arrowSchema = ArrowUtils.toArrowSchema(encoder.schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, bufferAllocator)
    val writer: ArrowWriter = ArrowWriter.create(root)
    val unloader = new VectorUnloader(root)
    val bytes = ByteString.newOutput()
    val channel = new WriteChannel(Channels.newChannel(bytes))

    try {
      // Convert and write the data to the vector root.
      val serializer = ExpressionEncoder(encoder).createSerializer()
      data.foreach(o => writer.write(serializer(o)))
      writer.finish()

      // Write the IPC Stream
      MessageSerializer.serialize(channel, root.getSchema)
      val batch = unloader.getRecordBatch
      try MessageSerializer.serialize(channel, batch)
      finally {
        batch.close()
      }
      ArrowStreamWriter.writeEndOfStream(channel, IpcOption.DEFAULT)

      // Done
      bytes.toByteString
    } finally {
      root.close()
    }
  }
}
