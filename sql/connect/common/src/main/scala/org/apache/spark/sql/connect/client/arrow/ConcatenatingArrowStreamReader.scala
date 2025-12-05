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
package org.apache.spark.sql.connect.client.arrow

import java.io.{InputStream, IOException}
import java.nio.channels.Channels

import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.ipc.{ArrowReader, ReadChannel}
import org.apache.arrow.vector.ipc.message.{ArrowDictionaryBatch, ArrowMessage, ArrowRecordBatch, MessageChannelReader, MessageResult, MessageSerializer}
import org.apache.arrow.vector.types.pojo.Schema

/**
 * An [[ArrowReader]] that concatenates multiple [[MessageIterator]]s into a single stream. Each
 * iterator represents a single IPC stream. The concatenated streams all must have the same
 * schema. If the schema is different an exception is thrown.
 *
 * In some cases we want to retain the messages (see `SparkResult`). Normally a stream reader
 * closes its messages when it consumes them. In order to prevent that from happening in
 * non-destructive mode we clone the messages before passing them to the reading logic.
 */
class ConcatenatingArrowStreamReader(
    allocator: BufferAllocator,
    input: Iterator[AbstractMessageIterator],
    destructive: Boolean)
    extends ArrowReader(allocator) {

  private[this] var totalBytesRead: Long = 0
  private[this] var current: AbstractMessageIterator = _

  override protected def readSchema(): Schema = {
    // readSchema() should only be called once during initialization.
    assert(current == null)
    if (!input.hasNext) {
      // ArrowStreamReader throws the same exception.
      throw new IOException("Unexpected end of input. Missing schema.")
    }
    current = input.next()
    current.schema
  }

  private def nextMessage(): ArrowMessage = {
    // readSchema() should have been invoked at this point so 'current' should be initialized.
    assert(current != null)
    // Try to find a non-empty message iterator.
    while (!current.hasNext && input.hasNext) {
      totalBytesRead += current.bytesRead
      current = input.next()
      if (current.schema != getVectorSchemaRoot.getSchema) {
        throw new IllegalStateException()
      }
    }
    if (current.hasNext) {
      current.next()
    } else {
      null
    }
  }

  override def loadNextBatch(): Boolean = {
    // Keep looping until we load a non-empty batch or until we exhaust the input.
    var message = nextMessage()
    while (message != null) {
      message match {
        case rb: ArrowRecordBatch =>
          loadRecordBatch(cloneIfNonDestructive(rb))
          if (getVectorSchemaRoot.getRowCount > 0) {
            return true
          }
        case db: ArrowDictionaryBatch =>
          loadDictionary(cloneIfNonDestructive(db))
      }
      message = nextMessage()
    }
    false
  }

  private def cloneIfNonDestructive(batch: ArrowRecordBatch): ArrowRecordBatch = {
    if (destructive) {
      return batch
    }
    cloneRecordBatch(batch)
  }

  private def cloneIfNonDestructive(batch: ArrowDictionaryBatch): ArrowDictionaryBatch = {
    if (destructive) {
      return batch
    }
    new ArrowDictionaryBatch(
      batch.getDictionaryId,
      cloneRecordBatch(batch.getDictionary),
      batch.isDelta)
  }

  private def cloneRecordBatch(batch: ArrowRecordBatch): ArrowRecordBatch = {
    new ArrowRecordBatch(
      batch.getLength,
      batch.getNodes,
      batch.getBuffers,
      batch.getBodyCompression,
      true,
      true)
  }

  override def bytesRead(): Long = {
    if (current != null) {
      totalBytesRead + current.bytesRead
    } else {
      0
    }
  }

  override def closeReadSource(): Unit = ()
}

trait AbstractMessageIterator extends Iterator[ArrowMessage] {
  def schema: Schema
  def bytesRead: Long
}

/**
 * Decode an Arrow IPC stream into individual messages. Please note that this iterator MUST have a
 * valid IPC stream as its input, otherwise construction will fail.
 */
class MessageIterator(input: InputStream, allocator: BufferAllocator)
    extends AbstractMessageIterator {
  private[this] val in = new ReadChannel(Channels.newChannel(input))
  private[this] val reader = new MessageChannelReader(in, allocator)
  private[this] var result: MessageResult = _

  // Eagerly read the schema.
  val schema: Schema = {
    val result = reader.readNext()
    if (result == null) {
      throw new IOException("Unexpected end of input. Missing schema.")
    }
    MessageSerializer.deserializeSchema(result.getMessage)
  }

  override def bytesRead: Long = reader.bytesRead()

  override def hasNext: Boolean = {
    if (result == null) {
      result = reader.readNext()
    }
    result != null
  }

  override def next(): ArrowMessage = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    val message = result.getMessage.headerType() match {
      case MessageHeader.RecordBatch =>
        MessageSerializer.deserializeRecordBatch(result.getMessage, bodyBuffer(result))
      case MessageHeader.DictionaryBatch =>
        MessageSerializer.deserializeDictionaryBatch(result.getMessage, bodyBuffer(result))
    }
    result = null
    message
  }

  private def bodyBuffer(result: MessageResult): ArrowBuf = {
    var buffer = result.getBodyBuffer
    if (buffer == null) {
      buffer = allocator.getEmpty
    }
    buffer
  }
}
