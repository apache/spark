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

package org.apache.spark.storage

import java.nio.ByteBuffer

private[spark] sealed trait BlockValue {
  def asIterator(blockId: BlockId, blockSerde: BlockSerializer): Iterator[Any]
  def asBytes(blockId: BlockId, blockSerde: BlockSerializer): ByteBuffer
}

private[spark] case class ByteBufferValue(buffer: ByteBuffer) extends BlockValue {
  def asIterator(blockId: BlockId, blockSerde: BlockSerializer): Iterator[Any] = {
    blockSerde.dataDeserialize(blockId, buffer)
  }
  def asBytes(blockId: BlockId, blockSerde: BlockSerializer): ByteBuffer = {
    buffer
  }
}

private[spark] case class IteratorValue(iterator: Iterator[Any]) extends BlockValue {
  override def asIterator(blockId: BlockId, blockSerde: BlockSerializer): Iterator[Any] = {
    iterator
  }
  override def asBytes(blockId: BlockId, blockSerde: BlockSerializer): ByteBuffer = {
    blockSerde.dataSerialize(blockId, iterator)
  }
}

private[spark] case class ArrayValue(values: Array[Any]) extends BlockValue {
  override def asIterator(blockId: BlockId, blockSerde: BlockSerializer): Iterator[Any] = {
    values.iterator
  }
  override def asBytes(blockId: BlockId, blockSerde: BlockSerializer): ByteBuffer = {
    blockSerde.dataSerialize(blockId, values.iterator)
  }
}
