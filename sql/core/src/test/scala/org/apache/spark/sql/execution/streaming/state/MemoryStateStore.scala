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

package org.apache.spark.sql.execution.streaming.state

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType

class MemoryStateStore extends StateStore() {
  import scala.jdk.CollectionConverters._
  private val map = new ConcurrentHashMap[UnsafeRow, UnsafeRow]

  override def iterator(colFamilyName: String): Iterator[UnsafeRowPair] = {
    map.entrySet.iterator.asScala.map { case e => new UnsafeRowPair(e.getKey, e.getValue) }
  }

  override def createColFamilyIfAbsent(
      colFamilyName: String,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useMultipleValuesPerKey: Boolean = false,
      isInternal: Boolean = false,
      useAvro: Boolean = false): Unit = {
    throw StateStoreErrors.multipleColumnFamiliesNotSupported("MemoryStateStoreProvider")
  }

  override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
    throw StateStoreErrors.removingColumnFamiliesNotSupported("MemoryStateStoreProvider")
  }

  override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = map.get(key)

  override def put(key: UnsafeRow, newValue: UnsafeRow, colFamilyName: String): Unit =
    map.put(key.copy(), newValue.copy())

  override def remove(key: UnsafeRow, colFamilyName: String): Unit = map.remove(key)

  override def commit(): Long = version + 1

  override def abort(): Unit = {}

  override def id: StateStoreId = null

  override def version: Long = 0

  override def metrics: StateStoreMetrics = new StateStoreMetrics(map.size, 0, Map.empty)

  override def hasCommitted: Boolean = true

  override def prefixScan(prefixKey: UnsafeRow, colFamilyName: String): Iterator[UnsafeRowPair] = {
    throw new UnsupportedOperationException("Doesn't support prefix scan!")
  }

  override def merge(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    throw new UnsupportedOperationException("Doesn't support multiple values per key")
  }

  override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
    throw new UnsupportedOperationException("Doesn't support multiple values per key")
  }

  override def put(key: Array[Byte], value: Array[Byte], colFamilyName: String): Unit = {
    throw new UnsupportedOperationException("Doesn't support bytearray operations")
  }

  override def remove(key: Array[Byte], colFamilyName: String): Unit = {
    throw new UnsupportedOperationException("Doesn't support bytearray operations")
  }

  override def get(key: Array[Byte], colFamilyName: String): Array[Byte] = {
    throw new UnsupportedOperationException("Doesn't support bytearray operations")
  }

  override def valuesIterator(key: Array[Byte], colFamilyName: String): Iterator[Array[Byte]] = {
    throw new UnsupportedOperationException("Doesn't support bytearray operations")
  }

  override def prefixScan(
      prefixKey: Array[Byte], colFamilyName: String): Iterator[ByteArrayPair] = {
    throw new UnsupportedOperationException("Doesn't support bytearray operations")
  }

  override def byteArrayIter(colFamilyName: String): Iterator[ByteArrayPair] = {
    throw new UnsupportedOperationException("Doesn't support bytearray operations")
  }
}
