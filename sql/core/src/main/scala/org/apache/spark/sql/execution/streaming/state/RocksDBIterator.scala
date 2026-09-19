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

import org.rocksdb.{RocksIterator => NativeRocksIterator}

/**
 * A RocksDB iterator that can be refreshed and repositioned without recreating its native
 * resources.
 */
private[state] class RocksDBIterator(
    iter: NativeRocksIterator,
    useColumnFamilies: Boolean,
    virtualColumnFamilyId: Option[Short],
    rowChecksumEnabled: Boolean,
    readVerifier: Option[KeyValueIntegrityVerifier],
    delimiterSize: Int) extends Iterator[ByteArrayPair] with AutoCloseable {

  private val byteArrayPair = new ByteArrayPair()

  def refresh(): Unit = iter.refresh()

  def seek(key: Array[Byte]): Unit = {
    val encodedKey = virtualColumnFamilyId match {
      case Some(id) => RocksDBStateStoreProvider.encodeStateRowWithPrefix(key, id)
      case None => key
    }
    iter.seek(encodedKey)
  }

  def seekToFirst(): Unit = iter.seekToFirst()

  override def hasNext: Boolean = {
    iter.isValid && virtualColumnFamilyId.forall { id =>
      RocksDBStateStoreProvider.getColumnFamilyBytesAsId(iter.key()) == id
    }
  }

  override def next(): ByteArrayPair = {
    val key = if (useColumnFamilies) {
      RocksDBStateStoreProvider.decodeStateRowWithPrefix(iter.key())
    } else {
      iter.key()
    }
    val value = if (rowChecksumEnabled) {
      KeyValueChecksumEncoder.decodeAndVerifyValueRowWithChecksum(
        readVerifier, iter.key(), iter.value(), delimiterSize)
    } else {
      iter.value()
    }

    byteArrayPair.set(key, value)
    iter.next()
    byteArrayPair
  }

  override def close(): Unit = iter.close()
}
