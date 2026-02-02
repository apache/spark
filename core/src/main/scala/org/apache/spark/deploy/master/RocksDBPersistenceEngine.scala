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

package org.apache.spark.deploy.master

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.rocksdb._

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer


/**
 * Stores data in RocksDB.
 *
 * @param dir Directory to setup RocksDB. Created if non-existent.
 * @param serializer Used to serialize our objects.
 */
private[master] class RocksDBPersistenceEngine(
    val dir: String,
    val serializer: Serializer)
  extends PersistenceEngine with Logging {

  RocksDB.loadLibrary()

  private val path = try {
    Files.createDirectories(Paths.get(dir))
  } catch {
    case _: FileAlreadyExistsException if Files.isSymbolicLink(Paths.get(dir)) =>
      Files.createDirectories(Paths.get(dir).toRealPath())
  }

  /**
   * Use full filter.
   * Disable compression in index data.
   *
   * https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#full-filters-new-format
   */
  private val tableFormatConfig = new BlockBasedTableConfig()
    .setFilterPolicy(new BloomFilter(10.0D, false))
    .setEnableIndexCompression(false)
    .setIndexBlockRestartInterval(8)
    .setFormatVersion(5)

  /**
   * Use ZSTD at the bottom most level to reduce the disk space
   * Use LZ4 at the other levels because it's better than Snappy in general.
   *
   * https://github.com/facebook/rocksdb/wiki/Compression#configuration
   */
  private val options = new Options()
    .setCreateIfMissing(true)
    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
    .setTableFormatConfig(tableFormatConfig)

  private val db: RocksDB = RocksDB.open(options, path.toString)

  override def persist(name: String, obj: Object): Unit = {
    val serialized = serializer.newInstance().serialize(obj)
    if (serialized.hasArray) {
      db.put(name.getBytes(UTF_8), serialized.array())
    } else {
      val bytes = new Array[Byte](serialized.remaining())
      serialized.get(bytes)
      db.put(name.getBytes(UTF_8), bytes)
    }
  }

  override def unpersist(name: String): Unit = {
    db.delete(name.getBytes(UTF_8))
  }

  override def read[T: ClassTag](name: String): Seq[T] = {
    val result = new ArrayBuffer[T]
    val iter = db.newIterator()
    try {
      iter.seek(name.getBytes(UTF_8))
      while (iter.isValid && new String(iter.key()).startsWith(name)) {
        result.append(serializer.newInstance().deserialize[T](ByteBuffer.wrap(iter.value())))
        iter.next()
      }
    } finally {
      iter.close()
    }
    result.toSeq
  }
}
