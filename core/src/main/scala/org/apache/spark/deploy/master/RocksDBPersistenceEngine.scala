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
import scala.collection.JavaConverters._

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
  private def createCfOptions(): ColumnFamilyOptions = {
    new ColumnFamilyOptions()
      .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setTableFormatConfig(tableFormatConfig)
  }

  private val KNOWN_PREFIXES = Seq("app_", "driver_", "worker_")

  private val dbOptions = new DBOptions()
    .setCreateIfMissing(true)
    .setCreateMissingColumnFamilies(true)

  // Discover existing column families first to ensure we can open an existing DB
  private val existingCfs = try {
    RocksDB.listColumnFamilies(new Options(), path.toString).asScala.map(new String(_, UTF_8))
  } catch {
    case _: RocksDBException => Seq(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8))
  }

  private val allCfs = (Seq(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8)) ++ KNOWN_PREFIXES ++ existingCfs).distinct

  private val cfDescriptors = new java.util.ArrayList[ColumnFamilyDescriptor]()
  allCfs.foreach { cfName =>
    cfDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), createCfOptions()))
  }

  private val cfHandles = new java.util.ArrayList[ColumnFamilyHandle]()
  private val db: RocksDB = RocksDB.open(dbOptions, path.toString, cfDescriptors, cfHandles)

  private val cfHandleMap: Map[String, ColumnFamilyHandle] = {
    allCfs.zipWithIndex.flatMap { case (name, idx) =>
      if (KNOWN_PREFIXES.contains(name)) Some(name -> cfHandles.get(idx))
      else None
    }.toMap
  }

  private val defaultCFHandle = cfHandles.get(allCfs.indexOf(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8)))

  private def getCFHandle(name: String): ColumnFamilyHandle = {
    cfHandleMap.find { case (prefix, _) => name.startsWith(prefix) }
      .map(_._2)
      .getOrElse(defaultCFHandle)
  }

  migrateOldData()

  private def migrateOldData(): Unit = {
    val iter = db.newIterator(defaultCFHandle)
    val writeBatch = new WriteBatch()
    var count = 0
    try {
      iter.seekToFirst()
      while (iter.isValid) {
        val key = iter.key()
        val keyStr = new String(key, UTF_8)
        val handle = cfHandleMap.find { case (prefix, _) => keyStr.startsWith(prefix) }.map(_._2)
        handle.foreach { h =>
          writeBatch.put(h, key, iter.value())
          writeBatch.delete(defaultCFHandle, key)
          count += 1
        }
        iter.next()
      }
    } finally {
      iter.close()
    }
    if (count > 0) {
      logInfo(s"Migrated $count records from default column family to specific column families.")
      val writeOptions = new WriteOptions().setSync(true)
      try {
        db.write(writeOptions, writeBatch)
      } finally {
        writeOptions.close()
      }
    }
    writeBatch.close()
  }

  override def persist(name: String, obj: Object): Unit = {
    val serialized = serializer.newInstance().serialize(obj)
    val cfHandle = getCFHandle(name)
    if (serialized.hasArray) {
      db.put(cfHandle, name.getBytes(UTF_8), serialized.array())
    } else {
      val bytes = new Array[Byte](serialized.remaining())
      serialized.get(bytes)
      db.put(cfHandle, name.getBytes(UTF_8), bytes)
    }
  }

  override def unpersist(name: String): Unit = {
    db.delete(getCFHandle(name), name.getBytes(UTF_8))
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val result = new ArrayBuffer[T]
    val cfHandle = getCFHandle(prefix)
    val iter = db.newIterator(cfHandle)
    try {
      val prefixBytes = prefix.getBytes(UTF_8)
      iter.seek(prefixBytes)
      while (iter.isValid && startsWith(iter.key(), prefixBytes)) {
        result.append(serializer.newInstance().deserialize[T](ByteBuffer.wrap(iter.value())))
        iter.next()
      }
    } finally {
      iter.close()
    }
    result.toSeq
  }

  private def startsWith(key: Array[Byte], prefix: Array[Byte]): Boolean = {
    if (key.length < prefix.length) return false
    var i = 0
    while (i < prefix.length) {
      if (key(i) != prefix(i)) return false
      i += 1
    }
    true
  }

  override def close(): Unit = {
    cfHandles.asScala.foreach(_.close())
    if (db != null) {
      db.close()
    }
  }
}
