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

import java.util.Map.Entry

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{StructField, StructType}

trait HDFSBackedStateStoreMap {
  def size(): Int
  def get(key: UnsafeRow): UnsafeRow
  def put(key: UnsafeRow, value: UnsafeRowWrapper): UnsafeRowWrapper
  def putAll(map: HDFSBackedStateStoreMap): Unit
  def remove(key: UnsafeRow): UnsafeRowWrapper
  def iterator(): Iterator[UnsafeRowPair]
  /** Returns entries in the underlying map and skips additional checks done by [[iterator]].
   * [[iterator]] should be preferred over this. */
  def entryIterator(): Iterator[Entry[UnsafeRow, UnsafeRowWrapper]]
  def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair]
}

object HDFSBackedStateStoreMap {
  // ConcurrentHashMap is used because it generates fail-safe iterators on filtering
  // - The iterator is weakly consistent with the map, i.e., iterator's data reflect the values in
  //   the map when the iterator was created
  // - Any updates to the map while iterating through the filtered iterator does not throw
  //   java.util.ConcurrentModificationException
  type MapType = java.util.concurrent.ConcurrentHashMap[UnsafeRow, UnsafeRowWrapper]

  def create(
      keySchema: StructType,
      numColsPrefixKey: Int,
      readVerifier: Option[KeyValueIntegrityVerifier]): HDFSBackedStateStoreMap = {
    if (numColsPrefixKey > 0) {
      new PrefixScannableHDFSBackedStateStoreMap(keySchema, numColsPrefixKey, readVerifier)
    } else {
      new NoPrefixHDFSBackedStateStoreMap(readVerifier)
    }
  }

  /** Get the value row from the value wrapper and verify it */
  def getAndVerifyValueRow(
      key: UnsafeRow,
      valueWrapper: UnsafeRowWrapper,
      readVerifier: Option[KeyValueIntegrityVerifier]): UnsafeRow = {
    Option(valueWrapper) match {
      case Some(value) =>
        readVerifier.foreach(_.verify(key, value))
        value.unsafeRow()
      case None => null
    }
  }
}

class NoPrefixHDFSBackedStateStoreMap(private val readVerifier: Option[KeyValueIntegrityVerifier])
    extends HDFSBackedStateStoreMap {
  private val map = new HDFSBackedStateStoreMap.MapType()

  override def size(): Int = map.size()

  override def get(key: UnsafeRow): UnsafeRow = {
    HDFSBackedStateStoreMap.getAndVerifyValueRow(key, map.get(key), readVerifier)
  }

  override def put(key: UnsafeRow, value: UnsafeRowWrapper): UnsafeRowWrapper = map.put(key, value)

  def putAll(other: HDFSBackedStateStoreMap): Unit = {
    other match {
      case o: NoPrefixHDFSBackedStateStoreMap => map.putAll(o.map)
      case _ => other.entryIterator().foreach { pair => put(pair.getKey, pair.getValue) }
    }
  }

  override def remove(key: UnsafeRow): UnsafeRowWrapper = map.remove(key)

  override def iterator(): Iterator[UnsafeRowPair] = {
    val unsafeRowPair = new UnsafeRowPair()
    entryIterator().map { entry =>
      val valueRow = HDFSBackedStateStoreMap
        .getAndVerifyValueRow(entry.getKey, entry.getValue, readVerifier)
      unsafeRowPair.withRows(entry.getKey, valueRow)
    }
  }

  /** Returns entries in the underlying map and skips additional checks done by [[iterator]].
   * [[iterator]] should be preferred over this. */
  override def entryIterator(): Iterator[Entry[UnsafeRow, UnsafeRowWrapper]] = {
    map.entrySet.asScala.iterator
  }

  override def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair] = {
    throw SparkUnsupportedOperationException()
  }
}

class PrefixScannableHDFSBackedStateStoreMap(
    keySchema: StructType,
    numColsPrefixKey: Int,
    private val readVerifier: Option[KeyValueIntegrityVerifier]) extends HDFSBackedStateStoreMap {

  private val map = new HDFSBackedStateStoreMap.MapType()

  // We are using ConcurrentHashMap here with the same rationalization we use ConcurrentHashMap on
  // HDFSBackedStateStoreMap.MapType.
  private val prefixKeyToKeysMap = new java.util.concurrent.ConcurrentHashMap[
    UnsafeRow, mutable.Set[UnsafeRow]]()

  private val prefixKeyFieldsWithIdx: Seq[(StructField, Int)] = {
    keySchema.zipWithIndex.take(numColsPrefixKey)
  }

  private val prefixKeyProjection: UnsafeProjection = {
    val refs = prefixKeyFieldsWithIdx.map(x => BoundReference(x._2, x._1.dataType, x._1.nullable))
    UnsafeProjection.create(refs)
  }

  override def size(): Int = map.size()

  override def get(key: UnsafeRow): UnsafeRow = {
    HDFSBackedStateStoreMap.getAndVerifyValueRow(key, map.get(key), readVerifier)
  }

  override def put(key: UnsafeRow, value: UnsafeRowWrapper): UnsafeRowWrapper = {
    val ret = map.put(key, value)

    val prefixKey = prefixKeyProjection(key).copy()
    prefixKeyToKeysMap.compute(prefixKey, (_, v) => {
      if (v == null) {
        val set = new mutable.HashSet[UnsafeRow]()
        set.add(key)
        set
      } else {
        v.add(key)
        v
      }
    })

    ret
  }

  def putAll(other: HDFSBackedStateStoreMap): Unit = {
    other match {
      case o: PrefixScannableHDFSBackedStateStoreMap =>
        map.putAll(o.map)
        o.prefixKeyToKeysMap.asScala.foreach { case (prefixKey, keySet) =>
          // Here we create a copy version of Set. Shallow-copying the prefix key map will lead
          // two maps having the same Set "instances" for values, meaning modifying the prefix map
          // on newer version will also affect on the prefix map on older version.
          val newSet = new mutable.HashSet[UnsafeRow]()
          newSet ++= keySet
          prefixKeyToKeysMap.put(prefixKey, newSet)
        }

      case _ => other.entryIterator().foreach { pair => put(pair.getKey, pair.getValue) }
    }
  }

  override def remove(key: UnsafeRow): UnsafeRowWrapper = {
    val ret = map.remove(key)

    if (ret != null) {
      val prefixKey = prefixKeyProjection(key).copy()
      prefixKeyToKeysMap.computeIfPresent(prefixKey, (_, v) => {
        v.remove(key)
        if (v.isEmpty) null else v
      })
    }

    ret
  }

  override def iterator(): Iterator[UnsafeRowPair] = {
    val unsafeRowPair = new UnsafeRowPair()
    entryIterator().map { entry =>
      val valueRow = HDFSBackedStateStoreMap
        .getAndVerifyValueRow(entry.getKey, entry.getValue, readVerifier)
      unsafeRowPair.withRows(entry.getKey, valueRow)
    }
  }

  /** Returns entries in the underlying map and skips additional checks done by [[iterator]].
   * [[iterator]] should be preferred over this. */
  override def entryIterator(): Iterator[Entry[UnsafeRow, UnsafeRowWrapper]] = {
    map.entrySet.asScala.iterator
  }

  override def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair] = {
    val unsafeRowPair = new UnsafeRowPair()
    prefixKeyToKeysMap.getOrDefault(prefixKey, mutable.Set.empty[UnsafeRow])
      .iterator
      .map { keyRow =>
        val valueRow = HDFSBackedStateStoreMap
          .getAndVerifyValueRow(keyRow, map.get(keyRow), readVerifier)
        unsafeRowPair.withRows(keyRow, valueRow)
      }
  }
}
