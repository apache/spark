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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{StructField, StructType}

trait HDFSBackedStateStoreMap {
  def size(): Int
  def get(key: UnsafeRow): UnsafeRow
  def put(key: UnsafeRow, value: UnsafeRow): UnsafeRow
  def putAll(map: HDFSBackedStateStoreMap): Unit
  def remove(key: UnsafeRow): UnsafeRow
  def iterator(): Iterator[UnsafeRowPair]
  def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair]
  def clear(): Unit
}

object HDFSBackedStateStoreMap {
  // ConcurrentHashMap is used because it generates fail-safe iterators on filtering
  // - The iterator is weakly consistent with the map, i.e., iterator's data reflect the values in
  //   the map when the iterator was created
  // - Any updates to the map while iterating through the filtered iterator does not throw
  //   java.util.ConcurrentModificationException
  type MapType = java.util.concurrent.ConcurrentHashMap[UnsafeRow, UnsafeRow]

  def create(keySchema: StructType, numColsPrefixKey: Int): HDFSBackedStateStoreMap = {
    if (numColsPrefixKey > 0) {
      new PrefixScannableHDFSBackedStateStoreMap(keySchema, numColsPrefixKey)
    } else {
      new NoPrefixHDFSBackedStateStoreMap()
    }
  }
}

class NoPrefixHDFSBackedStateStoreMap extends HDFSBackedStateStoreMap {
  private val map = new HDFSBackedStateStoreMap.MapType()

  override def size: Int = map.size()

  override def get(key: UnsafeRow): UnsafeRow = map.get(key)

  override def put(key: UnsafeRow, value: UnsafeRow): UnsafeRow = map.put(key, value)

  def putAll(other: HDFSBackedStateStoreMap): Unit = {
    other match {
      case o: NoPrefixHDFSBackedStateStoreMap => map.putAll(o.map)
      case _ => other.iterator().foreach { pair => put(pair.key, pair.value) }
    }
  }

  override def remove(key: UnsafeRow): UnsafeRow = map.remove(key)

  override def iterator(): Iterator[UnsafeRowPair] = {
    val unsafeRowPair = new UnsafeRowPair()
    map.entrySet.asScala.iterator.map { entry =>
      unsafeRowPair.withRows(entry.getKey, entry.getValue)
    }
  }

  override def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair] = {
    throw new UnsupportedOperationException("Prefix scan is not supported!")
  }

  override def clear(): Unit = map.clear()
}

class PrefixScannableHDFSBackedStateStoreMap(
    keySchema: StructType,
    numColsPrefixKey: Int) extends HDFSBackedStateStoreMap {

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

  override def size: Int = map.size()

  override def get(key: UnsafeRow): UnsafeRow = map.get(key)

  override def put(key: UnsafeRow, value: UnsafeRow): UnsafeRow = {
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

      case _ => other.iterator().foreach { pair => put(pair.key, pair.value) }
    }
  }

  override def remove(key: UnsafeRow): UnsafeRow = {
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
    map.entrySet.asScala.iterator.map { entry =>
      unsafeRowPair.withRows(entry.getKey, entry.getValue)
    }
  }

  override def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair] = {
    val unsafeRowPair = new UnsafeRowPair()
    prefixKeyToKeysMap.getOrDefault(prefixKey, mutable.Set.empty[UnsafeRow])
      .iterator
      .map { key => unsafeRowPair.withRows(key, map.get(key)) }
  }

  override def clear(): Unit = {
    map.clear()
    prefixKeyToKeysMap.clear()
  }
}
