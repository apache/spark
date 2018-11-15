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

package org.apache.spark.sql.catalyst.util

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{AtomicType, CalendarIntervalType, DataType, MapType}

/**
 * A builder of [[ArrayBasedMapData]], which fails if a null map key is detected, and removes
 * duplicated map keys w.r.t. the last wins policy.
 */
class ArrayBasedMapBuilder(keyType: DataType, valueType: DataType) extends Serializable {
  assert(!keyType.existsRecursively(_.isInstanceOf[MapType]), "key of map cannot be/contain map")

  private lazy val keyToIndex = keyType match {
    case _: AtomicType | _: CalendarIntervalType => mutable.HashMap.empty[Any, Int]
    case _ =>
      // for complex types, use interpreted ordering to be able to compare unsafe data with safe
      // data, e.g. UnsafeRow vs GenericInternalRow.
      mutable.TreeMap.empty[Any, Int](TypeUtils.getInterpretedOrdering(keyType))
  }

  // TODO: specialize it
  private lazy val keys = mutable.ArrayBuffer.empty[Any]
  private lazy val values = mutable.ArrayBuffer.empty[Any]

  private lazy val keyGetter = InternalRow.getAccessor(keyType)
  private lazy val valueGetter = InternalRow.getAccessor(valueType)

  def reset(): Unit = {
    keyToIndex.clear()
    keys.clear()
    values.clear()
  }

  def put(key: Any, value: Any): Unit = {
    if (key == null) {
      throw new RuntimeException("Cannot use null as map key.")
    }

    val maybeExistingIdx = keyToIndex.get(key)
    if (maybeExistingIdx.isDefined) {
      // Overwrite the previous value, as the policy is last wins.
      values(maybeExistingIdx.get) = value
    } else {
      keyToIndex.put(key, values.length)
      keys.append(key)
      values.append(value)
    }
  }

  // write a 2-field row, the first field is key and the second field is value.
  def put(entry: InternalRow): Unit = {
    if (entry.isNullAt(0)) {
      throw new RuntimeException("Cannot use null as map key.")
    }
    put(keyGetter(entry, 0), valueGetter(entry, 1))
  }

  def putAll(keyArray: Array[Any], valueArray: Array[Any]): Unit = {
    if (keyArray.length != valueArray.length) {
      throw new RuntimeException(
        "The key array and value array of MapData must have the same length.")
    }

    var i = 0
    while (i < keyArray.length) {
      put(keyArray(i), valueArray(i))
      i += 1
    }
  }

  def putAll(keyArray: ArrayData, valueArray: ArrayData): Unit = {
    if (keyArray.numElements() != valueArray.numElements()) {
      throw new RuntimeException(
        "The key array and value array of MapData must have the same length.")
    }

    var i = 0
    while (i < keyArray.numElements()) {
      put(keyGetter(keyArray, i), valueGetter(valueArray, i))
      i += 1
    }
  }

  def build(): ArrayBasedMapData = {
    new ArrayBasedMapData(new GenericArrayData(keys.toArray), new GenericArrayData(values.toArray))
  }

  def from(keyArray: ArrayData, valueArray: ArrayData): ArrayBasedMapData = {
    assert(keyToIndex.isEmpty, "'from' can only be called with a fresh GenericMapBuilder.")
    putAll(keyArray, valueArray)
    if (keyToIndex.size == keyArray.numElements()) {
      // If there is no duplicated map keys, creates the MapData with the input key and value array,
      // as they might already in unsafe format and are more efficient.
      new ArrayBasedMapData(keyArray, valueArray)
    } else {
      build()
    }
  }
}
