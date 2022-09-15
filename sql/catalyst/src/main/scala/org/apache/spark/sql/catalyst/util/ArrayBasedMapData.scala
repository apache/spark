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

import java.util.{Map => JavaMap}

import org.apache.spark.util.collection.Utils

/**
 * A simple `MapData` implementation which is backed by 2 arrays.
 *
 * Note that, user is responsible to guarantee that the key array does not have duplicated
 * elements, otherwise the behavior is undefined.
 */
class ArrayBasedMapData(val keyArray: ArrayData, val valueArray: ArrayData) extends MapData {
  require(keyArray.numElements() == valueArray.numElements())

  override def numElements(): Int = keyArray.numElements()

  override def copy(): MapData = new ArrayBasedMapData(keyArray.copy(), valueArray.copy())

  override def toString: String = {
    s"keys: $keyArray, values: $valueArray"
  }
}

object ArrayBasedMapData {
  /**
   * Creates a [[ArrayBasedMapData]] by applying the given converters over
   * each (key -> value) pair of the input [[java.util.Map]]
   *
   * @param javaMap Input map
   * @param keyConverter This function is applied over all the keys of the input map to
   *                     obtain the output map's keys
   * @param valueConverter This function is applied over all the values of the input map to
   *                       obtain the output map's values
   */
  def apply[K, V](
      javaMap: JavaMap[K, V],
      keyConverter: (Any) => Any,
      valueConverter: (Any) => Any): ArrayBasedMapData = {

    val keys: Array[Any] = new Array[Any](javaMap.size())
    val values: Array[Any] = new Array[Any](javaMap.size())

    var i: Int = 0
    val iterator = javaMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      keys(i) = keyConverter(entry.getKey)
      values(i) = valueConverter(entry.getValue)
      i += 1
    }
    ArrayBasedMapData(keys, values)
  }

  /**
   * Creates a [[ArrayBasedMapData]] by applying the given converters over
   * each (key -> value) pair of the input map
   *
   * @param map Input map
   * @param keyConverter This function is applied over all the keys of the input map to
   *                     obtain the output map's keys
   * @param valueConverter This function is applied over all the values of the input map to
   *                       obtain the output map's values
   */
  def apply(
      map: scala.collection.Map[_, _],
      keyConverter: (Any) => Any = identity,
      valueConverter: (Any) => Any = identity): ArrayBasedMapData = {
    ArrayBasedMapData(map.iterator, map.size, keyConverter, valueConverter)
  }

  /**
   * Creates a [[ArrayBasedMapData]] by applying the given converters over
   * each (key -> value) pair from the given iterator
   *
   * Note that, user is responsible to guarantee that the key array does not have duplicated
   * elements, otherwise the behavior is undefined.
   *
   * @param iterator Input iterator
   * @param size Number of elements
   * @param keyConverter This function is applied over all the keys extracted from the
   *                     given iterator to obtain the output map's keys
   * @param valueConverter This function is applied over all the values extracted from the
   *                       given iterator to obtain the output map's values
   */
  def apply(
      iterator: Iterator[(_, _)],
      size: Int,
      keyConverter: (Any) => Any,
      valueConverter: (Any) => Any): ArrayBasedMapData = {

    val keys: Array[Any] = new Array[Any](size)
    val values: Array[Any] = new Array[Any](size)

    var i = 0
    for ((key, value) <- iterator) {
      keys(i) = keyConverter(key)
      values(i) = valueConverter(value)
      i += 1
    }
    ArrayBasedMapData(keys, values)
  }

  /**
   * Creates a [[ArrayBasedMapData]] from a key and value array.
   *
   * Note that, user is responsible to guarantee that the key array does not have duplicated
   * elements, otherwise the behavior is undefined.
   */
  def apply(keys: Array[_], values: Array[_]): ArrayBasedMapData = {
    new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
  }

  def toScalaMap(map: ArrayBasedMapData): Map[Any, Any] = {
    val keys = map.keyArray.asInstanceOf[GenericArrayData].array
    val values = map.valueArray.asInstanceOf[GenericArrayData].array
    Utils.toMap(keys, values)
  }

  def toScalaMap(keys: Array[Any], values: Array[Any]): Map[Any, Any] = {
    Utils.toMap(keys, values)
  }

  def toScalaMap(keys: scala.collection.Seq[Any],
      values: scala.collection.Seq[Any]): Map[Any, Any] = {
    Utils.toMap(keys, values)
  }

  def toJavaMap(keys: Array[Any], values: Array[Any]): java.util.Map[Any, Any] = {
    Utils.toJavaMap(keys, values)
  }
}
