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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CharType, DataType, StringType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SparkErrorUtils

private[sql] object DuplicateMapKeyUtils {
  def cause(exception: Throwable): Option[SparkRuntimeException] = {
    SparkErrorUtils.getRootCause(exception) match {
      case cause: SparkRuntimeException if cause.getCondition == "DUPLICATED_MAP_KEY" =>
        Some(cause)
      case _ => None
    }
  }

  def unapply(exception: Throwable): Option[SparkRuntimeException] = cause(exception)

  /**
   * Builds a parsed JSON/XML object as a map.
   *
   * CHAR/VARCHAR keys: exact serialized names keep the last value, then
   * `spark.sql.mapKeyDedupPolicy` applies to normalized keys. Failed values still
   * occupy a slot so collisions are visible.
   *
   * Example: `from_json('{"a":1,"a ":2}', 'MAP<CHAR(2), INT>')` raises
   * DUPLICATED_MAP_KEY under EXCEPTION and keeps `a ` -> 2 under LAST_WIN.
   * Exact `{"a":1,"a":2}` is last-wins regardless of policy.
   *
   * Ordinary STRING keys keep historical last-wins. When
   * `collapseOrdinaryStringKeys` is true (XML), duplicates collapse via `Map`.
   * When false (JSON), retained pairs are stored as parallel arrays.
   */
  def buildParsedMap(
      entries: Seq[(UTF8String, UTF8String, Option[Any])],
      keyType: DataType,
      valueType: DataType,
      collapseOrdinaryStringKeys: Boolean): MapData = {
    keyType match {
      case _: CharType | _: VarcharType =>
        buildMapWithLastRawKeyWins(entries, keyType, valueType)
      case _ if collapseOrdinaryStringKeys =>
        ArrayBasedMapData(
          entries.flatMap { case (_, key, value) => value.map(key -> _) }.toMap)
      case _ =>
        val retained = entries.flatMap { case (_, key, value) => value.map(key -> _) }
        ArrayBasedMapData(retained.map(_._1).toArray, retained.map(_._2).toArray)
    }
  }

  private def buildMapWithLastRawKeyWins(
      entries: Seq[(UTF8String, UTF8String, Option[Any])],
      keyType: DataType,
      valueType: DataType): MapData = {
    val indices = lastOccurrenceIndices(entries.map(_._1).toArray)
    if (SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY) ==
        SQLConf.MapKeyDedupPolicy.EXCEPTION) {
      val distinctKeys = keyType match {
        case stringType: StringType if stringType.supportsBinaryEquality =>
          new java.util.HashSet[Any]()
        case _ =>
          new java.util.TreeSet[Any](TypeUtils.getInterpretedOrdering(keyType))
      }
      indices.foreach { index =>
        val key = entries(index)._2
        if (!distinctKeys.add(key)) {
          throw QueryExecutionErrors.duplicateMapKeyFoundError(key)
        }
      }
    }
    val builder = new ArrayBasedMapBuilder(keyType, valueType)
    indices.foreach { index =>
      val (_, normalizedKey, value) = entries(index)
      value.foreach(builder.put(normalizedKey, _))
    }
    builder.build()
  }

  private def lastOccurrenceIndices(rawKeys: Array[UTF8String]): Seq[Int] = {
    val lastIndices = mutable.LinkedHashMap.empty[UTF8String, Int]
    rawKeys.indices.foreach(index => lastIndices.update(rawKeys(index), index))
    lastIndices.values.toSeq
  }
}
