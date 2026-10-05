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
import org.apache.spark.sql.types.{DataType, StringType}
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
   * Builds a JSON or XML map with a constrained CHAR/VARCHAR key type from a raw-key
   * last-wins accumulator. Entries whose values failed conversion still occupy a slot, so
   * normalized-key collisions remain visible.
   *
   * CHAR/VARCHAR keys: repeated exact serialized names keep the last value, then
   * `spark.sql.mapKeyDedupPolicy` applies to normalized keys.
   *
   * Example: parsing `a` and `a ` as CHAR(2) keys raises
   * DUPLICATED_MAP_KEY under EXCEPTION and keeps `a ` -> 2 under LAST_WIN.
   * Exact duplicate JSON member names, as in `{"a":1,"a":2}`, use last-wins behavior
   * regardless of policy.
   */
  def buildConstrainedMap(
      lastEntries: mutable.LinkedHashMap[UTF8String, (UTF8String, Option[Any])],
      keyType: DataType,
      valueType: DataType): MapData = {
    if (SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY) ==
        SQLConf.MapKeyDedupPolicy.EXCEPTION) {
      val distinctKeys = keyType match {
        case stringType: StringType if stringType.supportsBinaryEquality =>
          new java.util.HashSet[Any]()
        case _ =>
          new java.util.TreeSet[Any](TypeUtils.getInterpretedOrdering(keyType))
      }
      val keys = mutable.ArrayBuffer.empty[Any]
      val values = mutable.ArrayBuffer.empty[Any]
      lastEntries.valuesIterator.foreach { case (key, value) =>
        if (!distinctKeys.add(key)) {
          throw QueryExecutionErrors.duplicateMapKeyFoundError(key)
        }
        value.foreach { v =>
          keys += key
          values += v
        }
      }
      ArrayBasedMapData(keys.toArray, values.toArray)
    } else {
      val builder = new ArrayBasedMapBuilder(keyType, valueType)
      lastEntries.valuesIterator.foreach { case (normalizedKey, value) =>
        value.foreach(builder.put(normalizedKey, _))
      }
      builder.build()
    }
  }
}
