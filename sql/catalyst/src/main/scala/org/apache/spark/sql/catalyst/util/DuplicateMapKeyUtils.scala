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
   * Builds an XML map with a constrained CHAR/VARCHAR key type.
   *
   * CHAR/VARCHAR keys: repeated XML keys after namespace handling keep the last value, then
   * `spark.sql.mapKeyDedupPolicy` applies to normalized keys. Failed values still
   * occupy a slot so collisions are visible.
   *
   * Example: parsing `a` and `a ` as CHAR(2) keys raises
   * DUPLICATED_MAP_KEY under EXCEPTION and keeps `a ` -> 2 under LAST_WIN.
   * Repeated XML keys after namespace handling use last-wins behavior regardless of policy.
   */
  def buildConstrainedMap(
      entries: Seq[(UTF8String, UTF8String, Option[Any])],
      keyType: DataType,
      valueType: DataType): MapData = {
    val lastEntries =
      mutable.LinkedHashMap.empty[UTF8String, (UTF8String, Option[Any])]
    entries.foreach { case (rawKey, normalizedKey, value) =>
      lastEntries.remove(rawKey)
      lastEntries.update(rawKey, (normalizedKey, value))
    }

    if (SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY) ==
        SQLConf.MapKeyDedupPolicy.EXCEPTION) {
      val distinctKeys = keyType match {
        case stringType: StringType if stringType.supportsBinaryEquality =>
          new java.util.HashSet[Any]()
        case _ =>
          new java.util.TreeSet[Any](TypeUtils.getInterpretedOrdering(keyType))
      }
      lastEntries.valuesIterator.foreach { case (key, _) =>
        if (!distinctKeys.add(key)) {
          throw QueryExecutionErrors.duplicateMapKeyFoundError(key)
        }
      }
    }
    val builder = new ArrayBasedMapBuilder(keyType, valueType)
    lastEntries.valuesIterator.foreach { case (normalizedKey, value) =>
      value.foreach(builder.put(normalizedKey, _))
    }
    builder.build()
  }
}
