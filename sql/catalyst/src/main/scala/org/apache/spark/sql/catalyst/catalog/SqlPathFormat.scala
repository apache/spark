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

package org.apache.spark.sql.catalyst.catalog

import scala.util.Try

import org.json4s.JsonAST.{JArray, JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

/**
 * Formatting helpers for the SQL Path stored in view and SQL function
 * metadata. The on-disk property stores path entries as a JSON array
 * of arrays:
 * {{{
 *   [["spark_catalog","default"],["system","builtin"]]
 * }}}
 * `toDescribeJson` converts these to the object form used by
 * `DESCRIBE AS JSON`:
 * {{{
 *   {"catalog_name": "spark_catalog", "namespace": ["default"]}
 * }}}
 * This supports multi-level namespaces.
 */
private[sql] object SqlPathFormat {

  /**
   * Build a JSON value for DESCRIBE AS JSON from a stored resolution
   * path string (JSON array of arrays persisted in the property).
   */
  def toDescribeJson(storedPathStr: String): Option[JValue] = {
    Try(parse(storedPathStr)) match {
      case scala.util.Success(JArray(entries)) if entries.nonEmpty =>
        val converted = entries.flatMap {
          case JArray(parts) =>
            val partStrs = parts.collect { case JString(s) => s }
            if (partStrs.isEmpty) None
            else Some(JObject(
              "catalog_name" -> JString(partStrs.head),
              "namespace" -> JArray(
                partStrs.tail.map(JString).toList)))
          case _ => None
        }
        if (converted.nonEmpty) Some(JArray(converted)) else None
      case _ => None
    }
  }

  /**
   * Format a JSON path value (array of objects with catalog_name and
   * namespace) as a human-readable string for DESCRIBE EXTENDED.
   * Example: `` `spark_catalog`.`default`, `system`.`builtin` ``
   */
  def formatForDisplay(jValue: JValue): Option[String] = {
    jValue match {
      case JArray(entries) =>
        Some(entries.map {
          case JObject(fields) =>
            val m = fields.toMap
            val cat = m.get("catalog_name")
              .map(_.values.toString).getOrElse("")
            val ns = m.get("namespace") match {
              case Some(JArray(parts)) =>
                parts.map(_.values.toString)
              case _ => Nil
            }
            val parts = (cat +: ns).filter(_.nonEmpty)
            if (parts.nonEmpty) parts.quoted else ""
          case _ => ""
        }.mkString(", "))
      case _ => Some(jValue.values.toString)
    }
  }
}
