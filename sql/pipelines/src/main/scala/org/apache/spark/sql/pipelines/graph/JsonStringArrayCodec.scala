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

package org.apache.spark.sql.pipelines.graph

import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.DeserializationFeature
import org.json4s.JsonAST.{JArray, JString, JValue}
import org.json4s.jackson.JsonMethods

/**
 * Codec for table property values that hold a list of strings as a JSON string array, e.g.
 * `["id","region"]`. Used for [[PipelinesTableProperties.declaredPropertyKeys]] and for the
 * column-name lists [[AutoCdcAuxiliaryTable]] records on auxiliary tables. An empty list
 * round-trips as `[]`; whether an empty list is meaningful is up to the caller.
 */
private[graph] object JsonStringArrayCodec {

  def serialize(values: Seq[String]): String = {
    JsonMethods.compact(JArray(values.map(JString(_)).toList))
  }

  /**
   * Reads one JSON value and fails on anything but whitespace after it. `JsonMethods.parse` stops
   * at the end of the first value, so `["p.a"]x` or `["p.a"],["p.b"]` would read as `["p.a"]`.
   */
  private val reader = JsonMethods.mapper
    .readerFor(classOf[JValue])
    .withFeatures(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

  /**
   * Parses a value written by [[serialize]]. `None` if it is not JSON, is followed by anything but
   * whitespace, or is not a JSON array whose elements are all strings, so that callers can treat a
   * malformed value as absent.
   */
  def parse(raw: String): Option[Seq[String]] = {
    val parsed = try Some(reader.readValue[JValue](raw)) catch { case NonFatal(_) => None }
    parsed.flatMap {
      case JArray(elems) =>
        val values = elems.collect { case JString(s) => s }
        if (values.size == elems.size) Some(values) else None
      case _ => None
    }
  }
}
