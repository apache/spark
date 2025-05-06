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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty

import org.apache.spark.internal.Logging
import org.apache.spark.util.JsonUtils

/**
 * A mapping of field-id to column names used by Spark Parquet readers
 * to match columns on Parquet files that do not have field-id in its schema.
 *
 * The mapping is provided as a json string. Here's an example of a mapping
 * that assign field ids to a table schema with nested fields.
 * [
 *  { "field-id": 1, "names": ["col1"] },
 *  { "field-id": 2, "names": ["col2"] },
 *  { "field-id": 3, "names": ["location"], "fields": [
 *       { "field-id": 4, "names": ["lat"] },
 *       { "field-id": 5, "names": ["long"] }]
 *  }
 * ]
 */
trait ParquetIdExternalMapping {
  // On nested mapping only, return the field-id of this nested mapping
  // mapping.getChild("field").getRootId == mapping.getFieldId("field")
  def getRootId: Option[Int]
  // field id for a given field name
  def getFieldId(key: String): Option[Int]
  // Nested mapping for child struct
  def getChild(key: String): ParquetIdExternalMapping
  // Nested mapping for array
  def getArray: ParquetIdExternalMapping = getChild("element")
  // Nested mapping for map key
  def getMapKey: ParquetIdExternalMapping = getChild("key")
  // Nested mapping for map value
  def getMapValue: ParquetIdExternalMapping = getChild("value")
}

object ParquetIdExternalMapping extends JsonUtils with Logging {
  object EmptyMapping extends ParquetIdExternalMapping {
    override def getRootId: Option[Int] = None
    override def getFieldId(key: String): Option[Int] = None
    override def getChild(key: String): ParquetIdExternalMapping = EmptyMapping
  }

  private class Mapping(fields: Array[Field], rootId: Option[Int] = None)
    extends ParquetIdExternalMapping {

    validate()

    val nameToFields: Map[String, Field] = fields.flatMap(f => f.names.map(n => n -> f)).toMap

    override def getRootId: Option[Int] = rootId

    override def getFieldId(key: String): Option[Int] = nameToFields.get(key).map(_.fieldId)

    override def getChild(key: String): ParquetIdExternalMapping =
      nameToFields.get(key)
        .map {
          case f if f.fields != null => new Mapping(f.fields, Some(f.fieldId))
          case _ => EmptyMapping
        }
        .getOrElse(EmptyMapping)

    /**
     * Validate if the following situation exist
     * * Duplicate field-id
     * * Duplicate names from fields of the same level
     */
    private def validate(): Unit = {
      def recursiveVisit(fs: Array[Field], ids: mutable.Set[Int]): Unit = {
        if (fs == null || fs.isEmpty) return
        val idsOnLevel = fs.map(_.fieldId).toSet
        if (idsOnLevel.size < fs.length || idsOnLevel.intersect(ids).nonEmpty) {
          throw new IllegalArgumentException("Duplicate field-ids")
        }
        val namesOnLevel = fs
          .flatMap({
            case f if f.names.length == f.names.toSet.size => f.names
            case _ => throw new IllegalArgumentException("Duplicate names within a field")
          })
          .toSet
        val expectedNameCount = fs.map(_.names.length).sum
        if (namesOnLevel.size < expectedNameCount) {
          throw new IllegalArgumentException("Duplicate names between fields")
        }
        val updatedIds = ids ++ idsOnLevel
        fs.foreach(f => recursiveVisit(f.fields, updatedIds))
      }
      recursiveVisit(fields, new mutable.HashSet())
    }
  }

  private case class Field(
      @JsonProperty("field-id") fieldId: Int,
      names: Array[String],
      fields: Array[Field])

  def fromJson(jsonStr: String): ParquetIdExternalMapping = {
    try {
      val jsonFields = mapper.readValue(jsonStr, classOf[Array[Field]])
      new Mapping(jsonFields)
    } catch {
      case NonFatal(e) =>
        logError(log"Exception when parse external id mapping", e)
        EmptyMapping
    }
  }
}
