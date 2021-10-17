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

package org.apache.spark.sql.execution.datasources.v2

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.IndexAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.index.SupportsIndex
import org.apache.spark.sql.connector.expressions.NamedReference

/**
 * Physical plan node for creating an index.
 */
case class CreateIndexExec(
    table: SupportsIndex,
    indexName: String,
    indexType: String,
    ignoreIfExists: Boolean,
    columns: Seq[(NamedReference, Map[String, String])],
    properties: Map[String, String])
  extends LeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    try {
      val colProperties: Array[util.Map[NamedReference, util.Map[String, String]]] = columns.map {
        case (column, map) =>
          val javaMap = new util.HashMap[String, String]
          map.foreach { case (key, value) => javaMap.put(key, value) }
          val propMap = new util.HashMap[NamedReference, util.Map[String, String]]
          propMap.put(column, javaMap)
          propMap
      }.toArray
      val indexProperties = new util.HashMap[String, String]
      properties.foreach { case (key, value) => indexProperties.put(key, value) }
      table match {
        case _: SupportsIndex =>
          val (cols, _) = columns.unzip
          table.asInstanceOf[SupportsIndex].createIndex(indexName, indexType, cols.toArray,
            colProperties, indexProperties)
        case _ =>
      }
    } catch {
      case _: IndexAlreadyExistsException if ignoreIfExists =>
        logWarning(s"Index ${indexName} already exists. Ignoring.")
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
