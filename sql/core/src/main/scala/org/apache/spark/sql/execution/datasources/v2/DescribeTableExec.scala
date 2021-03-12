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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericRowWithSchema}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsMetadataColumns, Table}
import org.apache.spark.sql.types.StructType

case class DescribeTableExec(
    output: Seq[Attribute],
    table: Table,
    isExtended: Boolean) extends V2CommandExec {

  private val toRow = {
    RowEncoder(StructType.fromAttributes(output)).resolveAndBind().createSerializer()
  }

  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    addSchema(rows)
    addPartitioning(rows)

    if (isExtended) {
      addMetadataColumns(rows)
      addTableDetails(rows)
    }
    rows.toSeq
  }

  private def addTableDetails(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += emptyRow()
    rows += toCatalystRow("# Detailed Table Information", "", "")
    rows += toCatalystRow("Name", table.name(), "")

    CatalogV2Util.TABLE_RESERVED_PROPERTIES.foreach(propKey => {
      if (table.properties.containsKey(propKey)) {
        rows += toCatalystRow(propKey.capitalize, table.properties.get(propKey), "")
      }
    })
    val properties =
      table.properties.asScala.toList
        .filter(kv => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(kv._1))
        .sortBy(_._1).map {
        case (key, value) => key + "=" + value
      }.mkString("[", ",", "]")
    rows += toCatalystRow("Table Properties", properties, "")
  }

  private def addSchema(rows: ArrayBuffer[InternalRow]): Unit = {
    rows ++= table.schema.map{ column =>
      toCatalystRow(
        column.name, column.dataType.simpleString, column.getComment().getOrElse(""))
    }
  }

  private def addMetadataColumns(rows: ArrayBuffer[InternalRow]): Unit = table match {
    case hasMeta: SupportsMetadataColumns if hasMeta.metadataColumns.nonEmpty =>
      rows += emptyRow()
      rows += toCatalystRow("# Metadata Columns", "", "")
      rows ++= hasMeta.metadataColumns.map { column =>
        toCatalystRow(
          column.name,
          column.dataType.simpleString,
          Option(column.comment()).getOrElse(""))
      }
    case _ =>
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += emptyRow()
    rows += toCatalystRow("# Partitioning", "", "")
    if (table.partitioning.isEmpty) {
      rows += toCatalystRow("Not partitioned", "", "")
    } else {
      rows ++= table.partitioning.zipWithIndex.map {
        case (transform, index) => toCatalystRow(s"Part $index", transform.describe(), "")
      }
    }
  }

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")

  private def toCatalystRow(strs: String*): InternalRow = {
    toRow(new GenericRowWithSchema(strs.toArray, schema)).copy()
  }
}
