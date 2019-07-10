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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.{Identifier, TableCatalog}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.plans.DescribeTableSchemas
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.sources.v2.Table

case class DescribeTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    isExtended: Boolean) extends LeafExecNode {

  import DescribeTableExec._

  override def output: Seq[AttributeReference] = DescribeTableSchemas.DESCRIBE_TABLE_ATTRIBUTES

  override protected def doExecute(): RDD[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    if (catalog.tableExists(ident)) {
      val table = catalog.loadTable(ident)
      addSchema(rows, table)

      if (isExtended) {
        addPartitioning(rows, table)
        addProperties(rows, table)
      }

    } else {
      rows += toCatalystRow(s"Table $ident does not exist.", "", "")
    }
    sparkContext.parallelize(rows)
  }

  private def addSchema(rows: ArrayBuffer[InternalRow], table: Table): Unit = {
    rows ++= table.schema.map{ column =>
      toCatalystRow(
        column.name, column.dataType.simpleString, column.getComment().getOrElse(""))
    }
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow], table: Table): Unit = {
    rows += EMPTY_ROW
    rows += toCatalystRow(" Partitioning", "", "")
    rows += toCatalystRow("--------------", "", "")
    if (table.partitioning.isEmpty) {
      rows += toCatalystRow("Not partitioned", "", "")
    } else {
      rows ++= table.partitioning.zipWithIndex.map {
        case (transform, index) => toCatalystRow(s"Part $index", transform.describe(), "")
      }
    }
  }

  private def addProperties(rows: ArrayBuffer[InternalRow], table: Table): Unit = {
    rows += EMPTY_ROW
    rows += toCatalystRow(" Table Property", " Value", "")
    rows += toCatalystRow("----------------", "-------", "")
    rows ++= table.properties.asScala.toList.sortBy(_._1).map {
      case (key, value) => toCatalystRow(key, value, "")
    }
  }
}

private object DescribeTableExec {
  private val ENCODER = RowEncoder(DescribeTableSchemas.DESCRIBE_TABLE_SCHEMA)
  private val EMPTY_ROW = toCatalystRow("", "", "")

  private def toCatalystRow(strs: String*): InternalRow = {
    ENCODER.resolveAndBind().toRow(
      new GenericRowWithSchema(strs.toArray, DescribeTableSchemas.DESCRIBE_TABLE_SCHEMA))
  }
}
