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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericRowWithSchema}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types.StructType

case class DescribeTableExec(
    output: Seq[Attribute],
    table: Table,
    isExtended: Boolean) extends LeafExecNode {

  private val encoder = RowEncoder(StructType.fromAttributes(output)).resolveAndBind()

  override protected def doExecute(): RDD[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    addSchema(rows)

    if (isExtended) {
      addPartitioning(rows)
      addProperties(rows)
    }
    sparkContext.parallelize(rows)
  }

  private def addSchema(rows: ArrayBuffer[InternalRow]): Unit = {
    rows ++= table.schema.map{ column =>
      toCatalystRow(
        column.name, column.dataType.simpleString, column.getComment().getOrElse(""))
    }
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += emptyRow()
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

  private def addProperties(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += emptyRow()
    rows += toCatalystRow(" Table Property", " Value", "")
    rows += toCatalystRow("----------------", "-------", "")
    rows ++= table.properties.asScala.toList.sortBy(_._1).map {
      case (key, value) => toCatalystRow(key, value, "")
    }
  }

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")

  private def toCatalystRow(strs: String*): InternalRow = {
    encoder.toRow(new GenericRowWithSchema(strs.toArray, schema)).copy()
  }
}
