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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.V2ViewDescription
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode

case class DescribeViewExec(
    output: Seq[Attribute],
    desc: V2ViewDescription,
    isExtended: Boolean) extends V2CommandExec with LeafExecNode {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override protected def run(): Seq[InternalRow] =
    if (isExtended) {
      (describeSchema :+ emptyRow) ++ describeExtended
    } else {
      describeSchema
    }

  private def describeSchema: Seq[InternalRow] =
    desc.schema.map { column =>
      toCatalystRow(
        column.name,
        column.dataType.simpleString,
        column.getComment().getOrElse(""))
    }

  private def emptyRow: InternalRow = toCatalystRow("", "", "")

  private def describeExtended: Seq[InternalRow] = {
    val outputColumns = desc.viewQueryColumnNames.mkString("[", ", ", "]")
    val tableProperties = desc.properties
        .map(p => p._1 + "=" + p._2)
        .mkString("[", ", ", "]")

    toCatalystRow("# Detailed View Information", "", "") ::
        toCatalystRow("Owner", desc.owner.getOrElse(""), "") ::
        toCatalystRow("Comment", desc.comment.getOrElse(""), "") ::
        toCatalystRow("View Text", desc.sql, "") ::
        toCatalystRow("View Catalog and Namespace", desc.viewCatalogAndNamespace.quoted, "") ::
        toCatalystRow("View Query Output Columns", outputColumns, "") ::
        toCatalystRow("Table Properties", tableProperties, "") ::
        toCatalystRow("Created By", desc.createEngineVersion.getOrElse(""), "") ::
        Nil
  }
}
