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

import java.net.URI

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.LogKeys.TABLE_NAME
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, Table, TableCatalog, TableInfo, V1Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Physical plan node for CREATE TABLE ... LIKE ... targeting a v2 catalog.
 *
 * Calls [[TableCatalog.createTableLike]] so that connectors can implement format-specific copy
 * semantics (e.g. Delta protocol inheritance, Iceberg sort order and format version). Connectors
 * must override [[TableCatalog.createTableLike]]; the default implementation throws
 * [[UnsupportedOperationException]].
 *
 * The [[TableInfo]] passed to [[TableCatalog.createTableLike]] contains all explicit information
 * for the new table: columns and partitioning copied from the source, constraints copied from
 * the source, user-specified TBLPROPERTIES / LOCATION / USING provider (if given), and
 * [[TableCatalog.PROP_OWNER]] set to the current user. Source table properties are intentionally
 * excluded so that connectors can decide which custom properties to clone via [[sourceTable]].
 */
case class CreateTableLikeExec(
    targetCatalog: TableCatalog,
    targetIdent: Identifier,
    sourceTable: Table,
    location: Option[URI],
    provider: Option[String],
    properties: Map[String, String],
    ifNotExists: Boolean) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    if (!targetCatalog.tableExists(targetIdent)) {
      try {
        val tableInfo = new TableInfo.Builder()
          .withColumns(targetColumns)
          .withPartitions(sourceTable.partitioning)
          .withConstraints(sourceTable.constraints)
          .withProperties(targetProperties.asJava)
          .build()
        targetCatalog.createTableLike(targetIdent, tableInfo, sourceTable)
      } catch {
        case _: TableAlreadyExistsException if ifNotExists =>
          logWarning(
            log"Table ${MDC(TABLE_NAME, targetIdent.quoted)} was created concurrently. Ignoring.")
      }
    } else if (!ifNotExists) {
      throw QueryCompilationErrors.tableAlreadyExistsError(targetIdent)
    }

    Seq.empty
  }

  // Derive target columns from source; for V1Table sources apply CharVarcharUtils to preserve
  // CHAR/VARCHAR types as declared rather than collapsed to StringType.
  private def targetColumns: Array[Column] =
    sourceTable match {
      case v1: V1Table =>
        CatalogV2Util.structTypeToV2Columns(CharVarcharUtils.getRawSchema(v1.catalogTable.schema))
      case _ =>
        sourceTable.columns
    }

  // Source table properties are intentionally excluded; connectors read sourceTable
  // to clone any additional format-specific or custom state they need.
  private def targetProperties: Map[String, String] =
    CatalogV2Util.withDefaultOwnership(
      properties ++
        provider.map(TableCatalog.PROP_PROVIDER -> _) ++
        location.map(uri => TableCatalog.PROP_LOCATION -> CatalogUtils.URIToString(uri)))
}
