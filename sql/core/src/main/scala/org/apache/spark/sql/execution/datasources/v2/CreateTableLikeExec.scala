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
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog, TableInfo, V1Table}
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
 * The [[TableInfo]] passed to [[TableCatalog.createTableLike]] contains only user-specified
 * overrides (TBLPROPERTIES, LOCATION), the resolved provider, and the current user as owner.
 * Schema, partitioning, source TBLPROPERTIES, and constraints are NOT pre-populated; connectors
 * read all source metadata directly from [[sourceTable]].
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
      // Resolve provider: USING clause overrides, else inherit from source.
      // The source provider is inherited so that the target uses the same format,
      // matching V1 CreateTableLikeCommand behavior. Whether the target catalog validates
      // or uses this property is catalog-specific (e.g. V2SessionCatalog validates it).
      val resolvedProvider = provider.orElse {
        sourceTable match {
          case v1: V1Table if v1.catalogTable.tableType == CatalogTableType.VIEW =>
            // When the source is a view, default to the session's default data source.
            // This matches V1 CreateTableLikeCommand behavior.
            Some(session.sessionState.conf.defaultDataSourceName)
          case v1: V1Table =>
            v1.catalogTable.provider
          case _ =>
            Option(sourceTable.properties.get(TableCatalog.PROP_PROVIDER))
        }
      }

      // Build overrides-only properties: user TBLPROPERTIES, resolved provider, location, owner.
      // Schema, partitioning, source TBLPROPERTIES, and constraints are NOT included here;
      // connectors read them directly from sourceTable.
      val locationProp: Option[(String, String)] =
        location.map(uri => TableCatalog.PROP_LOCATION -> CatalogUtils.URIToString(uri))

      val finalProps = CatalogV2Util.withDefaultOwnership(
        properties ++
          resolvedProvider.map(TableCatalog.PROP_PROVIDER -> _) ++
          locationProp)

      try {
        val tableInfo = new TableInfo.Builder()
          .withProperties(finalProps.asJava)
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
}
