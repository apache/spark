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
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableInfo}
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
 * The [[TableInfo]] passed to [[TableCatalog.createTableLike]] contains strictly user-specified
 * overrides: TBLPROPERTIES, LOCATION, and USING provider (only if explicitly given).
 * Schema, partitioning, source provider, source TBLPROPERTIES, constraints, and owner are NOT
 * pre-populated; connectors read all source metadata directly from [[sourceTable]] and are
 * responsible for setting the owner.
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
      // Build strictly user-specified overrides: explicit TBLPROPERTIES, LOCATION (if given),
      // and USING provider (if given). Provider and owner are not included here; connectors
      // are responsible for reading PROP_PROVIDER from sourceTable.properties() and for
      // setting the owner via CurrentUserContext.getCurrentUser.
      val locationProp: Option[(String, String)] =
        location.map(uri => TableCatalog.PROP_LOCATION -> CatalogUtils.URIToString(uri))

      val finalProps =
        properties ++
          provider.map(TableCatalog.PROP_PROVIDER -> _) ++
          locationProp

      try {
        val userSpecifiedOverrides = new TableInfo.Builder()
          .withProperties(finalProps.asJava)
          .build()
        targetCatalog.createTableLike(targetIdent, sourceTable, userSpecifiedOverrides)
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
