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
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog, TableInfo, V1Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Physical plan node for CREATE TABLE ... LIKE ... targeting a v2 catalog.
 *
 * Copies schema (columns) and partitioning from `sourceTable`. The following properties of the
 * source table are intentionally NOT copied (matching v1 behavior):
 *   - Table-level comments
 *   - Source table's TBLPROPERTIES (user-specified `properties` are used instead)
 *   - Statistics, owner, create time
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
      // 1. Extract columns from source. For V1Table sources use the raw schema so that
      //    CHAR/VARCHAR types are preserved as declared (without internal metadata expansion).
      val columns = sourceTable match {
        case v1: V1Table =>
          val rawSchema = CharVarcharUtils.getRawSchema(v1.catalogTable.schema)
          CatalogV2Util.structTypeToV2Columns(rawSchema)
        case _ =>
          sourceTable.columns()
      }

      // 2. Extract partitioning from source (includes both partition columns and bucket spec
      //    for V1Table, as V1Table.partitioning encodes both).
      val partitioning = sourceTable.partitioning

      // 3. Resolve provider: USING clause overrides, else copy from source.
      //    The source provider is inherited so that the target table uses the same format,
      //    matching V1 CreateTableLikeCommand behavior. Whether the target catalog validates
      //    or uses this property is catalog-specific (e.g. V2SessionCatalog validates it).
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

      // 4. Build final properties. User-specified TBLPROPERTIES are used as-is; source table
      //    properties are NOT copied. Provider, location, and owner are added if applicable.
      val locationProp: Option[(String, String)] =
        location.map(uri => TableCatalog.PROP_LOCATION -> CatalogUtils.URIToString(uri))

      val finalProps = CatalogV2Util.withDefaultOwnership(
        properties ++
          resolvedProvider.map(TableCatalog.PROP_PROVIDER -> _) ++
          locationProp)

      try {
        // Constraints are intentionally NOT copied: V1 tables have no constraint objects
        // (CHECK/PRIMARY KEY/UNIQUE/FOREIGN KEY are V2-only, added in Spark 4.1.0);
        // ForeignKey carries a catalog-specific Identifier that becomes stale cross-catalog;
        // constraint names risk collisions in the target namespace; and NOT NULL is already
        // captured in Column.nullable(). Use ALTER TABLE ADD CONSTRAINT after creation, or
        // add an INCLUDING CONSTRAINTS clause in the future (following PostgreSQL semantics).
        val tableInfo = new TableInfo.Builder()
          .withColumns(columns)
          .withPartitions(partitioning)
          .withProperties(finalProps.asJava)
          .build()
        targetCatalog.createTable(targetIdent, tableInfo)
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
