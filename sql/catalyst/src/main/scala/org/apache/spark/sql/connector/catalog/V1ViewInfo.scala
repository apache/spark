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
package org.apache.spark.sql.connector.catalog

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * A v1 [[CatalogTable]] (representing a session-catalog view) exposed through the v2
 * [[ViewInfo]] surface, mirroring the way [[V1Table]] exposes a v1 table CatalogTable through
 * the v2 [[Table]] surface. Holds the original [[CatalogTable]] in [[v1Table]] for v1-only
 * paths that need the full v1 metadata representation (e.g. `DescribeTableCommand`,
 * `ShowCreateTableCommand`, anything that calls `CatalogTable#toLinkedHashMap`).
 *
 * Note on `properties()`: the inherited [[ViewInfo#properties]] bag is built from the entire
 * `v1Table.properties` map, which intermixes user TBLPROPERTIES with v1-internal storage keys
 * (`view.sqlConfig.*`, `view.catalogAndNamespace.*`, `view.query.out.*`, `view.schemaMode`).
 * v2 view inspection / SET execs (`ShowV2ViewPropertiesExec`, `AlterV2ViewSetPropertiesExec`,
 * etc.) never see a `V1ViewInfo` -- `ResolveSessionCatalog` rewrites session-catalog views to
 * v1 commands first -- so the bag stays internal to v1-only paths. Consumers that do receive
 * a `V1ViewInfo` should prefer the typed accessors ([[ViewInfo#sqlConfigs]],
 * [[ViewInfo#currentNamespace]], [[ViewInfo#currentCatalog]], [[ViewInfo#queryColumnNames]],
 * [[ViewInfo#schemaMode]]) for the v1-internal fields rather than scraping `properties()` for
 * them.
 */
private[sql] class V1ViewInfo(val v1Table: CatalogTable)
    extends ViewInfo(V1ViewInfo.builderFrom(v1Table))

private[sql] object V1ViewInfo {
  /**
   * Convert a v1 [[CatalogTable]] view into a [[ViewInfo.Builder]] with the same fields.
   * Used as the {@code super(builder)} argument when constructing a [[V1ViewInfo]].
   */
  private def builderFrom(v1Table: CatalogTable): ViewInfo.Builder = {
    val builder = new ViewInfo.Builder()
    builder.withSchema(v1Table.schema)
    builder.withProperties(v1Table.properties.asJava)
    // v1 stores collation / comment in typed `CatalogTable` fields rather than in `properties`,
    // but consumers reading off [[ViewInfo]] (`ApplyDefaultCollation.fetchDefaultCollation`,
    // `ShowCreateV2ViewExec`, etc.) expect them under `PROP_COLLATION` / `PROP_COMMENT`. Bridge
    // them through the typed setters so the v2 surface sees the same view metadata regardless
    // of which catalog produced it.
    v1Table.collation.foreach(builder.withCollation)
    v1Table.comment.foreach(builder.withComment)
    // ViewInfo requires a non-null queryText; v1 views always have one, but defend against
    // an old/corrupt CatalogTable with `viewText = None` by falling back to an empty string.
    builder.withQueryText(v1Table.viewText.getOrElse(""))
    val cn = v1Table.viewCatalogAndNamespace
    if (cn.nonEmpty) {
      builder.withCurrentCatalog(cn.head)
      builder.withCurrentNamespace(cn.tail.toArray)
    }
    builder.withSqlConfigs(v1Table.viewSQLConfigs.asJava)
    Option(v1Table.viewSchemaMode).foreach(m => builder.withSchemaMode(m.toString))
    builder.withQueryColumnNames(v1Table.viewQueryColumnNames.toArray)
    builder
  }
}
