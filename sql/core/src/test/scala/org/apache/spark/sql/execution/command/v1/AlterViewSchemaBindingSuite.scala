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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

class AlterViewSchemaBindingSuite
  extends command.AlterViewSchemaBindingSuiteBase with ViewCommandSuiteBase {

  test("ALTER VIEW ... WITH SCHEMA preserves the frozen SQL path") {
    // `generateViewProperties(captureNewPath = false)` is the documented behavior for
    // ALTER VIEW WITH SCHEMA: the view's body resolution path must stay pinned to the
    // create-time PATH, not the caller's current PATH. This test creates the view under
    // PATH=a, then runs ALTER VIEW WITH SCHEMA EVOLUTION under PATH=b, and asserts that
    // the persisted VIEW_RESOLUTION_PATH still reflects PATH=a.
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      val viewName = "v_path_preserved_on_alter"
      val view = s"$catalog.$namespace.$viewName"
      sql(s"CREATE SCHEMA IF NOT EXISTS $catalog.alter_view_path_a")
      try {
        sql(s"SET PATH = $catalog.alter_view_path_a, system.builtin")
        sql(s"CREATE VIEW $view AS SELECT 1 AS x")
        val metaAfterCreate = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(viewName, Some(namespace)))
        val pathAfterCreate = metaAfterCreate.viewStoredResolutionPath
          .getOrElse(fail("Expected the view to persist a frozen SQL path"))
        val versionAfterCreate = metaAfterCreate.viewMetadataVersion
        val catalogAfterCreate = metaAfterCreate.viewCatalogAndNamespace
        val parsedCreate = CatalogManager.deserializePathEntries(pathAfterCreate)
          .getOrElse(fail(s"Expected a valid serialized path, got: $pathAfterCreate"))
        assert(parsedCreate.contains(Seq(catalog, "alter_view_path_a")),
          s"Frozen path should include alter_view_path_a; got: $parsedCreate")

        // Switch the live PATH to something else and run ALTER VIEW WITH SCHEMA.
        // The captureNewPath = false code path must NOT overwrite the frozen path.
        sql(s"SET PATH = $catalog.default, system.builtin")
        sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")

        val metaAfterAlter = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(viewName, Some(namespace)))
        val pathAfterAlter = metaAfterAlter.viewStoredResolutionPath
          .getOrElse(fail("Frozen SQL path was dropped by ALTER VIEW WITH SCHEMA"))
        assert(pathAfterAlter == pathAfterCreate,
          s"ALTER VIEW WITH SCHEMA must preserve the frozen path. " +
            s"Before: $pathAfterCreate; after: $pathAfterAlter")
        assert(metaAfterAlter.viewMetadataVersion == versionAfterCreate,
          s"ALTER VIEW WITH SCHEMA must preserve metadataVersion. " +
            s"Before: $versionAfterCreate; after: ${metaAfterAlter.viewMetadataVersion}")
        assert(metaAfterAlter.viewCatalogAndNamespace == catalogAfterCreate,
          s"ALTER VIEW WITH SCHEMA must preserve creation catalog/namespace. " +
            s"Before: $catalogAfterCreate; after: ${metaAfterAlter.viewCatalogAndNamespace}")
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql(s"DROP VIEW IF EXISTS $view")
        sql(s"DROP SCHEMA IF EXISTS $catalog.alter_view_path_a")
      }
    }
  }

  test("ALTER VIEW ... WITH SCHEMA preserves creation catalog and namespace") {
    // Legacy (metadataVersion 0) PATH reconstruction appends the creation catalog for
    // two-part names. SCHEMA BINDING must not rewrite catalog/namespace from the
    // ALTER-time session, or resolution would silently change.
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      val viewName = "v_catalog_preserved_on_alter"
      val view = s"$catalog.$namespace.$viewName"
      val createNs = "alter_view_catalog_ns"
      sql(s"CREATE SCHEMA IF NOT EXISTS $catalog.$createNs")
      try {
        sql(s"USE $catalog.$createNs")
        sql(s"CREATE VIEW $view AS SELECT 1 AS x")
        val metaAfterCreate = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(viewName, Some(namespace)))
        val catalogAfterCreate = metaAfterCreate.viewCatalogAndNamespace
        assert(catalogAfterCreate === Seq(catalog, createNs),
          s"Expected creation catalog/namespace [$catalog, $createNs]; got: $catalogAfterCreate")

        // ALTER under a different current namespace must keep the create-time values.
        sql(s"USE $catalog.$namespace")
        sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")

        val metaAfterAlter = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(viewName, Some(namespace)))
        assert(metaAfterAlter.viewCatalogAndNamespace == catalogAfterCreate,
          s"ALTER VIEW WITH SCHEMA must preserve creation catalog/namespace. " +
            s"Before: $catalogAfterCreate; after: ${metaAfterAlter.viewCatalogAndNamespace}")
      } finally {
        sql(s"USE $catalog.$namespace")
        sql(s"DROP VIEW IF EXISTS $view")
        sql(s"DROP SCHEMA IF EXISTS $catalog.$createNs")
      }
    }
  }
}
