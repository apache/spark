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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.SchemaEvolution
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTable.{VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX, VIEW_QUERY_OUTPUT_NUM_COLUMNS, VIEW_QUERY_OUTPUT_PREFIX, VIEW_SCHEMA_MODE}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

class AlterViewSchemaBindingSuite
  extends command.AlterViewSchemaBindingSuiteBase with ViewCommandSuiteBase {

  private def viewMeta(viewName: String): CatalogTable =
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(viewName, Some(namespace)))

  // Overwrites the view's stored query column names and keeps its schema.
  private def setStoredQueryColumnNames(viewName: String, names: Seq[String]): Unit = {
    val meta = viewMeta(viewName)
    val nameProps = names.zipWithIndex.map { case (name, i) =>
      s"$VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX$i" -> name
    }
    val props = meta.properties.filterNot(_._1.startsWith(VIEW_QUERY_OUTPUT_PREFIX)) ++
      nameProps + (VIEW_QUERY_OUTPUT_NUM_COLUMNS -> names.length.toString)
    spark.sessionState.catalog.alterTable(meta.copy(properties = props))
  }

  Seq("COMPENSATION", "BINDING", "TYPE EVOLUTION").foreach { mode =>
    test(s"leaving EVOLUTION for $mode rebuilds stale query column names") {
      val viewName = "v_evolution_stale_names"
      val view = s"$catalog.$namespace.$viewName"
      // Names left over from an older write, with fewer and with more columns than the schema.
      Seq(Seq("a", "b"), Seq("a", "b", "c", "d")).foreach { staleNames =>
        withView(view) {
          sql(s"CREATE VIEW $view WITH SCHEMA EVOLUTION AS SELECT 1 AS c1, 2 AS c2, 3 AS c3")
          setStoredQueryColumnNames(viewName, staleNames)

          sql(s"ALTER VIEW $view WITH SCHEMA $mode")
          checkAnswer(sql(s"SELECT * FROM $view"), Row(1, 2, 3))
          assert(viewMeta(viewName).viewQueryColumnNames == Seq("c1", "c2", "c3"))
        }
      }
    }
  }

  test("changing a non-EVOLUTION view keeps its stored query column names") {
    val viewName = "v_column_list_names"
    val view = s"$catalog.$namespace.$viewName"
    withView(view) {
      sql(s"CREATE VIEW $view (a, b) WITH SCHEMA BINDING AS SELECT 1 AS x, 2 AS y")
      sql(s"ALTER VIEW $view WITH SCHEMA COMPENSATION")
      val meta = viewMeta(viewName)
      assert(meta.viewQueryColumnNames == Seq("x", "y"))
      assert(meta.schema.fieldNames.toSeq == Seq("a", "b"))
      checkAnswer(sql(s"SELECT a, b FROM $view"), Row(1, 2))
    }
  }

  test("leaving EVOLUTION keeps stored query column names that match the schema size") {
    val viewName = "v_evolution_column_list"
    val view = s"$catalog.$namespace.$viewName"
    withView(view) {
      sql(s"CREATE VIEW $view (a, b) WITH SCHEMA BINDING AS SELECT 1 AS x, 2 AS y")
      // Older versions kept the stored names when such a view moved to EVOLUTION.
      val meta = viewMeta(viewName)
      spark.sessionState.catalog.alterTable(
        meta.copy(properties = meta.properties + (VIEW_SCHEMA_MODE -> SchemaEvolution.toString)))

      sql(s"ALTER VIEW $view WITH SCHEMA BINDING")
      checkAnswer(sql(s"SELECT a, b FROM $view"), Row(1, 2))
      assert(viewMeta(viewName).viewQueryColumnNames == Seq("x", "y"))
    }
  }

  test("leaving EVOLUTION keeps a Hive-created view without stored query column names") {
    val viewName = "v_hive_created"
    val view = s"$catalog.$namespace.$viewName"
    withView(view) {
      // Hive stores generated column names and no query column names.
      spark.sessionState.catalog.createTable(
        CatalogTable(
          identifier = TableIdentifier(viewName, Some(namespace)),
          tableType = CatalogTableType.VIEW,
          storage = CatalogStorageFormat.empty,
          schema = new StructType().add("_c0", IntegerType),
          viewText = Some("SELECT 1")),
        ignoreIfExists = false)

      sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")
      sql(s"ALTER VIEW $view WITH SCHEMA BINDING")
      checkAnswer(sql(s"SELECT * FROM $view"), Row(1))
      assert(viewMeta(viewName).viewQueryColumnNames.isEmpty)
    }
  }

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
        val pathAfterCreate = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(viewName, Some(namespace)))
          .viewStoredResolutionPath
          .getOrElse(fail("Expected the view to persist a frozen SQL path"))
        val parsedCreate = CatalogManager.deserializePathEntries(pathAfterCreate)
          .getOrElse(fail(s"Expected a valid serialized path, got: $pathAfterCreate"))
        assert(parsedCreate.contains(Seq(catalog, "alter_view_path_a")),
          s"Frozen path should include alter_view_path_a; got: $parsedCreate")

        // Switch the live PATH to something else and run ALTER VIEW WITH SCHEMA.
        // The captureNewPath = false code path must NOT overwrite the frozen path.
        sql(s"SET PATH = $catalog.default, system.builtin")
        sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")

        val pathAfterAlter = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(viewName, Some(namespace)))
          .viewStoredResolutionPath
          .getOrElse(fail("Frozen SQL path was dropped by ALTER VIEW WITH SCHEMA"))
        assert(pathAfterAlter == pathAfterCreate,
          s"ALTER VIEW WITH SCHEMA must preserve the frozen path. " +
            s"Before: $pathAfterCreate; after: $pathAfterAlter")
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql(s"DROP VIEW IF EXISTS $view")
        sql(s"DROP SCHEMA IF EXISTS $catalog.alter_view_path_a")
      }
    }
  }
}
