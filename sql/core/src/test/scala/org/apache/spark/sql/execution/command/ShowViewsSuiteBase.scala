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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.QueryTest

/**
 * Unified tests for `SHOW VIEWS` against V1 (session) and V2 view catalogs.
 */
trait ShowViewsSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "SHOW VIEWS"

  protected def namespace: String = "default"

  test("returns user-created views") {
    sql(s"CREATE VIEW $catalog.$namespace.v_show_views_a AS SELECT 1 AS x")
    sql(s"CREATE VIEW $catalog.$namespace.v_show_views_b AS SELECT 2 AS x")
    val rows = sql(s"SHOW VIEWS IN $catalog.$namespace").collect()
    val names = rows.map(_.getString(1)).toSet
    assert(names.contains("v_show_views_a"), s"v_show_views_a missing: $names")
    assert(names.contains("v_show_views_b"), s"v_show_views_b missing: $names")
  }

  test("LIKE pattern filters by name") {
    sql(s"CREATE VIEW $catalog.$namespace.show_views_match AS SELECT 1 AS x")
    sql(s"CREATE VIEW $catalog.$namespace.show_views_skip AS SELECT 1 AS x")
    val rows = sql(s"SHOW VIEWS IN $catalog.$namespace LIKE 'show_views_match'").collect()
    val names = rows.map(_.getString(1)).toSet
    assert(names.contains("show_views_match"))
    assert(!names.contains("show_views_skip"))
  }

  test("does not include non-view table entries") {
    // SHOW VIEWS lists views and only views. Both v1 (session catalog routing through
    // ShowTablesCommand-with-views-only) and v2 (`ShowViewsExec` routing through
    // `ViewCatalog.listViews`) should exclude tables, and both must mark `isTemporary` as
    // false for persistent view rows.
    val viewName = "v_show_views_only"
    val tableName = "t_not_in_show_views"
    val table = s"$catalog.$namespace.$tableName"
    sql(s"CREATE VIEW $catalog.$namespace.$viewName AS SELECT 1 AS x")
    withTable(table) {
      sql(s"CREATE TABLE $table (x INT) USING parquet")
      val rows = sql(s"SHOW VIEWS IN $catalog.$namespace").collect()
      val names = rows.map(_.getString(1)).toSet
      assert(names.contains(viewName), s"$viewName missing from SHOW VIEWS: $names")
      assert(!names.contains(tableName), s"non-view leaked into SHOW VIEWS: $names")
      rows.foreach(r => assert(!r.getBoolean(2),
        s"isTemporary must be false for persistent view rows: $r"))
    }
  }
}
