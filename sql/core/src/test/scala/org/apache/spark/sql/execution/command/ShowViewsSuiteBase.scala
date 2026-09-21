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

import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.QueryTest

/**
 * Unified tests for `SHOW VIEWS` against V1 (session) and V2 view catalogs.
 */
trait ShowViewsSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "SHOW VIEWS"

  protected def namespace: String = "default"

  protected implicit val formats: Formats = DefaultFormats

  /** Runs `SHOW VIEWS ... AS JSON` and returns the `views` array of the single result row. */
  protected def showViewsAsJson(query: String): Seq[JValue] = {
    val rows = sql(query).collect()
    assert(rows.length == 1, s"AS JSON must return exactly one row: ${rows.mkString(", ")}")
    (parse(rows.head.getString(0)) \ "views").children
  }

  protected def viewNames(views: Seq[JValue]): Seq[String] =
    views.map(v => (v \ "viewName").extract[String])

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

  test("AS JSON returns a single json_metadata column") {
    sql(s"CREATE VIEW $catalog.$namespace.v_json_schema AS SELECT 1 AS x")
    val df = sql(s"SHOW VIEWS IN $catalog.$namespace AS JSON")
    assert(df.schema.length == 1)
    assert(df.schema.head.name == "json_metadata")
  }

  test("AS JSON lists user-created views") {
    sql(s"CREATE VIEW $catalog.$namespace.v_json_a AS SELECT 1 AS x")
    sql(s"CREATE VIEW $catalog.$namespace.v_json_b AS SELECT 2 AS x")
    val views = showViewsAsJson(s"SHOW VIEWS IN $catalog.$namespace AS JSON")
    val names = viewNames(views)
    assert(names.contains("v_json_a"), s"v_json_a missing: $names")
    assert(names.contains("v_json_b"), s"v_json_b missing: $names")
    assert(names.distinct.length == names.length, s"duplicate entries: $names")
    // Scope to the two views this test created: this suite also runs under v1, where a
    // leftover temp view (empty namespace, isTemporary=true) would otherwise fail the asserts.
    val created = views.filter(v => names.contains((v \ "viewName").extract[String]))
    created.foreach { v =>
      assert((v \ "namespace").extract[Seq[String]] == Seq(namespace))
      assert(!(v \ "isTemporary").extract[Boolean])
    }
  }

  test("AS JSON applies the LIKE pattern") {
    sql(s"CREATE VIEW $catalog.$namespace.v_json_match AS SELECT 1 AS x")
    sql(s"CREATE VIEW $catalog.$namespace.v_json_skip AS SELECT 1 AS x")
    val names = viewNames(
      showViewsAsJson(s"SHOW VIEWS IN $catalog.$namespace LIKE 'v_json_match' AS JSON"))
    assert(names == Seq("v_json_match"), s"pattern did not filter: $names")
  }

  test("AS JSON excludes non-view table entries") {
    val table = s"$catalog.$namespace.t_json_not_a_view"
    sql(s"CREATE VIEW $catalog.$namespace.v_json_only AS SELECT 1 AS x")
    withTable(table) {
      sql(s"CREATE TABLE $table (x INT) USING parquet")
      val names = viewNames(showViewsAsJson(s"SHOW VIEWS IN $catalog.$namespace AS JSON"))
      assert(names.contains("v_json_only"), s"v_json_only missing: $names")
      assert(!names.contains("t_json_not_a_view"), s"non-view leaked: $names")
    }
  }

  test("AS JSON emits an empty array when no view matches") {
    val rows = sql(s"SHOW VIEWS IN $catalog.$namespace LIKE 'v_json_no_such_view' AS JSON")
      .collect()
    assert(rows.length == 1)
    // Asserted literally so that a rename of the `views` key cannot pass unnoticed.
    assert(rows.head.getString(0) == """{"views":[]}""")
  }

  test("AS JSON agrees with the tabular output") {
    sql(s"CREATE VIEW $catalog.$namespace.v_json_parity AS SELECT 1 AS x")
    val expected = sql(s"SHOW VIEWS IN $catalog.$namespace").collect()
      .map(r => (r.getString(0), r.getString(1), r.getBoolean(2))).toSet
    // mkString(".") matches QuotingUtils.quoted only because these namespaces are single-part;
    // it is not a stand-in for quoted namespace rendering in general.
    val actual = showViewsAsJson(s"SHOW VIEWS IN $catalog.$namespace AS JSON").map { v =>
      ((v \ "namespace").extract[Seq[String]].mkString("."),
        (v \ "viewName").extract[String],
        (v \ "isTemporary").extract[Boolean])
    }.toSet
    assert(actual == expected)
  }
}
