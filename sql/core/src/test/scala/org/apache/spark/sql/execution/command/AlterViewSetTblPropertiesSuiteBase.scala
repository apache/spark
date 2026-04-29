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

import org.apache.spark.sql.{AnalysisException, QueryTest}

/**
 * This base suite contains unified tests for the `ALTER VIEW ... SET TBLPROPERTIES` command
 * that check V1 (session) and V2 view catalogs:
 *
 *   - V2 view catalog: `org.apache.spark.sql.execution.command.v2.AlterViewSetTblPropertiesSuite`
 *   - V1 (session) view catalog:
 *     `org.apache.spark.sql.execution.command.v1.AlterViewSetTblPropertiesSuite`
 */
trait AlterViewSetTblPropertiesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "ALTER VIEW ... SET TBLPROPERTIES"

  protected def namespace: String = "default"

  protected def createView(view: String): Unit = {
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
  }

  /** Reads back the property value visible via SHOW TBLPROPERTIES, or `None` if absent. */
  protected def lookupProperty(view: String, key: String): Option[String] = {
    // SHOW TBLPROPERTIES <view> ('key') always returns a single row; on a missing key the row
    // carries a "does not have property" placeholder. Iterate the full property listing
    // instead so absence is unambiguous.
    sql(s"SHOW TBLPROPERTIES $view").collect()
      .find(_.getString(0) == key)
      .map(_.getString(1))
  }

  test("set a single property") {
    val view = s"$catalog.$namespace.v_set_one"
    createView(view)
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('k' = 'v')")
    assert(lookupProperty(view, "k").contains("v"))
  }

  test("set multiple properties at once") {
    val view = s"$catalog.$namespace.v_set_many"
    createView(view)
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('a' = '1', 'b' = '2')")
    assert(lookupProperty(view, "a").contains("1"))
    assert(lookupProperty(view, "b").contains("2"))
  }

  test("setting overwrites existing property") {
    val view = s"$catalog.$namespace.v_set_overwrite"
    createView(view)
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('k' = 'v1')")
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('k' = 'v2')")
    assert(lookupProperty(view, "k").contains("v2"))
  }

  test("missing view raises a clean analysis error") {
    val view = s"$catalog.$namespace.v_missing_set"
    val ex = intercept[AnalysisException] {
      sql(s"ALTER VIEW $view SET TBLPROPERTIES ('k' = 'v')")
    }
    // Both v1 and v2 paths surface a TABLE_OR_VIEW_NOT_FOUND-shaped error here; the exact
    // condition string can differ slightly between paths, so just assert non-empty message.
    assert(ex.getMessage.contains(view.split('.').last))
  }

  test("show TBLPROPERTIES reflects the set property") {
    val view = s"$catalog.$namespace.v_show_after_set"
    createView(view)
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('k' = 'v')")
    val rows = sql(s"SHOW TBLPROPERTIES $view").collect()
    assert(rows.exists(r => r.getString(0) == "k" && r.getString(1) == "v"),
      s"property k=v missing from SHOW TBLPROPERTIES: ${rows.mkString(", ")}")
  }

  test("read-after-write returns user-set value") {
    val view = s"$catalog.$namespace.v_read_after_set"
    createView(view)
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('answer' = '42')")
    assert(lookupProperty(view, "answer").contains("42"))
    val all = sql(s"SHOW TBLPROPERTIES $view").collect()
    assert(all.length >= 1, s"expected at least one property row, got: ${all.mkString(", ")}")
  }

  test("setting `comment` flows through to SHOW CREATE TABLE") {
    // v1 `AlterTableSetPropertiesCommand` updates the typed `CatalogTable.comment` field when
    // the user passes `'comment'` via SET TBLPROPERTIES, so SHOW CREATE TABLE renders the
    // comment in the COMMENT clause. The v2 path uses `ViewInfo.properties` as the source of
    // truth for `PROP_COMMENT` (see `AlterV2ViewSetPropertiesExec` and `ShowCreateV2ViewExec`),
    // so the same SET TBLPROPERTIES('comment' = ...) round-trips through SHOW CREATE TABLE.
    // Pin the cross-catalog parity here.
    val view = s"$catalog.$namespace.v_set_comment"
    createView(view)
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('comment' = 'a view comment')")
    val ddl = sql(s"SHOW CREATE TABLE $view").collect().head.getString(0)
    assert(ddl.contains("a view comment"),
      s"comment did not flow through to SHOW CREATE TABLE: $ddl")
  }
}
