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
 * Unified tests for `DROP VIEW` against V1 (session) and V2 view catalogs.
 */
trait DropViewSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "DROP VIEW"

  protected def namespace: String = "default"

  protected def viewExists(qualified: String): Boolean = {
    val parts = qualified.split('.').toSeq
    val nsAndView = parts.tail
    val ns = nsAndView.init.mkString(".")
    val name = nsAndView.last
    sql(s"SHOW VIEWS IN $catalog.$ns").collect().exists(_.getString(1) == name)
  }

  /**
   * Seed a non-view table at `qualified` (full `catalog.ns.name`) and run `body`. Same SQL
   * for v1 and v2 -- `InMemoryTableViewCatalog.createTable` accepts the parquet TableInfo
   * the same way the session catalog does, so both legs share this implementation.
   */
  protected final def withSeededTable(qualified: String)(body: => Unit): Unit = {
    withTable(qualified) {
      sql(s"CREATE TABLE $qualified (col STRING) USING parquet")
      body
    }
  }

  test("drop existing view") {
    val view = s"$catalog.$namespace.v_drop_basic"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    assert(viewExists(view))
    sql(s"DROP VIEW $view")
    assert(!viewExists(view))
  }

  test("drop missing view fails without IF EXISTS") {
    val view = s"$catalog.$namespace.v_drop_missing"
    intercept[AnalysisException] {
      sql(s"DROP VIEW $view")
    }
  }

  test("drop with IF EXISTS is a no-op when missing") {
    sql(s"DROP VIEW IF EXISTS $catalog.$namespace.v_drop_never_existed")
  }

  test("DROP VIEW on a non-view table entry surfaces WRONG_COMMAND_FOR_OBJECT_TYPE") {
    // Both v1 `DropTableCommand` and v2 `DropViewExec` route this case to
    // `WRONG_COMMAND_FOR_OBJECT_TYPE`, which renders "Use DROP TABLE instead" -- giving the
    // user the right command to retry. The `alternative` parameter on the rendered message
    // surfaces the suggestion that subclassed `EXPECT_*` errors otherwise carry only via
    // their subclass name.
    val view = s"$catalog.$namespace.v_drop_table_collide"
    withSeededTable(view) {
      val ex = intercept[AnalysisException] {
        sql(s"DROP VIEW $view")
      }
      assert(ex.getCondition == "WRONG_COMMAND_FOR_OBJECT_TYPE",
        s"unexpected error condition: ${ex.getCondition}")
      assert(ex.getMessage.contains("Use DROP TABLE instead"))
    }
  }
}
