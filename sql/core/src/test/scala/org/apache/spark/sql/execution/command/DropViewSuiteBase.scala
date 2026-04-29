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
   * Seed a non-view table at `qualified` (full `catalog.ns.name`) and run `body`. The leaf
   * suite implements the path-specific seeding (v1 SQL `CREATE TABLE`, v2 catalog API call)
   * and the matching cleanup so the test does not have to know which catalog is under test.
   */
  protected def withSeededTable(qualified: String)(body: => Unit): Unit

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

  test("DROP VIEW on a non-view table entry is rejected") {
    // Both paths refuse to drop a non-view table when the user said DROP VIEW, but the
    // error class differs as a pre-existing divergence: v1 `DropTableCommand` raises
    // `WRONG_COMMAND_FOR_OBJECT_TYPE` while v2 `DropViewExec` raises
    // `EXPECT_VIEW_NOT_TABLE`. Accept either so this test can run on both paths -- aligning
    // the two error classes is out of scope here.
    val view = s"$catalog.$namespace.v_drop_table_collide"
    withSeededTable(view) {
      val ex = intercept[AnalysisException] {
        sql(s"DROP VIEW $view")
      }
      val cond = ex.getCondition
      assert(
        cond.startsWith("EXPECT_VIEW_NOT_TABLE") || cond == "WRONG_COMMAND_FOR_OBJECT_TYPE",
        s"unexpected error condition: $cond")
    }
  }
}
