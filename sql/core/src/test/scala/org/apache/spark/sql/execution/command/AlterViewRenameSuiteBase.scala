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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

/**
 * Unified tests for `ALTER VIEW ... RENAME TO` against V1 (session) and V2 view catalogs.
 */
trait AlterViewRenameSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "ALTER VIEW ... RENAME TO"

  protected def namespace: String = "default"

  protected def createView(view: String): Unit = {
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
  }

  protected def viewExists(qualified: String): Boolean = {
    val parts = qualified.split('.').toSeq
    val nsAndView = parts.tail
    val ns = nsAndView.init.mkString(".")
    val name = nsAndView.last
    sql(s"SHOW VIEWS IN $catalog.$ns").collect().exists(_.getString(1) == name)
  }

  test("rename moves the entry") {
    val src = s"$catalog.$namespace.v_rename_src"
    val dstName = "v_rename_dst"
    createView(src)
    // v1 `AlterTableRenameCommand` requires a 1- or 2-part target identifier; a single name
    // renames in the same namespace. v2 accepts either form. Use the unqualified form so the
    // base test runs against both paths.
    sql(s"ALTER VIEW $src RENAME TO $dstName")
    assert(!viewExists(src), s"$src should be gone after rename")
    assert(viewExists(s"$catalog.$namespace.$dstName"),
      s"$catalog.$namespace.$dstName should exist after rename")
  }

  test("rename preserves the view body") {
    val src = s"$catalog.$namespace.v_rename_body_src"
    val dstName = "v_rename_body_dst"
    sql(s"CREATE VIEW $src AS SELECT 7 AS answer")
    sql(s"ALTER VIEW $src RENAME TO $dstName")
    checkAnswer(sql(s"SELECT * FROM $catalog.$namespace.$dstName"), Row(7))
  }

  test("renaming to an existing name fails") {
    val src = s"$catalog.$namespace.v_rename_collide_src"
    val dst = s"$catalog.$namespace.v_rename_collide_dst"
    createView(src)
    createView(dst)
    intercept[AnalysisException] {
      sql(s"ALTER VIEW $src RENAME TO v_rename_collide_dst")
    }
  }

  test("ALTER TABLE syntax on a view is rejected (use ALTER VIEW)") {
    // `ALTER TABLE x RENAME TO y` and `ALTER VIEW x RENAME TO y` use the same parser entry
    // (`UnresolvedTableOrView` + `isView` flag); when the resolved child is a view but the
    // syntax says TABLE, error with EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW. v1 enforces this in
    // `DDLUtils.verifyAlterTableType`; v2 enforces it in DataSourceV2Strategy.
    val view = s"$catalog.$namespace.v_rename_wrong_syntax"
    createView(view)
    val ex = intercept[AnalysisException] {
      sql(s"ALTER TABLE $view RENAME TO v_rename_wrong_syntax_dst")
    }
    assert(ex.getCondition.startsWith("EXPECT_TABLE_NOT_VIEW"),
      s"unexpected error condition: ${ex.getCondition}")
  }

  test("rename re-caches a previously cached view") {
    // v1 `AlterTableRenameCommand` and v2 `RenameTableExec` both capture the cached storage
    // level before rename and re-instate it on the new identifier afterwards. The v2 view
    // path (`RenameV2ViewExec`) follows the same pattern -- without it, a user-cached view
    // would silently lose its cache entry after RENAME.
    val src = s"$catalog.$namespace.v_rename_cached_src"
    val dstName = "v_rename_cached_dst"
    val dst = s"$catalog.$namespace.$dstName"
    createView(src)
    spark.catalog.cacheTable(src)
    assert(spark.catalog.isCached(src), "bad test: view was not cached in the first place")
    try {
      sql(s"ALTER VIEW $src RENAME TO $dstName")
      // After rename, the destination's plan must still be cached. Resolving the old name
      // post-rename throws TABLE_OR_VIEW_NOT_FOUND, so we only check the destination side.
      assert(spark.catalog.isCached(dst),
        s"$dst should still be cached after RENAME")
    } finally {
      spark.catalog.uncacheTable(dst)
    }
  }
}
