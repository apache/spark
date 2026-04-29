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
 * Unified tests for `SHOW COLUMNS` against a view, on V1 (session) and V2 view catalogs.
 */
trait ShowViewColumnsSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "SHOW COLUMNS on view"

  protected def namespace: String = "default"

  test("returns one row per column") {
    val view = s"$catalog.$namespace.v_show_cols_basic"
    sql(s"CREATE VIEW $view AS SELECT 1 AS a, 'x' AS b")
    val cols = sql(s"SHOW COLUMNS IN $view").collect().map(_.getString(0)).toSeq
    assert(cols == Seq("a", "b"))
  }

  test("respects user-specified column list on the view") {
    val view = s"$catalog.$namespace.v_show_cols_aliased"
    sql(s"CREATE VIEW $view (alpha, beta) AS SELECT 1, 2")
    val cols = sql(s"SHOW COLUMNS IN $view").collect().map(_.getString(0)).toSeq
    assert(cols == Seq("alpha", "beta"))
  }
}
