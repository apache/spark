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
 * Unified tests for `SHOW CREATE TABLE` against a view, on V1 (session) and V2 view catalogs.
 */
trait ShowCreateViewSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "SHOW CREATE TABLE on view"

  protected def namespace: String = "default"

  test("emits CREATE VIEW prefix") {
    val view = s"$catalog.$namespace.v_show_create_basic"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    val ddl = sql(s"SHOW CREATE TABLE $view").collect().head.getString(0)
    assert(ddl.startsWith("CREATE VIEW "), s"unexpected DDL: $ddl")
    assert(ddl.contains("AS SELECT 1 AS x"), s"unexpected DDL: $ddl")
  }

  test("includes user-set TBLPROPERTIES") {
    val view = s"$catalog.$namespace.v_show_create_props"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('k' = 'v')")
    val ddl = sql(s"SHOW CREATE TABLE $view").collect().head.getString(0)
    assert(ddl.contains("'k' = 'v'"), s"property missing in DDL: $ddl")
  }

  test("renders the column list") {
    val view = s"$catalog.$namespace.v_show_create_cols"
    sql(s"CREATE VIEW $view (col_alpha, col_beta) AS SELECT 1, 2")
    val ddl = sql(s"SHOW CREATE TABLE $view").collect().head.getString(0)
    // Use distinctive column names so `contains` checks aren't satisfied by tokens that
    // appear elsewhere in the rendered DDL (e.g. inside `CREATE VIEW`, `AS SELECT`, etc.).
    assert(ddl.contains("col_alpha"), s"col_alpha missing in DDL: $ddl")
    assert(ddl.contains("col_beta"), s"col_beta missing in DDL: $ddl")
  }
}
