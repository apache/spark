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
 * Unified tests for `DESCRIBE TABLE` against a view, on V1 (session) and V2 view catalogs.
 */
trait DescribeViewSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "DESCRIBE TABLE on view"

  protected def namespace: String = "default"

  test("describe emits one row per column") {
    val view = s"$catalog.$namespace.v_describe_basic"
    sql(s"CREATE VIEW $view AS SELECT 1 AS a, 'x' AS b")
    val rows = sql(s"DESCRIBE TABLE $view").collect()
    val cols = rows.map(r => r.getString(0) -> r.getString(1)).toMap
    assert(cols.get("a").contains("int"))
    assert(cols.get("b").contains("string"))
  }

  test("describe extended emits a detailed-info block for the view") {
    val view = s"$catalog.$namespace.v_describe_extended"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    val rows = sql(s"DESCRIBE TABLE EXTENDED $view").collect().map(_.getString(0))
    // v1 and v2 paths render slightly different headers ('# Detailed Table Information' vs
    // '# Detailed View Information'); accept either.
    assert(
      rows.contains("# Detailed Table Information") ||
        rows.contains("# Detailed View Information"),
      s"expected a detailed-info block in:\n${rows.mkString("\n")}")
  }
}
