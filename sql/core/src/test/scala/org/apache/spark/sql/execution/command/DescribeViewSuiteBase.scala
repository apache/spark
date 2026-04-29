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

  test("describe extended includes Catalog and View Text rows") {
    // Both v1 (`DescribeTableCommand` over a `CatalogTable` of type VIEW) and v2
    // (`DescribeV2ViewExec`) emit a `Catalog` row carrying the resolved catalog name and a
    // `View Text` row containing the view body, so users can read the actual definition out
    // of EXTENDED rather than going to SHOW CREATE TABLE for it.
    val view = s"$catalog.$namespace.v_describe_ext_body"
    sql(s"CREATE VIEW $view AS SELECT 7 AS x")
    val rows = sql(s"DESCRIBE TABLE EXTENDED $view").collect()
    val pairs = rows.map(r => r.getString(0) -> Option(r.getString(1)).getOrElse("")).toMap
    assert(pairs.get("Catalog").contains(catalog),
      s"expected Catalog=$catalog in:\n$pairs")
    assert(pairs.get("View Text").exists(_.contains("SELECT 7 AS x")),
      s"expected View Text containing 'SELECT 7 AS x' in:\n$pairs")
  }

  test("describe extended promotes Comment and Collation to top-level rows") {
    // v1 `CatalogTable.toJsonLinkedHashMap` and v2 `DescribeV2ViewExec` both render Comment /
    // Collation as their own rows in the EXTENDED block, separately from the generic
    // Properties row, so users don't have to scrape the Properties string for first-class
    // fields.
    val view = s"$catalog.$namespace.v_describe_first_class"
    sql(s"CREATE VIEW $view COMMENT 'hello' DEFAULT COLLATION UTF8_LCASE AS SELECT 'a' AS x")
    val rows = sql(s"DESCRIBE TABLE EXTENDED $view").collect().map { r =>
      r.getString(0) -> Option(r.getString(1)).getOrElse("")
    }.toMap
    assert(rows.get("Comment").contains("hello"),
      s"expected Comment=hello in:\n$rows")
    // v1 renders the collation name verbatim (UTF8_LCASE); v2 does the same.
    assert(rows.get("Collation").exists(_.equalsIgnoreCase("UTF8_LCASE")),
      s"expected Collation=UTF8_LCASE in:\n$rows")
  }
}
