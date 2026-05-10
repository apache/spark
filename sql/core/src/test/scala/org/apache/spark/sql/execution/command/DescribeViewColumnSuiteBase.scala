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
 * Unified tests for `DESCRIBE TABLE ... <column>` against a view, on V1 (session) and V2 view
 * catalogs.
 */
trait DescribeViewColumnSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "DESCRIBE TABLE COLUMN on view"

  protected def namespace: String = "default"

  test("emits col_name / data_type / comment rows") {
    val view = s"$catalog.$namespace.v_desc_col_basic"
    sql(s"CREATE VIEW $view AS SELECT 1 AS a")
    val rows = sql(s"DESCRIBE TABLE $view a").collect()
    val labels = rows.map(_.getString(0)).toSet
    assert(labels.contains("col_name"))
    assert(labels.contains("data_type"))
    assert(labels.contains("comment"))
  }

  test("data_type matches the column type") {
    val view = s"$catalog.$namespace.v_desc_col_type"
    sql(s"CREATE VIEW $view AS SELECT 1 AS a, 'x' AS b")
    val rows = sql(s"DESCRIBE TABLE $view b").collect()
    val pairs = rows.map(r => r.getString(0) -> r.getString(1)).toMap
    assert(pairs.get("col_name").contains("b"))
    assert(pairs.get("data_type").contains("string"))
  }
}
