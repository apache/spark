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
 * Unified tests for `SHOW TBLPROPERTIES` against a view, on V1 (session) and V2 view catalogs.
 */
trait ShowViewPropertiesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "SHOW TBLPROPERTIES on view"

  protected def namespace: String = "default"

  test("returns user-set property by key") {
    val view = s"$catalog.$namespace.v_show_props_one"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('k' = 'v')")
    val rows = sql(s"SHOW TBLPROPERTIES $view ('k')").collect()
    // SHOW TBLPROPERTIES <view> ('key') returns either (value) or (key, value) -- check the
    // last column either way.
    assert(rows.head.getString(rows.head.length - 1) == "v")
  }

  test("returns all user-set properties") {
    val view = s"$catalog.$namespace.v_show_props_all"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('a' = '1', 'b' = '2')")
    val rows = sql(s"SHOW TBLPROPERTIES $view").collect()
    val pairs = rows.map(r => r.getString(0) -> r.getString(1)).toMap
    assert(pairs.get("a").contains("1"))
    assert(pairs.get("b").contains("2"))
  }

  test("missing key returns the default not-found row") {
    val view = s"$catalog.$namespace.v_show_props_missing_key"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    val rows = sql(s"SHOW TBLPROPERTIES $view ('not_there')").collect()
    val value = rows.head.getString(rows.head.length - 1)
    assert(value.contains("does not have property"),
      s"expected a not-found message, got: $value")
  }
}
