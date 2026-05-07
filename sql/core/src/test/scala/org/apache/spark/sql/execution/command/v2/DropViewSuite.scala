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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier}
import org.apache.spark.sql.execution.command

class DropViewSuite extends command.DropViewSuiteBase with ViewCommandSuiteBase {

  test("V2: drop removes the entry from the catalog store") {
    val view = s"$catalog.$namespace.v2_drop_remove"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    assert(viewCatalog.viewExists(Identifier.of(Array(namespace), "v2_drop_remove")))
    sql(s"DROP VIEW $view")
    assert(!viewCatalog.viewExists(Identifier.of(Array(namespace), "v2_drop_remove")))
  }

  test("V2: DROP VIEW on a non-view table entry leaves the table untouched") {
    // The Base version of this scenario asserts the SQL behavior (rejection with
    // EXPECT_VIEW_NOT_TABLE); here we additionally pin the v2-only post-condition that
    // the underlying entry under the colliding identifier remains a table and was not
    // silently dropped by the rejected DROP VIEW.
    val name = "v2_drop_keeps_table"
    val view = s"$catalog.$namespace.$name"
    val ident = Identifier.of(Array(namespace), name)
    withSeededTable(view) {
      intercept[AnalysisException](sql(s"DROP VIEW $view"))
      assert(viewCatalog.tableExists(ident))
    }
  }

  test("V2: DROP VIEW on a non-ViewCatalog catalog fails") {
    withSQLConf(
      "spark.sql.catalog.no_view_drop_cat" -> classOf[BasicInMemoryTableCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("DROP VIEW no_view_drop_cat.default.v")
      }
      // Resolution fails because the catalog cannot host views; the exact error condition
      // depends on the resolver's not-found vs. capability-gate ordering.
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).matches(
        ".*(no such|not found|missing|not a view|cannot find|view).*"),
        s"unexpected error: ${ex.getMessage}")
    }
  }
}
