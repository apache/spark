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
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, TableCatalog}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

class AlterViewAsSuite extends command.AlterViewAsSuiteBase with ViewCommandSuiteBase {

  test("V2: ALTER VIEW AS picks up the namespace's default collation when the existing view " +
      "has none") {
    // Create the namespace with no default collation; create a view in it (PROP_COLLATION
    // unset). Then set the namespace default and ALTER VIEW AS -- the new ViewInfo must end
    // up with PROP_COLLATION = UTF8_LCASE (so v1Table.toCatalogTable's `collation` field is
    // set, and view-read time picks up UTF8_LCASE via AnalysisContext.collation).
    withSQLConf(SQLConf.SCHEMA_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      val viewName = "v2_alter_collation_inherit"
      val view = s"$catalog.$namespace.$viewName"
      sql(s"CREATE VIEW $view AS SELECT 'a' AS c1")
      assert(Option(viewCatalog
        .getStoredView(Array(namespace), viewName)
        .properties()
        .get(TableCatalog.PROP_COLLATION))
        .isEmpty)

      sql(s"ALTER NAMESPACE $catalog.$namespace DEFAULT COLLATION UTF8_LCASE")
      sql(s"ALTER VIEW $view AS SELECT 'x' AS c1, 'y' AS c2")

      val stored = viewCatalog.getStoredView(Array(namespace), viewName)
      assert(stored.properties().get(TableCatalog.PROP_COLLATION) == "UTF8_LCASE")
      // Read-time the view body's literal types reflect the inherited collation.
      val df = spark.table(view)
      assert(df.schema("c1").dataType === StringType("UTF8_LCASE"))
      assert(df.schema("c2").dataType === StringType("UTF8_LCASE"))
    }
  }

  test("V2: ALTER VIEW preserves PROP_OWNER (v1-parity)") {
    val view = s"$catalog.$namespace.v2_alter_keep_owner"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    val ownerBefore = viewCatalog.getStoredView(Array(namespace), "v2_alter_keep_owner")
      .properties().get(TableCatalog.PROP_OWNER)
    sql(s"ALTER VIEW $view AS SELECT 2 AS x")
    val ownerAfter = viewCatalog.getStoredView(Array(namespace), "v2_alter_keep_owner")
      .properties().get(TableCatalog.PROP_OWNER)
    assert(ownerBefore == ownerAfter)
  }

  test("V2: ALTER VIEW re-captures the current session's SQL configs") {
    val view = s"$catalog.$namespace.v2_alter_reconfig"
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      sql(s"ALTER VIEW $view AS SELECT 2 AS x")
    }
    val stored = viewCatalog.getStoredView(Array(namespace), "v2_alter_reconfig")
    val captured = stored.sqlConfigs().get(SQLConf.ANSI_ENABLED.key)
    assert(captured == "true",
      s"expected ALTER VIEW to re-capture ansi=true; got $captured")
  }

  test("V2: ALTER VIEW on non-ViewCatalog catalog fails with MISSING_CATALOG_ABILITY") {
    withSQLConf(
      "spark.sql.catalog.no_view_alter_cat" -> classOf[BasicInMemoryTableCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("ALTER VIEW no_view_alter_cat.default.does_not_matter AS SELECT 1")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }
}
