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
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, TableCatalog, ViewInfo}
import org.apache.spark.sql.execution.command

class CreateViewSuite extends command.CreateViewSuiteBase with ViewCommandSuiteBase {
  import testImplicits._

  test("V2: CREATE VIEW propagates DEFAULT COLLATION onto the stored ViewInfo") {
    val view = s"$catalog.$namespace.v2_create_collation"
    withTable("spark_catalog.default.src_coll") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.src_coll")
      sql(s"CREATE VIEW $view DEFAULT COLLATION UTF8_BINARY AS " +
        s"SELECT col FROM spark_catalog.default.src_coll")
      val stored = viewCatalog.getStoredView(Array(namespace), "v2_create_collation")
      assert(stored.properties().get(TableCatalog.PROP_COLLATION) == "UTF8_BINARY")
    }
  }

  test("V2: CREATE VIEW stamps PROP_OWNER on the stored TableInfo") {
    val view = s"$catalog.$namespace.v2_create_owner"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    val stored = viewCatalog.getStoredView(Array(namespace), "v2_create_owner")
    assert(stored.properties().containsKey(TableCatalog.PROP_OWNER))
  }

  test("V2: CREATE VIEW on a non-ViewCatalog catalog fails with MISSING_CATALOG_ABILITY.VIEWS") {
    withSQLConf(
      "spark.sql.catalog.no_view_cat" -> classOf[BasicInMemoryTableCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW no_view_cat.default.v AS SELECT 1")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }

  test("V2: CREATE VIEW IF NOT EXISTS over a table leaves the underlying TableInfo untouched") {
    // The Base version of this scenario asserts the SQL behavior (errors / no-op);
    // here we additionally pin the v2-only post-condition that the persisted entry under
    // the colliding identifier remains a `TableInfo` and is NOT silently swapped for a
    // `ViewInfo` by the IF NOT EXISTS path.
    val name = "v2_ifne_keeps_table"
    val view = s"$catalog.$namespace.$name"
    withSeededTable(view) {
      sql(s"CREATE VIEW IF NOT EXISTS $view AS SELECT 1 AS col")
      val stored = viewCatalog.getStoredInfo(Array(namespace), name)
      assert(!stored.isInstanceOf[ViewInfo])
    }
  }
}
