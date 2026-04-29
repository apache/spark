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
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier, TableCatalog, TableInfo, TableSummary}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.types.StructType

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

  test("V2: CREATE VIEW over a non-view table entry surfaces v1-parity errors") {
    val ident = Identifier.of(Array(namespace), "v2_create_table_collide")
    val tableInfo = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
      .build()
    viewCatalog.createTable(ident, tableInfo)
    try {
      // CREATE OR REPLACE VIEW must not silently destroy a non-view table -- v1 parity.
      val replaceEx = intercept[AnalysisException] {
        sql(s"CREATE OR REPLACE VIEW $catalog.$namespace.v2_create_table_collide AS " +
          "SELECT 1 AS col")
      }
      assert(replaceEx.getCondition == "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE")

      // Plain CREATE VIEW over a table surfaces TABLE_OR_VIEW_ALREADY_EXISTS, matching v1.
      val createEx = intercept[AnalysisException] {
        sql(s"CREATE VIEW $catalog.$namespace.v2_create_table_collide AS SELECT 1 AS col")
      }
      assert(createEx.getCondition == "TABLE_OR_VIEW_ALREADY_EXISTS")

      // CREATE VIEW IF NOT EXISTS is a no-op -- the table entry is untouched.
      sql(s"CREATE VIEW IF NOT EXISTS $catalog.$namespace.v2_create_table_collide AS " +
        "SELECT 1 AS col")
      val stored = viewCatalog.getStoredInfo(Array(namespace), "v2_create_table_collide")
      assert(!stored.isInstanceOf[org.apache.spark.sql.connector.catalog.ViewInfo])
    } finally {
      viewCatalog.dropTable(ident)
    }
  }
}
