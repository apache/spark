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
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier, TableInfo, TableSummary}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.types.StructType

class ShowViewsSuite extends command.ShowViewsSuiteBase with ViewCommandSuiteBase {

  test("V2: SHOW VIEWS does not include non-view table entries") {
    sql(s"CREATE VIEW $catalog.$namespace.v_v2_only_views AS SELECT 1 AS x")
    val tableIdent = Identifier.of(Array(namespace), "t_v2_not_in_show_views")
    viewCatalog.createTable(
      tableIdent,
      new TableInfo.Builder()
        .withSchema(new StructType().add("x", "int"))
        .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
        .build())
    try {
      val rows = sql(s"SHOW VIEWS IN $catalog.$namespace").collect()
      val names = rows.map(_.getString(1)).toSet
      assert(names.contains("v_v2_only_views"))
      assert(!names.contains("t_v2_not_in_show_views"),
        s"non-view leaked into SHOW VIEWS: $names")
      rows.foreach(r => assert(!r.getBoolean(2),
        s"isTemporary must be false for v2 SHOW VIEWS: $r"))
    } finally {
      viewCatalog.dropTable(tableIdent)
    }
  }

  test("V2: SHOW VIEWS on a non-ViewCatalog catalog fails") {
    withSQLConf(
      "spark.sql.catalog.no_view_show_cat" -> classOf[BasicInMemoryTableCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("SHOW VIEWS IN no_view_show_cat.default")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }
}
