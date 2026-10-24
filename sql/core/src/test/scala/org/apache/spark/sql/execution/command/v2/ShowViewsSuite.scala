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
import org.apache.spark.sql.connector.catalog.BasicInMemoryTableCatalog
import org.apache.spark.sql.execution.command

class ShowViewsSuite extends command.ShowViewsSuiteBase with ViewCommandSuiteBase {

  test("V2: SHOW VIEWS on a non-ViewCatalog catalog fails") {
    withSQLConf(
      "spark.sql.catalog.no_view_show_cat" -> classOf[BasicInMemoryTableCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("SHOW VIEWS IN no_view_show_cat.default")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }

  test("V2: SHOW VIEWS AS JSON on a non-ViewCatalog catalog fails") {
    withSQLConf(
      "spark.sql.catalog.no_view_show_cat" -> classOf[BasicInMemoryTableCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("SHOW VIEWS IN no_view_show_cat.default AS JSON")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }

  test("V2: AS JSON lists a view under a nested namespace") {
    sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.ns1.ns2")
    withNamespace(s"$catalog.ns1.ns2") {
      withView(s"$catalog.ns1.ns2.v_json_nested") {
        sql(s"CREATE VIEW $catalog.ns1.ns2.v_json_nested AS SELECT 1 AS x")
        val views = showViewsAsJson(s"SHOW VIEWS IN $catalog.ns1.ns2 AS JSON")
        val nested = views.filter(v => (v \ "viewName").extract[String] == "v_json_nested")
        assert(nested.length == 1, s"expected exactly one entry: ${viewNames(views)}")
        assert((nested.head \ "namespace").extract[Seq[String]] == Seq("ns1", "ns2"))
      }
    }
  }
}
