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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command

/**
 * Temp views live in the session catalog only, so they are covered here rather than in the
 * unified `ShowViewsSuiteBase`.
 */
class ShowViewsSuite extends command.ShowViewsSuiteBase with ViewCommandSuiteBase {

  test("AS JSON reports a local temp view as temporary") {
    withTempView("v_json_local_temp") {
      sql("CREATE TEMPORARY VIEW v_json_local_temp AS SELECT 1 AS x")
      val views = showViewsAsJson(s"SHOW VIEWS IN $catalog.$namespace AS JSON")
      val temp = views.filter(v => (v \ "viewName").extract[String] == "v_json_local_temp")
      assert(temp.length == 1, s"expected exactly one entry: ${viewNames(views)}")
      assert((temp.head \ "isTemporary").extract[Boolean])
      // A local temp view belongs to no database, so its namespace is empty.
      assert((temp.head \ "namespace").extract[Seq[String]].isEmpty)
    }
  }

  test("AS JSON lists global temp views under the global temp database") {
    withGlobalTempView("v_json_global_temp") {
      sql("CREATE GLOBAL TEMPORARY VIEW v_json_global_temp AS SELECT 1 AS x")
      val globalTempDb = spark.sharedState.globalTempDB
      val views = showViewsAsJson(s"SHOW VIEWS IN $globalTempDb AS JSON")
      val temp = views.filter(v => (v \ "viewName").extract[String] == "v_json_global_temp")
      assert(temp.length == 1, s"expected exactly one entry: ${viewNames(views)}")
      assert((temp.head \ "isTemporary").extract[Boolean])
      assert((temp.head \ "namespace").extract[Seq[String]] == Seq(globalTempDb))
    }
  }

  test("AS JSON agrees with the tabular output for a global temp view") {
    withGlobalTempView("v_json_global_parity") {
      sql("CREATE GLOBAL TEMPORARY VIEW v_json_global_parity AS SELECT 1 AS x")
      val globalTempDb = spark.sharedState.globalTempDB
      val expected = sql(s"SHOW VIEWS IN $globalTempDb").collect()
        .map(r => (r.getString(0), r.getString(1), r.getBoolean(2))).toSet
      val actual = showViewsAsJson(s"SHOW VIEWS IN $globalTempDb AS JSON").map { v =>
        ((v \ "namespace").extract[Seq[String]].mkString("."),
          (v \ "viewName").extract[String],
          (v \ "isTemporary").extract[Boolean])
      }.toSet
      assert(actual == expected)
    }
  }

  test("AS JSON without IN or FROM uses the current namespace") {
    sql(s"USE $catalog.$namespace")
    withView("v_json_current_ns") {
      sql("CREATE VIEW v_json_current_ns AS SELECT 1 AS x")
      val names = viewNames(showViewsAsJson("SHOW VIEWS AS JSON"))
      assert(names.contains("v_json_current_ns"), s"v_json_current_ns missing: $names")
    }
  }

  test("AS JSON rejects a nested namespace, matching the tabular command") {
    // Unlike ShowTablesJsonCommand, which falls back to ns.headOption for the session
    // catalog, ShowViewsJsonCommand rejects a nested namespace outright so AS JSON stays in
    // lockstep with the plain-text command instead of silently dropping segments.
    Seq("", " AS JSON").foreach { jsonSuffix =>
      val ex = intercept[AnalysisException] {
        sql(s"SHOW VIEWS IN $catalog.a.b$jsonSuffix")
      }
      assert(ex.getCondition == "NESTED_DATABASE_UNSUPPORTED_BY_V1_SESSION_CATALOG",
        s"unexpected condition for '$jsonSuffix': ${ex.getCondition}")
    }
  }

  test("AS JSON in a not existing namespace fails the same way as the tabular command") {
    Seq("", " AS JSON").foreach { jsonSuffix =>
      val ex = intercept[AnalysisException] {
        sql(s"SHOW VIEWS IN $catalog.v_json_no_such_namespace$jsonSuffix")
      }
      assert(ex.getCondition == "SCHEMA_NOT_FOUND",
        s"unexpected condition for '$jsonSuffix': ${ex.getCondition}")
    }
  }
}
