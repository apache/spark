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

package org.apache.spark.sql.catalyst.expressions

import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ParseSqlAnalyzeSuite extends QueryTest with SharedSparkSession {

  private def evalJson(sql: String, mode: String = "ANALYZE"): JValue = {
    val result = spark.sql(s"SELECT parse_sql($sql, '$mode')").head().getString(0)
    parse(result)
  }

  private def evalStatement(sql: String, mode: String = "ANALYZE"): JObject = {
    evalJson("'" + sql.replace("'", "''") + "'", mode) match {
      case JArray(List(value: JObject)) => value
      case other => fail(s"expected one statement object, got: $other")
    }
  }

  test("ANALYZE types selected columns and PARSE does not") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val analyzed = evalStatement("SELECT 1 AS a", "ANALYZE")
      assert(analyzed \ "parse_success" === JBool(true))
      assert(analyzed \ "analyze_success" === JBool(true))
      assert(analyzed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a"))), "type" -> JString("INT")))))

      val parsed = evalStatement("SELECT 1 AS a", "PARSE")
      assert(parsed \ "analyze_success" === JNothing)
      assert(parsed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a")))))))
    }
  }

  test("ANALYZE fully qualifies tables and builtin routines") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_analyze_t") {
        sql("CREATE TABLE parse_sql_analyze_t (a INT, b STRING) USING parquet")
        val j = evalStatement(
          "SELECT count(a) AS c FROM parse_sql_analyze_t", "analyze")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" === JArray(List(JArray(List(
          JString("spark_catalog"), JString("default"), JString("parse_sql_analyze_t"))))))
        assert(j \ "function_references" === JArray(List(JArray(List(
          JString("system"), JString("builtin"), JString("count"))))))
        assert(j \ "select_list" === JArray(List(
          JObject("name" -> JArray(List(JString("c"))), "type" -> JString("BIGINT")))))
      }
    }
  }

  test("ANALYZE leaves temp view names unqualified") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempView("parse_sql_analyze_v") {
        sql("CREATE OR REPLACE TEMP VIEW parse_sql_analyze_v AS SELECT 1 AS a, 'x' AS b")
        val j = evalStatement("SELECT a, b FROM parse_sql_analyze_v")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" ===
          JArray(List(JArray(List(JString("parse_sql_analyze_v"))))))
        assert(j \ "select_list" === JArray(List(
          JObject("name" -> JArray(List(JString("a"))), "type" -> JString("INT")),
          JObject("name" -> JArray(List(JString("b"))), "type" -> JString("STRING")))))
      }
    }
  }

  test("ANALYZE keeps parse metadata when analysis fails") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val j = evalStatement("SELECT a FROM missing_parse_sql_analyze_t")
      assert(j \ "parse_success" === JBool(true))
      assert(j \ "analyze_success" === JBool(false))
      assert(j \ "statement_identifier" === JString("SELECT"))
      assert(j \ "source_table_references" ===
        JArray(List(JArray(List(JString("missing_parse_sql_analyze_t"))))))
      val errorClass = (j \ "error" \ "errorClass").asInstanceOf[JString].s
      assert(errorClass == "TABLE_OR_VIEW_NOT_FOUND" ||
        errorClass.contains("TABLE_OR_VIEW_NOT_FOUND"))
    }
  }
}
