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

package org.apache.spark.sql.catalyst.parser

import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SparkFunSuite

/**
 * Pin Table 39 codes and parser-surface contracts that goldens do not cover.
 * Behavioral coverage lives in sql-tests/inputs/parse-sql.sql.
 */
class ParseSqlResultSuite extends SparkFunSuite {

  private def obj(sql: String): JObject =
    parse(ParseSqlResult.fromSql(sql)).asInstanceOf[JObject]

  private def tableRefs(sql: String): Set[Seq[String]] =
    (obj(sql) \ "table_references").asInstanceOf[JArray].arr.map {
      case JArray(parts) => parts.map(_.asInstanceOf[JString].s)
      case other => fail(s"unexpected table_references entry: $other")
    }.toSet

  test("Table 39 standard code pairs are pinned") {
    assert(SqlStatementCodes.Select.statementCode === 21)
    assert(SqlStatementCodes.Insert.statementCode === 50)
    assert(SqlStatementCodes.DeleteWhere.statementCode === 19)
    assert(SqlStatementCodes.UpdateWhere.statementCode === 82)
    assert(SqlStatementCodes.Merge.statementCode === 128)
    assert(SqlStatementCodes.CreateTable.statementCode === 77)
    assert(SqlStatementCodes.CreateView.statementCode === 84)
    assert(SqlStatementCodes.DropTable.statementCode === 32)
    assert(SqlStatementCodes.AlterTable.statementCode === 4)
    assert(SqlStatementCodes.TruncateTable.statementCode === 139)
    assert(SqlStatementCodes.Unrecognized.statementCode === 0)
    assert(SqlStatementCodes.CacheTable.statementCode < 0)
    assert(SqlStatementCodes.BeginEnd.statementCode === -22)
    assert(SqlStatementCodes.Explain.statementCode === -23)
    assert(SqlStatementCodes.Set.statementCode === -24)
  }

  test("SELECT classification uses Table 39 SELECT / code 21") {
    val j = obj("SELECT a FROM t")
    assert(j \ "parse_success" === JBool(true))
    assert(j \ "statement_identifier" === JString("SELECT"))
    assert(j \ "statement_code" === JInt(21))
  }

  test("SparkSqlParser-only statements get Spark codes") {
    val explain = obj("EXPLAIN SELECT 1")
    assert(explain \ "statement_identifier" === JString("EXPLAIN"))
    assert(explain \ "statement_code" === JInt(-23))

    val set = obj("SET spark.sql.adaptive.enabled=true")
    assert(set \ "statement_identifier" === JString("SET"))
    assert(set \ "statement_code" === JInt(-24))

    val addJar = obj("ADD JAR /tmp/x.jar")
    assert(addJar \ "statement_identifier" === JString("ADD JAR"))
    assert(addJar \ "statement_code" === JInt(-26))
  }

  test("CTE names and correlation aliases are omitted from table_references") {
    val sql =
      """WITH cte AS (SELECT a FROM hidden_base)
        |SELECT a FROM cte""".stripMargin
    assert(tableRefs(sql) === Set(Seq("hidden_base")))
  }

  test("CREATE VIEW exposes select_list from the query body") {
    val j = obj("CREATE VIEW v AS SELECT a, b FROM t")
    assert(j \ "statement_identifier" === JString("CREATE VIEW"))
    assert(j \ "statement_code" === JInt(84))
    // View target and source table both count as lineage refs.
    assert(tableRefs("CREATE VIEW v AS SELECT a, b FROM t") ===
      Set(Seq("v"), Seq("t")))
    assert(j \ "select_list" === JArray(List(
      JObject("name" -> JArray(List(JString("a")))),
      JObject("name" -> JArray(List(JString("b"))))
    )))
    assert((j \ "select_list")(0) \ "expression" === JNothing)
  }

  test("syntax error returns STANDARD error JSON without throwing") {
    val j = obj("SELEC FROM t")
    assert(j \ "parse_success" === JBool(false))
    assert(j \ "error" \ "errorClass" === JString("PARSE_SYNTAX_ERROR"))
  }
}
