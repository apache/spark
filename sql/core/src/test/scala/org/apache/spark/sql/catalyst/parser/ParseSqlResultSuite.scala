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
import org.apache.spark.sql.internal.SQLConf

/**
 * Pin Table 39 codes and contracts that goldens do not cover.
 * Behavioral coverage lives in sql-tests/inputs/parse-sql.sql.
 */
class ParseSqlResultSuite extends SparkFunSuite {

  private def obj(sql: String): JObject =
    parse(ParseSqlResult.fromSql(sql)).asInstanceOf[JObject]

  private def tableRefs(sql: String): Set[Seq[String]] =
    obj(sql) \ "table_references" match {
      case JNothing => Set.empty
      case JArray(arr) => arr.map {
        case JArray(parts) => parts.map(_.asInstanceOf[JString].s)
        case other => fail(s"unexpected table_references entry: $other")
      }.toSet
      case other => fail(s"unexpected table_references: $other")
    }

  test("Table 39 standard and Spark code pairs are pinned") {
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
    assert(SqlStatementCodes.CreateMetricViewStmt.statementCode === -37)
  }

  test("TABLE and VALUES classify as SELECT") {
    val table = obj("TABLE t")
    assert(table \ "statement_identifier" === JString("SELECT"))
    assert(table \ "statement_code" === JInt(21))
    assert(tableRefs("TABLE t") === Set(Seq("t")))

    // Eager inlining must not flip VALUES between SELECT and Unrecognized.
    Seq(true, false).foreach { eager =>
      SQLConf.withExistingConf(new SQLConf) {
        SQLConf.get.setConf(SQLConf.EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED, eager)
        val values = obj("VALUES (1), (2)")
        assert(values \ "statement_identifier" === JString("SELECT"),
          s"eager=$eager")
        assert(values \ "statement_code" === JInt(21), s"eager=$eager")
      }
    }
  }

  test("CREATE FUNCTION and DECLARE VARIABLE are not table_references") {
    assert(tableRefs("CREATE FUNCTION f AS 'x' USING JAR 'y.jar'").isEmpty)
    assert(tableRefs("DECLARE VARIABLE x INT").isEmpty)
    // Contrast: CREATE VIEW still reports the view target.
    assert(tableRefs("CREATE VIEW v AS SELECT 1 AS a") === Set(Seq("v")))
  }

  test("positional markers inside BEGIN END are counted once") {
    val j = obj("BEGIN SELECT * FROM t WHERE a = ?; END")
    assert(j \ "parse_success" === JBool(true))
    assert(j \ "statement_identifier" === JString("BEGIN END"))
    assert(j \ "parameter_markers" \ "unnamed_count" === JInt(1))
    assert(j \ "parameter_markers" \ "named" === JNothing)
    assert(tableRefs("BEGIN SELECT * FROM t WHERE a = ?; END") === Set(Seq("t")))
  }

  test("syntax error returns STANDARD error JSON without throwing") {
    val j = obj("SELEC FROM t")
    assert(j \ "parse_success" === JBool(false))
    assert(j \ "error" \ "errorClass" === JString("PARSE_SYNTAX_ERROR"))
  }
}
