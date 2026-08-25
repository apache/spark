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

  private def tableRefs(sql: String, field: String): Set[Seq[String]] =
    obj(sql) \ field match {
      case JNothing => Set.empty
      case JArray(arr) => arr.map {
        case JArray(parts) => parts.map(_.asInstanceOf[JString].s)
        case other => fail(s"unexpected $field entry: $other")
      }.toSet
      case other => fail(s"unexpected $field: $other")
    }

  private def sourceTableRefs(sql: String): Set[Seq[String]] =
    tableRefs(sql, "source_table_references")

  private def targetTableRefs(sql: String): Set[Seq[String]] =
    tableRefs(sql, "target_table_references")

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
    assert(sourceTableRefs("TABLE t") === Set(Seq("t")))

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

  test("CREATE FUNCTION and DECLARE VARIABLE are not table references") {
    assert(sourceTableRefs("CREATE FUNCTION f AS 'x' USING JAR 'y.jar'").isEmpty)
    assert(targetTableRefs("CREATE FUNCTION f AS 'x' USING JAR 'y.jar'").isEmpty)
    assert(sourceTableRefs("DECLARE VARIABLE x INT").isEmpty)
    assert(targetTableRefs("DECLARE VARIABLE x INT").isEmpty)
    // Contrast: CREATE VIEW still reports the view target and query source.
    assert(targetTableRefs("CREATE VIEW v AS SELECT 1 AS a") === Set(Seq("v")))
    assert(sourceTableRefs("CREATE VIEW v AS SELECT 1 AS a").isEmpty)
  }

  test("DML and DDL split target and source table references") {
    assert(targetTableRefs("INSERT INTO t SELECT 1") === Set(Seq("t")))
    assert(sourceTableRefs("INSERT INTO t SELECT 1").isEmpty)

    assert(targetTableRefs("DELETE FROM t WHERE a = 1") === Set(Seq("t")))
    assert(sourceTableRefs("DELETE FROM t WHERE a = 1").isEmpty)

    assert(targetTableRefs("UPDATE t SET a = 1 WHERE b = 2") === Set(Seq("t")))
    assert(sourceTableRefs("UPDATE t SET a = 1 WHERE b = 2").isEmpty)

    assert(targetTableRefs(
      "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE") === Set(Seq("t")))
    assert(sourceTableRefs(
      "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE") === Set(Seq("s")))

    assert(targetTableRefs("CREATE TABLE x AS SELECT a FROM src") === Set(Seq("x")))
    assert(sourceTableRefs("CREATE TABLE x AS SELECT a FROM src") === Set(Seq("src")))

    assert(targetTableRefs("DROP TABLE t") === Set(Seq("t")))
    assert(sourceTableRefs("DROP TABLE t").isEmpty)
  }

  test("CTE aliases only shadow references within their own scope") {
    // The inner CTE named real_t must not hide the outer real table real_t.
    assert(sourceTableRefs(
      "SELECT * FROM real_t WHERE EXISTS (" +
        "WITH real_t AS (SELECT * FROM inner_base) SELECT * FROM real_t)") ===
      Set(Seq("real_t"), Seq("inner_base")))
    // A definition sees only preceding aliases, so b here is the real table.
    assert(sourceTableRefs(
      "WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS x) SELECT * FROM a") ===
      Set(Seq("b")))
  }

  test("positional markers inside BEGIN END are counted once") {
    val j = obj("BEGIN SELECT * FROM t WHERE a = ?; END")
    assert(j \ "parse_success" === JBool(true))
    assert(j \ "statement_identifier" === JString("BEGIN END"))
    assert(j \ "parameter_markers" \ "unnamed_count" === JInt(1))
    assert(j \ "parameter_markers" \ "named" === JNothing)
    assert(sourceTableRefs("BEGIN SELECT * FROM t WHERE a = ?; END") === Set(Seq("t")))
  }

  test("syntax error returns STANDARD error JSON without throwing") {
    val j = obj("SELEC FROM t")
    assert(j \ "parse_success" === JBool(false))
    assert(j \ "error" \ "errorClass" === JString("PARSE_SYNTAX_ERROR"))
  }
}
