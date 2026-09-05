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

  private def objs(sql: String): List[JObject] =
    parse(ParseSqlResult.fromSql(sql)) match {
      case JArray(values) => values.map(_.asInstanceOf[JObject])
      case other => fail(s"expected JSON array, got: $other")
    }

  private def obj(sql: String): JObject = objs(sql) match {
    case value :: Nil => value
    case other => fail(s"expected one statement, got ${other.size}: $other")
  }

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

  test("batches return one statement object per statement with source spans") {
    Seq("select 1; select 2", "select 1; select 2;").foreach { sql =>
      val statements = objs(sql)
      assert(statements.size === 2)
      assert(statements.map(_ \ "start") === Seq(JInt(1), JInt(11)))
      assert(statements.map(_ \ "length") === Seq(JInt(8), JInt(8)))
      assert(statements.map(_ \ "parse_success") === Seq(JBool(true), JBool(true)))
      assert(statements.map(_ \ "statement_identifier") ===
        Seq(JString("SELECT"), JString("SELECT")))
    }

    val statements = objs("  SELECT 1 ;\n SELECT 2;  ")
    assert(statements.map(_ \ "start") === Seq(JInt(3), JInt(15)))
    assert(statements.map(_ \ "length") === Seq(JInt(8), JInt(8)))

    val sqlWithDroppedComment = "SELECT 1; /* SELECT 2 */; SELECT 2"
    val commentStatements = objs(sqlWithDroppedComment)
    assert(commentStatements.map(_ \ "start") ===
      Seq(JInt(1), JInt(sqlWithDroppedComment.lastIndexOf("SELECT 2") + 1)))

    val emoji = "\uD83D\uDE00"
    val unicodeSql = s"SELECT '$emoji$emoji'; SELECT 2;"
    val unicodeStatements = objs(unicodeSql)
    val spans = unicodeStatements.map { statement =>
      val JInt(start) = statement \ "start"
      val JInt(length) = statement \ "length"
      (start.toInt, length.toInt)
    }
    assert(spans === Seq((1, 13), (16, 8)))
    assert(spans.map { case (start, length) =>
      unicodeSql.substring(start - 1, start - 1 + length)
    } === Seq(s"SELECT '$emoji$emoji'", "SELECT 2"))
  }

  test("batch errors are isolated and preserve statement order") {
    val statements = objs("SELECT 1; SELEC 2; SELECT 3")
    assert(statements.map(_ \ "start") === Seq(JInt(1), JInt(11), JInt(20)))
    assert(statements.map(_ \ "length") === Seq(JInt(8), JInt(7), JInt(8)))
    assert(statements.map(_ \ "parse_success") ===
      Seq(JBool(true), JBool(false), JBool(true)))
    assert(statements(1) \ "error" \ "errorClass" === JString("PARSE_SYNTAX_ERROR"))
  }

  test("SQL scripts remain one statement in a batch") {
    val statements = objs("BEGIN SELECT 1; SELECT 2; END; SELECT 3")
    assert(statements.size === 2)
    assert(statements.map(_ \ "start") === Seq(JInt(1), JInt(32)))
    assert(statements.map(_ \ "length") === Seq(JInt(29), JInt(8)))
    assert(statements.map(_ \ "statement_identifier") ===
      Seq(JString("BEGIN END"), JString("SELECT")))
  }

  test("empty batches contain no statements") {
    Seq("", "  ", ";;", "-- comment").foreach { sql =>
      assert(objs(sql).isEmpty, sql)
    }
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

  test("dynamic INSERT targets retain query metadata") {
    val sql = "INSERT INTO IDENTIFIER(lower('T')) SELECT a AS result FROM src"
    val insert = obj(sql)
    assert(insert \ "statement_identifier" === JString("INSERT"))
    assert(insert \ "statement_code" === JInt(50))
    assert(sourceTableRefs(sql) === Set(Seq("src")))
    assert(insert \ "select_list" === JArray(List(
      JObject("name" -> JArray(List(JString("result")))))))
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

    val insert = "WITH t AS (SELECT * FROM src) INSERT INTO t SELECT * FROM t"
    assert(targetTableRefs(insert) === Set(Seq("t")))
    assert(sourceTableRefs(insert) === Set(Seq("src")))

    val otherDml = Seq(
      "WITH t AS (SELECT * FROM src) DELETE FROM t WHERE a = 1",
      "WITH t AS (SELECT * FROM src) UPDATE t SET a = 1",
      "WITH t AS (SELECT * FROM src) " +
        "MERGE INTO t USING t AS s ON t.a = s.a WHEN MATCHED THEN DELETE")
    otherDml.foreach { sql =>
      assert(targetTableRefs(sql).isEmpty, sql)
      assert(sourceTableRefs(sql) === Set(Seq("src")), sql)
    }
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
