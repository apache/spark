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

class ParseCommandResultSuite extends SparkFunSuite {

  private def json(sql: String): JValue = parse(ParseCommandResult.fromSql(sql))

  private def obj(sql: String): JObject = json(sql).asInstanceOf[JObject]

  private def field(sql: String, name: String): JValue =
    obj(sql).obj.find(_._1 == name).map(_._2).getOrElse(JNothing)

  test("SELECT classification uses Table 39 SELECT / code 21") {
    val j = obj("SELECT a FROM t")
    assert(j \ "parse_success" === JBool(true))
    assert(j \ "statement_identifier" === JString("SELECT"))
    assert(j \ "statement_code" === JInt(21))
    assert(j \ "statement_type" ===
      JString("direct select statement: multiple rows"))
    assert(j \ "statement_class" === JString("SQL-data statement"))
  }

  test("CREATE TABLE and CTAS share CREATE TABLE code 77") {
    val create = obj("CREATE TABLE t (a INT)")
    assert(create \ "statement_identifier" === JString("CREATE TABLE"))
    assert(create \ "statement_code" === JInt(77))
    assert(create \ "as_subquery" === JNothing)

    val ctas = obj("CREATE TABLE t AS SELECT 1 AS a")
    assert(ctas \ "statement_identifier" === JString("CREATE TABLE"))
    assert(ctas \ "statement_code" === JInt(77))
    assert(ctas \ "as_subquery" === JBool(true))
  }

  test("DML statement identifiers and codes") {
    assert(field("INSERT INTO t SELECT 1", "statement_identifier") ===
      JString("INSERT"))
    assert(field("INSERT INTO t SELECT 1", "statement_code") === JInt(50))

    assert(field("DELETE FROM t WHERE a = 1", "statement_identifier") ===
      JString("DELETE WHERE"))
    assert(field("DELETE FROM t WHERE a = 1", "statement_code") === JInt(19))

    assert(field("UPDATE t SET a = 1 WHERE b = 2", "statement_identifier") ===
      JString("UPDATE WHERE"))
    assert(field("UPDATE t SET a = 1 WHERE b = 2", "statement_code") === JInt(82))

    assert(field(
      "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
      "statement_identifier") === JString("MERGE"))
    assert(field(
      "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
      "statement_code") === JInt(128))
  }

  test("table and function references are multipart sequences") {
    val j = obj("SELECT db.my_func(a), count(b) FROM cat.ns.t1 JOIN t2")
    assert(j \ "table_references" === JArray(List(
      JArray(List(JString("cat"), JString("ns"), JString("t1"))),
      JArray(List(JString("t2")))
    )))
    val funcs = (j \ "function_references").asInstanceOf[JArray].arr
    assert(funcs.contains(
      JArray(List(JString("db"), JString("my_func")))))
    assert(funcs.contains(JArray(List(JString("count")))))
  }

  test("select_list exposes name parts and expression text") {
    val j = obj("SELECT t.a AS x, b + 1 FROM t")
    val list = (j \ "select_list").asInstanceOf[JArray].arr
    assert(list.length === 2)
    assert(list.head \ "name" === JArray(List(JString("x"))))
    assert((list.head \ "expression").asInstanceOf[JString].s.contains("a"))
  }

  test("named and unnamed parameter markers") {
    val named = obj("SELECT * FROM t WHERE a = :foo AND b = :bar")
    assert(named \ "parameter_markers" \ "named" ===
      JArray(List(JString("foo"), JString("bar"))))
    assert(named \ "parameter_markers" \ "unnamed_count" === JInt(0))

    val unnamed = obj("SELECT * FROM t WHERE a = ? AND b = ?")
    assert(unnamed \ "parameter_markers" \ "named" === JArray(Nil))
    assert(unnamed \ "parameter_markers" \ "unnamed_count" === JInt(2))
  }

  test("syntax error returns STANDARD error JSON without throwing") {
    val j = obj("SELEC FROM t")
    assert(j \ "parse_success" === JBool(false))
    assert(j \ "error" \ "errorClass" === JString("PARSE_SYNTAX_ERROR"))
    assert((j \ "error" \ "messageTemplate") != JNothing)
    assert(j \ "error" \ "sqlState" === JString("42601"))
  }

  test("Spark-only statements use negative implementation-defined codes") {
    val j = obj("CACHE TABLE t")
    assert(j \ "parse_success" === JBool(true))
    assert(j \ "statement_identifier" === JString("CACHE TABLE"))
    assert(j \ "statement_code" === JInt(-1))
    assert(j \ "statement_class" ===
      JString("implementation-defined statement"))
  }

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
  }

  private def tableRefs(sql: String): Set[Seq[String]] =
    (obj(sql) \ "table_references").asInstanceOf[JArray].arr.map {
      case JArray(parts) => parts.map(_.asInstanceOf[JString].s)
      case other => fail(s"unexpected table_references entry: $other")
    }.toSet

  private def funcRefs(sql: String): Set[Seq[String]] =
    (obj(sql) \ "function_references").asInstanceOf[JArray].arr.map {
      case JArray(parts) => parts.map(_.asInstanceOf[JString].s)
      case other => fail(s"unexpected function_references entry: $other")
    }.toSet

  test("CTE body tables are collected (UnresolvedWith innerChildren)") {
    // CTE definitions are innerChildren, not children - easy to miss in walks.
    val sql =
      """WITH cte AS (SELECT a FROM hidden_base)
        |SELECT a FROM cte""".stripMargin
    assert(tableRefs(sql) === Set(Seq("hidden_base"), Seq("cte")))
  }

  test("multi-CTE chain and nested CTE definitions") {
    val sql =
      """WITH
        |  a AS (SELECT id FROM base_a),
        |  b AS (
        |    WITH nested AS (SELECT id FROM base_nested)
        |    SELECT n.id FROM nested n JOIN base_b b ON n.id = b.id
        |  )
        |SELECT a.id, b.id FROM a JOIN b ON a.id = b.id""".stripMargin
    assert(tableRefs(sql) === Set(
      Seq("base_a"),
      Seq("base_nested"),
      Seq("nested"),
      Seq("base_b"),
      Seq("a"),
      Seq("b")))
  }

  test("CTE with expression subqueries, functions, and parameters") {
    val sql =
      """WITH filtered AS (
        |  SELECT upper(x) AS u, my_schema.my_udf(y) AS v
        |  FROM src
        |  WHERE z IN (SELECT z FROM lookup WHERE flag = :flag)
        |    AND EXISTS (SELECT 1 FROM probe WHERE probe.id = src.id)
        |)
        |SELECT u, count(v) FROM filtered WHERE u = ? GROUP BY u""".stripMargin
    assert(tableRefs(sql) === Set(
      Seq("src"), Seq("lookup"), Seq("probe"), Seq("filtered")))
    assert(funcRefs(sql).contains(Seq("upper")))
    assert(funcRefs(sql).contains(Seq("my_schema", "my_udf")))
    assert(funcRefs(sql).contains(Seq("count")))
    val params = obj(sql) \ "parameter_markers"
    assert(params \ "named" === JArray(List(JString("flag"))))
    assert(params \ "unnamed_count" === JInt(1))
  }

  test("WITH on INSERT / CTAS reaches CTE and target tables") {
    val insertSql =
      """INSERT INTO dest
        |WITH s AS (SELECT a FROM src WHERE a > 0)
        |SELECT a FROM s""".stripMargin
    assert(tableRefs(insertSql) === Set(Seq("dest"), Seq("src"), Seq("s")))

    val ctasSql =
      """CREATE TABLE dest AS
        |WITH s AS (SELECT a FROM src)
        |SELECT a FROM s""".stripMargin
    val ctas = obj(ctasSql)
    assert(ctas \ "as_subquery" === JBool(true))
    assert(tableRefs(ctasSql).contains(Seq("src")))
    assert(tableRefs(ctasSql).contains(Seq("s")))
  }

  test("nested FROM / scalar / EXISTS subqueries outside CTEs") {
    val sql =
      """SELECT
        |  (SELECT max(v) FROM scalar_src) AS m,
        |  t.a
        |FROM outer_t t
        |JOIN (SELECT id FROM join_src) j ON t.id = j.id
        |WHERE EXISTS (SELECT 1 FROM exists_src e WHERE e.id = t.id)
        |  AND t.a IN (SELECT a FROM in_src)""".stripMargin
    assert(tableRefs(sql) === Set(
      Seq("scalar_src"),
      Seq("outer_t"),
      Seq("join_src"),
      Seq("exists_src"),
      Seq("in_src")))
    assert(funcRefs(sql).contains(Seq("max")))
  }

  test("MERGE collects target, source, and action-expression tables") {
    val sql =
      """MERGE INTO tgt t
        |USING (SELECT id FROM src) s
        |ON t.id = s.id
        |WHEN MATCHED AND t.flag IN (SELECT flag FROM flags) THEN
        |  UPDATE SET t.v = (SELECT v FROM vals WHERE vals.id = t.id)
        |WHEN NOT MATCHED THEN
        |  INSERT (id) VALUES (s.id)""".stripMargin
    val refs = tableRefs(sql)
    assert(refs.contains(Seq("tgt")))
    assert(refs.contains(Seq("src")))
    assert(refs.contains(Seq("flags")))
    assert(refs.contains(Seq("vals")))
  }
}
