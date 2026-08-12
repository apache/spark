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
    assert(j \ "statement_type" === JNothing)
    assert(j \ "statement_class" === JNothing)
  }

  test("CREATE TABLE and CTAS share CREATE TABLE code 77") {
    val create = obj("CREATE TABLE t (a INT)")
    assert(create \ "statement_identifier" === JString("CREATE TABLE"))
    assert(create \ "statement_code" === JInt(77))
    assert(create \ "as_subquery" === JNothing)

    val ctas = obj("CREATE TABLE t AS SELECT 1 AS a")
    assert(ctas \ "statement_identifier" === JString("CREATE TABLE"))
    assert(ctas \ "statement_code" === JInt(77))
    assert(ctas \ "as_subquery" === JNothing)
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

  test("parse errors expose line and position") {
    val sql =
      """SELECT *
        |FROM t
        |ORDER BY a
        |CLUSTER BY b""".stripMargin
    val error = obj(sql) \ "error"
    assert(error \ "errorClass" ===
      JString("UNSUPPORTED_FEATURE.COMBINATION_QUERY_RESULT_CLAUSES"))
    assert(error \ "line" === JInt(3))
    assert(error \ "position" === JInt(0))
    val context = (error \ "queryContext").asInstanceOf[JArray].arr.head
    assert(context \ "startIndex" === JInt(17))
  }

  test("multiline script errors expose the script line") {
    val sql =
      """BEGIN
        |  SELECT 1;
        |  SELEC 2;
        |END""".stripMargin
    val error = obj(sql) \ "error"
    assert(error \ "errorClass" === JString("PARSE_SYNTAX_ERROR"))
    assert(error \ "line" === JInt(3))
    assert(error \ "position" === JInt(8))
  }

  test("SQL scripting validation errors expose their origin") {
    val sql =
      """BEGIN
        |  lbl_begin: BEGIN
        |    SELECT 1;
        |  END lbl_end;
        |END""".stripMargin
    val error = obj(sql) \ "error"
    assert(error \ "errorClass" === JString("LABELS_MISMATCH"))
    assert(error \ "line" === JInt(2))
    assert(error \ "position" === JInt(2))
  }

  test("parse-only validation returns error classes beyond syntax errors") {
    val cases = Seq(
      "" -> "PARSE_EMPTY_STATEMENT",
      "USE bad-name" -> "INVALID_IDENTIFIER",
      "WITH c AS (SELECT 1), c AS (SELECT 2) SELECT * FROM c" ->
        "DUPLICATED_CTE_NAMES",
      "MERGE INTO target USING source ON target.id = source.id" ->
        "MERGE_WITHOUT_WHEN",
      "DROP FUNCTION catalog.schema.func" ->
        "INVALID_SQL_SYNTAX.UNSUPPORTED_SQL_STATEMENT",
      "SELECT 1 AS IDENTIFIER('alias.field')" ->
        "IDENTIFIER_TOO_MANY_NAME_PARTS",
      "SELECT DATE 'not-a-date'" -> "INVALID_TYPED_LITERAL")
    cases.foreach { case (sql, errorClass) =>
      assert(obj(sql) \ "error" \ "errorClass" === JString(errorClass), sql)
    }
  }

  test("Spark-only statements use negative implementation-defined codes") {
    val j = obj("CACHE TABLE t")
    assert(j \ "parse_success" === JBool(true))
    assert(j \ "statement_identifier" === JString("CACHE TABLE"))
    assert(j \ "statement_code" === JInt(-1))
    assert(j \ "statement_class" === JNothing)
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
    assert(SqlStatementCodes.BeginEnd.statementCode === -22)
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
    assert(ctas \ "statement_identifier" === JString("CREATE TABLE"))
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

  test("functions are collected from expression positions and table-valued functions") {
    val sql =
      """SELECT coalesce(t.a, 0), sum(abs(t.b)) OVER (
        |  PARTITION BY lower(t.c) ORDER BY length(t.d))
        |FROM left_t t
        |JOIN right_t r ON hash(t.id) = hash(r.id)
        |JOIN LATERAL range(cast(t.n AS BIGINT)) rng
        |WHERE startswith(t.c, 'x')
        |  AND EXISTS (SELECT max(s.v) FROM scalar_t s WHERE s.id = t.id)
        |GROUP BY coalesce(t.a, 0), t.b, t.c, t.d
        |HAVING count_if(t.b > 0) > 0
        |ORDER BY greatest(t.a, 1)""".stripMargin
    assert(tableRefs(sql) === Set(Seq("left_t"), Seq("right_t"), Seq("scalar_t")))
    assert(funcRefs(sql) === Set(
      Seq("coalesce"),
      Seq("sum"),
      Seq("abs"),
      Seq("lower"),
      Seq("length"),
      Seq("hash"),
      Seq("range"),
      Seq("startswith"),
      Seq("max"),
      Seq("count_if"),
      Seq("greatest")))
  }

  test("functions in wrapped DDL expressions are collected") {
    val sql =
      """CREATE TABLE target (
        |  created DATE DEFAULT current_date(),
        |  normalized STRING DEFAULT upper('x')
        |)""".stripMargin
    assert(funcRefs(sql) === Set(Seq("current_date"), Seq("upper")))
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

  test("BEGIN END script classification uses Spark code -22") {
    val j = obj("BEGIN SELECT 1; END")
    assert(j \ "parse_success" === JBool(true))
    assert(j \ "statement_identifier" === JString("BEGIN END"))
    assert(j \ "statement_code" === JInt(-22))
    // Compound scripts have no single primary select list.
    assert(j \ "select_list" === JArray(Nil))
  }

  test("BEGIN END walks SingleStatement.parsedPlan for tables and functions") {
    // SingleStatement.children skips the statement root (e.g. Project), so a
    // naive collectWithSubqueries misses project-list functions.
    val sql =
      """BEGIN
        |  SELECT count(a), upper(b) FROM script_t WHERE c = :p;
        |END""".stripMargin
    assert(tableRefs(sql) === Set(Seq("script_t")))
    assert(funcRefs(sql).contains(Seq("count")))
    assert(funcRefs(sql).contains(Seq("upper")))
    assert(obj(sql) \ "parameter_markers" \ "named" ===
      JArray(List(JString("p"))))
  }

  test("BEGIN END with IF / WHILE / FOR collects nested statement refs") {
    val sql =
      """BEGIN
        |  IF (SELECT flag FROM gate) THEN
        |    INSERT INTO dest SELECT * FROM src_if;
        |  ELSE
        |    DELETE FROM src_else WHERE id IN (SELECT id FROM doomed);
        |  END IF;
        |  WHILE (SELECT cont FROM ctrl) DO
        |    UPDATE tgt SET v = 1 WHERE id IN (SELECT id FROM while_src);
        |  END WHILE;
        |  FOR x AS SELECT id FROM for_src DO
        |    SELECT my_udf(id) FROM for_body WHERE id = x.id;
        |  END FOR;
        |END""".stripMargin
    val refs = tableRefs(sql)
    assert(refs === Set(
      Seq("gate"),
      Seq("dest"),
      Seq("src_if"),
      Seq("src_else"),
      Seq("doomed"),
      Seq("ctrl"),
      Seq("tgt"),
      Seq("while_src"),
      Seq("for_src"),
      Seq("for_body")))
    assert(funcRefs(sql).contains(Seq("my_udf")))
  }

  test("BEGIN END exception handler body tables are collected") {
    val sql =
      """BEGIN
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |  BEGIN
        |    INSERT INTO err_log SELECT * FROM failing_row;
        |  END;
        |  SELECT a FROM main_t;
        |END""".stripMargin
    assert(tableRefs(sql) === Set(
      Seq("err_log"), Seq("failing_row"), Seq("main_t")))
  }

  test("BEGIN END with CTE inside script body") {
    val sql =
      """BEGIN
        |  WITH c AS (SELECT a FROM cte_base)
        |  SELECT a FROM c;
        |END""".stripMargin
    assert(tableRefs(sql) === Set(Seq("cte_base"), Seq("c")))
  }

  test("multiline script collects functions and tables from all control-flow branches") {
    val sql =
      """BEGIN
        |  CASE upper(:kind)
        |    WHEN lower('a') THEN
        |      SELECT max(a) FROM case_a;
        |    ELSE
        |      SELECT min(b) FROM case_else;
        |  END CASE;
        |  REPEAT
        |    INSERT INTO repeat_target
        |    SELECT transform(items, x -> abs(x)) FROM repeat_source;
        |  UNTIL EXISTS (SELECT 1 FROM repeat_done WHERE ready())
        |  END REPEAT;
        |END""".stripMargin
    assert(tableRefs(sql) === Set(
      Seq("case_a"),
      Seq("case_else"),
      Seq("repeat_target"),
      Seq("repeat_source"),
      Seq("repeat_done")))
    assert(funcRefs(sql) === Set(
      Seq("upper"),
      Seq("lower"),
      Seq("max"),
      Seq("min"),
      Seq("transform"),
      Seq("abs"),
      Seq("ready")))
  }
}
