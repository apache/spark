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

package org.apache.spark.sql

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Limit
import org.apache.spark.sql.functions.{array, call_function, lit, map, map_from_arrays, map_from_entries, str_to_map, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ParametersSuite extends QueryTest with SharedSparkSession with PlanTest {

  test("bind named parameters") {
    val sqlText =
      """
        |SELECT id, id % :div as c0
        |FROM VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9) AS t(id)
        |WHERE id < :constA
        |""".stripMargin
    val args = Map("div" -> 3, "constA" -> 4L)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(0, 0) :: Row(1, 1) :: Row(2, 2) :: Row(3, 0) :: Nil)

    checkAnswer(
      spark.sql("""SELECT contains('Spark \'SQL\'', :subStr)""", Map("subStr" -> "SQL")),
      Row(true))
  }

  test("bind positional parameters") {
    val sqlText =
      """
        |SELECT id, id % ? as c0
        |FROM VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9) AS t(id)
        |WHERE id < ?
        |""".stripMargin
    val args = Array(3, 4L)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(0, 0) :: Row(1, 1) :: Row(2, 2) :: Row(3, 0) :: Nil)

    checkAnswer(
      spark.sql("""SELECT contains('Spark \'SQL\'', ?)""", Array("SQL")),
      Row(true))
  }

  test("parameter binding is case sensitive") {
    checkAnswer(
      spark.sql("SELECT :p, :P", Map("p" -> 1, "P" -> 2)),
      Row(1, 2)
    )

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("select :P", Map("p" -> 1))
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "P"),
      context = ExpectedContext(
        fragment = ":P",
        start = 7,
        stop = 8))
  }

  test("named parameters in CTE") {
    val sqlText =
      """
        |WITH w1 AS (SELECT :p1 AS p)
        |SELECT p + :p2 FROM w1
        |""".stripMargin
    val args = Map("p1" -> 1, "p2" -> 2)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(3))
  }

  test("positional parameters in CTE") {
    val sqlText =
      """
        |WITH w1 AS (SELECT ? AS p)
        |SELECT p + ? FROM w1
        |""".stripMargin
    val args = Array(1, 2)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(3))
  }

  test("named parameters in nested CTE") {
    val sqlText =
      """
        |WITH w1 AS
        |  (WITH w2 AS (SELECT :p1 AS p) SELECT p + :p2 AS p2 FROM w2)
        |SELECT p2 + :p3 FROM w1
        |""".stripMargin
    val args = Map("p1" -> 1, "p2" -> 2, "p3" -> 3)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(6))
  }

  test("positional parameters in nested CTE") {
    val sqlText =
      """
        |WITH w1 AS
        |  (WITH w2 AS (SELECT ? AS p) SELECT p + ? AS p2 FROM w2)
        |SELECT p2 + ? FROM w1
        |""".stripMargin
    val args = Array(1, 2, 3)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(6))
  }

  test("named parameters in subquery expression") {
    val sqlText = "SELECT (SELECT max(id) + :p1 FROM range(10)) + :p2"
    val args = Map("p1" -> 1, "p2" -> 2)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(12))
  }

  test("positional parameters in subquery expression") {
    val sqlText = "SELECT (SELECT max(id) + ? FROM range(10)) + ?"
    val args = Array(1, 2)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(12))
  }

  test("named parameters in nested subquery expression") {
    val sqlText = "SELECT (SELECT (SELECT max(id) + :p1 FROM range(10)) + :p2) + :p3"
    val args = Map("p1" -> 1, "p2" -> 2, "p3" -> 3)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(15))
  }

  test("positional parameters in nested subquery expression") {
    val sqlText = "SELECT (SELECT (SELECT max(id) + ? FROM range(10)) + ?) + ?"
    val args = Array(1, 2, 3)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(15))
  }

  test("named parameters in subquery expression inside CTE") {
    val sqlText =
      """
        |WITH w1 AS (SELECT (SELECT max(id) + :p1 FROM range(10)) + :p2 AS p)
        |SELECT p + :p3 FROM w1
        |""".stripMargin
    val args = Map("p1" -> 1, "p2" -> 2, "p3" -> 3)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(15))
  }

  test("positional parameters in subquery expression inside CTE") {
    val sqlText =
      """
        |WITH w1 AS (SELECT (SELECT max(id) + ? FROM range(10)) + ? AS p)
        |SELECT p + ? FROM w1
        |""".stripMargin
    val args = Array(1, 2, 3)
    checkAnswer(
      spark.sql(sqlText, args),
      Row(15))
  }

  test("named parameter in identifier clause") {
    val sqlText =
      "SELECT IDENTIFIER('T.' || :p1 || '1') FROM VALUES(1) T(c1)"
    val args = Map("p1" -> "c")
    checkAnswer(
      spark.sql(sqlText, args),
      Row(1))
  }

  test("positional parameter in identifier clause") {
    val sqlText =
      "SELECT IDENTIFIER('T.' || ? || '1') FROM VALUES(1) T(c1)"
    val args = Array("c")
    checkAnswer(
      spark.sql(sqlText, args),
      Row(1))
  }

  test("named parameter in identifier clause in DDL and utility commands") {
    spark.sql("CREATE VIEW IDENTIFIER(:p1)(c1) AS SELECT 1", args = Map("p1" -> "v"))
    spark.sql("ALTER VIEW IDENTIFIER(:p1) AS SELECT 2 AS c1", args = Map("p1" -> "v"))
    checkAnswer(
      spark.sql("SHOW COLUMNS FROM IDENTIFIER(:p1)", args = Map("p1" -> "v")),
      Row("c1"))
    spark.sql("DROP VIEW IDENTIFIER(:p1)", args = Map("p1" -> "v"))
  }

  test("positional parameter in identifier clause in DDL and utility commands") {
    spark.sql("CREATE VIEW IDENTIFIER(?)(c1) AS SELECT 1", args = Array("v"))
    spark.sql("ALTER VIEW IDENTIFIER(?) AS SELECT 2 AS c1", args = Array("v"))
    checkAnswer(
      spark.sql("SHOW COLUMNS FROM IDENTIFIER(?)", args = Array("v")),
      Row("c1"))
    spark.sql("DROP VIEW IDENTIFIER(?)", args = Array("v"))
  }

  test("named parameters in INSERT") {
    withTable("t") {
      sql("CREATE TABLE t (col INT) USING json")
      spark.sql("INSERT INTO t SELECT :p", Map("p" -> 1))
      checkAnswer(spark.table("t"), Row(1))
    }
  }

  test("positional parameters in INSERT") {
    withTable("t") {
      sql("CREATE TABLE t (col INT) USING json")
      spark.sql("INSERT INTO t SELECT ?", Array(1))
      checkAnswer(spark.table("t"), Row(1))
    }
  }

  test("named parameters not allowed in view body ") {
    val sqlText = "CREATE VIEW v AS SELECT :p AS p"
    val args = Map("p" -> 1)
    checkError(
      exception = intercept[ParseException] {
        spark.sql(sqlText, args)
      },
      condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      parameters = Map("statement" -> "the query of CREATE VIEW"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = sqlText.length - 1))
  }

  test("positional parameters not allowed in view body ") {
    val sqlText = "CREATE VIEW v AS SELECT ? AS p"
    val args = Array(1)
    checkError(
      exception = intercept[ParseException] {
        spark.sql(sqlText, args)
      },
      condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      parameters = Map("statement" -> "the query of CREATE VIEW"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = sqlText.length - 1))
  }

  test("named parameters not allowed in view body - WITH and scalar subquery") {
    val sqlText = "CREATE VIEW v AS WITH cte(a) AS (SELECT (SELECT :p) AS a)  SELECT a FROM cte"
    val args = Map("p" -> 1)
    checkError(
      exception = intercept[ParseException] {
        spark.sql(sqlText, args)
      },
      condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      parameters = Map("statement" -> "the query of CREATE VIEW"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = sqlText.length - 1))
  }

  test("positional parameters not allowed in view body - WITH and scalar subquery") {
    val sqlText = "CREATE VIEW v AS WITH cte(a) AS (SELECT (SELECT ?) AS a)  SELECT a FROM cte"
    val args = Array(1)
    checkError(
      exception = intercept[ParseException] {
        spark.sql(sqlText, args)
      },
      condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      parameters = Map("statement" -> "the query of CREATE VIEW"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = sqlText.length - 1))
  }

  test("named parameters not allowed in view body - nested WITH and EXIST") {
    val sqlText =
      """CREATE VIEW v AS
        |SELECT a as a
        |FROM (WITH cte(a) AS (SELECT CASE WHEN EXISTS(SELECT :p) THEN 1 END AS a)
        |SELECT a FROM cte)""".stripMargin
    val args = Map("p" -> 1)
    checkError(
      exception = intercept[ParseException] {
        spark.sql(sqlText, args)
      },
      condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      parameters = Map("statement" -> "the query of CREATE VIEW"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = sqlText.length - 1))
  }

  test("positional parameters not allowed in view body - nested WITH and EXIST") {
    val sqlText =
      """CREATE VIEW v AS
        |SELECT a as a
        |FROM (WITH cte(a) AS (SELECT CASE WHEN EXISTS(SELECT ?) THEN 1 END AS a)
        |SELECT a FROM cte)""".stripMargin
    val args = Array(1)
    checkError(
      exception = intercept[ParseException] {
        spark.sql(sqlText, args)
      },
      condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      parameters = Map("statement" -> "the query of CREATE VIEW"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = sqlText.length - 1))
  }

  test("non-substituted named parameters") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("select :abc, :def", Map("abc" -> 1))
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "def"),
      context = ExpectedContext(
        fragment = ":def",
        start = 13,
        stop = 16))
    checkError(
      exception = intercept[AnalysisException] {
        sql("select :abc").collect()
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "abc"),
      context = ExpectedContext(
        fragment = ":abc",
        start = 7,
        stop = 10))
  }

  test("non-substituted positional parameters") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("select ?, ?", Array(1))
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "_10"),
      context = ExpectedContext(
        fragment = "?",
        start = 10,
        stop = 10))
    checkError(
      exception = intercept[AnalysisException] {
        sql("select ?").collect()
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "_7"),
      context = ExpectedContext(
        fragment = "?",
        start = 7,
        stop = 7))
  }

  test("literal argument of named parameter in `sql()`") {
    val sqlText =
      """SELECT s FROM VALUES ('Jeff /*__*/ Green'), ('E\'Twaun Moore'), ('Vander Blue') AS t(s)
        |WHERE s = :player_name""".stripMargin
    checkAnswer(
      spark.sql(sqlText, args = Map("player_name" -> lit("E'Twaun Moore"))),
      Row("E'Twaun Moore") :: Nil)
    checkAnswer(
      spark.sql(sqlText, args = Map("player_name" -> lit("Vander Blue--comment"))),
      Nil)
    checkAnswer(
      spark.sql(sqlText, args = Map("player_name" -> lit("Jeff /*__*/ Green"))),
      Row("Jeff /*__*/ Green") :: Nil)

    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      checkAnswer(
        spark.sql(
          sqlText = """
                      |SELECT d
                      |FROM VALUES (DATE'1970-01-01'), (DATE'2023-12-31') AS t(d)
                      |WHERE d < :currDate
                      |""".stripMargin,
          args = Map("currDate" -> lit(LocalDate.of(2023, 4, 1)))),
        Row(LocalDate.of(1970, 1, 1)) :: Nil)
      checkAnswer(
        spark.sql(
          sqlText = """
                      |SELECT d
                      |FROM VALUES (TIMESTAMP_LTZ'1970-01-01 01:02:03 Europe/Amsterdam'),
                      |            (TIMESTAMP_LTZ'2023-12-31 04:05:06 America/Los_Angeles') AS t(d)
                      |WHERE d < :currDate
                      |""".stripMargin,
          args = Map("currDate" -> lit(Instant.parse("2023-04-01T00:00:00Z")))),
        Row(LocalDateTime.of(1970, 1, 1, 1, 2, 3)
          .atZone(ZoneId.of("Europe/Amsterdam"))
          .toInstant) :: Nil)
    }
  }

  test("literal argument of positional parameter in `sql()`") {
    val sqlText =
      """SELECT s FROM VALUES ('Jeff /*__*/ Green'), ('E\'Twaun Moore'), ('Vander Blue') AS t(s)
        |WHERE s = ?""".stripMargin
    checkAnswer(
      spark.sql(sqlText, args = Array(lit("E'Twaun Moore"))),
      Row("E'Twaun Moore") :: Nil)
    checkAnswer(
      spark.sql(sqlText, args = Array(lit("Vander Blue--comment"))),
      Nil)
    checkAnswer(
      spark.sql(sqlText, args = Array(lit("Jeff /*__*/ Green"))),
      Row("Jeff /*__*/ Green") :: Nil)

    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      checkAnswer(
        spark.sql(
          sqlText = """
                      |SELECT d
                      |FROM VALUES (DATE'1970-01-01'), (DATE'2023-12-31') AS t(d)
                      |WHERE d < ?
                      |""".stripMargin,
          args = Array(lit(LocalDate.of(2023, 4, 1)))),
        Row(LocalDate.of(1970, 1, 1)) :: Nil)
      checkAnswer(
        spark.sql(
          sqlText = """
                      |SELECT d
                      |FROM VALUES (TIMESTAMP_LTZ'1970-01-01 01:02:03 Europe/Amsterdam'),
                      |            (TIMESTAMP_LTZ'2023-12-31 04:05:06 America/Los_Angeles') AS t(d)
                      |WHERE d < ?
                      |""".stripMargin,
          args = Array(lit(Instant.parse("2023-04-01T00:00:00Z")))),
        Row(LocalDateTime.of(1970, 1, 1, 1, 2, 3)
          .atZone(ZoneId.of("Europe/Amsterdam"))
          .toInstant) :: Nil)
    }
  }

  test("unused positional arguments") {
    checkAnswer(
      spark.sql("SELECT ?, ?", Array(1, "abc", 3.14f)),
      Row(1, "abc"))
  }

  test("mixing of positional and named parameters") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("select :param1, ?", Map("param1" -> 1))
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "_16"),
      context = ExpectedContext(
        fragment = "?",
        start = 16,
        stop = 16))

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("select :param1, ?", Array(1))
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "param1"),
      context = ExpectedContext(
        fragment = ":param1",
        start = 7,
        stop = 13))
  }

  test("SPARK-44680: parameters in DEFAULT") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(
          "CREATE TABLE t11(c1 int default :parm) USING parquet",
          args = Map("parm" -> 5))
      },
      condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      parameters = Map("statement" -> "DEFAULT"),
      context = ExpectedContext(
        fragment = "default :parm",
        start = 24,
        stop = 36))
  }

  test("SPARK-44783: arrays as parameters") {
    checkAnswer(
      spark.sql("SELECT array_position(:arrParam, 'abc')", Map("arrParam" -> Array.empty[String])),
      Row(0))
    checkAnswer(
      spark.sql("SELECT array_position(?, 0.1D)", Array(Array.empty[Double])),
      Row(0))
    checkAnswer(
      spark.sql("SELECT array_contains(:arrParam, 10)", Map("arrParam" -> Array(10, 20, 30))),
      Row(true))
    checkAnswer(
      spark.sql("SELECT array_contains(?, ?)", Array(Array("a", "b", "c"), "b")),
      Row(true))
    checkAnswer(
      spark.sql("SELECT :arr[1]", Map("arr" -> Array(10, 20, 30))),
      Row(20))
    checkAnswer(
      spark.sql("SELECT ?[?]", Array(Array(1f, 2f, 3f), 0)),
      Row(1f))
    checkAnswer(
      spark.sql("SELECT :arr[0][1]", Map("arr" -> Array(Array(1, 2), Array(20), Array.empty[Int]))),
      Row(2))
    checkAnswer(
      spark.sql("SELECT ?[?][?]", Array(Array(Array(1f, 2f), Array.empty[Float], Array(3f)), 0, 1)),
      Row(2f))
  }

  test("SPARK-45033: maps as parameters") {
    import org.apache.spark.util.ArrayImplicits._
    def fromArr(keys: Array[_], values: Array[_]): Column = {
      map_from_arrays(lit(keys), lit(values))
    }
    def callFromArr(keys: Array[_], values: Array[_]): Column = {
      call_function("map_from_arrays", lit(keys), lit(values))
    }
    def createMap(keys: Array[_], values: Array[_]): Column = {
      val zipped = keys.map(k => lit(k)).zip(values.map(v => lit(v)))
      map(zipped.flatMap { case (k, v) => Seq(k, v) }.toImmutableArraySeq: _*)
    }
    def callMap(keys: Array[_], values: Array[_]): Column = {
      val zipped = keys.map(k => lit(k)).zip(values.map(v => lit(v)))
      call_function("map", zipped.flatMap { case (k, v) => Seq(k, v) }.toImmutableArraySeq: _*)
    }
    def fromEntries(keys: Array[_], values: Array[_]): Column = {
      val structures = keys.zip(values)
        .map { case (k, v) => struct(lit(k), lit(v))}
      map_from_entries(array(structures.toImmutableArraySeq: _*))
    }
    def callFromEntries(keys: Array[_], values: Array[_]): Column = {
      val structures = keys.zip(values)
        .map { case (k, v) => struct(lit(k), lit(v))}
      call_function("map_from_entries", call_function("array", structures.toImmutableArraySeq: _*))
    }

    Seq(fromArr(_, _), createMap(_, _), callFromArr(_, _), callMap(_, _)).foreach { f =>
      checkAnswer(
        spark.sql("SELECT map_contains_key(:mapParam, 0)",
          Map("mapParam" -> f(Array.empty[Int], Array.empty[String]))),
        Row(false))
      checkAnswer(
        spark.sql("SELECT map_contains_key(?, 'a')",
          Array(f(Array.empty[String], Array.empty[Double]))),
        Row(false))
    }
    Seq(fromArr(_, _), createMap(_, _), fromEntries(_, _),
      callFromArr(_, _), callMap(_, _), callFromEntries(_, _)).foreach { f =>
      checkAnswer(
        spark.sql("SELECT element_at(:mapParam, 'a')",
          Map("mapParam" -> f(Array("a"), Array(0)))),
        Row(0))
      checkAnswer(
        spark.sql("SELECT element_at(?, 'a')", Array(f(Array("a"), Array(0)))),
        Row(0))
      checkAnswer(
        spark.sql("SELECT :m[10]", Map("m" -> f(Array(10, 20, 30), Array(0, 1, 2)))),
        Row(0))
      checkAnswer(
        spark.sql("SELECT ?[?]", Array(f(Array(1f, 2f, 3f), Array(1, 2, 3)), 2f)),
        Row(2))
    }
    checkAnswer(
      spark.sql("SELECT :m['a'][1]",
        Map("m" ->
          map_from_arrays(
            lit(Array("a")),
            array(map_from_arrays(lit(Array(1)), lit(Array(2))))))),
      Row(2))
    // `str_to_map` is not supported
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("SELECT :m['a'][1]",
          Map("m" ->
            map_from_arrays(
              lit(Array("a")),
              array(str_to_map(lit("a:1,b:2,c:3"))))))
      },
      condition = "INVALID_SQL_ARG",
      parameters = Map("name" -> "m"),
      context = ExpectedContext(
        fragment = "map_from_arrays",
        callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("SPARK-46481: Test variable folding") {
    sql("DECLARE a INT = 1")
    sql("SET VAR a = 1")
    val expected = sql("SELECT 42 WHERE 1 = 1").queryExecution.optimizedPlan
    val variableDirectly = sql("SELECT 42 WHERE 1 = a").queryExecution.optimizedPlan
    val parameterizedSpark =
      spark.sql("SELECT 42 WHERE 1 = ?", Array(1)).queryExecution.optimizedPlan
    val parameterizedSql =
      spark.sql("EXECUTE IMMEDIATE 'SELECT 42 WHERE 1 = ?' USING a").queryExecution.optimizedPlan

    comparePlans(expected, variableDirectly)
    comparePlans(expected, parameterizedSpark)
    comparePlans(expected, parameterizedSql)
  }

  test("SPARK-49017: bind named parameters with IDENTIFIER clause") {
    withTable("testtab") {
      // Create table
      spark.sql("create table testtab (id int, name string)")

      // Insert into table using single param
      spark.sql("insert into identifier(:tab) values(1, 'test1')", Map("tab" -> "testtab"))

      // Select from table using param
      checkAnswer(spark.sql("select * from identifier(:tab)", Map("tab" -> "testtab")),
        Seq(Row(1, "test1")))

      // Insert into table using multiple params
      spark.sql("insert into identifier(:tab) values(2, :name)",
        Map("tab" -> "testtab", "name" -> "test2"))

      // Select from table using param
      checkAnswer(sql("select * from testtab"), Seq(Row(1, "test1"), Row(2, "test2")))

      // Insert into table using multiple params and idents
      sql("insert into testtab values(2, 'test3')")

      // Select from table using param
      checkAnswer(spark.sql("select identifier(:col) from identifier(:tab) where :name == name",
        Map("tab" -> "testtab", "name" -> "test2", "col" -> "id")), Seq(Row(2)))
    }
  }

  test("SPARK-49017: bind positional parameters with IDENTIFIER clause") {
    withTable("testtab") {
      // Create table
      spark.sql("create table testtab (id int, name string)")

      // Insert into table using single param
      spark.sql("insert into identifier(?) values(1, 'test1')",
        Array("testtab"))

      // Select from table using param
      checkAnswer(spark.sql("select * from identifier(?)", Array("testtab")),
        Seq(Row(1, "test1")))

      // Insert into table using multiple params
      spark.sql("insert into identifier(?) values(2, ?)",
        Array("testtab", "test2"))

      // Select from table using param
      checkAnswer(sql("select * from testtab"), Seq(Row(1, "test1"), Row(2, "test2")))

      // Insert into table using multiple params and idents
      sql("insert into testtab values(2, 'test3')")

      // Select from table using param
      checkAnswer(spark.sql("select identifier(?) from identifier(?) where ? == name",
        Array("id", "testtab", "test2")), Seq(Row(2)))
    }
  }

  test("SPARK-49017: bind named parameters with IDENTIFIER clause in create table as") {
    withTable("testtab", "testtab1") {

      sql("create table testtab (id int, name string)")
      sql("insert into testtab values(1, 'test1')")

      // create table with parameters in query
      spark.sql(
        """create table identifier(:tab) as
          | select * from testtab where identifier(:col) == 1""".stripMargin,
        Map("tab" -> "testtab1", "col" -> "id"))

      checkAnswer(sql("select * from testtab1"), Seq(Row(1, "test1")))
    }
  }

  test("SPARK-46999: bind parameters for nested IDENTIFIER clause") {
    val query = sql(
      """
        |EXECUTE IMMEDIATE
        |'SELECT IDENTIFIER(?)(IDENTIFIER(?)) FROM VALUES (\'abc\') AS (col)'
        |USING 'UPPER', 'col'
        |""".stripMargin)
    checkAnswer(query, Row("ABC"))
  }

  test("SPARK-48843: Prevent infinite loop with BindParameters") {
    val df =
      sql("EXECUTE IMMEDIATE 'SELECT SUM(c1) num_sum FROM VALUES (?), (?) AS t(c1) ' USING 5, 6;")
    val analyzedPlan = Limit(Literal.create(100), df.queryExecution.logical)
    spark.sessionState.analyzer.executeAndCheck(analyzedPlan, df.queryExecution.tracker)
    checkAnswer(df, Row(11))
  }

  test("SPARK-49398: Cache Table with parameter markers in select query should throw " +
    "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT") {
    val sqlText = "CACHE TABLE CacheTable as SELECT 1 + :param1"
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(sqlText, Map("param1" -> "1")).show()
      },
      condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      parameters = Map("statement" -> "the query of CACHE TABLE"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = sqlText.length - 1)
    )
  }

  test("SPARK-49398: Cache Table with parameter in identifier should work") {
    val cacheName = "MyCacheTable"
    withCache(cacheName) {
      spark.sql("CACHE TABLE IDENTIFIER(:param) as SELECT 1 as c1", Map("param" -> cacheName))
      checkAnswer(
        spark.sql("SHOW COLUMNS FROM IDENTIFIER(?)", args = Array(cacheName)),
        Row("c1"))
    }
  }
}
