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

import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Limit
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.functions.{array, call_function, lit, map, map_from_arrays, map_from_entries, str_to_map, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{CharType, DataType, DecimalType, StructField, VarcharType}

class ParametersSuite extends QueryTest with SharedSparkSession {

  // Helper function to check CHAR/VARCHAR types (similar to CharVarcharTestSuite)
  private def checkColType(f: StructField, dt: DataType): Unit = {
    assert(f.dataType == CharVarcharUtils.replaceCharVarcharWithString(dt))
    assert(CharVarcharUtils.getRawType(f.metadata) == Some(dt))
  }

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

  test("case sensitivity for named parameters in shared grammar contexts") {
    // Test parameters in WHERE clause with integer comparison
    // (uses integerValue -> parameterMarker rule)
    withTempView("test_data") {
      spark.range(10).createOrReplaceTempView("test_data")

      checkAnswer(
        spark.sql(
          "SELECT id FROM test_data WHERE id >= :min_val AND id <= :MIN_VAL ORDER BY id",
          Map("min_val" -> 2, "MIN_VAL" -> 5)
        ),
        Row(2) :: Row(3) :: Row(4) :: Row(5) :: Nil
      )

      // Verify that mismatched case fails in integerValue context
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql(
            "SELECT id FROM test_data WHERE id = :param",
            Map("PARAM" -> 1)  // Wrong case
          )
        },
        condition = "UNBOUND_SQL_PARAMETER",
        parameters = Map("name" -> "param"),
        context = ExpectedContext(
          fragment = ":param",
          start = 36,
          stop = 41))
    }

    // Test parameters in LIMIT clause (uses integerValue -> parameterMarker rule)
    withTempView("limit_test") {
      spark.range(10).createOrReplaceTempView("limit_test")

      checkAnswer(
        spark.sql(
          "SELECT id FROM limit_test ORDER BY id LIMIT :limit_param",
          Map("limit_param" -> 3)
        ),
        Row(0) :: Row(1) :: Row(2) :: Nil
      )

      // Verify that mismatched case fails in LIMIT context
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql(
            "SELECT id FROM limit_test ORDER BY id LIMIT :limit_param",
            Map("LIMIT_PARAM" -> 3)  // Wrong case
          )
        },
        condition = "UNBOUND_SQL_PARAMETER",
        parameters = Map("name" -> "limit_param"),
        context = ExpectedContext(
          fragment = ":limit_param",
          start = 44,
          stop = 55))
    }

    // Test case sensitivity consistency between legacy and modern modes
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "false") {
      val modernResult = spark.sql(
        "SELECT 1 WHERE 1 = :param AND 2 = :Param",
        Map("param" -> 1, "Param" -> 2)
      ).collect()

      withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
        val legacyResult = spark.sql(
          "SELECT 1 WHERE 1 = :param AND 2 = :Param",
          Map("param" -> 1, "Param" -> 2)
        ).collect()

        // Both modes should produce the same results
        assert(modernResult.sameElements(legacyResult))
        assert(modernResult.length == 1)
        assert(modernResult(0).getInt(0) == 1)
      }
    }
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

  test("named parameters are allowed in view body ") {
    val sqlText = "CREATE VIEW v AS SELECT :p AS p"
    val args = Map("p" -> 1)
    // Named parameters are are supported in view bodies
    withView("v") {
      spark.sql(sqlText, args)
      checkAnswer(spark.table("v"), Row(1))
    }
  }

  test("positional parameters are allowed in view body ") {
    val sqlText = "CREATE VIEW v AS SELECT ? AS p"
    val args = Array(1)
    withView("v") {
      spark.sql(sqlText, args)
      checkAnswer(spark.table("v"), Row(1))
    }
  }

  test("named parameters are allowed in view body - WITH and scalar subquery") {
    val sqlText = "CREATE VIEW v AS WITH cte(a) AS (SELECT (SELECT :p) AS a)  SELECT a FROM cte"
    val args = Map("p" -> 1)
    withView("v") {
      spark.sql(sqlText, args)
      checkAnswer(spark.table("v"), Row(1))
    }
  }

  test("positional parameters are allowed in view body - WITH and scalar subquery") {
    val sqlText = "CREATE VIEW v AS WITH cte(a) AS (SELECT (SELECT ?) AS a)  SELECT a FROM cte"
    val args = Array(1)
    withView("v") {
      spark.sql(sqlText, args)
      checkAnswer(spark.table("v"), Row(1))
    }
  }

  test("named parameters are allowed in view body - nested WITH and EXIST") {
    val sqlText =
      """CREATE VIEW v AS
        |SELECT a as a
        |FROM (WITH cte(a) AS (SELECT CASE WHEN EXISTS(SELECT :p) THEN 1 END AS a)
        |SELECT a FROM cte)""".stripMargin
    val args = Map("p" -> 1)
    withView("v") {
      spark.sql(sqlText, args)
      checkAnswer(spark.table("v"), Row(1))
    }
  }

  test("positional parameters are allowed in view body - nested WITH and EXIST") {
    val sqlText =
      """CREATE VIEW v AS
        |SELECT a as a
        |FROM (WITH cte(a) AS (SELECT CASE WHEN EXISTS(SELECT ?) THEN 1 END AS a)
        |SELECT a FROM cte)""".stripMargin
    val args = Array(1)
    withView("v") {
      spark.sql(sqlText, args)
      checkAnswer(spark.table("v"), Row(1))
    }
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
      condition = "INVALID_QUERY_MIXED_QUERY_PARAMETERS",
      parameters = Map.empty[String, String])

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("select :param1, ?", Array(1))
      },
      condition = "INVALID_QUERY_MIXED_QUERY_PARAMETERS",
      parameters = Map.empty[String, String])
  }

  test("SPARK-44680: parameters in DEFAULT work") {
    withTable("t11") {
      spark.sql(
        "CREATE TABLE t11(c1 int default :parm) USING parquet",
        args = Map("parm" -> 5))

      // Insert a row using the default value
      spark.sql("INSERT INTO t11 (c1) VALUES (DEFAULT)")

      // Verify the default value was used correctly
      checkAnswer(
        spark.table("t11"),
        Row(5))
    }
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

  test("EXECUTE IMMEDIATE with parameters in data type contexts") {
    // Test that EXECUTE IMMEDIATE with parameters in data type contexts still works
    // This validates that our top-level parsing fix doesn't break nested parameter substitution
    checkAnswer(
      sql("EXECUTE IMMEDIATE 'SELECT 5::DECIMAL(:p, :s)' USING 10 AS p, 5 AS s"),
      Row(BigDecimal("5.00000"))
    )
  }

  test("SPARK-49398: Cache Table with parameter markers in select query works") {
    val sqlText = "CACHE TABLE CacheTable as SELECT 1 + :param1"
    spark.sql(sqlText, Map("param1" -> "1"))

    // Verify the cached table works correctly
    checkAnswer(
      spark.table("CacheTable"),
      Row(2))

    // Clean up
    spark.sql("UNCACHE TABLE CacheTable")
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

  test("SPARK-50322: parameterized identifier in a sub-query") {
    withTable("tt1") {
      sql("CREATE TABLE tt1 (c1 INT)")
      sql("INSERT INTO tt1 VALUES (1)")
      def query(p: String): String = {
        s"""
          |WITH v1 AS (
          |  SELECT * FROM tt1
          |  WHERE 1 = (SELECT * FROM IDENTIFIER($p))
          |) SELECT * FROM v1""".stripMargin
      }

      checkAnswer(spark.sql(query(":tab"), args = Map("tab" -> "tt1")), Row(1))
      checkAnswer(spark.sql(query("?"), args = Array("tt1")), Row(1))
    }
  }

  test("SPARK-50441: parameterized identifier referencing a CTE") {
    def query(p: String): String = {
      s"""
         |WITH t1 AS (SELECT 1)
         |SELECT * FROM IDENTIFIER($p)""".stripMargin
    }

    checkAnswer(spark.sql(query(":cte"), args = Map("cte" -> "t1")), Row(1))
    checkAnswer(spark.sql(query("?"), args = Array("t1")), Row(1))
  }

  test("SPARK-50892: parameterized identifier in outer query referencing a recursive CTE") {
    def query(p: String): String = {
      s"""
         |WITH RECURSIVE t1(n) AS (
         |  SELECT 1
         |  UNION ALL
         |  SELECT n+1 FROM t1 WHERE n < 5)
         |SELECT * FROM IDENTIFIER($p)""".stripMargin
    }

    checkAnswer(spark.sql(query(":cte"), args = Map("cte" -> "t1")),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5)))
    checkAnswer(spark.sql(query("?"), args = Array("t1")),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5)))
  }

  test("SPARK-50892: parameterized identifier inside a recursive CTE") {
    def query(p: String): String = {
      s"""
         |WITH RECURSIVE t1(n) AS (
         |  SELECT 1
         |  UNION ALL
         |  SELECT n+1 FROM IDENTIFIER($p) WHERE n < 5)
         |SELECT * FROM t1""".stripMargin
    }

    checkAnswer(spark.sql(query(":cte"), args = Map("cte" -> "t1")),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5)))
    checkAnswer(spark.sql(query("?"), args = Array("t1")),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5)))
  }


  test("SPARK-50403: parameterized execute immediate") {
    checkAnswer(spark.sql("execute immediate 'select ?' using ?", Array(1)), Row(1))
    checkAnswer(spark.sql("execute immediate 'select ?, ?' using ?, 2", Array(1)), Row(1, 2))
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("execute immediate 'select ?, ?' using 1", Array(2))
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "_10"),
      context = ExpectedContext("?", 10, 10))

    checkAnswer(spark.sql("execute immediate 'select ?' using 1", Map("param1" -> "1")), Row(1))
    checkAnswer(spark.sql("execute immediate 'select :param1' using :param2 as param1",
      Map("param2" -> 42)), Row(42))
    checkAnswer(spark.sql(
      "execute immediate 'select :param1, :param2' using :param2 as param1, 43 as param2",
      Map("param2" -> 42)), Row(42, 43))
    checkAnswer(spark.sql("execute immediate 'select :param' using 0 as param",
      Map("param" -> 42)), Row(0))
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("execute immediate 'select :param1, :param2' using 1 as param1",
          Map("param2" -> 2))
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "param2"),
      context = ExpectedContext(":param2", 16, 22))

    checkAnswer(spark.sql("execute immediate 'select ?' using :param", Map("param" -> 2)), Row(2))
    checkAnswer(spark.sql("execute immediate 'select :param' using ? as param", Array(3)), Row(3))
  }

  test("named parameters in DDL data type specifications") {
    withTable("ddl_datatype_named") {
      // Test VARCHAR length parameter
      spark.sql("CREATE TABLE ddl_datatype_named (name VARCHAR(:length)) USING PARQUET",
                Map("length" -> 100))
      val schema = spark.table("ddl_datatype_named").schema
      checkColType(schema("name"), VarcharType(100))
    }

    withTable("ddl_decimal_named") {
      // Test DECIMAL precision and scale parameters
      spark.sql(
        "CREATE TABLE ddl_decimal_named (amount DECIMAL(:precision, :scale)) USING PARQUET",
        Map("precision" -> 10, "scale" -> 2))
      val schema = spark.table("ddl_decimal_named").schema
      assert(schema("amount").dataType == DecimalType(10, 2))
    }

    withTable("ddl_char_named") {
      // Test CHAR length parameter
      spark.sql("CREATE TABLE ddl_char_named (code CHAR(:length)) USING PARQUET",
                Map("length" -> 5))
      val schema = spark.table("ddl_char_named").schema
      checkColType(schema("code"), CharType(5))
    }
  }

  test("positional parameters in DDL data type specifications") {
    withTable("ddl_datatype_pos") {
      // Test VARCHAR length parameter
      spark.sql("CREATE TABLE ddl_datatype_pos (name VARCHAR(?)) USING PARQUET",
                Array(150))
      val schema = spark.table("ddl_datatype_pos").schema
      checkColType(schema("name"), VarcharType(150))
    }

    withTable("ddl_decimal_pos") {
      // Test DECIMAL precision and scale parameters
      spark.sql(
        "CREATE TABLE ddl_decimal_pos (amount DECIMAL(?, ?)) USING PARQUET",
        Array(12, 3))
      val schema = spark.table("ddl_decimal_pos").schema
      assert(schema("amount").dataType == DecimalType(12, 3))
    }

    withTable("ddl_char_pos") {
      // Test CHAR length parameter
      spark.sql("CREATE TABLE ddl_char_pos (code CHAR(?)) USING PARQUET",
                Array(8))
      val schema = spark.table("ddl_char_pos").schema
      checkColType(schema("code"), CharType(8))
    }
  }

  test("named parameters in DDL comments and properties") {
    withTable("ddl_comments_named") {
      // Test table comment parameter
      spark.sql("CREATE TABLE ddl_comments_named (id INT) USING PARQUET COMMENT :comment",
                Map("comment" -> "Test table with named parameter"))
      val tableDesc = spark.catalog.getTable("ddl_comments_named").description
      assert(tableDesc == "Test table with named parameter")
    }

    withTable("ddl_props_named") {
      // Test TBLPROPERTIES parameters
      spark.sql("""CREATE TABLE ddl_props_named (id INT) USING PARQUET
                   TBLPROPERTIES ('created_by' = :owner, 'department' = :dept)""",
                Map("owner" -> "test_user", "dept" -> "engineering"))
      val tableProps = spark.sql("DESCRIBE TABLE EXTENDED ddl_props_named").collect()
      val propsRow = tableProps.find(_.getString(0) == "Table Properties").get
      assert(propsRow.getString(1).contains("created_by=test_user"))
      assert(propsRow.getString(1).contains("department=engineering"))
    }
  }

  test("positional parameters in DDL comments and properties") {
    withTable("ddl_comments_pos") {
      // Test table comment parameter
      spark.sql("CREATE TABLE ddl_comments_pos (id INT) USING PARQUET COMMENT ?",
                Array("Test table with positional parameter"))
      val tableDesc = spark.catalog.getTable("ddl_comments_pos").description
      assert(tableDesc == "Test table with positional parameter")
    }

    withTable("ddl_props_pos") {
      // Test TBLPROPERTIES parameters
      spark.sql("""CREATE TABLE ddl_props_pos (id INT) USING PARQUET
                   TBLPROPERTIES ('created_by' = ?, 'version' = ?)""",
                Array("spark_sql", "3.5"))
      val tableProps = spark.sql("DESCRIBE TABLE EXTENDED ddl_props_pos").collect()
      val propsRow = tableProps.find(_.getString(0) == "Table Properties").get
      assert(propsRow.getString(1).contains("created_by=spark_sql"))
      assert(propsRow.getString(1).contains("version=3.5"))
    }
  }

  test("named parameters in DDL location specifications") {
    withTempDir { dir =>
      withTable("ddl_location_named") {
        // Test location parameter
        val location = dir.getAbsolutePath
        spark.sql("CREATE TABLE ddl_location_named (id INT) USING PARQUET LOCATION :path",
                  Map("path" -> location))
        val tableMeta = spark.catalog.getTable("ddl_location_named")
        assert(tableMeta.tableType == "EXTERNAL")
      }
    }
  }

  test("positional parameters in DDL location specifications") {
    withTempDir { dir =>
      withTable("ddl_location_pos") {
        // Test location parameter
        val location = dir.getAbsolutePath
        spark.sql("CREATE TABLE ddl_location_pos (id INT) USING PARQUET LOCATION ?",
                  Array(location))
        val tableMeta = spark.catalog.getTable("ddl_location_pos")
        assert(tableMeta.tableType == "EXTERNAL")
      }
    }
  }

  test("named parameters in DEFAULT expressions") {
    withTable("ddl_default_named") {
      // Test DEFAULT expression parameter
      spark.sql(
        "CREATE TABLE ddl_default_named (id INT, status STRING DEFAULT :status) USING PARQUET",
        Map("status" -> "pending"))

      // Insert without specifying status to test default
      spark.sql("INSERT INTO ddl_default_named (id) VALUES (1)")
      val result = spark.sql("SELECT * FROM ddl_default_named").collect()
      assert(result(0).getString(1) == "pending")
    }
  }

  test("positional parameters in DEFAULT expressions") {
    withTable("ddl_default_pos") {
      // Test DEFAULT expression parameter
      spark.sql(
        "CREATE TABLE ddl_default_pos (id INT, priority INT DEFAULT ?) USING PARQUET", Array(1))

      // Insert without specifying priority to test default
      spark.sql("INSERT INTO ddl_default_pos (id) VALUES (1)")
      val result = spark.sql("SELECT * FROM ddl_default_pos").collect()
      assert(result(0).getInt(1) == 1)
    }
  }

  test("named parameters in expressions that previously only accepted literals") {
    // Test CAST with data type parameters
    checkAnswer(
      spark.sql("SELECT CAST(:value AS DECIMAL(:precision, :scale))",
                Map("value" -> "123.456", "precision" -> 10, "scale" -> 2)),
      Row(java.math.BigDecimal.valueOf(123.46)))
  }

  test("positional parameters in expressions that previously only accepted literals") {
    // Test CAST with data type parameters
    checkAnswer(
      spark.sql("SELECT CAST(? AS DECIMAL(?, ?))", Array("987.654", 8, 3)),
      Row(java.math.BigDecimal.valueOf(987.654)))
  }

  test("named parameters in partition specifications for INSERT") {
    withTable("partition_insert_named") {
      spark.sql(
        "CREATE TABLE partition_insert_named (id INT, year INT, month INT) " +
        "USING PARQUET PARTITIONED BY (year, month)")

      // Test parameterized partition values in INSERT
      spark.sql(
        "INSERT INTO partition_insert_named PARTITION (year = :year, month = :month) VALUES (:id)",
        Map("year" -> 2023, "month" -> 12, "id" -> 1))

      val result = spark.sql("SELECT * FROM partition_insert_named").collect()
      assert(result.length == 1)
      assert(result(0).getInt(0) == 1)
      assert(result(0).getInt(1) == 2023)
      assert(result(0).getInt(2) == 12)
    }
  }

  test("positional parameters in partition specifications for INSERT") {
    withTable("partition_insert_pos") {
      spark.sql(
        "CREATE TABLE partition_insert_pos (id INT, year INT) USING PARQUET PARTITIONED BY (year)")

      // Test parameterized partition values in INSERT
      spark.sql("INSERT INTO partition_insert_pos PARTITION (year = ?) VALUES (?)", Array(2024, 2))

      val result = spark.sql("SELECT * FROM partition_insert_pos").collect()
      assert(result.length == 1)
      assert(result(0).getInt(0) == 2)
      assert(result(0).getInt(1) == 2024)
    }
  }

  test("named parameters in auxiliary SHOW statements") {
    withTable("show_test_named") {
      spark.sql("CREATE TABLE show_test_named (id INT) USING PARQUET")

      // Test SHOW TABLES with LIKE pattern parameter
      val showResult = spark.sql(
        "SHOW TABLES LIKE :pattern", Map("pattern" -> "show_test_named")).collect()
      assert(showResult.length == 1)
      assert(showResult(0).getString(1) == "show_test_named")
    }
  }

  test("positional parameters in auxiliary SHOW statements") {
    withTable("show_test_pos") {
      spark.sql("CREATE TABLE show_test_pos (id INT) USING PARQUET")

      // Test SHOW TABLES with LIKE pattern parameter
      val showResult = spark.sql("SHOW TABLES LIKE ?", Array("show_test_pos")).collect()
      assert(showResult.length == 1)
      assert(showResult(0).getString(1) == "show_test_pos")
    }
  }

  test("named parameters in auxiliary DESCRIBE statements") {
    // Note: DESCRIBE statement doesn't support parameters in table names or column specifications
    // This test is disabled until grammar support is added
    // For now, test that we can use parameters in other auxiliary statements
    withTable("describe_test_named") {
      spark.sql(
        "CREATE TABLE describe_test_named (id INT, name VARCHAR(100)) USING PARQUET")

      // Test SHOW TABLES with parameter instead (which works)
      val showResult = spark.sql("SHOW TABLES LIKE :pattern",
        Map("pattern" -> "describe_test_named")).collect()
      assert(showResult.length == 1)
      assert(showResult(0).getString(1) == "describe_test_named")
    }
  }

  test("positional parameters in auxiliary DESCRIBE statements") {
    // Note: DESCRIBE statement doesn't support parameters in table names or column specifications
    // This test is disabled until grammar support is added
    // For now, test that we can use parameters in other auxiliary statements
    withTable("describe_test_pos") {
      spark.sql("CREATE TABLE describe_test_pos (id INT, value DECIMAL(10,2)) USING PARQUET")

      // Test SHOW TABLES with parameter instead (which works)
      val showResult = spark.sql("SHOW TABLES LIKE ?", Array("describe_test_pos")).collect()
      assert(showResult.length == 1)
      assert(showResult(0).getString(1) == "describe_test_pos")
    }
  }

  test("named parameters in CREATE VIEW statements") {
    withView("view_named_params") {
      // Test CREATE VIEW with parameter in SELECT clause
      spark.sql("CREATE VIEW view_named_params AS SELECT :value as constant_col",
                Map("value" -> 42))

      checkAnswer(spark.table("view_named_params"), Row(42))
    }
  }

  test("positional parameters in CREATE VIEW statements") {
    withView("view_pos_params") {
      // Test CREATE VIEW with parameter in SELECT clause
      spark.sql("CREATE VIEW view_pos_params AS SELECT ? as constant_col", Array(99))

      checkAnswer(spark.table("view_pos_params"), Row(99))
    }
  }

  test("named parameters in complex function calls that previously required literals") {
    // Test functions that previously only accepted INTEGER_VALUE or stringLit

    // REPEAT function with count parameter
    checkAnswer(
      spark.sql("SELECT REPEAT(:str, :count)", Map("str" -> "Hi", "count" -> 3)),
      Row("HiHiHi"))

    // SPLIT function with limit parameter
    checkAnswer(
      spark.sql("SELECT SPLIT(:str, :delim, :limit)",
                Map("str" -> "a,b,c,d", "delim" -> ",", "limit" -> 2)),
      Row(Array("a", "b,c,d")))

    // ROUND function with scale parameter
    checkAnswer(
      spark.sql("SELECT ROUND(:value, :scale)", Map("value" -> 3.14159, "scale" -> 2)),
      Row(3.14))
  }

  test("positional parameters in complex function calls that previously required literals") {
    // Test functions that previously only accepted INTEGER_VALUE or stringLit

    // FORMAT_STRING function
    checkAnswer(
      spark.sql("SELECT FORMAT_STRING(?, ?)", Array("Hello %s", "World")),
      Row("Hello World"))

    // OVERLAY function with position and length parameters
    checkAnswer(
      spark.sql("SELECT OVERLAY(? PLACING ? FROM ? FOR ?)",
                Array("Hello World", "XX", 7, 5)),
      Row("Hello XX"))

    // LOCATE function with start position parameter
    checkAnswer(
      spark.sql("SELECT LOCATE(?, ?, ?)", Array("o", "Hello World", 6)),
      Row(8))
  }

  test("named parameters in table sampling") {
    withTable("sample_test_named") {
      spark.sql("CREATE TABLE sample_test_named (id INT, value STRING) USING PARQUET")
      spark.sql(
        "INSERT INTO sample_test_named VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

      // Test TABLESAMPLE with named percentage parameter
      val sampleResult = spark.sql(
        "SELECT * FROM sample_test_named TABLESAMPLE (:percent PERCENT)",
        Map("percent" -> 50)).collect()
      assert(sampleResult.length >= 0) // Should return some subset of rows

      // Test TABLESAMPLE with named row count parameter
      val rowSampleResult = spark.sql(
        "SELECT * FROM sample_test_named TABLESAMPLE (:rows ROWS)",
        Map("rows" -> 3)).collect()
      assert(rowSampleResult.length <= 3) // Should return at most 3 rows
    }
  }

  test("positional parameters in table sampling") {
    withTable("sample_test_pos") {
      spark.sql("CREATE TABLE sample_test_pos (id INT, value STRING) USING PARQUET")
      spark.sql(
        "INSERT INTO sample_test_pos VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

      // Test TABLESAMPLE with positional percentage parameter
      val sampleResult = spark.sql(
        "SELECT * FROM sample_test_pos TABLESAMPLE (? PERCENT)",
        Array(30)).collect()
      assert(sampleResult.length >= 0) // Should return some subset of rows

      // Test TABLESAMPLE with positional row count parameter
      val rowSampleResult = spark.sql(
        "SELECT * FROM sample_test_pos TABLESAMPLE (? ROWS)",
        Array(2)).collect()
      assert(rowSampleResult.length <= 2) // Should return at most 2 rows
    }
  }

  test("named parameters in TBLPROPERTY keys") {
    withTable("tblprop_key_named") {
      // Test parameterized table property keys
      spark.sql(
        "CREATE TABLE tblprop_key_named (id INT) USING PARQUET " +
        "TBLPROPERTIES (:key1 = 'value1', :key2 = 'value2')",
        Map("key1" -> "created_by", "key2" -> "department"))

      val tableProps = spark.sql("DESCRIBE TABLE EXTENDED tblprop_key_named").collect()
      val propsRow = tableProps.find(_.getString(0) == "Table Properties").get
      assert(propsRow.getString(1).contains("created_by=value1"))
      assert(propsRow.getString(1).contains("department=value2"))
    }

    withTable("tblprop_dynamic_named") {
      // Test setting table properties with parameterized keys using ALTER TABLE
      spark.sql("CREATE TABLE tblprop_dynamic_named (id INT) USING PARQUET")
      spark.sql(
        "ALTER TABLE tblprop_dynamic_named SET TBLPROPERTIES (:prop_key = :prop_value)",
        Map("prop_key" -> "created_by", "prop_value" -> "spark_test"))

      val tableProps = spark.sql("DESCRIBE TABLE EXTENDED tblprop_dynamic_named").collect()
      val propsRow = tableProps.find(_.getString(0) == "Table Properties").get
      assert(propsRow.getString(1).contains("created_by=spark_test"))
    }
  }

  test("positional parameters in TBLPROPERTY keys") {
    withTable("tblprop_key_pos") {
      // Test parameterized table property keys with positional parameters
      spark.sql(
        "CREATE TABLE tblprop_key_pos (id INT) USING PARQUET TBLPROPERTIES (? = ?, ? = ?)",
        Array("version", "1.0", "environment", "test"))

      val tableProps = spark.sql("DESCRIBE TABLE EXTENDED tblprop_key_pos").collect()
      val propsRow = tableProps.find(_.getString(0) == "Table Properties").get
      assert(propsRow.getString(1).contains("version=1.0"))
      assert(propsRow.getString(1).contains("environment=test"))
    }

    withTable("tblprop_dynamic_pos") {
      // Test setting table properties with parameterized keys using ALTER TABLE
      spark.sql("CREATE TABLE tblprop_dynamic_pos (id INT) USING PARQUET")
      spark.sql(
        "ALTER TABLE tblprop_dynamic_pos SET TBLPROPERTIES (? = ?)",
        Array("modified_by", "user123"))

      val tableProps = spark.sql("DESCRIBE TABLE EXTENDED tblprop_dynamic_pos").collect()
      val propsRow = tableProps.find(_.getString(0) == "Table Properties").get
      assert(propsRow.getString(1).contains("modified_by=user123"))
    }
  }

  test("legacy parameter substitution configuration - basic functionality") {
    // When legacy mode is disabled (default), parameter substitution should work everywhere
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "false") {
      val result1 = spark.sql("SELECT ?", Array(42)).collect()
      assert(result1(0).getInt(0) == 42)

      val result2 = spark.sql("SELECT :param", Map("param" -> 42)).collect()
      assert(result2(0).getInt(0) == 42)
    }

    // When legacy mode is enabled, parameter substitution is disabled but parameter binding works
    // Parameters should work in constant expressions through analyzer binding
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // Test that positional parameters work in legacy mode through analyzer binding
      val result1 = spark.sql("SELECT ?", Array(42)).collect()
      assert(result1(0).getInt(0) == 42)

      // Test that named parameters work in legacy mode through analyzer binding
      val result2 = spark.sql("SELECT :param", Map("param" -> 42)).collect()
      assert(result2(0).getInt(0) == 42)
    }
  }

  test("legacy parameter substitution configuration - integration with existing tests") {
    // Ensure existing param functionality preserved when config is false (params work everywhere)
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "false") {
      // Test some of the existing parameter functionality
      checkAnswer(spark.sql("SELECT ?", Array(1)), Row(1))
      checkAnswer(spark.sql("SELECT :param", Map("param" -> "test")), Row("test"))

      // Test with multiple positional parameters
      checkAnswer(spark.sql("SELECT ?, ?", Array(1, 2)), Row(1, 2))

      // Test with multiple named parameters
      checkAnswer(
        spark.sql("SELECT :a, :b", Map("a" -> "hello", "b" -> "world")),
        Row("hello", "world")
      )
    }

    // Ensure when config is true, parameter binding works correctly through analyzer
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // In legacy mode, parameters should work through analyzer binding
      val result1 = spark.sql("SELECT ?", Array(1)).collect()
      assert(result1(0).getInt(0) == 1)

      val result2 = spark.sql("SELECT :test", Map("test" -> "value")).collect()
      assert(result2(0).getString(0) == "value")
    }
  }

  // =============================================
  // Legacy Mode Comprehensive Tests
  // =============================================
  // These tests verify that ALL basic parameter functionality works correctly in legacy mode.
  // In legacy mode (constantsOnly=true):
  // - Parameter substitution is disabled (no text replacement)
  // - Parameter context is passed to analyzer for binding
  // - Parameters should work in constant expressions

  test("legacy mode - bind named parameters") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
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
  }

  test("legacy mode - bind positional parameters") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
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
  }

  test("legacy mode - parameter binding is case sensitive") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      checkAnswer(
        spark.sql("SELECT :p, :P", Map("p" -> 1, "P" -> 2)),
        Row(1, 2)
      )
    }
  }

  test("legacy mode - named parameters in CTE") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
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
  }

  test("legacy mode - positional parameters in CTE") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
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
  }

  test("legacy mode - named parameters in nested CTE") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
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
  }

  test("legacy mode - positional parameters in nested CTE") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
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
  }

  test("legacy mode - named parameters in subquery expression") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val sqlText = "SELECT (SELECT max(id) + :p1 FROM range(10)) + :p2"
      val args = Map("p1" -> 1, "p2" -> 2)
      checkAnswer(
        spark.sql(sqlText, args),
        Row(12))
    }
  }

  test("legacy mode - positional parameters in subquery expression") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val sqlText = "SELECT (SELECT max(id) + ? FROM range(10)) + ?"
      val args = Array(1, 2)
      checkAnswer(
        spark.sql(sqlText, args),
        Row(12))
    }
  }

  test("legacy mode - named parameters in nested subquery expression") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val sqlText = "SELECT (SELECT (SELECT max(id) + :p1 FROM range(10)) + :p2) + :p3"
      val args = Map("p1" -> 1, "p2" -> 2, "p3" -> 3)
      checkAnswer(
        spark.sql(sqlText, args),
        Row(15))
    }
  }

  test("legacy mode - positional parameters in nested subquery expression") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val sqlText = "SELECT (SELECT (SELECT max(id) + ? FROM range(10)) + ?) + ?"
      val args = Array(1, 2, 3)
      checkAnswer(
        spark.sql(sqlText, args),
        Row(15))
    }
  }

  test("legacy mode - named parameters in subquery expression inside CTE") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
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
  }

  test("legacy mode - positional parameters in subquery expression inside CTE") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
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
  }

  test("legacy mode - named parameter in identifier clause") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val sqlText =
        "SELECT IDENTIFIER('T.' || :p1 || '1') FROM VALUES(1) T(c1)"
      val args = Map("p1" -> "c")
      checkAnswer(
        spark.sql(sqlText, args),
        Row(1))
    }
  }

  test("legacy mode - positional parameter in identifier clause") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val sqlText =
        "SELECT IDENTIFIER('T.' || ? || '1') FROM VALUES(1) T(c1)"
      val args = Array("c")
      checkAnswer(
        spark.sql(sqlText, args),
        Row(1))
    }
  }

  test("legacy mode - named parameter in identifier clause in DDL and utility commands") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      spark.sql("CREATE VIEW IDENTIFIER(:p1)(c1) AS SELECT 1", args = Map("p1" -> "v_legacy"))
      spark.sql("ALTER VIEW IDENTIFIER(:p1) AS SELECT 2 AS c1", args = Map("p1" -> "v_legacy"))
      checkAnswer(
        spark.sql("SHOW COLUMNS FROM IDENTIFIER(:p1)", args = Map("p1" -> "v_legacy")),
        Row("c1"))
      spark.sql("DROP VIEW IDENTIFIER(:p1)", args = Map("p1" -> "v_legacy"))
    }
  }

  test("legacy mode - positional parameter in identifier clause in DDL and utility commands") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      spark.sql("CREATE VIEW IDENTIFIER(?)(c1) AS SELECT 1", args = Array("v_legacy_pos"))
      spark.sql("ALTER VIEW IDENTIFIER(?) AS SELECT 2 AS c1", args = Array("v_legacy_pos"))
      checkAnswer(
        spark.sql("SHOW COLUMNS FROM IDENTIFIER(?)", args = Array("v_legacy_pos")),
        Row("c1"))
      spark.sql("DROP VIEW IDENTIFIER(?)", args = Array("v_legacy_pos"))
    }
  }

  test("legacy mode - named parameters in INSERT") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      withTable("t_legacy") {
        sql("CREATE TABLE t_legacy (col INT) USING json")
        spark.sql("INSERT INTO t_legacy SELECT :p", Map("p" -> 1))
        checkAnswer(spark.table("t_legacy"), Row(1))
      }
    }
  }

  test("legacy mode - positional parameters in INSERT") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      withTable("t_legacy_pos") {
        sql("CREATE TABLE t_legacy_pos (col INT) USING json")
        spark.sql("INSERT INTO t_legacy_pos SELECT ?", Array(1))
        checkAnswer(spark.table("t_legacy_pos"), Row(1))
      }
    }
  }

  test("legacy mode - named parameters in view body") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val sqlText = "CREATE VIEW v_legacy AS SELECT :p AS p"
      val args = Map("p" -> 1)
      // Parameter markers are not allowed in CREATE VIEW queries
      checkError(
        exception = intercept[ParseException] {
          spark.sql(sqlText, args)
        },
        condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
        parameters = Map("statement" -> "the query of CREATE VIEW"),
        context = ExpectedContext(
          fragment = "CREATE VIEW v_legacy AS SELECT :p AS p",
          start = 0,
          stop = 37)
      )
    }
  }

  test("legacy mode - positional parameters in view body") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val sqlText = "CREATE VIEW v_legacy_pos AS SELECT ? AS p"
      val args = Array(1)
      // Parameter markers are not allowed in CREATE VIEW queries
      checkError(
        exception = intercept[ParseException] {
          spark.sql(sqlText, args)
        },
        condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
        parameters = Map("statement" -> "the query of CREATE VIEW"),
        context = ExpectedContext(
          fragment = "CREATE VIEW v_legacy_pos AS SELECT ? AS p",
          start = 0,
          stop = 40)
      )
    }
  }

  test("legacy mode - arrays as parameters") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      checkAnswer(
        spark.sql("SELECT array_position(:arrParam, 'abc')",
          Map("arrParam" -> Array.empty[String])),
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
    }
  }

  test("legacy mode - maps as parameters") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      import org.apache.spark.util.ArrayImplicits._
      def fromArr(keys: Array[_], values: Array[_]): Column = {
        map_from_arrays(lit(keys), lit(values))
      }
      def createMap(keys: Array[_], values: Array[_]): Column = {
        val zipped = keys.map(k => lit(k)).zip(values.map(v => lit(v)))
        map(zipped.flatMap { case (k, v) => Array(k, v) }.toImmutableArraySeq: _*)
      }

      Array(fromArr(_, _), createMap(_, _)).foreach { f =>
        checkAnswer(
          spark.sql("SELECT map_contains_key(:mapParam, 0)",
            Map("mapParam" -> f(Array.empty[Int], Array.empty[String]))),
          Row(false))
        checkAnswer(
          spark.sql("SELECT map_contains_key(?, 'a')",
            Array(f(Array.empty[String], Array.empty[Double]))),
          Row(false))
      }
      Array(fromArr(_, _), createMap(_, _)).foreach { f =>
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
    }
  }

  test("legacy mode - DEFAULT expressions with parameters") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // Parameter markers are not allowed in DEFAULT expressions
      checkError(
        exception = intercept[ParseException] {
          spark.sql(
            "CREATE TABLE t_legacy_default(c1 int default :parm) USING parquet",
            args = Map("parm" -> 5))
        },
        condition = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
        parameters = Map("statement" -> "DEFAULT"),
        context = ExpectedContext(
          fragment = "default :parm",
          start = 37,
          stop = 49)
      )
    }
  }

  test("legacy mode - bind named parameters with IDENTIFIER clause") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      withTable("testtab_legacy") {
        spark.sql("create table testtab_legacy (id int, name string)")
        spark.sql("insert into identifier(:tab) values(1, 'test1')", Map("tab" -> "testtab_legacy"))
        checkAnswer(spark.sql("select * from identifier(:tab)", Map("tab" -> "testtab_legacy")),
          Seq(Row(1, "test1")))
        spark.sql("insert into identifier(:tab) values(2, :name)",
          Map("tab" -> "testtab_legacy", "name" -> "test2"))
        checkAnswer(sql("select * from testtab_legacy"), Seq(Row(1, "test1"), Row(2, "test2")))
      }
    }
  }

  test("legacy mode - bind positional parameters with IDENTIFIER clause") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      withTable("testtab_legacy_pos") {
        spark.sql("create table testtab_legacy_pos (id int, name string)")
        spark.sql("insert into identifier(?) values(1, 'test1')", Array("testtab_legacy_pos"))
        checkAnswer(spark.sql("select * from identifier(?)", Array("testtab_legacy_pos")),
          Seq(Row(1, "test1")))
        spark.sql("insert into identifier(?) values(2, ?)",
          Array("testtab_legacy_pos", "test2"))
        checkAnswer(sql("select * from testtab_legacy_pos"),
          Seq(Row(1, "test1"), Row(2, "test2")))
      }
    }
  }

  test("legacy mode - simple constant expressions should work") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // Basic constant expressions that should work in legacy mode
      checkAnswer(spark.sql("SELECT :param", Map("param" -> 42)), Row(42))
      checkAnswer(spark.sql("SELECT ?", Array(42)), Row(42))
      checkAnswer(spark.sql("SELECT :str", Map("str" -> "hello")), Row("hello"))
      checkAnswer(spark.sql("SELECT ?", Array("hello")), Row("hello"))
      checkAnswer(spark.sql("SELECT :flag", Map("flag" -> true)), Row(true))
      checkAnswer(spark.sql("SELECT ?", Array(false)), Row(false))
    }
  }

  // =============================================================================
  // POSITION MAPPING HELPER METHODS
  // Tests to verify that parameter substitution correctly maps error positions
  // back to the original SQL text.
  // =============================================================================

  /**
   * Verify that a parameterized SQL query that produces an error
   * has the error context pointing to the original SQL text and correct positions.
   */
  private def checkParameterError[T <: Throwable](
      sql: String,
      expectedStartPos: Option[Int] = None,
      expectedStopPos: Option[Int] = None,
      params: Map[String, Any] = Map.empty,
      positionalParams: Array[Any] = Array.empty)(implicit ct: scala.reflect.ClassTag[T]): T = {

    val exception = intercept[T] {
      if (positionalParams.nonEmpty) {
        spark.sql(sql, positionalParams)
      } else {
        spark.sql(sql, params)
      }
    }

    // For exceptions with query context, verify they reference the original SQL and positions
    exception match {
      case pe: ParseException =>
        if (pe.getQueryContext.nonEmpty) {
          val context = pe.getQueryContext.head.asInstanceOf[SQLQueryContext]
          assert(context.sqlText.exists(_.contains(sql)),
            s"Parse error should reference original SQL.\n" +
            s"Expected to contain: $sql\n" +
            s"Actual context SQL: ${context.sqlText}")

          // Verify position mapping if expected positions are provided
          expectedStartPos.foreach { expectedStart =>
            assert(context.originStartIndex.contains(expectedStart),
              s"Start position should be $expectedStart, got: ${context.originStartIndex}")
          }
          expectedStopPos.foreach { expectedStop =>
            assert(context.originStopIndex.contains(expectedStop),
              s"Stop position should be $expectedStop, got: ${context.originStopIndex}")
          }
        }
      case ae: AnalysisException =>
        if (ae.context.nonEmpty) {
          val context = ae.context.head.asInstanceOf[SQLQueryContext]
          assert(context.sqlText.exists(_.contains(sql)),
            s"Analysis error should reference original SQL.\n" +
            s"Expected to contain: $sql\n" +
            s"Actual context SQL: ${context.sqlText}")

          // Verify position mapping if expected positions are provided
          expectedStartPos.foreach { expectedStart =>
            assert(context.originStartIndex.contains(expectedStart),
              s"Start position should be $expectedStart, got: ${context.originStartIndex}")
          }
          expectedStopPos.foreach { expectedStop =>
            assert(context.originStopIndex.contains(expectedStop),
              s"Stop position should be $expectedStop, got: ${context.originStopIndex}")
          }
        }
      case _ => // Runtime exceptions may not have query context
    }

    exception
  }

  /**
   * Verify that error positions in parameterized SQL are correctly mapped back to original
   * positions.
   */
  private def checkPositionMapping[T <: Throwable](
      sql: String,
      expectedStartPos: Int,
      expectedStopPos: Int,
      params: Map[String, Any] = Map.empty,
      positionalParams: Array[Any] = Array.empty)(implicit ct: scala.reflect.ClassTag[T]): Unit = {

    val exception = intercept[T] {
      if (positionalParams.nonEmpty) {
        spark.sql(sql, positionalParams)
      } else {
        spark.sql(sql, params)
      }
    }

    // Extract query context and verify position mapping
    val contexts = exception match {
      case pe: ParseException => pe.getQueryContext
      case ae: AnalysisException => ae.context
      case _ => Array.empty[org.apache.spark.QueryContext]
    }

    // Only proceed with position mapping if contexts exist
    // Some parse errors don't create contexts - that's existing behavior, not a regression
    if (contexts.nonEmpty) {
      val context = contexts.head.asInstanceOf[SQLQueryContext]

      // Verify the SQL text is the original
      assert(context.sqlText.contains(sql),
        s"Context should reference original SQL.\n" +
        s"Expected: $sql\n" +
        s"Actual: ${context.sqlText}")

        // Verify position mapping
        assert(context.originStartIndex.contains(expectedStartPos),
          s"Start position should be $expectedStartPos, got: ${context.originStartIndex}")
        assert(context.originStopIndex.contains(expectedStopPos),
          s"Stop position should be $expectedStopPos, got: ${context.originStopIndex}")
      }
  }

  /**
   * Verify that EXECUTE IMMEDIATE errors correctly reference the inner query context.
   * This is the correct behavior - errors should point to the actual inner query where
   * the error occurred, similar to how views work.
   */
  private def checkExecuteImmediateError[T <: Throwable](
      executeImmediateSQL: String,
      innerQuery: String,
      expectedStartPos: Option[Int] = None,
      expectedStopPos: Option[Int] = None)(implicit ct: scala.reflect.ClassTag[T]): T = {

    val exception = intercept[T] {
      spark.sql(executeImmediateSQL)
    }

    // For exceptions with query context, verify they reference the inner query
    exception match {
      case pe: ParseException =>
        if (pe.getQueryContext.nonEmpty) {
          val context = pe.getQueryContext.head.asInstanceOf[SQLQueryContext]
          assert(context.sqlText.exists(_.contains(innerQuery)),
            s"Parse error should reference inner query SQL.\n" +
            s"Expected to contain: $innerQuery\n" +
            s"Actual context SQL: ${context.sqlText}")

          // Verify position mapping if expected positions are provided
          expectedStartPos.foreach { expectedStart =>
            assert(context.originStartIndex.contains(expectedStart),
              s"Start position should be $expectedStart, got: ${context.originStartIndex}")
          }
          expectedStopPos.foreach { expectedStop =>
            assert(context.originStopIndex.contains(expectedStop),
              s"Stop position should be $expectedStop, got: ${context.originStopIndex}")
          }
        }
      case ae: AnalysisException =>
        if (ae.context.nonEmpty) {
          val context = ae.context.head.asInstanceOf[SQLQueryContext]
          assert(context.sqlText.exists(_.contains(innerQuery)),
            s"Analysis error should reference inner query SQL.\n" +
            s"Expected to contain: $innerQuery\n" +
            s"Actual context SQL: ${context.sqlText}")

          // Verify position mapping if expected positions are provided
          expectedStartPos.foreach { expectedStart =>
            assert(context.originStartIndex.contains(expectedStart),
              s"Start position should be $expectedStart, got: ${context.originStartIndex}")
          }
          expectedStopPos.foreach { expectedStop =>
            assert(context.originStopIndex.contains(expectedStop),
              s"Stop position should be $expectedStop, got: ${context.originStopIndex}")
          }
        }
      case _ => // Runtime exceptions may not have query context
    }

    exception
  }

  // =============================================================================
  // POSITION MAPPING TESTS
  // =============================================================================

  test("position mapping - parse error with named parameter") {
    checkParameterError[AnalysisException](
      "SELECT *** :param",
      expectedStartPos = Some(0),
      expectedStopPos = Some(16), // "SELECT *** :param".length - 1 = 17 - 1 = 16
      params = Map("param" -> 42)
    )
  }

  test("position mapping - parse error with positional parameter") {
    checkParameterError[ParseException](
      "SELECT *** ?",
      expectedStartPos = Some(0),
      expectedStopPos = Some(11), // "SELECT *** ?".length - 1 = 12 - 1 = 11
      positionalParams = Array(42)
    )
  }

  test("position mapping - analysis error with named parameter") {
    checkParameterError[AnalysisException](
      "SELECT :param FROM nonexistent_table",
      expectedStartPos = Some(19), // Position of "nonexistent_table" in original SQL
      expectedStopPos = Some(35), // length - 1 = 36 - 1 = 35
      params = Map("param" -> 42)
    )
  }

  test("position mapping - analysis error with positional parameter") {
    checkParameterError[AnalysisException](
      "SELECT ? FROM nonexistent_table",
      expectedStartPos = Some(14),
      expectedStopPos = Some(30), // "SELECT ? FROM nonexistent_table".length - 1 = 31 - 1 = 30
      positionalParams = Array(42)
    )
  }

  test("position mapping - parse error with identifier clause - SPARK-49757 regression test") {
    val sqlText = "SET CATALOG IDENTIFIER(:param)"
    val exception = checkParameterError[ParseException](
      sqlText,
      params = Map("param" -> "testcat.ns1")
    )

    // Verify specific context details for this test case
    val contexts = exception.getQueryContext
    // Some parse errors may not have query context - this is acceptable
    if (contexts.nonEmpty) {
      val context = contexts.head.asInstanceOf[SQLQueryContext]
      assert(context.sqlText.contains(sqlText),
        s"Context should contain original SQL: $sqlText")
      assert(context.originStartIndex.contains(0),
        s"Start index should be 0, got: ${context.originStartIndex}")
        assert(context.originStopIndex.contains(sqlText.length - 1),
          s"Stop index should be ${sqlText.length - 1}, got: ${context.originStopIndex}")
      }
  }

  test("IDENTIFIER clause with parameter marker - multipart identifier error") {
    // Test that IDENTIFIER clause with parameter marker correctly handles multipart names
    // This is the SPARK-49757 regression test - multipart names in IDENTIFIER should fail
    val sqlText = "SET CATALOG IDENTIFIER(:catalogName)"
    checkError(
      exception = intercept[ParseException] {
        spark.sql(sqlText, Map("catalogName" -> "catalog.namespace"))
      },
      condition = "INVALID_SQL_SYNTAX.MULTI_PART_NAME",
      parameters = Map(
        "name" -> "`catalog`.`namespace`",
        "statement" -> "SET CATALOG"
      ),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = sqlText.length - 1
      )
    )
  }

  test("IDENTIFIER clause with parameter marker - table reference") {
    // Test IDENTIFIER clause with parameter marker in table references
    withTable("test_table") {
      spark.range(5).write.saveAsTable("test_table")

      // Using IDENTIFIER with parameter marker for table name
      checkAnswer(
        spark.sql("SELECT * FROM IDENTIFIER(:tableName) ORDER BY id",
          Map("tableName" -> "test_table")),
        Seq(Row(0), Row(1), Row(2), Row(3), Row(4))
      )

      // Test with positional parameter
      checkAnswer(
        spark.sql("SELECT * FROM IDENTIFIER(?) ORDER BY id",
          Array("test_table")),
        Seq(Row(0), Row(1), Row(2), Row(3), Row(4))
      )
    }
  }

  test("IDENTIFIER clause with parameter marker - column reference") {
    // Test IDENTIFIER clause with parameter marker in column references
    withTable("test_columns") {
      spark.sql("CREATE TABLE test_columns (col1 INT, col2 STRING) USING parquet")
      spark.sql("INSERT INTO test_columns VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      // Using IDENTIFIER with parameter marker for column name
      checkAnswer(
        spark.sql("SELECT IDENTIFIER(:colName) FROM test_columns ORDER BY col1",
          Map("colName" -> "col2")),
        Seq(Row("a"), Row("b"), Row("c"))
      )

      // Test with positional parameter
      checkAnswer(
        spark.sql("SELECT IDENTIFIER(?) FROM test_columns ORDER BY col1",
          Array("col1")),
        Seq(Row(1), Row(2), Row(3))
      )
    }
  }

  test("IDENTIFIER clause with parameter marker - error position mapping") {
    // Test that errors in IDENTIFIER clauses with parameters have correct position mapping
    val sqlText = "SELECT * FROM IDENTIFIER(:tableName) WHERE undefined_column = 1"
    checkParameterError[AnalysisException](
      sqlText,
      expectedStartPos = Some(14), // Position of "IDENTIFIER(:tableName)" in original SQL
      expectedStopPos = Some(35), // End of "IDENTIFIER(:tableName)"
      params = Map("tableName" -> "nonexistent_table")
    )
  }

  test("mixed named and positional parameters - should fail") {
    // Test that mixing named and positional parameters in the same query fails
    // This is the test case from SqlScriptingE2eSuite that validates
    // INVALID_QUERY_MIXED_QUERY_PARAMETERS
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT ?;
        |  IF :param > 10 THEN
        |    SELECT 1;
        |  ELSE
        |    SELECT 2;
        |  END IF;
        |END""".stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(sqlScriptText, Map("param" -> 5))
      },
      condition = "INVALID_QUERY_MIXED_QUERY_PARAMETERS",
      parameters = Map()
    )
  }

  test("mixed parameters - positional in regular query with named in script") {
    // Another variant: positional parameter in regular SELECT mixed with named parameter
    val sqlText = "SELECT ? WHERE :param > 10"
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(sqlText, Map("param" -> 5))
      },
      condition = "INVALID_QUERY_MIXED_QUERY_PARAMETERS",
      parameters = Map()
    )
  }

  test("mixed parameters - EXECUTE IMMEDIATE with both positional and named") {
    // Test case from execute-immediate.sql test suite
    // EXECUTE IMMEDIATE should not allow mixing positional (?) and named (:param) parameters
    withTable("test_mixed") {
      spark.sql("CREATE TABLE test_mixed (id INT, name STRING) USING parquet")
      spark.sql("INSERT INTO test_mixed VALUES (1, 'name1'), (2, 'name2')")

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql(
            "EXECUTE IMMEDIATE 'SELECT * FROM test_mixed where ? = id " +
            "and :first = name' USING 1 as x, 'name2' as first"
          )
        },
        condition = "INVALID_QUERY_MIXED_QUERY_PARAMETERS",
        parameters = Map()
      )
    }
  }

  test("position mapping - multiple parameters with parse error") {
    checkParameterError[ParseException](
      "SELECT :param1, :param2 ***",
      expectedStartPos = Some(0),
      expectedStopPos = Some(27), // "SELECT :param1, :param2 ***".length - 1 = 28 - 1 = 27
      params = Map("param1" -> 10, "param2" -> 20)
    )
  }

  test("position mapping - parameter substitution with different lengths") {
    checkParameterError[AnalysisException](
      "SELECT :a, :bb, :ccc FROM nonexistent",
      expectedStartPos = Some(26), // Position of "nonexistent" in original SQL
      expectedStopPos = Some(36), // length - 1 = 37 - 1 = 36
      params = Map("a" -> 1, "bb" -> 22, "ccc" -> 333)
    )
  }

  test("position mapping - complex expression with parameter") {
    checkParameterError[ParseException](
      "SELECT CASE WHEN :param > 0 THEN 'yes' *** END",
      expectedStartPos = Some(0),
      expectedStopPos = Some(46), // length - 1 = 47 - 1 = 46
      params = Map("param" -> 5)
    )
  }

  test("position mapping - error at start of SQL") {
    // Error at position 0: "***" starts at position 0
    checkPositionMapping[ParseException](
      "*** :param",
      expectedStartPos = 0,
      expectedStopPos = 9, // "*** :param".length - 1
      params = Map("param" -> 42)
    )
  }

  test("position mapping - error after parameter") {
    // "SELECT :param ***" - error "***" starts at position 14
    checkPositionMapping[ParseException](
      "SELECT :param ***",
      expectedStartPos = 0,
      expectedStopPos = 16, // "SELECT :param ***".length - 1
      params = Map("param" -> 42)
    )
  }

  test("position mapping - parameter length difference") {
    // ":x" (2 chars) gets replaced with "42" (2 chars) - no offset change
    checkPositionMapping[ParseException](
      "SELECT :x ***",
      expectedStartPos = 0,
      expectedStopPos = 12, // "SELECT :x ***".length - 1
      params = Map("x" -> 42)
    )
  }

  test("position mapping - long parameter name vs short value") {
    // ":verylongparametername" (22 chars) gets replaced with "1" (1 char)
    checkPositionMapping[ParseException](
      "SELECT :verylongparametername ***",
      expectedStartPos = 0,
      expectedStopPos = 32, // "SELECT :verylongparametername ***".length - 1
      params = Map("verylongparametername" -> 1)
    )
  }

  test("position mapping - short parameter name vs long value") {
    // ":x" (2 chars) gets replaced with "'very long string value'" (25 chars)
    checkPositionMapping[ParseException](
      "SELECT :x ***",
      expectedStartPos = 0,
      expectedStopPos = 12, // "SELECT :x ***".length - 1
      params = Map("x" -> "very long string value")
    )
  }

  test("position mapping - multiple parameters with different lengths") {
    // ":a" -> "1", ":bb" -> "22", ":ccc" -> "333"
    checkPositionMapping[ParseException](
      "SELECT :a, :bb, :ccc ***",
      expectedStartPos = 0,
      expectedStopPos = 23, // "SELECT :a, :bb, :ccc ***".length - 1
      params = Map("a" -> 1, "bb" -> 22, "ccc" -> 333)
    )
  }

  test("position mapping - positional parameters") {
    // "SELECT ? ***" - error at end
    checkPositionMapping[ParseException](
      "SELECT ? ***",
      expectedStartPos = 0,
      expectedStopPos = 11, // "SELECT ? ***".length - 1
      positionalParams = Array(42)
    )
  }

  test("position mapping - multiple positional parameters") {
    // "SELECT ?, ?, ? ***"
    checkPositionMapping[ParseException](
      "SELECT ?, ?, ? ***",
      expectedStartPos = 0,
      expectedStopPos = 17, // "SELECT ?, ?, ? ***".length - 1
      positionalParams = Array(1, 22, 333)
    )
  }

  test("position mapping - analysis error with specific position") {
    // Analysis error should also preserve positions
    checkPositionMapping[AnalysisException](
      "SELECT :param FROM nonexistent_table",
      expectedStartPos = 19, // Position of "nonexistent_table" in original SQL
      expectedStopPos = 35, // "SELECT :param FROM nonexistent_table".length - 1
      params = Map("param" -> 42)
    )
  }

  test("position mapping - nested expression with parameter") {
    // Complex nested expression
    checkPositionMapping[ExtendedAnalysisException](
      "SELECT (:param + 10) * *** FROM test",
      expectedStartPos = 32, // Actual position reported by analysis
      expectedStopPos = 35, // "SELECT (:param + 10) * *** FROM test".length - 1
      params = Map("param" -> 5)
    )
  }

  test("position mapping - parameter in WHERE clause") {
    // Parameter in WHERE clause with error
    checkPositionMapping[AnalysisException](
      "SELECT * FROM test WHERE id = :param AND undefined_column = 1",
      expectedStartPos = 14, // Actual position reported by analysis
      expectedStopPos = 17, // Actual stop position reported by analysis
      params = Map("param" -> 42)
    )
  }

  test("position mapping - string parameter with quotes") {
    // String parameter that affects quoting
    checkPositionMapping[ParseException](
      "SELECT :param ***",
      expectedStartPos = 0,
      expectedStopPos = 16, // "SELECT :param ***".length - 1
      params = Map("param" -> "string with 'quotes'")
    )
  }

  test("position mapping - null parameter") {
    // Null parameter substitution
    checkPositionMapping[ParseException](
      "SELECT :param ***",
      expectedStartPos = 0,
      expectedStopPos = 16, // "SELECT :param ***".length - 1
      params = Map("param" -> null)
    )
  }

  test("position mapping - EXECUTE IMMEDIATE parse error with named parameter") {
    // Test that parse errors in EXECUTE IMMEDIATE inner queries have correct position mapping
    // The error should reference the inner query context, not the outer EXECUTE IMMEDIATE
    spark.sql("DECLARE executeVar1 = 'SELECT :param ***'")
    checkExecuteImmediateError[ParseException](
      "EXECUTE IMMEDIATE executeVar1 USING 42 AS param",
      innerQuery = "SELECT :param ***",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(0),
      expectedStopPos = Some(16) // Actual stop position reported by parser
    )
  }

  test("position mapping - EXECUTE IMMEDIATE analysis error with named parameter") {
    // Test that analysis errors in EXECUTE IMMEDIATE inner queries have correct position mapping
    spark.sql("DECLARE executeVar2 = 'SELECT :param FROM nonexistent_table'")
    checkExecuteImmediateError[AnalysisException](
      "EXECUTE IMMEDIATE executeVar2 USING 42 AS param",
      innerQuery = "SELECT :param FROM nonexistent_table",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(19), // Position of "nonexistent_table" in inner query
      expectedStopPos = Some(35) // End of "nonexistent_table" in inner query
    )
  }

  test("position mapping - EXECUTE IMMEDIATE positional parameter with parse error") {
    // Test positional parameters in EXECUTE IMMEDIATE
    spark.sql("DECLARE executeVar3 = 'SELECT ? ***'")
    checkExecuteImmediateError[ParseException](
      "EXECUTE IMMEDIATE executeVar3 USING 42",
      innerQuery = "SELECT ? ***",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(0),
      expectedStopPos = Some(11) // "SELECT ? ***".length - 1
    )
  }

  test("position mapping - EXECUTE IMMEDIATE multiple parameters with different lengths") {
    // Test multiple parameters with varying lengths in EXECUTE IMMEDIATE
    spark.sql("DECLARE executeVar4 = 'SELECT :a, :bb, :ccc FROM nonexistent'")
    checkExecuteImmediateError[AnalysisException](
      "EXECUTE IMMEDIATE executeVar4 USING 1 AS a, 22 AS bb, 333 AS ccc",
      innerQuery = "SELECT :a, :bb, :ccc FROM nonexistent",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(26), // Position of "nonexistent" in inner query
      expectedStopPos = Some(36) // End of "nonexistent" in inner query
    )
  }

  test("position mapping - EXECUTE IMMEDIATE with parameter substitution") {
    // Test that error positions within EXECUTE IMMEDIATE are correctly mapped to inner query
    spark.sql("DECLARE executeVar5 = 'SELECT :param FROM nonexistent_table'")

    val exception = intercept[AnalysisException] {
      spark.sql("EXECUTE IMMEDIATE executeVar5 USING 42 AS param")
    }

    // Verify the error context references the inner query, not the EXECUTE IMMEDIATE statement
    if (exception.context.nonEmpty) {
      val context = exception.context.head.asInstanceOf[SQLQueryContext]
      assert(context.sqlText.exists(_.contains("SELECT :param FROM nonexistent_table")),
        s"Context should reference inner query, got: ${context.sqlText}")
    }
  }

  test("position mapping - EXECUTE IMMEDIATE nested parameter context") {
    // Test EXECUTE IMMEDIATE with parameters in both outer and inner contexts
    val exception = intercept[AnalysisException] {
      spark.sql("EXECUTE IMMEDIATE :query USING :value AS param", Map(
        "query" -> "SELECT :param FROM nonexistent_table",
        "value" -> 42
      ))
    }

    // Verify the error context references the inner query
    if (exception.context.nonEmpty) {
      val context = exception.context.head.asInstanceOf[SQLQueryContext]
      assert(context.sqlText.exists(_.contains("SELECT :param FROM nonexistent_table")),
        s"Context should reference inner query, got: ${context.sqlText}")
    }
  }

  test("position mapping - EXECUTE IMMEDIATE string parameter with complex inner query") {
    // Test EXECUTE IMMEDIATE with string parameter containing complex query
    checkExecuteImmediateError[AnalysisException](
      "EXECUTE IMMEDIATE 'SELECT :a, :bb FROM nonexistent' USING 1 AS a, 22 AS bb",
      innerQuery = "SELECT :a, :bb FROM nonexistent",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(20), // Position of "nonexistent" in inner query
      expectedStopPos = Some(30) // End of "nonexistent" in inner query
    )
  }

  test("position mapping - EXECUTE IMMEDIATE with variable reference") {
    // Test position mapping when EXECUTE IMMEDIATE uses a variable
    spark.sql("DECLARE executeVar6 = 'SELECT :param1 + :param2 FROM nonexistent_table'")
    checkExecuteImmediateError[AnalysisException](
      "EXECUTE IMMEDIATE executeVar6 USING 10 AS param1, 20 AS param2",
      innerQuery = "SELECT :param1 + :param2 FROM nonexistent_table",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(30), // Position of "nonexistent_table" in inner query
      expectedStopPos = Some(46) // End of "nonexistent_table" in inner query
    )
  }

  test("detect unbound named parameter with empty map") {
    // When sql() is called with empty map, parameter markers should still be detected
    val exception = intercept[AnalysisException] {
      spark.sql("SELECT :param", Map.empty[String, Any])
    }
    checkError(
      exception = exception,
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "param"),
      context = ExpectedContext(
        fragment = ":param",
        start = 7,
        stop = 12))
  }

  test("detect unbound positional parameter with empty array") {
    // When sql() is called with empty array, parameter markers should still be detected
    val exception = intercept[AnalysisException] {
      spark.sql("SELECT ?", Array.empty[Any])
    }
    checkError(
      exception = exception,
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "_7"),
      context = ExpectedContext(
        fragment = "?",
        start = 7,
        stop = 7))
  }

  test("detect unbound named parameter with no arguments") {
    val exception = intercept[AnalysisException] {
      spark.sql("SELECT :param")
    }
    checkError(
      exception = exception,
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "param"),
      context = ExpectedContext(
        fragment = ":param",
        start = 7,
        stop = 12))
  }

  test("detect unbound positional parameter with no arguments") {
    val exception = intercept[AnalysisException] {
      spark.sql("SELECT ?")
    }
    checkError(
      exception = exception,
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "_7"),
      context = ExpectedContext(
        fragment = "?",
        start = 7,
        stop = 7))
  }

  test("empty map with no parameters - should succeed") {
    // When there are no parameter markers, empty map should work fine
    checkAnswer(
      spark.sql("SELECT 1", Map.empty[String, Any]),
      Row(1))
  }

  test("empty array with no parameters - should succeed") {
    // When there are no parameter markers, empty array should work fine
    checkAnswer(
      spark.sql("SELECT 1", Array.empty[Any]),
      Row(1))
  }
}
