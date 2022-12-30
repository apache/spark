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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, ExpressionSet}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_REFERENCE
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Lateral column alias base suite with LCA off, extended by LateralColumnAliasSuite with LCA on.
 * Should test behaviors remaining the same no matter LCA conf is on or off.
 */
class LateralColumnAliasSuiteBase extends QueryTest with SharedSparkSession {
  // by default the tests in this suites run with LCA off
  val lcaEnabled: Boolean = false
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
                             (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED.key -> lcaEnabled.toString) {
        testFun
      }
    }
  }

  protected val testTable: String = "employee"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(
      s"""
         |CREATE TABLE $testTable (
         |  dept INTEGER,
         |  name String,
         |  salary INTEGER,
         |  bonus INTEGER,
         |  properties STRUCT<joinYear INTEGER, mostRecentEmployer STRING>)
         |USING orc
         |""".stripMargin)
    sql(
      s"""
         |INSERT INTO $testTable VALUES
         |  (1, 'amy', 10000, 1000, named_struct('joinYear', 2019, 'mostRecentEmployer', 'A')),
         |  (2, 'alex', 12000, 1200, named_struct('joinYear', 2017, 'mostRecentEmployer', 'A')),
         |  (1, 'cathy', 9000, 1200, named_struct('joinYear', 2020, 'mostRecentEmployer', 'B')),
         |  (2, 'david', 10000, 1300, named_struct('joinYear', 2019, 'mostRecentEmployer', 'C')),
         |  (6, 'jen', 12000, 1200, named_struct('joinYear', 2018, 'mostRecentEmployer', 'D'))
         |""".stripMargin)
  }

  override def afterAll(): Unit = {
    try {
      sql(s"DROP TABLE IF EXISTS $testTable")
    } finally {
      super.afterAll()
    }
  }

  protected def withLCAOff(f: => Unit): Unit = {
    withSQLConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED.key -> "false") {
      f
    }
  }
  protected def withLCAOn(f: => Unit): Unit = {
    withSQLConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED.key -> "true") {
      f
    }
  }

  test("Lateral alias conflicts with table column - Project") {
    checkAnswer(
      sql(
        "select salary * 2 as salary, salary * 2 + bonus as " +
          s"new_income from $testTable where name = 'amy'"),
      Row(20000, 21000))

    checkAnswer(
      sql(
        "select salary * 2 as salary, (salary + bonus) * 3 - (salary + bonus) as " +
          s"new_income from $testTable where name = 'amy'"),
      Row(20000, 22000))

    checkAnswer(
      sql(s"SELECT named_struct('joinYear', 2022) AS properties, properties.joinYear " +
        s"FROM $testTable WHERE name = 'amy'"),
      Row(Row(2022), 2019))

    checkAnswer(
      sql(s"SELECT named_struct('name', 'someone') AS $testTable, $testTable.name " +
        s"FROM $testTable WHERE name = 'amy'"),
      Row(Row("someone"), "amy"))

    // CTE table
    checkAnswer(
      sql(
        s"""
           |WITH temp_table(x, y) AS (SELECT 1, 2)
           |SELECT 100 AS x, x + 1
           |FROM temp_table
           |""".stripMargin
      ),
      Row(100, 2))
  }

  test("Lateral alias conflicts with table column - Aggregate") {
    checkAnswer(
      sql(
        s"""
           |SELECT
           |  sum(salary) AS salary,
           |  sum(bonus) AS bonus,
           |  avg(salary) AS avg_s,
           |  avg(salary + bonus) AS avg_t
           |FROM $testTable GROUP BY dept ORDER BY dept
           |""".stripMargin),
      Row(19000, 2200, 9500.0, 10600.0) ::
        Row(22000, 2500, 11000.0, 12250.0) ::
        Row(12000, 1200, 12000.0, 13200.0) ::
        Nil)

    // TODO: how does it correctly resolve to the right dept in SORT?
    checkAnswer(
      sql(s"SELECT avg(bonus) AS dept, dept, avg(salary) " +
        s"FROM $testTable GROUP BY dept ORDER BY dept"),
      Row(1100, 1, 9500.0) :: Row(1250, 2, 11000) :: Row(1200, 6, 12000) :: Nil
    )

    checkAnswer(
      sql("SELECT named_struct('joinYear', 2022) AS properties, min(properties.joinYear) " +
        s"FROM $testTable GROUP BY dept ORDER BY dept"),
      Row(Row(2022), 2019) :: Row(Row(2022), 2017) :: Row(Row(2022), 2018) :: Nil)

    checkAnswer(
      sql(s"SELECT named_struct('salary', 20000) AS $testTable, avg($testTable.salary) " +
        s"FROM $testTable GROUP BY dept ORDER BY dept"),
      Row(Row(20000), 9500) :: Row(Row(20000), 11000) :: Row(Row(20000), 12000) :: Nil)

    // CTE table
    checkAnswer(
      sql(
        s"""
           |WITH temp_table(x, y) AS (SELECT 1, 2)
           |SELECT 100 AS x, x + 1
           |FROM temp_table
           |GROUP BY x
           |""".stripMargin),
      Row(100, 2))
  }
}

/**
 * Lateral column alias base with LCA on.
 */
class LateralColumnAliasSuite extends LateralColumnAliasSuiteBase {
  // by default the tests in this suites run with LCA on
  override val lcaEnabled: Boolean = true

  // mark special testcases test both LCA on and off
  protected def testOnAndOff(testName: String, testTags: Tag*)(testFun: => Any)
                            (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*)(testFun)
  }

  private def checkDuplicatedAliasErrorHelper(
      query: String, parameters: Map[String, String]): Unit = {
    checkError(
      exception = intercept[AnalysisException] {sql(query)},
      errorClass = "AMBIGUOUS_LATERAL_COLUMN_ALIAS",
      sqlState = "42000",
      parameters = parameters
    )
  }

  private def checkAnswerWhenOnAndExceptionWhenOff(
      query: String, expectedAnswerLCAOn: Seq[Row]): Unit = {
    withLCAOn { checkAnswer(sql(query), expectedAnswerLCAOn) }
    withLCAOff {
      assert(intercept[AnalysisException]{ sql(query) }
        .getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION")
    }
  }

  testOnAndOff("Lateral alias basics - Project") {
    checkAnswerWhenOnAndExceptionWhenOff(
      s"select dept as d, d + 1 as e from $testTable where name = 'amy'",
      Row(1, 2) :: Nil)

    checkAnswerWhenOnAndExceptionWhenOff(
      s"select salary * 2 as new_salary, new_salary + bonus from $testTable where name = 'amy'",
      Row(20000, 21000) :: Nil)
    checkAnswerWhenOnAndExceptionWhenOff(
      s"select salary * 2 as new_salary, new_salary + bonus * 2 as new_income from $testTable" +
        s" where name = 'amy'",
      Row(20000, 22000) :: Nil)

    checkAnswerWhenOnAndExceptionWhenOff(
      "select salary * 2 as new_salary, (new_salary + bonus) * 3 - new_salary * 2 as " +
        s"new_income from $testTable where name = 'amy'",
      Row(20000, 23000) :: Nil)

    // should referring to the previously defined LCA
    checkAnswerWhenOnAndExceptionWhenOff(
      s"SELECT salary * 1.5 AS d, d, 10000 AS d FROM $testTable WHERE name = 'jen'",
      Row(18000, 18000, 10000) :: Nil)

    // LCA and conflicted table column mixed
    checkAnswerWhenOnAndExceptionWhenOff(
      "select salary * 2 as salary, (salary + bonus) * 2 as bonus, " +
        s"salary + bonus as prev_income, prev_income + bonus + salary from $testTable" +
        " where name = 'amy'",
      Row(20000, 22000, 11000, 22000) :: Nil)
  }

  testOnAndOff("Lateral alias basics - Aggregate") {
    // doesn't support lca used in aggregation functions
    withLCAOn(
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT 10000 AS lca, count(lca) FROM $testTable GROUP BY dept")
        },
        errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_AGGREGATE_FUNC",
        sqlState = "0A000",
        parameters = Map(
          "lca" -> "`lca`",
          "aggFunc" -> "\"count(lateralAliasReference(lca))\""
        )))
    withLCAOn(
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT dept AS lca, avg(lca) FROM $testTable GROUP BY dept")
        },
        errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_AGGREGATE_FUNC",
        sqlState = "0A000",
        parameters = Map(
          "lca" -> "`lca`",
          "aggFunc" -> "\"avg(lateralAliasReference(lca))\""
        )))
    // doesn't support nested aggregate expressions
    withLCAOn(
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT sum(salary) AS a, avg(a) FROM $testTable")
        },
        errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_AGGREGATE_FUNC",
        sqlState = "0A000",
        parameters = Map(
          "lca" -> "`a`",
          "aggFunc" -> "\"avg(lateralAliasReference(a))\""
        )))

    // literal as LCA, used in various cases of expressions
    checkAnswerWhenOnAndExceptionWhenOff(
        s"""
           |SELECT
           |  10000 AS baseline_salary,
           |  baseline_salary * 1.5,
           |  baseline_salary + dept * 10000,
           |  baseline_salary + avg(bonus)
           |FROM $testTable
           |GROUP BY dept
           |ORDER BY dept
           |""".stripMargin,
      Row(10000, 15000.0, 20000, 11100.0) ::
        Row(10000, 15000.0, 30000, 11250.0) ::
        Row(10000, 15000.0, 70000, 11200.0) :: Nil
    )

    // grouping attribute as LCA, used in various cases of expressions
    checkAnswerWhenOnAndExceptionWhenOff(
        s"""
           |SELECT
           |  salary + 1000 AS new_salary,
           |  new_salary - 1000 AS prev_salary,
           |  new_salary - salary,
           |  new_salary - avg(salary)
           |FROM $testTable
           |GROUP BY salary
           |ORDER BY salary
           |""".stripMargin,
      Row(10000, 9000, 1000, 1000.0) ::
        Row(11000, 10000, 1000, 1000.0) ::
        Row(13000, 12000, 1000, 1000.0) :: Nil
    )

    // aggregate expression as LCA, used in various cases of expressions
    checkAnswerWhenOnAndExceptionWhenOff(
        s"""
           |SELECT
           |  sum(salary) AS dept_salary_sum,
           |  sum(bonus) AS dept_bonus_sum,
           |  dept_salary_sum * 1.5,
           |  concat(string(dept_salary_sum), ': dept', string(dept)),
           |  dept_salary_sum + sum(bonus),
           |  dept_salary_sum + dept_bonus_sum,
           |  avg(salary * 1.5 + 10000 + bonus * 1.0) AS avg_total,
           |  avg_total
           |FROM $testTable
           |GROUP BY dept
           |ORDER BY dept
           |""".stripMargin,
      Row(19000, 2200, 28500.0, "19000: dept1", 21200, 21200, 25350, 25350) ::
        Row(22000, 2500, 33000.0, "22000: dept2", 24500, 24500, 27750, 27750) ::
        Row(12000, 1200, 18000.0, "12000: dept6", 13200, 13200, 29200, 29200) ::
        Nil
    )
    checkAnswerWhenOnAndExceptionWhenOff(
      s"SELECT sum(salary) AS s, s + sum(bonus) AS total FROM $testTable",
      Row(53000, 58900) :: Nil
    )

    // grouping expression are correctly recognized and pushed down
    checkAnswer(
      sql(
        s"""
           |SELECT dept AS a, dept + 10 AS b, avg(salary) + dept, avg(salary) AS c,
           |       c + dept, avg(salary + dept), count(dept)
           |FROM $testTable GROUP BY dept ORDER BY dept
           |""".stripMargin),
      Row(1, 11, 9501, 9500, 9501, 9501, 2) ::
        Row(2, 12, 11002, 11000, 11002, 11002, 2) ::
        Row(6, 16, 12006, 12000, 12006, 12006, 1) :: Nil)

    // two grouping expressions
    checkAnswer(
      sql(
        s"""
           |SELECT dept + salary, avg(salary) + dept, avg(bonus) AS c, c + salary + dept,
           |       avg(bonus) + salary
           |FROM $testTable GROUP BY dept, salary  HAVING dept = 2 ORDER BY dept, salary
           |""".stripMargin
      ),
      Row(10002, 10002, 1300, 11302, 11300) :: Row(12002, 12002, 1200, 13202, 13200) :: Nil
    )

    // LCA and conflicted table column mixed
    checkAnswerWhenOnAndExceptionWhenOff(
      s"""
         |SELECT
         |  sum(salary) AS salary,
         |  sum(bonus) AS bonus,
         |  avg(salary) AS avg_s,
         |  avg(salary + bonus) AS avg_t,
         |  avg_s + avg_t
         |FROM $testTable GROUP BY dept ORDER BY dept
         |""".stripMargin,
      Row(19000, 2200, 9500.0, 10600.0, 20100.0) ::
        Row(22000, 2500, 11000.0, 12250.0, 23250.0) ::
        Row(12000, 1200, 12000.0, 13200.0, 25200.0) :: Nil)
  }

  test("Duplicated lateral alias names - Project") {
    // Has duplicated names but not referenced is fine
    checkAnswer(
      sql(s"SELECT salary AS d, bonus AS d FROM $testTable WHERE name = 'jen'"),
      Row(12000, 1200)
    )
    checkAnswer(
      sql(s"SELECT salary AS d, d, 10000 AS d FROM $testTable WHERE name = 'jen'"),
      Row(12000, 12000, 10000)
    )
    checkAnswer(
      sql(s"SELECT salary * 1.5 AS d, d, 10000 AS d FROM $testTable WHERE name = 'jen'"),
      Row(18000, 18000, 10000)
    )
    checkAnswer(
      sql(s"SELECT salary + 1000 AS new_salary, new_salary * 1.0 AS new_salary " +
        s"FROM $testTable WHERE name = 'jen'"),
      Row(13000, 13000.0))

    // Referencing duplicated names raises error
    checkDuplicatedAliasErrorHelper(
      s"SELECT salary * 1.5 AS d, d, 10000 AS d, d + 1 FROM $testTable",
      parameters = Map("name" -> "`d`", "n" -> "2")
    )
    checkDuplicatedAliasErrorHelper(
      s"SELECT 10000 AS d, d * 1.0, salary * 1.5 AS d, d FROM $testTable",
      parameters = Map("name" -> "`d`", "n" -> "2")
    )
    checkDuplicatedAliasErrorHelper(
      s"SELECT salary AS d, d + 1 AS d, d + 1 AS d FROM $testTable",
      parameters = Map("name" -> "`d`", "n" -> "2")
    )
    checkDuplicatedAliasErrorHelper(
      s"SELECT salary * 1.5 AS d, d, bonus * 1.5 AS d, d + d FROM $testTable",
      parameters = Map("name" -> "`d`", "n" -> "2")
    )

    checkAnswer(
      sql(
        s"""
           |SELECT salary * 1.5 AS salary, salary, 10000 AS salary, salary
           |FROM $testTable
           |WHERE name = 'jen'
           |""".stripMargin),
      Row(18000, 12000, 10000, 12000)
    )
  }

  test("Duplicated lateral alias names - Aggregate") {
    // Has duplicated names but not referenced is fine
    checkAnswer(
      sql(s"SELECT dept AS d, name AS d FROM $testTable GROUP BY dept, name ORDER BY dept, name"),
      Row(1, "amy") :: Row(1, "cathy") :: Row(2, "alex") :: Row(2, "david") :: Row(6, "jen") :: Nil
    )
    checkAnswer(
      sql(s"SELECT dept AS d, d, 10 AS d FROM $testTable GROUP BY dept ORDER BY dept"),
      Row(1, 1, 10) :: Row(2, 2, 10) :: Row(6, 6, 10) :: Nil
    )
    checkAnswer(
      sql(s"SELECT sum(salary * 1.5) AS d, d, 10 AS d FROM $testTable GROUP BY dept ORDER BY dept"),
      Row(28500, 28500, 10) :: Row(33000, 33000, 10) :: Row(18000, 18000, 10) :: Nil
    )
    checkAnswer(
      sql(
        s"""
           |SELECT sum(salary * 1.5) AS d, d, d + sum(bonus) AS d
           |FROM $testTable
           |GROUP BY dept
           |ORDER BY dept
           |""".stripMargin),
      Row(28500, 28500, 30700) :: Row(33000, 33000, 35500) :: Row(18000, 18000, 19200) :: Nil
    )

    // Referencing duplicated names raises error
    checkDuplicatedAliasErrorHelper(
      s"SELECT dept * 2.0 AS d, d, 10000 AS d, d + 1 FROM $testTable GROUP BY dept",
      parameters = Map("name" -> "`d`", "n" -> "2")
    )
    checkDuplicatedAliasErrorHelper(
      s"SELECT 10000 AS d, d * 1.0, dept * 2.0 AS d, d FROM $testTable GROUP BY dept",
      parameters = Map("name" -> "`d`", "n" -> "2")
    )
    checkDuplicatedAliasErrorHelper(
      s"SELECT avg(salary) AS d, d * 1.0, avg(bonus * 1.5) AS d, d FROM $testTable GROUP BY dept",
      parameters = Map("name" -> "`d`", "n" -> "2")
    )
    checkDuplicatedAliasErrorHelper(
      s"SELECT dept AS d, d + 1 AS d, d + 1 AS d FROM $testTable GROUP BY dept",
      parameters = Map("name" -> "`d`", "n" -> "2")
    )

    checkAnswer(
      sql(s"""
             |SELECT avg(salary * 1.5) AS salary, sum(salary), dept AS salary, avg(salary)
             |FROM $testTable
             |GROUP BY dept
             |HAVING dept = 6
             |""".stripMargin),
      Row(18000, 12000, 6, 12000)
    )
  }

  testOnAndOff("Lateral alias conflicts with OuterReference - Project") {
    // an attribute can both be resolved as LCA and OuterReference
    val query1 =
      s"""
         |SELECT *
         |FROM range(1, 7)
         |WHERE (
         |  SELECT id2
         |  FROM (SELECT 1 AS id, id + 1 AS id2)) > 5
         |ORDER BY id
         |""".stripMargin
    withLCAOff { checkAnswer(sql(query1), Row(5) :: Row(6) :: Nil) }
    withLCAOn { checkAnswer(sql(query1), Seq.empty) }

    // an attribute can only be resolved as LCA
    val query2 =
      s"""
         |SELECT *
         |FROM range(1, 7)
         |WHERE (
         |  SELECT id2
         |  FROM (SELECT 1 AS id1, id1 + 1 AS id2)) > 5
         |""".stripMargin
    withLCAOff {
      assert(intercept[AnalysisException] { sql(query2) }
        .getErrorClass == "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION")
    }
    withLCAOn { checkAnswer(sql(query2), Seq.empty) }

    // an attribute should only be resolved as OuterReference
    val query3 =
      s"""
         |SELECT *
         |FROM range(1, 7) outer_table
         |WHERE (
         |  SELECT id2
         |  FROM (SELECT 1 AS id, outer_table.id + 1 AS id2)) > 5
         |""".stripMargin
    withLCAOff { checkAnswer(sql(query3), Row(5) :: Row(6) :: Nil) }
    withLCAOn { checkAnswer(sql(query3), Row(5) :: Row(6) :: Nil) }

    // a bit complex subquery that the id + 1 is first wrapped with OuterReference
    // test if lca rule strips the OuterReference and resolves to lateral alias
    val query4 =
    s"""
       |SELECT *
       |FROM range(1, 7)
       |WHERE (
       |  SELECT id2
       |  FROM (SELECT dept * 2.0 AS id, id + 1 AS id2 FROM $testTable)) > 5
       |ORDER BY id
       |""".stripMargin
    withLCAOff { intercept[AnalysisException] { sql(query4) } }
    withLCAOn {
      val analyzedPlan = sql(query4).queryExecution.analyzed
      assert(!analyzedPlan.containsPattern(OUTER_REFERENCE))
      // but running it triggers exception
      // checkAnswer(sql(query4), Range(1, 7).map(Row(_)))
    }
  }
  // TODO: more tests on LCA in subquery

  test("Lateral alias conflicts with OuterReference - Aggregate") {
    // test if lca rule strips the OuterReference and resolves to lateral alias
    val query =
      s"""
         |SELECT *
         |FROM range(1, 7)
         |WHERE (
         |  SELECT id2
         |  FROM (SELECT avg(salary * 1.0) AS id, id + 1 AS id2 FROM $testTable GROUP BY dept)) > 5
         |""".stripMargin
    val analyzedPlan = sql(query).queryExecution.analyzed
    assert(!analyzedPlan.containsPattern(OUTER_REFERENCE))
  }

  test("Lateral alias of a complex type") {
    // test both Project and Aggregate
    // TODO(anchovyu): re-enable aggregate tests when fixed the having issue
    val querySuffixes = Seq(""/* , s"FROM $testTable GROUP BY dept HAVING dept = 6" */)
    querySuffixes.foreach { querySuffix =>
      checkAnswer(
        sql(s"SELECT named_struct('a', 1) AS foo, foo.a + 1 AS bar, bar + 1 $querySuffix"),
        Row(Row(1), 2, 3))
      checkAnswer(
        sql("SELECT named_struct('a', named_struct('b', 1)) AS foo, foo.a.b + 1 AS bar " +
          s"$querySuffix"),
        Row(Row(Row(1)), 2))

      checkAnswer(
        sql(s"SELECT array(1, 2, 3) AS foo, foo[1] AS bar, bar + 1 $querySuffix"),
        Row(Seq(1, 2, 3), 2, 3))
    checkAnswer(
      sql("SELECT array(array(1, 2), array(1, 2, 3), array(100)) AS foo, foo[2][0] + 1 AS bar " +
          s"$querySuffix"),
        Row(Seq(Seq(1, 2), Seq(1, 2, 3), Seq(100)), 101))
    checkAnswer(
      sql("SELECT array(named_struct('a', 1), named_struct('a', 2)) AS foo, foo[0].a + 1 AS bar" +
          s" $querySuffix"),
        Row(Seq(Row(1), Row(2)), 2))

      checkAnswer(
        sql(s"SELECT map('a', 1, 'b', 2) AS foo, foo['b'] AS bar, bar + 1 $querySuffix"),
        Row(Map("a" -> 1, "b" -> 2), 2, 3))
    }

    checkAnswer(
      sql("SELECT named_struct('s', salary * 1.0) AS foo, foo.s + 1 AS bar, bar + 1 " +
        s"FROM $testTable WHERE dept = 1 ORDER BY name"),
      Row(Row(10000), 10001, 10002) :: Row(Row(9000), 9001, 9002) :: Nil)

    checkAnswer(
      sql(s"SELECT properties AS foo, foo.joinYear AS bar, bar + 1 " +
        s"FROM $testTable GROUP BY properties HAVING properties.mostRecentEmployer = 'B'"),
      Row(Row(2020, "B"), 2020, 2021))

    checkAnswer(
      sql(s"SELECT named_struct('avg_salary', avg(salary)) AS foo, foo.avg_salary + 1 AS bar " +
        s"FROM $testTable GROUP BY dept ORDER BY dept"),
      Row(Row(9500), 9501) :: Row(Row(11000), 11001) :: Row(Row(12000), 12001) :: Nil
    )
  }

  test("Lateral alias reference attribute further be used by upper plan") {
    // underlying this is not in the scope of lateral alias project but things already supported
    checkAnswer(
      sql(s"SELECT properties AS new_properties, new_properties.joinYear AS new_join_year " +
        s"FROM $testTable WHERE dept = 1 ORDER BY new_join_year DESC"),
      Row(Row(2020, "B"), 2020) :: Row(Row(2019, "A"), 2019) :: Nil
    )

    checkAnswer(
      sql(s"SELECT avg(bonus) AS avg_bonus, avg_bonus * 1.0 AS new_avg_bonus, avg(salary) " +
        s"FROM $testTable GROUP BY dept ORDER BY new_avg_bonus"),
      Row(1100, 1100, 9500.0) :: Row(1200, 1200, 12000) :: Row(1250, 1250, 11000) :: Nil
    )
  }

  test("Lateral alias chaining") {
    // Project
    checkAnswer(
      sql(
        s"""
           |SELECT bonus * 1.1 AS new_bonus, salary + new_bonus AS new_base,
           |       new_base * 1.1 AS new_total, new_total - new_base AS r,
           |       new_total - r
           |FROM $testTable WHERE name = 'cathy'
           |""".stripMargin),
      Row(1320, 10320, 11352, 1032, 10320)
    )

    checkAnswer(
      sql("SELECT 1 AS a, a + 1 AS b, b - 1, b + 1 AS c, c + 1 AS d, d - a AS e, e + 1"),
      Row(1, 2, 1, 3, 4, 3, 4)
    )

    // Aggregate
    checkAnswer(
      sql(
        s"""
           |SELECT
           |  dept,
           |  sum(salary) AS salary_sum,
           |  salary_sum + sum(bonus) AS salary_total,
           |  salary_total * 1.5 AS new_total,
           |  new_total - salary_sum
           |FROM $testTable
           |GROUP BY dept
           |ORDER BY dept
           |""".stripMargin),
      Row(1, 19000, 21200, 31800.0, 12800.0) ::
        Row(2, 22000, 24500, 36750.0, 14750.0) ::
        Row(6, 12000, 13200, 19800.0, 7800.0) :: Nil
    )
  }

  test("non-deterministic expression as LCA is evaluated only once") {
    val querySuffixes = Seq(s"FROM $testTable", s"FROM $testTable GROUP BY dept")
    querySuffixes.foreach { querySuffix =>
      sql(s"SELECT dept, rand(0) AS r, r $querySuffix").collect().toSeq.foreach { row =>
        assert(QueryTest.compare(row(1), row(2)))
      }
      sql(s"SELECT dept + rand(0) AS r, r $querySuffix").collect().toSeq.foreach { row =>
        assert(QueryTest.compare(row(0), row(1)))
      }
    }
    sql(s"SELECT avg(salary) + rand(0) AS r, r ${querySuffixes(1)}").collect().toSeq.foreach {
      row => assert(QueryTest.compare(row(0), row(1)))
    }
  }

  test("Case insensitive lateral column alias") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkAnswer(
        sql(s"SELECT salary AS new_salary, New_Salary + 1 FROM $testTable WHERE name = 'jen'"),
        Row(12000, 12001))
      checkAnswer(
        sql(
          s"""
             |SELECT avg(salary) AS AVG_SALARY, avg_salary + avg(bonus)
             |FROM $testTable
             |GROUP BY dept
             |HAVING dept = 1
             |""".stripMargin),
        Row(9500, 10600))
    }
  }

  test("Attribute cannot be resolved by LCA remain unresolved") {
    assert(intercept[AnalysisException] {
      sql(s"SELECT dept AS d, d AS new_dept, new_dep + 1 AS newer_dept FROM $testTable")
    }.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION")

    assert(intercept[AnalysisException] {
      sql(s"SELECT count(name) AS cnt, cnt + 1, count(unresovled) FROM $testTable GROUP BY dept")
    }.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION")

    assert(intercept[AnalysisException] {
      sql(s"SELECT * FROM range(1, 7) WHERE (" +
        s"SELECT id2 FROM (SELECT 1 AS id, other_id + 1 AS id2)) > 5")
    }.getErrorClass == "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION")
  }

  test("Pushed-down aggregateExpressions should have no duplicates") {
    val query = s"""
       |SELECT dept, avg(salary) AS a, a + avg(bonus), dept + 1,
       |       concat(string(dept), string(avg(bonus))), avg(salary)
       |FROM $testTable
       |GROUP BY dept
       |HAVING dept = 2
       |""".stripMargin
    val analyzedPlan = sql(query).queryExecution.analyzed
    analyzedPlan.collect {
      case Aggregate(_, aggregateExpressions, _) =>
        val extracted = aggregateExpressions.collect {
          case Alias(child, _) => child
          case a: Attribute => a
        }
        val expressionSet = ExpressionSet(extracted)
        assert(
          extracted.size == expressionSet.size,
          "The pushed-down aggregateExpressions in Aggregate should have no duplicates " +
            s"after extracted from Alias. Current aggregateExpressions: $aggregateExpressions")
    }
  }

  test("Aggregate expressions not eligible to lift up, throws same error as inline") {
    def checkSameMissingAggregationError(q1: String, q2: String, expressionParam: String): Unit = {
      Seq(q1, q2).foreach { query =>
        val e = intercept[AnalysisException] { sql(query) }
        assert(e.getErrorClass == "MISSING_AGGREGATION")
        assert(e.messageParameters.get("expression").exists(_ == expressionParam))
      }
    }

    val suffix = s"FROM $testTable GROUP BY dept"
    checkSameMissingAggregationError(
      s"SELECT dept AS a, dept, salary $suffix",
      s"SELECT dept AS a, a,    salary $suffix",
      "\"salary\"")
    checkSameMissingAggregationError(
      s"SELECT dept AS a, dept + salary $suffix",
      s"SELECT dept AS a, a    + salary $suffix",
      "\"salary\"")
    checkSameMissingAggregationError(
      s"SELECT avg(salary) AS a, avg(salary) + bonus $suffix",
      s"SELECT avg(salary) AS a, a           + bonus $suffix",
      "\"bonus\"")
    checkSameMissingAggregationError(
      s"SELECT dept AS a, dept, avg(salary) + bonus + 10 $suffix",
      s"SELECT dept AS a, a,    avg(salary) + bonus + 10 $suffix",
      "\"bonus\"")
    checkSameMissingAggregationError(
      s"SELECT avg(salary) AS a, avg(salary), dept FROM $testTable GROUP BY dept + 10",
      s"SELECT avg(salary) AS a, a,           dept FROM $testTable GROUP BY dept + 10",
      "\"dept\"")
    checkSameMissingAggregationError(
      s"SELECT avg(salary) AS a, avg(salary) + dept + 10 FROM $testTable GROUP BY dept + 10",
      s"SELECT avg(salary) AS a, a           + dept + 10 FROM $testTable GROUP BY dept + 10",
      "\"dept\"")
    Seq(
      s"SELECT dept AS a, dept, " +
        s"(SELECT count(col) FROM VALUES (1), (2) AS data(col) WHERE col = dept) $suffix",
      s"SELECT dept AS a, a, " +
        s"(SELECT count(col) FROM VALUES (1), (2) AS data(col) WHERE col = dept) $suffix"
    ).foreach { query =>
      val e = intercept[AnalysisException] { sql(query) }
      assert(e.getErrorClass == "_LEGACY_ERROR_TEMP_2423") }

    // one exception: no longer throws NESTED_AGGREGATE_FUNCTION but UNSUPPORTED_FEATURE
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"SELECT avg(salary) AS a, avg(a) FROM $testTable GROUP BY dept")
      },
      errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_AGGREGATE_FUNC",
      sqlState = "0A000",
      parameters = Map("lca" -> "`a`", "aggFunc" -> "\"avg(lateralAliasReference(a))\"")
    )
  }

  test("Leaf expression as aggregate expressions should be eligible to lift up") {
    // literal
    sql(s"select 1, avg(salary) as m, m + 1 from $testTable group by dept")
      .queryExecution.assertAnalyzed
    // leaf expression current_date, now and etc
    sql(s"select current_date(), max(salary) as m, m + 1 from $testTable group by dept")
      .queryExecution.assertAnalyzed
    sql("select dateadd(month, 5, current_date()), min(salary) as m, m + 1 as n " +
      s"from $testTable group by dept").queryExecution.assertAnalyzed
    sql(s"select now() as n, dateadd(day, -1, n) from $testTable group by name")
      .queryExecution.assertAnalyzed
  }

  test("Aggregate expressions containing no aggregate or grouping expressions still resolves") {
    // Note these queries are without HAVING, otherwise during resolution the grouping or aggregate
    // functions in having will be added to Aggregate by rule ResolveAggregateFunctions
    checkAnswer(
      sql("SELECT named_struct('a', named_struct('b', 1)) AS foo, foo.a.b + 1 AS bar " +
        s"FROM $testTable GROUP BY dept"),
      Row(Row(Row(1)), 2) :: Row(Row(Row(1)), 2) :: Row(Row(Row(1)), 2) :: Nil)

    checkAnswer(sql(s"select 1 as a, a + 1 from $testTable group by dept"),
      Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil)
  }
}
