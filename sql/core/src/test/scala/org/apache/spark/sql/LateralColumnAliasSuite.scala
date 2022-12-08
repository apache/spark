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

import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_REFERENCE
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class LateralColumnAliasSuite extends QueryTest with SharedSparkSession {
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

  val lcaEnabled: Boolean = true
  // by default the tests in this suites run with LCA on
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED.key -> lcaEnabled.toString) {
        testFun
      }
    }
  }
  // mark special testcases test both LCA on and off
  protected def testOnAndOff(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*)(testFun)
  }

  private def withLCAOff(f: => Unit): Unit = {
    withSQLConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED.key -> "false") {
      f
    }
  }
  private def withLCAOn(f: => Unit): Unit = {
    withSQLConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED.key -> "true") {
      f
    }
  }

  testOnAndOff("Lateral alias basics - Project") {
    def checkAnswerWhenOnAndExceptionWhenOff(query: String, expectedAnswerLCAOn: Row): Unit = {
      withLCAOn { checkAnswer(sql(query), expectedAnswerLCAOn) }
      withLCAOff {
        assert(intercept[AnalysisException]{ sql(query) }
          .getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION")
      }
    }

    checkAnswerWhenOnAndExceptionWhenOff(
      s"select dept as d, d + 1 as e from $testTable where name = 'amy'",
      Row(1, 2))

    checkAnswerWhenOnAndExceptionWhenOff(
      s"select salary * 2 as new_salary, new_salary + bonus from $testTable where name = 'amy'",
      Row(20000, 21000))
    checkAnswerWhenOnAndExceptionWhenOff(
      s"select salary * 2 as new_salary, new_salary + bonus * 2 as new_income from $testTable" +
        s" where name = 'amy'",
      Row(20000, 22000))

    checkAnswerWhenOnAndExceptionWhenOff(
      "select salary * 2 as new_salary, (new_salary + bonus) * 3 - new_salary * 2 as " +
        s"new_income from $testTable where name = 'amy'",
      Row(20000, 23000))

    // should referring to the previously defined LCA
    checkAnswerWhenOnAndExceptionWhenOff(
      s"SELECT salary * 1.5 AS d, d, 10000 AS d FROM $testTable WHERE name = 'jen'",
      Row(18000, 18000, 10000)
    )
  }

  test("Duplicated lateral alias names - Project") {
    def checkDuplicatedAliasErrorHelper(query: String, parameters: Map[String, String]): Unit = {
      checkError(
        exception = intercept[AnalysisException] {sql(query)},
        errorClass = "AMBIGUOUS_LATERAL_COLUMN_ALIAS",
        sqlState = "42000",
        parameters = parameters
      )
    }

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
      sql(
        "select salary * 2 as salary, (salary + bonus) * 2 as bonus, " +
          s"salary + bonus as prev_income, prev_income + bonus + salary from $testTable" +
          " where name = 'amy'"),
      Row(20000, 22000, 11000, 22000))

    checkAnswer(
      sql(s"SELECT named_struct('joinYear', 2022) AS properties, properties.joinYear " +
        s"FROM $testTable WHERE name = 'amy'"),
      Row(Row(2022), 2019))

    checkAnswer(
      sql(s"SELECT named_struct('name', 'someone') AS $testTable, $testTable.name " +
        s"FROM $testTable WHERE name = 'amy'"),
      Row(Row("someone"), "amy"))
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
    withLCAOff { intercept[AnalysisException] { sql(query4) } } // surprisingly can't run ..
    withLCAOn {
      val analyzedPlan = sql(query4).queryExecution.analyzed
      assert(!analyzedPlan.containsPattern(OUTER_REFERENCE))
      // but running it triggers exception
      // checkAnswer(sql(query4), Range(1, 7).map(Row(_)))
    }
  }
  // TODO: more tests on LCA in subquery

  test("Lateral alias of a complex type - Project") {
    checkAnswer(
      sql("SELECT named_struct('a', 1) AS foo, foo.a + 1 AS bar, bar + 1"),
      Row(Row(1), 2, 3))

    checkAnswer(
      sql("SELECT named_struct('a', named_struct('b', 1)) AS foo, foo.a.b + 1 AS bar"),
      Row(Row(Row(1)), 2)
    )

    checkAnswer(
      sql("SELECT array(1, 2, 3) AS foo, foo[1] AS bar, bar + 1"),
      Row(Seq(1, 2, 3), 2, 3)
    )
    checkAnswer(
      sql("SELECT array(array(1, 2), array(1, 2, 3), array(100)) AS foo, foo[2][0] + 1 AS bar"),
      Row(Seq(Seq(1, 2), Seq(1, 2, 3), Seq(100)), 101)
    )
    checkAnswer(
      sql("SELECT array(named_struct('a', 1), named_struct('a', 2)) AS foo, foo[0].a + 1 AS bar"),
      Row(Seq(Row(1), Row(2)), 2)
    )

    checkAnswer(
      sql("SELECT map('a', 1, 'b', 2) AS foo, foo['b'] AS bar, bar + 1"),
      Row(Map("a" -> 1, "b" -> 2), 2, 3)
    )
  }

  test("Lateral alias reference attribute further be used by upper plan - Project") {
    // this is out of the scope of lateral alias project functionality requirements, but naturally
    // supported by the current design
    checkAnswer(
      sql(s"SELECT properties AS new_properties, new_properties.joinYear AS new_join_year " +
        s"FROM $testTable WHERE dept = 1 ORDER BY new_join_year DESC"),
      Row(Row(2020, "B"), 2020) :: Row(Row(2019, "A"), 2019) :: Nil
    )
  }

  test("Lateral alias chaining - Project") {
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
  }
}
