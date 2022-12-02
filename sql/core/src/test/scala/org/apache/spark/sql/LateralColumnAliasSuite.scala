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
    sql(s"CREATE TABLE $testTable (dept INTEGER, name String, salary INTEGER, bonus INTEGER) " +
      s"using orc")
    sql(
      s"""
         |INSERT INTO $testTable VALUES
         |  (1, 'amy', 10000, 1000),
         |  (2, 'alex', 12000, 1200),
         |  (1, 'cathy', 9000, 1200),
         |  (2, 'david', 10000, 1300),
         |  (6, 'jen', 12000, 1200)
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
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED.key -> lcaEnabled.toString) {
        testFun
      }
    }
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

  test("Lateral alias in project") {
    checkAnswer(sql(s"select dept as d, d + 1 as e from $testTable where name = 'amy'"),
      Row(1, 2))

    checkAnswer(
      sql(
        s"select salary * 2 as new_salary, new_salary + bonus from $testTable where name = 'amy'"),
      Row(20000, 21000))
    checkAnswer(
      sql(
        s"select salary * 2 as new_salary, new_salary + bonus * 2 as new_income from $testTable" +
          s" where name = 'amy'"),
      Row(20000, 22000))

    checkAnswer(
      sql(
        "select salary * 2 as new_salary, (new_salary + bonus) * 3 - new_salary * 2 as " +
          s"new_income from $testTable where name = 'amy'"),
      Row(20000, 23000))

    // When the lateral alias conflicts with the table column, it should resolved as the table
    // column
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

    // Corner cases for resolution order
    checkAnswer(
      sql(s"SELECT salary * 1.5 AS d, d, 10000 AS d FROM $testTable WHERE name = 'jen'"),
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

  test("Lateral alias conflicts with OuterReference - Project") {
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
  // TODO: LCA in subquery
}
