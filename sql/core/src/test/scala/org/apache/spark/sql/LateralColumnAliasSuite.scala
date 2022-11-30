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

  test("temp test") {
    sql(s"SELECT count(name) AS b, b FROM $testTable GROUP BY dept")
    sql(s"SELECT dept AS a, count(name) AS b, a, b FROM $testTable GROUP BY dept")
    sql(s"SELECT avg(salary) AS a, count(name) AS b, a, b, a + b FROM $testTable GROUP BY dept")
    sql(s"SELECT dept, count(name) AS b, dept + b FROM $testTable GROUP BY dept")
    sql(s"SELECT count(bonus), count(salary * 1.5 + 10000 + bonus * 1.0) AS a, a " +
      s"FROM $testTable GROUP BY dept")
  }

  test("Lateral alias in aggregation") {
    // literal as LCA, used in various cases of expressions
//    checkAnswer(
//      sql(
//        s"""
//           |SELECT
//           |  10000 AS baseline_salary,
//           |  baseline_salary * 1.5,
//           |  baseline_salary + dept * 10000,
//           |  baseline_salary + avg(bonus),
//           |  avg(baseline_salary * 1.5),
//           |  avg(baseline_salary * 1.5) + dept * 10000,
//           |  avg(baseline_salary * 1.5) + avg(bonus)
//           |FROM $testTable
//           |GROUP BY dept
//           |ORDER BY dept
//           |""".stripMargin
//      ),
//      Row(10000, 15000.0, 20000, 11100.0, 15000.0, 25000.0, 16100.0) ::
//        Row(10000, 15000.0, 30000, 11250.0, 15000.0, 35000.0, 16250.0) ::
//        Row(10000, 15000.0, 70000, 11200.0, 15000.0, 75000.0, 16200.0) :: Nil
//    )

    // grouping attribute as LCA, used in various cases of expressions
//    checkAnswer(
//      sql(
//        s"""
//           |SELECT
//           |  salary + 1000 AS new_salary,
//           |  new_salary - 1000 AS prev_salary,
//           |  new_salary - salary,
//           |  new_salary - avg(salary),
//           |  avg(new_salary) - 1000,
//           |  avg(new_salary) - salary,
//           |  avg(new_salary) - avg(salary),
//           |  avg(new_salary) - avg(prev_salary)
//           |FROM $testTable
//           |GROUP BY salary
//           |ORDER BY salary
//           |""".stripMargin),
//      Row(10000, 9000, 1000, 1000.0, 9000.0, 1000.0, 1000.0, 1000.0) ::
//        Row(11000, 10000, 1000, 1000.0, 10000.0, 1000.0, 1000.0, 1000.0) ::
//        Row(13000, 12000, 1000, 1000.0, 12000.0, 1000.0, 1000.0, 1000.0) ::
//        Nil
//    )

    // aggregate expression as LCA, used in various cases of expressions
//    checkAnswer(
//      sql(
//        s"""
//           |SELECT
//           |  sum(salary) AS dept_salary_sum,
//           |  sum(bonus) AS dept_bonus_sum,
//           |  dept_salary_sum * 1.5,
//           |  concat(string(dept_salary_sum), ': dept', string(dept)),
//           |  dept_salary_sum + sum(bonus),
//           |  dept_salary_sum + dept_bonus_sum
//           |FROM $testTable
//           |GROUP BY dept
//           |ORDER BY dept
//           |""".stripMargin
//      ),
//      Row(19000, 2200, 28500.0, "19000: dept1", 21200, 21200) ::
//        Row(22000, 2500, 33000.0, "22000: dept2", 24500, 24500) ::
//        Row(12000, 1200, 18000.0, "12000: dept6", 13200, 13200) ::
//        Nil
//    )
    checkAnswer(
      sql(s"SELECT sum(salary) AS s, s + sum(bonus) AS total FROM $testTable"),
      Row(53000, 58900)
    )

    // Doesn't support nested aggregate expressions
    // TODO: add error class and use CheckError
    intercept[AnalysisException] {
      sql(s"SELECT sum(salary) AS a, avg(a) FROM $testTable")
    }

    // chaining
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

    // conflict names with table columns
    checkAnswer(
      sql(
        s"""
           |SELECT
           |  sum(salary) AS salary,
           |  sum(bonus) AS bonus,
           |  avg(salary) AS avg_s,
           |  avg(salary + bonus) AS avg_t,
           |  avg_s + avg_t
           |FROM $testTable
           |GROUP BY dept
           |ORDER BY dept
           |""".stripMargin),
      Row(19000, 2200, 9500.0, 10600.0, 20100.0) ::
        Row(22000, 2500, 11000.0, 12250.0, 23250.0) ::
        Row(12000, 1200, 12000.0, 13200.0, 25200.0) ::
        Nil)
  }

}
