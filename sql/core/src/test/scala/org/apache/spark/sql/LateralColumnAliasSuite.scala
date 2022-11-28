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
      withSQLConf(SQLConf.LATERAL_COLUMN_ALIAS_ENABLED.key -> lcaEnabled.toString) {
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
}
