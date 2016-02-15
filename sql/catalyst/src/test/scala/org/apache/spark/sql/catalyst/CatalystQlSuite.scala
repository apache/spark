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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.unsafe.types.CalendarInterval

class CatalystQlSuite extends PlanTest {
  val parser = new CatalystQl()

  test("test case insensitive") {
    val result = Project(UnresolvedAlias(Literal(1)):: Nil, OneRowRelation)
    assert(result === parser.parsePlan("seLect 1"))
    assert(result === parser.parsePlan("select 1"))
    assert(result === parser.parsePlan("SELECT 1"))
  }

  test("test NOT operator with comparison operations") {
    val parsed = parser.parsePlan("SELECT NOT TRUE > TRUE")
    val expected = Project(
      UnresolvedAlias(
        Not(
          GreaterThan(Literal(true), Literal(true)))
      ) :: Nil,
      OneRowRelation)
    comparePlans(parsed, expected)
  }

  test("test Union Distinct operator") {
    val parsed1 = parser.parsePlan("SELECT * FROM t0 UNION SELECT * FROM t1")
    val parsed2 = parser.parsePlan("SELECT * FROM t0 UNION DISTINCT SELECT * FROM t1")
    val expected =
      Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
        Subquery("u_1",
          Distinct(
            Union(
              Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
                UnresolvedRelation(TableIdentifier("t0"), None)),
              Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
                UnresolvedRelation(TableIdentifier("t1"), None))))))
    comparePlans(parsed1, expected)
    comparePlans(parsed2, expected)
  }

  test("test Union All operator") {
    val parsed = parser.parsePlan("SELECT * FROM t0 UNION ALL SELECT * FROM t1")
    val expected =
      Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
        Subquery("u_1",
          Union(
            Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
              UnresolvedRelation(TableIdentifier("t0"), None)),
            Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
              UnresolvedRelation(TableIdentifier("t1"), None)))))
    comparePlans(parsed, expected)
  }

  test("support hive interval literal") {
    def checkInterval(sql: String, result: CalendarInterval): Unit = {
      val parsed = parser.parsePlan(sql)
      val expected = Project(
        UnresolvedAlias(
          Literal(result)
        ) :: Nil,
        OneRowRelation)
      comparePlans(parsed, expected)
    }

    def checkYearMonth(lit: String): Unit = {
      checkInterval(
        s"SELECT INTERVAL '$lit' YEAR TO MONTH",
        CalendarInterval.fromYearMonthString(lit))
    }

    def checkDayTime(lit: String): Unit = {
      checkInterval(
        s"SELECT INTERVAL '$lit' DAY TO SECOND",
        CalendarInterval.fromDayTimeString(lit))
    }

    def checkSingleUnit(lit: String, unit: String): Unit = {
      checkInterval(
        s"SELECT INTERVAL '$lit' $unit",
        CalendarInterval.fromSingleUnitString(unit, lit))
    }

    checkYearMonth("123-10")
    checkYearMonth("496-0")
    checkYearMonth("-2-3")
    checkYearMonth("-123-0")

    checkDayTime("99 11:22:33.123456789")
    checkDayTime("-99 11:22:33.123456789")
    checkDayTime("10 9:8:7.123456789")
    checkDayTime("1 0:0:0")
    checkDayTime("-1 0:0:0")
    checkDayTime("1 0:0:1")

    for (unit <- Seq("year", "month", "day", "hour", "minute", "second")) {
      checkSingleUnit("7", unit)
      checkSingleUnit("-7", unit)
      checkSingleUnit("0", unit)
    }

    checkSingleUnit("13.123456789", "second")
    checkSingleUnit("-13.123456789", "second")
  }

  test("support scientific notation") {
    def assertRight(input: String, output: Double): Unit = {
      val parsed = parser.parsePlan("SELECT " + input)
      val expected = Project(
        UnresolvedAlias(
          Literal(output)
        ) :: Nil,
        OneRowRelation)
      comparePlans(parsed, expected)
    }

    assertRight("9.0e1", 90)
    assertRight(".9e+2", 90)
    assertRight("0.9e+2", 90)
    assertRight("900e-1", 90)
    assertRight("900.0E-1", 90)
    assertRight("9.e+1", 90)

    intercept[AnalysisException](parser.parsePlan("SELECT .e3"))
  }

  test("parse expressions") {
    compareExpressions(
      parser.parseExpression("prinln('hello', 'world')"),
      UnresolvedFunction(
        "prinln", Literal("hello") :: Literal("world") :: Nil, false))

    compareExpressions(
      parser.parseExpression("1 + r.r As q"),
      Alias(Add(Literal(1), UnresolvedAttribute("r.r")), "q")())

    compareExpressions(
      parser.parseExpression("1 - f('o', o(bar))"),
      Subtract(Literal(1),
        UnresolvedFunction("f",
          Literal("o") ::
          UnresolvedFunction("o", UnresolvedAttribute("bar") :: Nil, false) ::
          Nil, false)))

    intercept[AnalysisException](parser.parseExpression("1 - f('o', o(bar)) hello * world"))
  }

  test("table identifier") {
    assert(TableIdentifier("q") === parser.parseTableIdentifier("q"))
    assert(TableIdentifier("q", Some("d")) === parser.parseTableIdentifier("d.q"))
    intercept[AnalysisException](parser.parseTableIdentifier(""))
    intercept[AnalysisException](parser.parseTableIdentifier("d.q.g"))
  }

  test("parse union/except/intersect") {
    parser.parsePlan("select * from t1 union all select * from t2")
    parser.parsePlan("select * from t1 union distinct select * from t2")
    parser.parsePlan("select * from t1 union select * from t2")
    parser.parsePlan("select * from t1 except select * from t2")
    parser.parsePlan("select * from t1 intersect select * from t2")
    parser.parsePlan("(select * from t1) union all (select * from t2)")
    parser.parsePlan("(select * from t1) union distinct (select * from t2)")
    parser.parsePlan("(select * from t1) union (select * from t2)")
    parser.parsePlan("select * from ((select * from t1) union (select * from t2)) t")
  }

  test("window function: better support of parentheses") {
    parser.parsePlan("select sum(product + 1) over (partition by ((1) + (product / 2)) " +
      "order by 2) from windowData")
    parser.parsePlan("select sum(product + 1) over (partition by (1 + (product / 2)) " +
      "order by 2) from windowData")
    parser.parsePlan("select sum(product + 1) over (partition by ((product / 2) + 1) " +
      "order by 2) from windowData")

    parser.parsePlan("select sum(product + 1) over (partition by ((product) + (1)) order by 2) " +
      "from windowData")
    parser.parsePlan("select sum(product + 1) over (partition by ((product) + 1) order by 2) " +
      "from windowData")
    parser.parsePlan("select sum(product + 1) over (partition by (product + (1)) order by 2) " +
      "from windowData")
  }
}
