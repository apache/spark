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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions.{Literal, GreaterThan, Not, Attribute}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project, LogicalPlan, Command}
import org.apache.spark.unsafe.types.CalendarInterval

private[sql] case class TestCommand(cmd: String) extends LogicalPlan with Command {
  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}

private[sql] class SuperLongKeywordTestParser extends AbstractSparkSQLParser {
  protected val EXECUTE = Keyword("THISISASUPERLONGKEYWORDTEST")

  override protected lazy val start: Parser[LogicalPlan] = set

  private lazy val set: Parser[LogicalPlan] =
    EXECUTE ~> ident ^^ {
      case fileName => TestCommand(fileName)
    }
}

private[sql] class CaseInsensitiveTestParser extends AbstractSparkSQLParser {
  protected val EXECUTE = Keyword("EXECUTE")

  override protected lazy val start: Parser[LogicalPlan] = set

  private lazy val set: Parser[LogicalPlan] =
    EXECUTE ~> ident ^^ {
      case fileName => TestCommand(fileName)
    }
}

class SqlParserSuite extends PlanTest {

  test("test long keyword") {
    val parser = new SuperLongKeywordTestParser
    assert(TestCommand("NotRealCommand") ===
      parser.parse("ThisIsASuperLongKeyWordTest NotRealCommand"))
  }

  test("test case insensitive") {
    val parser = new CaseInsensitiveTestParser
    assert(TestCommand("NotRealCommand") === parser.parse("EXECUTE NotRealCommand"))
    assert(TestCommand("NotRealCommand") === parser.parse("execute NotRealCommand"))
    assert(TestCommand("NotRealCommand") === parser.parse("exEcute NotRealCommand"))
  }

  test("test NOT operator with comparison operations") {
    val parsed = SqlParser.parse("SELECT NOT TRUE > TRUE")
    val expected = Project(
      UnresolvedAlias(
        Not(
          GreaterThan(Literal(true), Literal(true)))
      ) :: Nil,
      OneRowRelation)
    comparePlans(parsed, expected)
  }

  test("support hive interval literal") {
    def checkInterval(sql: String, result: CalendarInterval): Unit = {
      val parsed = SqlParser.parse(sql)
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
      val parsed = SqlParser.parse("SELECT " + input)
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

    intercept[RuntimeException](SqlParser.parse("SELECT .e3"))
  }
}
