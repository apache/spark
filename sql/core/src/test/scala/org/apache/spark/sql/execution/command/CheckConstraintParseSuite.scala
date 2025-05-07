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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedTable}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{AddConstraint, ColumnDefinition, CreateTable, ReplaceTable, UnresolvedTableSpec}
import org.apache.spark.sql.types.StringType

class CheckConstraintParseSuite extends ConstraintParseSuiteBase {
  override val validConstraintCharacteristics =
    super.validConstraintCharacteristics ++ super.enforcedConstraintCharacteristics

  val constraint1 = CheckConstraint(
    child = GreaterThan(UnresolvedAttribute("a"), Literal(0)),
    condition = "a > 0",
    userProvidedName = "c1",
    tableName = "t")
  val constraint2 = CheckConstraint(
    child = EqualTo(UnresolvedAttribute("b"), Literal("foo")),
    condition = "b = 'foo'",
    userProvidedName = "c2",
    tableName = "t")

  val unnamedConstraint = constraint1.withUserProvidedName(null)

  test("Create table with one check constraint - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0)) USING parquet"
    verifyConstraints(sql, Seq(constraint1))
  }

  test("Create table with two check constraints - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0), " +
      "CONSTRAINT c2 CHECK (b = 'foo')) USING parquet"

    verifyConstraints(sql, Seq(constraint1, constraint2))
  }

  test("Create table with valid characteristic - table level") {
    validConstraintCharacteristics.foreach {
      case (enforcedStr, relyStr, characteristic) =>
        val sql = s"CREATE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0) " +
          s"$enforcedStr $relyStr) USING parquet"
        val constraint = constraint1.withUserProvidedCharacteristic(characteristic)
        verifyConstraints(sql, Seq(constraint))
    }
  }

  test("Create table with invalid characteristic - table level") {
    invalidConstraintCharacteristics.foreach { case (characteristic1, characteristic2) =>
      val constraintStr = s"CONSTRAINT c1 CHECK (a > 0) $characteristic1 $characteristic2"
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT c1 CHECK (a > 0) $characteristic1 $characteristic2",
        start = 33,
        stop = 61 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = intercept[ParseException] {
          parsePlan(s"CREATE TABLE t (a INT, b STRING, $constraintStr ) USING parquet")
        },
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }

  test("Create table with one check constraint - column level") {
    val sql = "CREATE TABLE t (a INT CONSTRAINT c1 CHECK (a > 0), b STRING) USING parquet"
    verifyConstraints(sql, Seq(constraint1))
  }

  test("Create table with two check constraints - column level") {
    val sql = "CREATE TABLE t (a INT CONSTRAINT c1 CHECK (a > 0), " +
      "b STRING CONSTRAINT c2 CHECK (b = 'foo')) USING parquet"
    verifyConstraints(sql, Seq(constraint1, constraint2))
  }

  test("Create table with mixed column and table level check constraints") {
    val sql = "CREATE TABLE t (a INT CONSTRAINT c1 CHECK (a > 0), b STRING, " +
      "CONSTRAINT c2 CHECK (b = 'foo')) USING parquet"
    verifyConstraints(sql, Seq(constraint1, constraint2))
  }

  test("Create table with valid characteristic - column level") {
    validConstraintCharacteristics.foreach {
      case (enforcedStr, relyStr, characteristic) =>
        val sql = s"CREATE TABLE t (a INT CONSTRAINT c1 CHECK (a > 0)" +
          s" $enforcedStr $relyStr, b STRING) USING parquet"
        val constraint = constraint1.withUserProvidedCharacteristic(characteristic)
        verifyConstraints(sql, Seq(constraint))
    }
  }

  test("Create table with invalid characteristic - column level") {
    invalidConstraintCharacteristics.foreach { case (characteristic1, characteristic2) =>
      val constraintStr = s"CONSTRAINT c1 CHECK (a > 0) $characteristic1 $characteristic2"
      val sql = s"CREATE TABLE t (a INT $constraintStr, b STRING) USING parquet"
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT c1 CHECK (a > 0) $characteristic1 $characteristic2",
        start = 22,
        stop = 50 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = intercept[ParseException] {
          parsePlan(sql)
        },
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }

  test("Create table with column 'constraint'") {
    val sql = "CREATE TABLE t (constraint STRING) USING parquet"
    val columns = Seq(ColumnDefinition("constraint", StringType))
    val expected = createExpectedPlan(columns, Seq.empty)
    comparePlans(parsePlan(sql), expected)
  }

  test("Replace table with one check constraint - table level") {
    val sql = "REPLACE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0)) USING parquet"
    verifyConstraints(sql, Seq(constraint1), isCreateTable = false)
  }

  test("Replace table with two check constraints - table level") {
    val sql = "REPLACE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0), " +
      "CONSTRAINT c2 CHECK (b = 'foo')) USING parquet"

    verifyConstraints(sql, Seq(constraint1, constraint2), isCreateTable = false)
  }

  test("Replace table with valid characteristic - table level") {
    validConstraintCharacteristics.foreach {
      case (enforcedStr, relyStr, characteristic) =>
        val sql = s"REPLACE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0) " +
          s"$enforcedStr $relyStr) USING parquet"
        val constraint = constraint1.withUserProvidedCharacteristic(characteristic)
        verifyConstraints(sql, Seq(constraint), isCreateTable = false)
    }
  }

  test("Replace table with invalid characteristic") {
    invalidConstraintCharacteristics.foreach { case (characteristic1, characteristic2) =>
      val constraintStr = s"CONSTRAINT c1 CHECK (a > 0) $characteristic1 $characteristic2"
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT c1 CHECK (a > 0) $characteristic1 $characteristic2",
        start = 34,
        stop = 62 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = intercept[ParseException] {
          parsePlan(s"REPLACE TABLE t (a INT, b STRING, $constraintStr ) USING parquet")
        },
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }

  test("Replace table with column 'constraint'") {
    val sql = "REPLACE TABLE t (constraint STRING) USING parquet"
    val columns = Seq(ColumnDefinition("constraint", StringType))
    val expected = createExpectedPlan(columns, Seq.empty, isCreateTable = false)
    comparePlans(parsePlan(sql), expected)
  }

  test("Add check constraint") {
    val sql =
      """
        |ALTER TABLE a.b.t ADD CONSTRAINT c1 CHECK (a > 0)
        |""".stripMargin
    val parsed = parsePlan(sql)
    val expected = AddConstraint(
      UnresolvedTable(
        Seq("a", "b", "t"),
        "ALTER TABLE ... ADD CONSTRAINT"),
      constraint1)
    comparePlans(parsed, expected)
  }

  test("Add invalid check constraint name") {
    val sql =
      """
        |ALTER TABLE a.b.t ADD CONSTRAINT c1-c3 CHECK (d > 0)
        |""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    checkError(e, "INVALID_IDENTIFIER", "42602", Map("ident" -> "c1-c3"))
  }

  test("Add invalid check constraint expression") {
    val sql =
      """
        |ALTER TABLE a.b.t ADD CONSTRAINT c1 CHECK (d >)
        |""".stripMargin
    val msg = intercept[ParseException] {
      parsePlan(sql)
    }.getMessage
    assert(msg.contains("Syntax error at or near ')'"))
  }

  test("Add check constraint with valid characteristic") {
    validConstraintCharacteristics.foreach { case (enforcedStr, relyStr, characteristic) =>
      val sql =
        s"""
           |ALTER TABLE a.b.t ADD CONSTRAINT c1 CHECK (d > 0) $enforcedStr $relyStr
           |""".stripMargin
      val parsed = parsePlan(sql)
      val expected = AddConstraint(
        UnresolvedTable(
          Seq("a", "b", "t"),
          "ALTER TABLE ... ADD CONSTRAINT"),
        CheckConstraint(
          child = GreaterThan(UnresolvedAttribute("d"), Literal(0)),
          condition = "d > 0",
          userProvidedName = "c1",
          tableName = "t",
          userProvidedCharacteristic = characteristic
        ))
      comparePlans(parsed, expected)
    }
  }

  test("Add check constraint with invalid characteristic") {
    invalidConstraintCharacteristics.foreach { case (characteristic1, characteristic2) =>
      val sql =
        s"ALTER TABLE a.b.t ADD CONSTRAINT c1 CHECK (d > 0) $characteristic1 $characteristic2"

      val e = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT c1 CHECK (d > 0) $characteristic1 $characteristic2",
        start = 22,
        stop = 50 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = e,
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }

  test("Create table with unnamed check constraint") {
    Seq(
      "CREATE TABLE a.b.t (a INT, b STRING, CHECK (a > 0))",
      "CREATE TABLE a.b.t (a INT CHECK (a > 0), b STRING)"
    ).foreach { sql =>
      val plan = parsePlan(sql)
      plan match {
        case c: CreateTable =>
          val tableSpec = c.tableSpec.asInstanceOf[UnresolvedTableSpec]
          assert(tableSpec.constraints.size == 1)
          assert(tableSpec.constraints.head == unnamedConstraint)
          assert(tableSpec.constraints.head.name.matches("t_chk_[0-9a-f]{7}"))

        case other =>
          fail(s"Expected CreateTable, but got: $other")
      }
    }
  }

  test("Replace table with unnamed check constraint") {
    Seq(
      "REPLACE TABLE t (a INT, b STRING, CHECK (a > 0))",
      "REPLACE TABLE t (a INT CHECK (a > 0), b STRING)"
    ).foreach { sql =>
      val plan = parsePlan(sql)
      plan match {
        case c: ReplaceTable =>
          val tableSpec = c.tableSpec.asInstanceOf[UnresolvedTableSpec]
          assert(tableSpec.constraints.size == 1)
          assert(tableSpec.constraints.head == unnamedConstraint)
          assert(tableSpec.constraints.head.name.matches("t_chk_[0-9a-f]{7}"))

        case other =>
          fail(s"Expected ReplaceTable, but got: $other")
      }
    }
  }

  test("Add unnamed check constraint") {
    val sql =
      """
        |ALTER TABLE a.b.t ADD CHECK (a > 0)
        |""".stripMargin
    val plan = parsePlan(sql)
    plan match {
      case a: AddConstraint =>
        val table = a.table.asInstanceOf[UnresolvedTable]
        assert(table.multipartIdentifier == Seq("a", "b", "t"))
        assert(a.tableConstraint == unnamedConstraint)
        assert(a.tableConstraint.name.matches("t_chk_[0-9a-f]{7}"))

      case other =>
        fail(s"Expected AddConstraint, but got: $other")
    }
  }

  test("NOT ENFORCED is not supported for CHECK -- table level") {
    notEnforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"""
           |CREATE TABLE a.b.t (a INT, b STRING, CONSTRAINT C1 CHECK (a > 0) $characteristic)
           |""".stripMargin

      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT C1 CHECK (a > 0) $characteristic"
      )

      checkError(
        exception = intercept[ParseException] {
          parsePlan(sql)
        },
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map(
          "characteristic" -> "NOT ENFORCED",
          "constraintType" -> "CHECK"),
        queryContext = Array(expectedContext))
    }
  }

  test("NOT ENFORCED is not supported for CHECK -- column level") {
    notEnforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"""
           |CREATE TABLE a.b.t (a INT CHECK (a > 0) $characteristic, b STRING)
           |""".stripMargin

      val expectedContext = ExpectedContext(
        fragment = s"CHECK (a > 0) $characteristic"
      )

      checkError(
        exception = intercept[ParseException] {
          parsePlan(sql)
        },
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map(
          "characteristic" -> "NOT ENFORCED",
          "constraintType" -> "CHECK"),
        queryContext = Array(expectedContext))
    }
  }

  test("NOT ENFORCED is not supported for CHECK -- ALTER TABLE") {
    notEnforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"""
           |ALTER TABLE a.b.t ADD CONSTRAINT C1 CHECK (a > 0) $characteristic
           |""".stripMargin

      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT C1 CHECK (a > 0) $characteristic"
      )

      checkError(
        exception = intercept[ParseException] {
          parsePlan(sql)
        },
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map(
          "characteristic" -> "NOT ENFORCED",
          "constraintType" -> "CHECK"),
        queryContext = Array(expectedContext))
    }
  }
}
