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

import org.apache.spark.sql.catalyst.analysis.UnresolvedTable
import org.apache.spark.sql.catalyst.expressions.UniqueConstraint
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{AddConstraint, CreateTable, ReplaceTable, UnresolvedTableSpec}

class UniqueConstraintParseSuite extends ConstraintParseSuiteBase {

  test("Create table with unnamed unique constraint") {
    Seq(
      "CREATE TABLE t (a INT, b STRING, UNIQUE (a))",
      "CREATE TABLE t (a INT UNIQUE, b STRING)"
    ).foreach { sql =>
      val plan = parsePlan(sql)
      plan match {
        case c: CreateTable =>
          val tableSpec = c.tableSpec.asInstanceOf[UnresolvedTableSpec]
          assert(tableSpec.constraints.size == 1)
          assert(tableSpec.constraints.head.isInstanceOf[UniqueConstraint])
          assert(tableSpec.constraints.head.name.matches("t_uniq_[0-9a-f]{7}"))

        case other =>
          fail(s"Expected CreateTable, but got: $other")
      }
    }
  }

  test("Create table with composite unique constraint - table level") {
    Seq(
      "CREATE TABLE t (a INT, b STRING, UNIQUE (a, b))",
      "CREATE TABLE t (a INT, b STRING, UNIQUE (b, a))"
    ).foreach { sql =>
      val plan = parsePlan(sql)
      plan match {
        case c: CreateTable =>
          val tableSpec = c.tableSpec.asInstanceOf[UnresolvedTableSpec]
          assert(tableSpec.constraints.size == 1)
          assert(tableSpec.constraints.head.isInstanceOf[UniqueConstraint])
          assert(tableSpec.constraints.head.name.matches("t_uniq_[0-9a-f]{7}"))

        case other =>
          fail(s"Expected CreateTable, but got: $other")
      }
    }
  }

  test("Create table with multiple unique constraints") {
    Seq(
      "CREATE TABLE t (a INT UNIQUE, b STRING, UNIQUE (b))",
      "CREATE TABLE t (a INT, UNIQUE (a), b STRING UNIQUE)"
    ).foreach { sql =>
      val plan = parsePlan(sql)
      plan match {
        case c: CreateTable =>
          val tableSpec = c.tableSpec.asInstanceOf[UnresolvedTableSpec]
          assert(tableSpec.constraints.size == 2)
          assert(tableSpec.constraints.head.isInstanceOf[UniqueConstraint])
          assert(tableSpec.constraints.head.name.matches("t_uniq_[0-9a-f]{7}"))
          assert(tableSpec.constraints.last.isInstanceOf[UniqueConstraint])
          assert(tableSpec.constraints.last.name.matches("t_uniq_[0-9a-f]{7}"))

        case other =>
          fail(s"Expected CreateTable, but got: $other")
      }
    }
  }

  test("Add unnamed unique constraint") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD UNIQUE (d)
        |""".stripMargin
    val plan = parsePlan(sql)
    plan match {
      case a: AddConstraint =>
        val table = a.table.asInstanceOf[UnresolvedTable]
        assert(table.multipartIdentifier == Seq("a", "b", "c"))
        assert(a.tableConstraint.isInstanceOf[UniqueConstraint])
        assert(a.tableConstraint.name.matches("c_uniq_[0-9a-f]{7}"))

      case other =>
        fail(s"Expected AddConstraint, but got: $other")
    }
  }

  test("Create table with named unique constraint - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, CONSTRAINT uk1 UNIQUE (a)) USING parquet"
    val constraint = UniqueConstraint(
      columns = Seq("a"),
      name = "uk1"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with named unique constraint - column level") {
    val sql = "CREATE TABLE t (a INT CONSTRAINT uk1 UNIQUE, b STRING) USING parquet"
    val constraint = UniqueConstraint(
      columns = Seq("a"),
      name = "uk1"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Replace table with unnamed unique constraint") {
    Seq(
      "REPLACE TABLE t (a INT, b STRING, UNIQUE (a))",
      "REPLACE TABLE t (a INT UNIQUE, b STRING)"
    ).foreach { sql =>
      val plan = parsePlan(sql)
      plan match {
        case c: ReplaceTable =>
          val tableSpec = c.tableSpec.asInstanceOf[UnresolvedTableSpec]
          assert(tableSpec.constraints.size == 1)
          assert(tableSpec.constraints.head.isInstanceOf[UniqueConstraint])
          assert(tableSpec.constraints.head.name.matches("t_uniq_[0-9a-f]{7}"))

        case other =>
          fail(s"Expected ReplaceTable, but got: $other")
      }
    }
  }

  test("Replace table with composite unique constraint - table level") {
    Seq(
      "REPLACE TABLE t (a INT, b STRING, UNIQUE (a, b))",
      "REPLACE TABLE t (a INT, b STRING, UNIQUE (b, a))"
    ).foreach { sql =>
      val plan = parsePlan(sql)
      plan match {
        case c: ReplaceTable =>
          val tableSpec = c.tableSpec.asInstanceOf[UnresolvedTableSpec]
          assert(tableSpec.constraints.size == 1)
          assert(tableSpec.constraints.head.isInstanceOf[UniqueConstraint])
          assert(tableSpec.constraints.head.name.matches("t_uniq_[0-9a-f]{7}"))

        case other =>
          fail(s"Expected ReplaceTable, but got: $other")
      }
    }
  }

  test("Replace table with multiple unique constraints") {
    Seq(
      "REPLACE TABLE t (a INT UNIQUE, b STRING, UNIQUE (b))",
      "REPLACE TABLE t (a INT, UNIQUE (a), b STRING UNIQUE)"
    ).foreach { sql =>
      val plan = parsePlan(sql)
      plan match {
        case c: ReplaceTable =>
          val tableSpec = c.tableSpec.asInstanceOf[UnresolvedTableSpec]
          assert(tableSpec.constraints.size == 2)
          assert(tableSpec.constraints.head.isInstanceOf[UniqueConstraint])
          assert(tableSpec.constraints.head.name.matches("t_uniq_[0-9a-f]{7}"))
          assert(tableSpec.constraints.last.isInstanceOf[UniqueConstraint])
          assert(tableSpec.constraints.last.name.matches("t_uniq_[0-9a-f]{7}"))

        case other =>
          fail(s"Expected ReplaceTable, but got: $other")
      }
    }
  }

  test("Replace table with named unique constraint - table level") {
    val sql = "REPLACE TABLE t (a INT, b STRING, CONSTRAINT uk1 UNIQUE (a)) USING parquet"
    val constraint = UniqueConstraint(
      columns = Seq("a"),
      name = "uk1"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with named unique constraint - column level") {
    val sql = "REPLACE TABLE t (a INT CONSTRAINT uk1 UNIQUE, b STRING) USING parquet"
    val constraint = UniqueConstraint(
      columns = Seq("a"),
      name = "uk1"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Add unique constraint") {
    Seq(
      ("consTrainT abcdEF", "abcdEF"),
      ("CONSTRAINT uk1", "uk1")).foreach { case (constraintName, expectedName) =>
      val sql =
        s"""
           |ALTER TABLE a.b.c ADD $constraintName UNIQUE (email, username)
           |""".stripMargin
      val parsed = parsePlan(sql)
      val expected = AddConstraint(
        UnresolvedTable(
          Seq("a", "b", "c"),
          "ALTER TABLE ... ADD CONSTRAINT"),
        UniqueConstraint(
          name = expectedName,
          columns = Seq("email", "username")
        ))
      comparePlans(parsed, expected)
    }
  }

  test("Add invalid unique constraint name") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT uk-1 UNIQUE (email)
        |""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    checkError(e, "PARSE_SYNTAX_ERROR", "42601", Map("error" -> "'-'", "hint" -> ""))
  }

  test("Add unique constraint with empty columns") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT uk1 UNIQUE ()
        |""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    checkError(e, "PARSE_SYNTAX_ERROR", "42601", Map("error" -> "')'", "hint" -> ""))
  }

  test("Add unique constraint with valid characteristic") {
    validConstraintCharacteristics.foreach { case (enforcedStr, relyStr, characteristic) =>
      val sql =
        s"""
           |ALTER TABLE a.b.c ADD CONSTRAINT uk1 UNIQUE (email) $enforcedStr $relyStr
           |""".stripMargin
      val parsed = parsePlan(sql)
      val expected = AddConstraint(
        UnresolvedTable(
          Seq("a", "b", "c"),
          "ALTER TABLE ... ADD CONSTRAINT"),
        UniqueConstraint(
          name = "uk1",
          columns = Seq("email"),
          characteristic = characteristic
        ))
      comparePlans(parsed, expected)
    }
  }

  test("Add unique constraint with invalid characteristic") {
    invalidConstraintCharacteristics.foreach { case (characteristic1, characteristic2) =>
      val sql =
        s"ALTER TABLE a.b.c ADD CONSTRAINT uk1 UNIQUE (email) $characteristic1 $characteristic2"

      val e = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT uk1 UNIQUE (email) $characteristic1 $characteristic2",
        start = 22,
        stop = 52 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = e,
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for unique -- create table with unnamed constraint") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"CREATE TABLE t (id INT UNIQUE $characteristic) USING parquet"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"UNIQUE $characteristic",
        start = 23,
        stop = 29 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "UNIQUE"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for unique -- create table with named constraint") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"CREATE TABLE t (id INT CONSTRAINT uk1 UNIQUE $characteristic) USING parquet"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT uk1 UNIQUE $characteristic",
        start = 23,
        stop = 44 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "UNIQUE"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for unique -- alter table") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"ALTER TABLE a.b.c ADD CONSTRAINT uni UNIQUE (id) $characteristic"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT uni UNIQUE (id) $characteristic",
        start = 22,
        stop = 48 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "UNIQUE"),
        queryContext = Array(expectedContext))
    }
  }
}
