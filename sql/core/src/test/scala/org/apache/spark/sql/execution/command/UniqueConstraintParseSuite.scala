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
import org.apache.spark.sql.catalyst.plans.logical.AddConstraint

class UniqueConstraintParseSuite extends ConstraintParseSuiteBase {
  test("Create table with unique constraint - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, UNIQUE (a)) USING parquet"
    val constraint = UniqueConstraint(columns = Seq("a"), tableName = "t")
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with named unique constraint - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, CONSTRAINT uk1 UNIQUE (a)) USING parquet"
    val constraint = UniqueConstraint(
      columns = Seq("a"),
      name = "uk1",
      tableName = "t"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with composite unique constraint - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, UNIQUE (a, b)) USING parquet"
    val constraint = UniqueConstraint(
      columns = Seq("a", "b"),
      tableName = "t"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with unique constraint - column level") {
    val sql = "CREATE TABLE t (a INT UNIQUE, b STRING) USING parquet"
    val constraint = UniqueConstraint(
      columns = Seq("a"),
      tableName = "t"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with named unique constraint - column level") {
    val sql = "CREATE TABLE t (a INT CONSTRAINT uk1 UNIQUE, b STRING) USING parquet"
    val constraint = UniqueConstraint(
      columns = Seq("a"),
      name = "uk1",
      tableName = "t"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with multiple unique constraints") {
    val sql = "CREATE TABLE t (a INT UNIQUE, b STRING, UNIQUE (b)) USING parquet"
    val constraint1 = UniqueConstraint(columns = Seq("a"), tableName = "t")
    val constraint2 = UniqueConstraint(columns = Seq("b"), tableName = "t")
    val constraints = Seq(constraint1, constraint2)
    verifyConstraints(sql, constraints)
  }

  test("Add unique constraint") {
    Seq(("", null), ("CONSTRAINT uk1", "uk1")).foreach { case (constraintName, expectedName) =>
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
          tableName = "c",
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
          tableName = "c",
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
