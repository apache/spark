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
import org.apache.spark.sql.catalyst.expressions.PrimaryKeyConstraint
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.AddConstraint

class PrimaryKeyConstraintParseSuite extends ConstraintParseSuiteBase {
  override val validConstraintCharacteristics =
    super.validConstraintCharacteristics ++ notEnforcedConstraintCharacteristics

  test("Create table with primary key - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, PRIMARY KEY (a)) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a"),
      tableName = "t",
      userProvidedName = null)
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with named primary key - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, CONSTRAINT pk1 PRIMARY KEY (a)) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a"),
      tableName = "t",
      userProvidedName = "pk1"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with composite primary key - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, PRIMARY KEY (a, b)) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a", "b"),
      tableName = "t",
      userProvidedName = null)
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with primary key - column level") {
    val sql = "CREATE TABLE t (a INT PRIMARY KEY, b STRING) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a"),
      tableName = "t",
      userProvidedName = null)
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with named primary key - column level") {
    val sql = "CREATE TABLE t (a INT CONSTRAINT pk1 PRIMARY KEY, b STRING) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a"),
      tableName = "t",
      userProvidedName = "pk1"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with multiple primary keys should fail") {
    val expectedContext = ExpectedContext(
      fragment = "a INT PRIMARY KEY, b STRING, PRIMARY KEY (a, b)",
      start = 16,
      stop = 62
    )
    checkError(
      exception = intercept[ParseException] {
        parsePlan("CREATE TABLE t (a INT PRIMARY KEY, b STRING, PRIMARY KEY (a, b)) USING parquet")
      },
      condition = "MULTIPLE_PRIMARY_KEYS",
      parameters = Map("columns" -> "(a), (a, b)"),
      queryContext = Array(expectedContext))
  }

  test("Replace table with primary key - table level") {
    val sql = "REPLACE TABLE t (a INT, b STRING, PRIMARY KEY (a)) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a"),
      tableName = "t",
      userProvidedName = null)
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with named primary key - table level") {
    val sql = "REPLACE TABLE t (a INT, b STRING, CONSTRAINT pk1 PRIMARY KEY (a)) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a"),
      tableName = "t",
      userProvidedName = "pk1"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with composite primary key - table level") {
    val sql = "REPLACE TABLE t (a INT, b STRING, PRIMARY KEY (a, b)) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a", "b"),
      tableName = "t",
      userProvidedName = null)
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with primary key - column level") {
    val sql = "REPLACE TABLE t (a INT PRIMARY KEY, b STRING) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a"),
      tableName = "t",
      userProvidedName = null)
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with named primary key - column level") {
    val sql = "REPLACE TABLE t (a INT CONSTRAINT pk1 PRIMARY KEY, b STRING) USING parquet"
    val constraint = PrimaryKeyConstraint(
      columns = Seq("a"),
      tableName = "t",
      userProvidedName = "pk1"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with multiple primary keys should fail") {
    val expectedContext = ExpectedContext(
      fragment = "a INT PRIMARY KEY, b STRING, PRIMARY KEY (b)",
      start = 17,
      stop = 60
    )
    checkError(
      exception = intercept[ParseException] {
        parsePlan("REPLACE TABLE t (a INT PRIMARY KEY, b STRING, PRIMARY KEY (b)) USING parquet")
      },
      condition = "MULTIPLE_PRIMARY_KEYS",
      parameters = Map("columns" -> "(a), (b)"),
      queryContext = Array(expectedContext))
  }

  test("Add primary key constraint") {
    Seq(("", null), ("CONSTRAINT pk1", "pk1")).foreach { case (constraintName, expectedName) =>
      val sql =
        s"""
           |ALTER TABLE a.b.c ADD $constraintName PRIMARY KEY (id, name)
           |""".stripMargin
      val parsed = parsePlan(sql)
      val expected = AddConstraint(
        UnresolvedTable(
          Seq("a", "b", "c"),
          "ALTER TABLE ... ADD CONSTRAINT"),
        PrimaryKeyConstraint(
          userProvidedName = expectedName,
          tableName = "c",
          columns = Seq("id", "name")
        ))
      comparePlans(parsed, expected)
    }
  }

  test("Add invalid primary key constraint name") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT pk-1 PRIMARY KEY (id)
        |""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    checkError(e, "PARSE_SYNTAX_ERROR", "42601", Map("error" -> "'-'", "hint" -> ""))
  }

  test("Add primary key constraint with empty columns") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT pk1 PRIMARY KEY ()
        |""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    checkError(e, "PARSE_SYNTAX_ERROR", "42601", Map("error" -> "')'", "hint" -> ""))
  }

  test("Add primary key constraint with valid characteristic") {
    validConstraintCharacteristics.foreach { case (enforcedStr, relyStr, characteristic) =>
      val sql =
        s"""
           |ALTER TABLE a.b.c ADD CONSTRAINT pk1 PRIMARY KEY (id) $enforcedStr $relyStr
           |""".stripMargin
      val parsed = parsePlan(sql)
      val expected = AddConstraint(
        UnresolvedTable(
          Seq("a", "b", "c"),
          "ALTER TABLE ... ADD CONSTRAINT"),
        PrimaryKeyConstraint(
          userProvidedName = "pk1",
          columns = Seq("id"),
          tableName = "c",
          userProvidedCharacteristic = characteristic
        ))
      comparePlans(parsed, expected)
    }
  }

  test("Add primary key constraint with invalid characteristic") {
    invalidConstraintCharacteristics.foreach { case (characteristic1, characteristic2) =>
      val sql =
        s"ALTER TABLE a.b.c ADD CONSTRAINT pk1 PRIMARY KEY (id) $characteristic1 $characteristic2"

      val e = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT pk1 PRIMARY KEY (id) $characteristic1 $characteristic2",
        start = 22,
        stop = 54 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = e,
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for primary key -- create table with unnamed constraint") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"CREATE TABLE t (id INT PRIMARY KEY $characteristic) USING parquet"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"PRIMARY KEY $characteristic",
        start = 23,
        stop = 34 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "PRIMARY KEY"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for primary key -- create table with named constraint") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"CREATE TABLE t (id INT, CONSTRAINT pk1 PRIMARY KEY (id) $characteristic) USING parquet"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT pk1 PRIMARY KEY (id) $characteristic",
        start = 24,
        stop = 55 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "PRIMARY KEY"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for primary key -- alter table") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"ALTER TABLE a.b.c ADD CONSTRAINT pk1 PRIMARY KEY (id) $characteristic"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT pk1 PRIMARY KEY (id) $characteristic",
        start = 22,
        stop = 53 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "PRIMARY KEY"),
        queryContext = Array(expectedContext))
    }
  }
}
