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
import org.apache.spark.sql.catalyst.expressions.ForeignKeyConstraint
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.AddConstraint

class ForeignKeyConstraintParseSuite extends ConstraintParseSuiteBase {

  override val validConstraintCharacteristics =
    super.validConstraintCharacteristics ++ notEnforcedConstraintCharacteristics

  test("Create table with foreign key - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING," +
      " FOREIGN KEY (a) REFERENCES parent(id)) USING parquet"
    val constraint = ForeignKeyConstraint(
      tableName = "t",
      childColumns = Seq("a"),
      parentTableId = Seq("parent"),
      parentColumns = Seq("id")
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with named foreign key - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, CONSTRAINT fk1 FOREIGN KEY (a)" +
      " REFERENCES parent(id)) USING parquet"
    val constraint = ForeignKeyConstraint(
      childColumns = Seq("a"),
      parentTableId = Seq("parent"),
      parentColumns = Seq("id"),
      userProvidedName = "fk1",
      tableName = "t"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with foreign key - column level") {
    val sql = "CREATE TABLE t (a INT REFERENCES parent(id), b STRING) USING parquet"
    val constraint = ForeignKeyConstraint(
      tableName = "t",
      childColumns = Seq("a"),
      parentTableId = Seq("parent"),
      parentColumns = Seq("id")
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with named foreign key - column level") {
    val sql = "CREATE TABLE t (a INT CONSTRAINT fk1 REFERENCES parent(id), b STRING) USING parquet"
    val constraint = ForeignKeyConstraint(
      childColumns = Seq("a"),
      parentTableId = Seq("parent"),
      parentColumns = Seq("id"),
      userProvidedName = "fk1",
      tableName = "t"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Replace table with foreign key - table level") {
    val sql = "REPLACE TABLE t (a INT, b STRING," +
      " FOREIGN KEY (a) REFERENCES parent(id)) USING parquet"
    val constraint = ForeignKeyConstraint(
      tableName = "t",
      childColumns = Seq("a"),
      parentTableId = Seq("parent"),
      parentColumns = Seq("id")
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with named foreign key - table level") {
    val sql = "REPLACE TABLE t (a INT, b STRING, CONSTRAINT fk1 FOREIGN KEY (a)" +
      " REFERENCES parent(id)) USING parquet"
    val constraint = ForeignKeyConstraint(
      childColumns = Seq("a"),
      parentTableId = Seq("parent"),
      parentColumns = Seq("id"),
      userProvidedName = "fk1",
      tableName = "t"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with foreign key - column level") {
    val sql = "REPLACE TABLE t (a INT REFERENCES parent(id), b STRING) USING parquet"
    val constraint = ForeignKeyConstraint(
      tableName = "t",
      childColumns = Seq("a"),
      parentTableId = Seq("parent"),
      parentColumns = Seq("id")
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Replace table with named foreign key - column level") {
    val sql = "REPLACE TABLE t (a INT CONSTRAINT fk1 REFERENCES parent(id), b STRING) USING parquet"
    val constraint = ForeignKeyConstraint(
      childColumns = Seq("a"),
      parentTableId = Seq("parent"),
      parentColumns = Seq("id"),
      userProvidedName = "fk1",
      tableName = "t"
    )
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints, isCreateTable = false)
  }

  test("Add foreign key constraint") {
    Seq(
      ("", null),
      ("CONSTRAINT fk1", "fk1")).foreach { case (constraintName, expectedName) =>
      val sql =
        s"""
           |ALTER TABLE orders ADD $constraintName FOREIGN KEY (customer_id)
           |REFERENCES customers (id)
           |""".stripMargin
      val parsed = parsePlan(sql)
      val expected = AddConstraint(
        UnresolvedTable(
          Seq("orders"),
          "ALTER TABLE ... ADD CONSTRAINT"),
        ForeignKeyConstraint(
          userProvidedName = expectedName,
          tableName = "orders",
          childColumns = Seq("customer_id"),
          parentTableId = Seq("customers"),
          parentColumns = Seq("id")
        ))
      comparePlans(parsed, expected)
    }
  }

  test("Add invalid foreign key constraint name") {
    val sql =
      """
        |ALTER TABLE orders ADD CONSTRAINT fk-1 FOREIGN KEY (customer_id)
        |REFERENCES customers (id)
        |""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    checkError(e, "PARSE_SYNTAX_ERROR", "42601", Map("error" -> "'-'", "hint" -> ""))
  }

  test("Add foreign key constraint with empty columns") {
    val sql =
      """
        |ALTER TABLE orders ADD CONSTRAINT fk1 FOREIGN KEY ()
        |REFERENCES customers (id)
        |""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    checkError(e, "PARSE_SYNTAX_ERROR", "42601", Map("error" -> "')'", "hint" -> ""))
  }

  test("Add foreign key constraint with valid characteristic") {
    validConstraintCharacteristics.foreach { case (enforcedStr, relyStr, characteristic) =>
      val sql =
        s"""
           |ALTER TABLE orders ADD CONSTRAINT fk1 FOREIGN KEY (customer_id)
           |REFERENCES customers (id) $enforcedStr $relyStr
           |""".stripMargin
      val parsed = parsePlan(sql)
      val expected = AddConstraint(
        UnresolvedTable(
          Seq("orders"),
          "ALTER TABLE ... ADD CONSTRAINT"),
        ForeignKeyConstraint(
          userProvidedName = "fk1",
          tableName = "orders",
          childColumns = Seq("customer_id"),
          parentTableId = Seq("customers"),
          parentColumns = Seq("id"),
          userProvidedCharacteristic = characteristic
        ))
      comparePlans(parsed, expected)
    }
  }

  test("Add foreign key constraint with invalid characteristic") {
    invalidConstraintCharacteristics.foreach { case (characteristic1, characteristic2) =>
      val sql =
        s"""
           |ALTER TABLE orders ADD CONSTRAINT fk1 FOREIGN KEY (customer_id)
           |REFERENCES customers (id) $characteristic1 $characteristic2
           |""".stripMargin

      val e = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT fk1 FOREIGN KEY (customer_id)\nREFERENCES customers (id) " +
          s"$characteristic1 $characteristic2",
        start = 24,
        stop = 91 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = e,
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for foreign key -- create table with unnamed constraint") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"CREATE TABLE t (id INT REFERENCES parent(id) $characteristic) USING parquet"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"REFERENCES parent(id) $characteristic",
        start = 23,
        stop = 44 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "FOREIGN KEY"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for foreign key -- create table with named constraint") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"CREATE TABLE t (id INT, CONSTRAINT fk1 FOREIGN KEY (id)" +
          s" REFERENCES parent(id) $characteristic) USING parquet"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT fk1 FOREIGN KEY (id) REFERENCES parent(id) $characteristic",
        start = 24,
        stop = 77 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "FOREIGN KEY"),
        queryContext = Array(expectedContext))
    }
  }

  test("ENFORCED is not supported for foreign key -- alter table") {
    enforcedConstraintCharacteristics.foreach { case (c1, c2, _) =>
      val characteristic = if (c2.isEmpty) {
        c1
      } else {
        s"$c1 $c2"
      }
      val sql =
        s"ALTER TABLE a.b.c ADD CONSTRAINT fk1 FOREIGN KEY (id)" +
          s" REFERENCES parent(id) $characteristic"
      val error = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT fk1 FOREIGN KEY (id) REFERENCES parent(id) $characteristic",
        start = 22,
        stop = 75 + characteristic.length
      )
      checkError(
        exception = error,
        condition = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        parameters = Map("characteristic" -> "ENFORCED", "constraintType" -> "FOREIGN KEY"),
        queryContext = Array(expectedContext))
    }
  }
}
