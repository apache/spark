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

package org.apache.spark.sql.collation

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class IndeterminateCollationTestSuite extends QueryTest with SharedSparkSession {

  val testTableName = "tst_table"
  val dataSource = "parquet"

  def withTestTable(testCode: => Unit): Unit = {
    withTable(testTableName) {
      sql(s"""
           |CREATE TABLE $testTableName (
           |  c1 STRING COLLATE UTF8_LCASE,
           |  c2 STRING COLLATE UTF8_BINARY
           |) USING $dataSource
           |""".stripMargin)
      testCode
    }
  }

  def assertIndeterminateCollationInExpressionError(query: => DataFrame): Unit = {
    val exception = intercept[SparkThrowable] {
      query
    }
    assert(exception.getCondition === "INDETERMINATE_COLLATION")
  }

  def assertIndeterminateCollationInSchemaError(columnPaths: String*)(query: => DataFrame): Unit = {
    checkError(
      exception = intercept[SparkThrowable] {
          query
      },
      condition = "INDETERMINATE_COLLATION_IN_SCHEMA",
      parameters = Map("columnPaths" -> columnPaths.mkString(", "))
    )
  }

  test("cannot use indeterminate collation name") {
    checkError(
      intercept[SparkThrowable] {
        sql("SELECT 'a' COLLATE NULL")
      },
      "COLLATION_INVALID_NAME",
      parameters = Map("proposals" -> "nl", "collationName" -> "NULL")
    )

    intercept[SparkThrowable] {
      StringType("NULL")
    }
  }

  test("concat supports indeterminate collation") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")

      checkAnswer(
        sql(s"SELECT c1 || c2 FROM $testTableName"),
        Seq(Row("ab")))

      checkAnswer(
        sql(s"SELECT c1 || c2 as c3 FROM $testTableName"),
        Seq(Row("ab")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || c2) FROM $testTableName"),
        // TODO: probably should be just NULL
        Seq(Row("SYSTEM.BUILTIN.NULL")))
    }
  }

  test("functions that don't support indeterminate collations") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")

      val expressions = Seq(
        "c1 = c2",
        "c1 != c2",
        "substring(c1 || c2, 1, 1)",
        "length(c1 || c2)",
      )

      expressions.foreach { expr =>
        assertIndeterminateCollationInExpressionError {
          sql(s"SELECT $expr FROM $testTableName")
        }
      }
    }
  }

  test("insert works with indeterminate collation") {
    withTestTable {
      sql(s"""
           |INSERT INTO $testTableName
           |SELECT c1 || c2, c2 || c1
           |FROM VALUES ('a', 'b') AS t(c1, c2)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $testTableName"),
        Seq(Row("ab", "ba")))
    }
  }

  test("can't create table as select fails with indeterminate collation") {
    withTestTable {
      assertIndeterminateCollationInSchemaError("concat(c1, c2)") {
        sql(s"""
             |CREATE TABLE t AS
             |SELECT c1 || c2 FROM $testTableName
             |""".stripMargin)
      }

      assertIndeterminateCollationInSchemaError("myCol", "arr.element", "map.value", "struct.f1") {
        sql(s"""
             |CREATE TABLE t
             |USING $dataSource
             |AS SELECT
             |  c1 || c2 AS myCol,
             |  array(c1 || c2) AS arr,
             |  map('a', c1 || c2) AS map,
             |  named_struct('f1', c1 || c2, 'f2', c2) AS struct
             |FROM $testTableName
             |""".stripMargin)
      }
    }
  }

  test("can create a view with indeterminate collation") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")
      sql(s"INSERT INTO $testTableName VALUES ('c', 'd')")

      withView("v") {
        sql(s"""
             |CREATE VIEW v AS
             |SELECT c1 || c2 as col FROM $testTableName
             |""".stripMargin)

        checkAnswer(
          sql("SELECT * FROM v"),
          Seq(Row("ab"), Row("cd")))
      }
    }
  }
}
