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

import org.apache.spark.{SparkRuntimeException, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
    val exception = intercept[AnalysisException] {
      query
    }
    assert(exception.getCondition === "INDETERMINATE_COLLATION_IN_EXPRESSION")
  }

  def assertRuntimeIndeterminateCollationError(query: => DataFrame): Unit = {
    val exception = intercept[SparkRuntimeException] {
      query.collect()
    }
    assert(exception.getCondition === "INDETERMINATE_COLLATION")
  }

  def assertIndeterminateCollationInSchemaError(columnPaths: String*)(
      query: => DataFrame): Unit = {
    checkError(
      exception = intercept[AnalysisException] {
        query.collect()
      },
      condition = "INDETERMINATE_COLLATION_IN_SCHEMA",
      parameters = Map("columnPaths" -> columnPaths.mkString(", ")))
  }

  test("cannot use indeterminate collation name") {
    checkError(
      intercept[SparkThrowable] {
        sql("SELECT 'a' COLLATE NULL")
      },
      "COLLATION_INVALID_NAME",
      parameters = Map("proposals" -> "nl", "collationName" -> "NULL"))

    checkError(
      intercept[SparkThrowable] {
        sql("SELECT CAST('a' AS STRING COLLATE NULL)")
      },
      "COLLATION_INVALID_NAME",
      parameters = Map("proposals" -> "nl", "collationName" -> "NULL"))

    checkError(
      exception = intercept[SparkThrowable] {
        StringType("NULL")
      },
      condition = "COLLATION_INVALID_NAME",
      parameters = Map("proposals" -> "nl", "collationName" -> "NULL"))
  }

  test("various expressions that support indeterminate collation") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")

      val expressions = Seq(
        "c1 || c2",
        "concat(c1, c2)",
        "concat_ws(' ', c1, c2)",
        "length(c1 || c2)",
        "array(c1 || c2)",
        "map('a', c1 || c2)",
        "named_struct('f1', c1 || c2, 'f2', c2)",
        "repeat(c1 || c2, 2)",
        "elt(1, c1 || c2, c2)",
        "coalesce(c1 || c2, c2)")

      expressions.foreach { expr =>
        sql(s"SELECT $expr FROM $testTableName").collect()
      }

      checkAnswer(sql(s"SELECT COLLATION(c1 || c2) FROM $testTableName"), Seq(Row("null")))
    }
  }

  test("expressions that don't support indeterminate collations and fail in analyzer") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")

      val expressions = Seq(
        "c1 = c2",
        "c1 != c2",
        "c1 > c2",
        "STARTSWITH(c1 || c2, c1)",
        "ENDSWITH(c1 || c2, c2)",
        "UPPER(c1 || c2) = 'AB'",
        "INITCAP(c1 || c2) = 'Ab'",
        "FIND_IN_SET(c1 || c2, 'a,b')",
        "INSTR(c1 || c2, c1)",
        "LOCATE(c1, c1 || c2)")

      expressions.foreach { expr =>
        assertIndeterminateCollationInExpressionError {
          sql(s"SELECT $expr FROM $testTableName")
        }
      }
    }
  }

  test("expressions that don't support indeterminate collation and fail in runtime") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")

      val expressions = Seq("str_to_map(c1 || c2, 'a', 'b')")

      expressions.foreach { expr =>
        assertRuntimeIndeterminateCollationError {
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

      checkAnswer(sql(s"SELECT * FROM $testTableName"), Seq(Row("ab", "ba")))
    }
  }

  test("insert with dataframe api") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")

      val schema = StructType(Seq(
        StructField("c1", StringType),
        StructField("c2", StringType("UNICODE"))
      ))
      val dataframe = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row("a", "b"))), schema
      )

      val indeterminateDf = dataframe.select(
        expr("concat(c1, c2)").alias("c1"),
        expr("concat(c2, c1)").alias("c2")
      )

      indeterminateDf.write.mode("overwrite").insertInto(testTableName)

      checkError(
        exception = intercept[SparkThrowable] {
          indeterminateDf.write.mode("overwrite").saveAsTable(testTableName)
        },
        condition = "INDETERMINATE_COLLATION_IN_SCHEMA",
        parameters = Map("columnPaths" -> "c1, c2"))
    }
  }

  test("create table as select fails with indeterminate collation") {
    withTestTable {
      assertIndeterminateCollationInSchemaError("concat(c1, c2)") {
        sql(s"""
             |CREATE TABLE t AS
             |SELECT c1 || c2 FROM $testTableName
             |""".stripMargin)
      }

      assertIndeterminateCollationInSchemaError("col") {
        sql(s"""
             |CREATE TABLE t AS
             |SELECT concat_ws(', ', c1, c2) as col FROM $testTableName
             |""".stripMargin)
      }

      assertIndeterminateCollationInSchemaError("arr.element", "map.value", "struct.f1")(sql(s"""
             |CREATE TABLE t
             |USING $dataSource
             |AS SELECT
             |  array(c1 || c2) AS arr,
             |  map('a', c1 || c2) AS map,
             |  named_struct('f1', c1 || c2, 'f2', c2) AS struct
             |FROM $testTableName
             |""".stripMargin))
    }
  }

  test("can't create a view with indeterminate collation") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")
      sql(s"INSERT INTO $testTableName VALUES ('c', 'd')")

      withView("v") {
        assertIndeterminateCollationInSchemaError("col") {
          sql(s"""
               |CREATE VIEW v AS
               |SELECT c1 || c2 as col FROM $testTableName
               |""".stripMargin)
        }
      }
    }
  }

  test("can't alter a view with indeterminate collation") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")
      sql(s"INSERT INTO $testTableName VALUES ('c', 'd')")

      withView("v") {
        sql(s"CREATE VIEW v AS SELECT 'a'")

        assertIndeterminateCollationInSchemaError("col") {
          sql(s"""
               |ALTER VIEW v AS
               |SELECT c1 || c2 as col FROM $testTableName
               |""".stripMargin)
        }
      }
    }
  }

  test("can call show on indeterminate collation") {
    withTestTable {
      sql(s"INSERT INTO $testTableName VALUES ('a', 'b')")
      sql(s"INSERT INTO $testTableName VALUES ('c', 'd')")

      sql(s"SELECT c1 || c2 as col FROM $testTableName").show()
    }
  }
}
