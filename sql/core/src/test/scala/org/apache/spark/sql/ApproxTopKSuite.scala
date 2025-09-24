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

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException}
import org.apache.spark.sql.test.SharedSparkSession


class ApproxTopKSuite extends QueryTest with SharedSparkSession {

  val itemsWithTopK: Seq[(String, Seq[Row])] = Seq(
    ("0, 0, 1, 1, 2, 3, 4, 4",
      Seq(Row(0, 2), Row(4, 2), Row(1, 2), Row(2, 1), Row(3, 1))), // Int
    ("'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd'",
      Seq(Row("c", 4), Row("d", 2), Row("a", 1), Row("b", 1))), // String
    ("(true), (true), (false), (true), (true), (false), (false)",
      Seq(Row(true, 4), Row(false, 3))), // Boolean
    ("cast(0 AS BYTE), cast(0 AS BYTE), cast(0 AS BYTE), cast(0 AS BYTE), " +
      "cast(1 AS BYTE), cast(1 AS BYTE), cast(1 AS BYTE), cast(2 AS BYTE)",
      Seq(Row(0, 4), Row(1, 3), Row(2, 1))), // Byte
    ("cast(0 AS SHORT), cast(0 AS SHORT), cast(0 AS SHORT), cast(0 AS SHORT), " +
      "cast(1 AS SHORT), cast(1 AS SHORT), cast(1 AS SHORT), cast(2 AS SHORT)",
      Seq(Row(0, 4), Row(1, 3), Row(2, 1))), // Short
    ("cast(0 AS LONG), cast(0 AS LONG), cast(0 AS LONG), cast(0 AS LONG), " +
      "cast(1 AS LONG), cast(1 AS LONG), cast(1 AS LONG), cast(2 AS LONG)",
      Seq(Row(0, 4), Row(1, 3), Row(2, 1))), // Long
    ("cast(0.0 AS FLOAT), cast(0.0 AS FLOAT), cast(0.0 AS FLOAT), cast(0.0 AS FLOAT), " +
      "cast(1.0 AS FLOAT), cast(1.0 AS FLOAT), cast(1.0 AS FLOAT), cast(2.0 AS FLOAT)",
      Seq(Row(0.0, 4), Row(1.0, 3), Row(2.0, 1))), // Float
    ("cast(0.0 AS DOUBLE), cast(0.0 AS DOUBLE), cast(0.0 AS DOUBLE), cast(0.0 AS DOUBLE), " +
      "cast(1.0 AS DOUBLE), cast(1.0 AS DOUBLE), cast(1.0 AS DOUBLE), cast(2.0 AS DOUBLE)",
      Seq(Row(0.0, 4), Row(1.0, 3), Row(2.0, 1))), // Double
    ("DATE'2025-01-01', DATE'2025-01-01', DATE'2025-01-01', DATE'2025-01-01', " +
      "DATE'2025-01-02', DATE'2025-01-02', DATE'2025-01-02', DATE'2025-01-03'",
      Seq(Row(Date.valueOf("2025-01-01"), 4), Row(Date.valueOf("2025-01-02"), 3),
        Row(Date.valueOf("2025-01-03"), 1))), // Date
    ("TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-01-01 00:00:00', " +
      "TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-01-02 00:00:00'",
      Seq(Row(Timestamp.valueOf("2025-01-01 00:00:00"), 3),
        Row(Timestamp.valueOf("2025-01-02 00:00:00"), 1))), // Timestamp
    ("TIMESTAMP_NTZ'2025-01-01 00:00:00', TIMESTAMP_NTZ'2025-01-01 00:00:00', " +
      "TIMESTAMP_NTZ'2025-01-01 00:00:00', TIMESTAMP_NTZ'2025-01-02 00:00:00'",
      Seq(Row(LocalDateTime.of(2025, 1, 1, 0, 0), 3),
        Row(LocalDateTime.of(2025, 1, 2, 0, 0), 1))), // Timestamp_ntz
    ("CAST(0.0 AS DECIMAL(4, 1)), CAST(0.0 AS DECIMAL(4, 1)), " +
      "CAST(0.0 AS DECIMAL(4, 1)), CAST(1.0 AS DECIMAL(4, 1)), " +
      "CAST(1.0 AS DECIMAL(4, 1)), CAST(2.0 AS DECIMAL(4, 1))",
      Seq(Row(new java.math.BigDecimal("0.0"), 3),
        Row(new java.math.BigDecimal("1.0"), 2),
        Row(new java.math.BigDecimal("2.0"), 1))), // Decimal(4, 1)
    ("CAST(0.0 AS DECIMAL(10, 2)), CAST(0.0 AS DECIMAL(10, 2)), " +
      "CAST(0.0 AS DECIMAL(10, 2)), CAST(1.0 AS DECIMAL(10, 2)), " +
      "CAST(1.0 AS DECIMAL(10, 2)), CAST(2.0 AS DECIMAL(10, 2))",
      Seq(Row(new java.math.BigDecimal("0.00"), 3),
        Row(new java.math.BigDecimal("1.00"), 2),
        Row(new java.math.BigDecimal("2.00"), 1))), // Decimal(10, 2)
    ("CAST(0.0 AS DECIMAL(20, 3)), CAST(0.0 AS DECIMAL(20, 3)), " +
      "CAST(0.0 AS DECIMAL(20, 3)), CAST(1.0 AS DECIMAL(20, 3)), " +
      "CAST(1.0 AS DECIMAL(20, 3)), CAST(2.0 AS DECIMAL(20, 3))",
      Seq(Row(new java.math.BigDecimal("0.000"), 3),
        Row(new java.math.BigDecimal("1.000"), 2),
        Row(new java.math.BigDecimal("2.000"), 1))), // Decimal(20, 3)
    ("(0.0), (0.0), (0.0), (0.0), (1.0), (1.0), (1.0), (2.0)",
      Seq(Row(new java.math.BigDecimal("0.0"), 4),
        Row(new java.math.BigDecimal("1.0"), 3),
        Row(new java.math.BigDecimal("2.0"), 1))) // Decimal default
  )

  /////////////////////////////////
  // approx_top_k tests
  /////////////////////////////////

  test("SPARK-52515: test of 1 parameter") {
    val res = sql(
      "SELECT approx_top_k(expr) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);"
    )
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2), Row(1, 2), Row(2, 1), Row(3, 1))))
  }

  test("SPARK-52515: test of 2 parameter") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    checkAnswer(res, Row(Seq(Row("c", 4), Row("d", 2))))
  }

  test("SPARK-52515: test of 3 parameter") {
    val res = sql(
      "SELECT approx_top_k(expr, 10, 100) FROM VALUES (0), (1), (1), (2), (2), (2) AS tab(expr);"
    )
    checkAnswer(res, Row(Seq(Row(2, 3), Row(1, 2), Row(0, 1))))
  }

  gridTest("SPARK-52515: test of different types")(itemsWithTopK) {
    case (input, expected) =>
      val res = sql(s"SELECT approx_top_k(expr) FROM VALUES $input AS tab(expr);")
      checkAnswer(res, Row(expected))
  }

  test("SPARK-52515: invalid k value null") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, NULL) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NULL_ARG",
      parameters = Map("argName" -> "`k`")
    )
  }

  test("SPARK-52515: invalid k value < 1") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, 0) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NON_POSITIVE_ARG",
      parameters = Map("argName" -> "`k`", "argValue" -> "0")
    )
  }

  test("SPARK-52515: invalid k value > Int.MaxValue") {
    withSQLConf("spark.sql.ansi.enabled" -> true.toString) {
      val k: Long = Int.MaxValue + 1L
      checkError(
        exception = intercept[SparkArithmeticException] {
          sql(s"SELECT approx_top_k(expr, $k) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
        },
        condition = "CAST_OVERFLOW",
        parameters = Map(
          "value" -> (k.toString + "L"),
          "sourceType" -> "\"BIGINT\"",
          "targetType" -> "\"INT\"",
          "ansiConfig" -> "\"spark.sql.ansi.enabled\""
        )
      )
    }
  }

  test("SPARK-52515: invalid maxItemsTracked value null") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, 5, NULL) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NULL_ARG",
      parameters = Map("argName" -> "`maxItemsTracked`")
    )
  }

  test("SPARK-52515: invalid maxItemsTracked value < 1") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, 5, 0) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NON_POSITIVE_ARG",
      parameters = Map("argName" -> "`maxItemsTracked`", "argValue" -> "0")
    )
  }

  test("SPARK-52515: invalid maxItemsTracked > 1000000") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, 10, 1000001) FROM VALUES (0), (1) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_MAX_ITEMS_TRACKED_EXCEEDS_LIMIT",
      parameters = Map("maxItemsTracked" -> "1000001", "limit" -> "1000000")
    )
  }

  test("SPARK-52515: invalid maxItemsTracked < k") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, 10, 5) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_MAX_ITEMS_TRACKED_LESS_THAN_K",
      parameters = Map("maxItemsTracked" -> "5", "k" -> "10")
    )
  }

  test("SPARK-52515: does not count NULL values") {
    val res = sql(
      "SELECT approx_top_k(expr, 2)" +
        "FROM VALUES 'a', 'a', 'b', 'b', 'b', NULL, NULL, NULL AS tab(expr);")
    checkAnswer(res, Row(Seq(Row("b", 3), Row("a", 2))))
  }

  /////////////////////////////////
  // approx_top_k_accumulate and
  // approx_top_k_estimate tests
  /////////////////////////////////

  test("SPARK-52588: accumulate and estimate of Integer with default parameters") {
    val res = sql("SELECT approx_top_k_estimate(approx_top_k_accumulate(expr)) " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (3), (4) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0, 3), Row(1, 2), Row(4, 1), Row(2, 1), Row(3, 1))))
  }

  test("SPARK-52588: accumulate and estimate of String") {
    val res = sql("SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 2) " +
      "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    checkAnswer(res, Row(Seq(Row("c", 4), Row("d", 2))))
  }

  test("SPARK-52588: accumulate and estimate of Decimal(4, 1)") {
    val res = sql("SELECT approx_top_k_estimate(approx_top_k_accumulate(expr, 10)) " +
      "FROM VALUES CAST(0.0 AS DECIMAL(4, 1)), CAST(0.0 AS DECIMAL(4, 1)), " +
      "CAST(0.0 AS DECIMAL(4, 1)), CAST(1.0 AS DECIMAL(4, 1)), " +
      "CAST(1.0 AS DECIMAL(4, 1)), CAST(2.0 AS DECIMAL(4, 1)) AS tab(expr);")
    checkAnswer(res, Row(Seq(
      Row(new java.math.BigDecimal("0.0"), 3),
      Row(new java.math.BigDecimal("1.0"), 2),
      Row(new java.math.BigDecimal("2.0"), 1))))
  }

  test("SPARK-52588: accumulate and estimate of Decimal(20, 3)") {
    val res = sql("SELECT approx_top_k_estimate(approx_top_k_accumulate(expr, 10), 2) " +
      "FROM VALUES CAST(0.0 AS DECIMAL(20, 3)), CAST(0.0 AS DECIMAL(20, 3)), " +
      "CAST(0.0 AS DECIMAL(20, 3)), CAST(1.0 AS DECIMAL(20, 3)), " +
      "CAST(1.0 AS DECIMAL(20, 3)), CAST(2.0 AS DECIMAL(20, 3)) AS tab(expr);")
    checkAnswer(res, Row(Seq(
      Row(new java.math.BigDecimal("0.000"), 3),
      Row(new java.math.BigDecimal("1.000"), 2))))
  }

  gridTest("SPARK-52588: accumulate and estimate of different types")(itemsWithTopK) {
    case (input, expected) =>
      val res = sql(s"SELECT approx_top_k_estimate(approx_top_k_accumulate(expr)) " +
        s"FROM VALUES $input AS tab(expr);")
      checkAnswer(res, Row(expected))
  }

  test("SPARK-52588: invalid accumulate if maxItemsTracked is null") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k_accumulate(expr, NULL) FROM VALUES 0, 1, 2 AS tab(expr);")
          .collect()
      },
      condition = "APPROX_TOP_K_NULL_ARG",
      parameters = Map("argName" -> "`maxItemsTracked`")
    )
  }

  test("SPARK-52588: invalid accumulate if maxItemsTracked < 1") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k_accumulate(expr, 0) FROM VALUES 0, 1, 2 AS tab(expr);")
          .collect()
      },
      condition = "APPROX_TOP_K_NON_POSITIVE_ARG",
      parameters = Map("argName" -> "`maxItemsTracked`", "argValue" -> "0")
    )
  }

  test("SPARK-52588: invalid accumulate if maxItemsTracked > 1000000") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k_accumulate(expr, 1000001) FROM VALUES (0) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_MAX_ITEMS_TRACKED_EXCEEDS_LIMIT",
      parameters = Map("maxItemsTracked" -> "1000001", "limit" -> "1000000")
    )
  }

  test("SPARK-52588: invalid estimate if k is null") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), NULL) " +
          "FROM VALUES 0, 1, 2 AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NULL_ARG",
      parameters = Map("argName" -> "`k`")
    )
  }

  test("SPARK-52588: invalid estimate if k < 1") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 0) " +
          "FROM VALUES 0, 1, 2 AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NON_POSITIVE_ARG",
      parameters = Map("argName" -> "`k`", "argValue" -> "0")
    )
  }

  test("SPARK-52588: invalid estimate if k > Int.MaxValue") {
    withSQLConf("spark.sql.ansi.enabled" -> true.toString) {
      val k: Long = Int.MaxValue + 1L
      checkError(
        exception = intercept[SparkArithmeticException] {
          sql(s"SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), $k) " +
            "FROM VALUES 0, 1, 2 AS tab(expr);").collect()
        },
        condition = "CAST_OVERFLOW",
        parameters = Map(
          "value" -> (k.toString + "L"),
          "sourceType" -> "\"BIGINT\"",
          "targetType" -> "\"INT\"",
          "ansiConfig" -> "\"spark.sql.ansi.enabled\""
        )
      )
    }
  }

  test("SPARK-52588: invalid estimate if k > maxItemsTracked") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k_estimate(approx_top_k_accumulate(expr, 5), 10) " +
          "FROM VALUES 0, 1, 2 AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_MAX_ITEMS_TRACKED_LESS_THAN_K",
      parameters = Map("maxItemsTracked" -> "5", "k" -> "10")
    )
  }
}
