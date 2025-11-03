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
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.errors.DataTypeErrors.toSQLType
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampNTZType, TimestampType}


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

  test("SPARK-53947: count NULL values") {
    val res = sql(
      "SELECT approx_top_k(expr, 2)" +
        "FROM VALUES 'a', 'a', 'b', 'b', 'b', NULL, NULL, NULL, NULL AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(null, 4), Row("b", 3))))
  }

  test("SPARK-53947: null is not in top k") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) FROM VALUES 'a', 'a', 'b', 'b', 'b', NULL AS tab(expr)"
    )
    checkAnswer(res, Row(Seq(Row("b", 3), Row("a", 2))))
  }

  test("SPARK-53947: null is the last in top k") {
    val res = sql(
      "SELECT approx_top_k(expr, 3) FROM VALUES 0, 0, 1, 1, 1, NULL AS tab(expr)"
    )
    checkAnswer(res, Row(Seq(Row(1, 3), Row(0, 2), Row(null, 1))))
  }

  test("SPARK-53947: null + frequent items < k") {
    val res = sql(
      """SELECT approx_top_k(expr, 5)
        |FROM VALUES cast(0.0 AS DECIMAL(4, 1)), cast(0.0 AS DECIMAL(4, 1)),
        |cast(0.1 AS DECIMAL(4, 1)), cast(0.1 AS DECIMAL(4, 1)), cast(0.1 AS DECIMAL(4, 1)),
        |NULL AS tab(expr)""".stripMargin)
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.1"), 3),
        Row(new java.math.BigDecimal("0.0"), 2),
        Row(null, 1))))
  }

  test("SPARK-53947: work on typed column with only NULL values") {
    val res = sql(
      "SELECT approx_top_k(expr) FROM VALUES cast(NULL AS INT), cast(NULL AS INT) AS tab(expr)"
    )
    checkAnswer(res, Row(Seq(Row(null, 2))))
  }

  test("SPARK-53947: invalid item void columns") {
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql("SELECT approx_top_k(expr) FROM VALUES (NULL), (NULL), (NULL) AS tab(expr)")
      },
      condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
      parameters = Map(
        "sqlExpr" -> "\"approx_top_k(expr, 5, 10000)\"",
        "msg" -> "void columns are not supported",
        "hint" -> ""
      ),
      queryContext = Array(ExpectedContext("approx_top_k(expr)", 7, 24))
    )
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

  test("SPARK-53960: accumulate and estimate count NULL values") {
    val res = sql(
      """SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 2)
        |FROM VALUES 'a', 'a', 'b', 'b', 'b', NULL, NULL, NULL, NULL AS tab(expr)""".stripMargin)
    checkAnswer(res, Row(Seq(Row(null, 4), Row("b", 3))))
  }

  test("SPARK-53960: accumulate and estimate null is not in top k") {
    val res = sql(
      """SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 2)
        |FROM VALUES 'a', 'a', 'b', 'b', 'b', NULL AS tab(expr)""".stripMargin)
    checkAnswer(res, Row(Seq(Row("b", 3), Row("a", 2))))
  }

  test("SPARK-53960: accumulate and estimate null is the last in top k") {
    val res = sql(
      """SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 3)
        |FROM VALUES 0, 0, 1, 1, 1, NULL AS tab(expr)""".stripMargin)
    checkAnswer(res, Row(Seq(Row(1, 3), Row(0, 2), Row(null, 1))))
  }

  test("SPARK-53960: accumulate and estimate null + frequent items < k") {
    val res = sql(
      """SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 5)
        |FROM VALUES cast(0.0 AS DECIMAL(4, 1)), cast(0.0 AS DECIMAL(4, 1)),
        |cast(0.1 AS DECIMAL(4, 1)), cast(0.1 AS DECIMAL(4, 1)), cast(0.1 AS DECIMAL(4, 1)),
        |NULL AS tab(expr)""".stripMargin)
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.1"), 3),
        Row(new java.math.BigDecimal("0.0"), 2),
        Row(null, 1))))
  }

  test("SPARK-53960: accumulate and estimate work on typed column with only NULL values") {
    val res = sql(
      """SELECT approx_top_k_estimate(approx_top_k_accumulate(expr))
        |FROM VALUES cast(NULL AS INT), cast(NULL AS INT) AS tab(expr)""".stripMargin)
    checkAnswer(res, Row(Seq(Row(null, 2))))
  }

  test("SPARK-53960: accumulate a column of all nulls with type - success") {
    withView("accumulation") {
      val res = sql(
        """SELECT approx_top_k_accumulate(expr) AS acc
          |FROM VALUES cast(NULL AS INT), cast(NULL AS INT) AS tab(expr)""".stripMargin)

      assert(res.collect().length == 1)
      res.createOrReplaceTempView("accumulation")
      val est = sql("SELECT approx_top_k_estimate(acc) FROM accumulation;")
      checkAnswer(est, Row(Seq(Row(null, 2))))

    }
  }

  test("SPARK-53960: accumulate a column of all nulls without type - fail") {
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql("""SELECT approx_top_k_accumulate(expr)
            |FROM VALUES (NULL), (NULL), (NULL), (NULL) AS tab(expr)""".stripMargin)
      },
      condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
      parameters = Map(
        "sqlExpr" -> "\"approx_top_k_accumulate(expr, 10000)\"",
        "msg" -> "void columns are not supported",
        "hint" -> ""
      ),
      queryContext = Array(ExpectedContext("approx_top_k_accumulate(expr)", 7, 35))
    )
  }

  /////////////////////////////////
  // approx_top_k_combine
  /////////////////////////////////

  def setupMixedSizeAccumulations(size1: Int, size2: Int): Unit = {
    sql(s"SELECT approx_top_k_accumulate(expr, $size1) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
      .createOrReplaceTempView("accumulation1")

    sql(s"SELECT approx_top_k_accumulate(expr, $size2) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
      .createOrReplaceTempView("accumulation2")

    sql("SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2")
      .createOrReplaceTempView("unioned")
  }

  def setupMixedTypeAccumulation(seq1: Seq[Any], seq2: Seq[Any]): Unit = {
    sql(s"SELECT approx_top_k_accumulate(expr, 10) as acc " +
      s"FROM VALUES ${seq1.mkString(", ")} AS tab(expr);")
      .createOrReplaceTempView("accumulation1")

    sql(s"SELECT approx_top_k_accumulate(expr, 10) as acc " +
      s"FROM VALUES ${seq2.mkString(", ")} AS tab(expr);")
      .createOrReplaceTempView("accumulation2")

    sql("SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2")
      .createOrReplaceTempView("unioned")
  }

  val mixedNumberTypes: Seq[(DataType, String, Seq[Any])] = Seq(
    (IntegerType, "INT",
      Seq(0, 0, 0, 1, 1, 2, 2, 3)),
    (ByteType, "TINYINT",
      Seq("cast(0 AS BYTE)", "cast(0 AS BYTE)", "cast(1 AS BYTE)")),
    (ShortType, "SMALLINT",
      Seq("cast(0 AS SHORT)", "cast(0 AS SHORT)", "cast(1 AS SHORT)")),
    (LongType, "BIGINT",
      Seq("cast(0 AS LONG)", "cast(0 AS LONG)", "cast(1 AS LONG)")),
    (FloatType, "FLOAT",
      Seq("cast(0 AS FLOAT)", "cast(0 AS FLOAT)", "cast(1 AS FLOAT)")),
    (DoubleType, "DOUBLE",
      Seq("cast(0 AS DOUBLE)", "cast(0 AS DOUBLE)", "cast(1 AS DOUBLE)")),
    (DecimalType(4, 2), "DECIMAL(4,2)",
      Seq("cast(0 AS DECIMAL(4, 2))", "cast(0 AS DECIMAL(4, 2))", "cast(1 AS DECIMAL(4, 2))")),
    (DecimalType(10, 2), "DECIMAL(10,2)",
      Seq("cast(0 AS DECIMAL(10, 2))", "cast(0 AS DECIMAL(10, 2))", "cast(1 AS DECIMAL(10, 2))")),
    (DecimalType(20, 3), "DECIMAL(20,3)",
      Seq("cast(0 AS DECIMAL(20, 3))", "cast(0 AS DECIMAL(20, 3))", "cast(1 AS DECIMAL(20, 3))"))
  )

  val mixedDateTimeTypes: Seq[(DataType, String, Seq[String])] = Seq(
    (DateType, "DATE",
      Seq("DATE'2025-01-01'", "DATE'2025-01-01'", "DATE'2025-01-02'")),
    (TimestampType, "TIMESTAMP",
      Seq("TIMESTAMP'2025-01-01 00:00:00'", "TIMESTAMP'2025-01-01 00:00:00'")),
    (TimestampNTZType, "TIMESTAMP_NTZ",
      Seq("TIMESTAMP_NTZ'2025-01-01 00:00:00'", "TIMESTAMP_NTZ'2025-01-01 00:00:00'")
    )
  )

  // positive tests for approx_top_k_combine on every types
  gridTest("SPARK-52798: same type, same size, specified combine size - success")(itemsWithTopK) {
    case (input, expected) =>
      withView("accumulation1", "accumulation2", "combines") {
        sql(s"SELECT approx_top_k_accumulate(expr) AS acc FROM VALUES $input AS tab(expr);")
          .createOrReplaceTempView("accumulation1")
        sql(s"SELECT approx_top_k_accumulate(expr) AS acc FROM VALUES $input AS tab(expr);")
          .createOrReplaceTempView("accumulation2")
        sql("SELECT approx_top_k_combine(acc, 30) as com " +
          "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
          .createOrReplaceTempView("combined")
        val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
        // expected should be doubled because we combine two identical sketches
        val expectedDoubled = expected.map {
          case Row(value: Any, count: Int) => Row(value, count * 2)
        }
        checkAnswer(est, Row(expectedDoubled))
      }
  }

  test("SPARK-52798: same type, same size, specified combine size - success") {
    withView("accumulation1", "accumulation2", "unioned", "combined") {
      setupMixedSizeAccumulations(10, 10)

      sql("SELECT approx_top_k_combine(acc, 30) as com FROM unioned")
        .createOrReplaceTempView("combined")

      val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
      checkAnswer(est, Row(Seq(Row(2, 4), Row(1, 4), Row(0, 3), Row(3, 3), Row(4, 2))))
    }
  }

  test("SPARK-52798: same type, same size, unspecified combine size - success") {
    withView("accumulation1", "accumulation2", "unioned", "combined") {
      setupMixedSizeAccumulations(10, 10)

      sql("SELECT approx_top_k_combine(acc) as com FROM unioned")
        .createOrReplaceTempView("combined")

      val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
      checkAnswer(est, Row(Seq(Row(2, 4), Row(1, 4), Row(0, 3), Row(3, 3), Row(4, 2))))
    }
  }

  test("SPARK-52798: same type, different size, specified combine size - success") {
    withView("accumulation1", "accumulation2", "unioned", "combined") {
      setupMixedSizeAccumulations(10, 20)

      sql("SELECT approx_top_k_combine(acc, 30) as com FROM unioned")
        .createOrReplaceTempView("combined")

      val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
      checkAnswer(est, Row(Seq(Row(2, 4), Row(1, 4), Row(0, 3), Row(3, 3), Row(4, 2))))
    }
  }

  test("SPARK-52798: same type, different size, unspecified combine size - fail") {
    withView("accumulation1", "accumulation2", "unioned") {
      setupMixedSizeAccumulations(10, 20)

      val comb = sql("SELECT approx_top_k_combine(acc) as com FROM unioned")

      checkError(
        exception = intercept[SparkRuntimeException] {
          comb.collect()
        },
        condition = "APPROX_TOP_K_SKETCH_SIZE_NOT_MATCH",
        parameters = Map("size1" -> "10", "size2" -> "20")
      )
    }
  }

  gridTest("SPARK-52798: invalid combine size - fail")(Seq((10, 10), (10, 20))) {
    case (size1, size2) =>
      withView("accumulation1", "accumulation2", "unioned") {
        setupMixedSizeAccumulations(size1, size2)
        checkError(
          exception = intercept[SparkRuntimeException] {
            sql("SELECT approx_top_k_combine(acc, 0) as com FROM unioned").collect()
          },
          condition = "APPROX_TOP_K_NON_POSITIVE_ARG",
          parameters = Map("argName" -> "`maxItemsTracked`", "argValue" -> "0")
        )
      }
  }

  test("SPARK-52798: among different number or datetime types - fail at combine") {
    def checkMixedTypeError(mixedTypeSeq: Seq[(DataType, String, Seq[Any])]): Unit = {
      for (i <- 0 until mixedTypeSeq.size - 1) {
        for (j <- i + 1 until mixedTypeSeq.size) {
          val (type1, _, seq1) = mixedTypeSeq(i)
          val (type2, _, seq2) = mixedTypeSeq(j)
          setupMixedTypeAccumulation(seq1, seq2)
          withView("accumulation1", "accumulation2", "unioned") {
            checkError(
              exception = intercept[SparkRuntimeException] {
                sql("SELECT approx_top_k_combine(acc, 30) as com FROM unioned;").collect()
              },
              condition = "APPROX_TOP_K_SKETCH_TYPE_NOT_MATCH",
              parameters = Map("type1" -> toSQLType(type1), "type2" -> toSQLType(type2))
            )
          }
        }
      }
    }

    checkMixedTypeError(mixedNumberTypes)
    checkMixedTypeError(mixedDateTimeTypes)
  }

  // enumerate all combinations of number and datetime types
  gridTest("SPARK-52798: number vs datetime - fail on UNION")(
    for {
      (type1, typeName1, seq1) <- mixedNumberTypes
      (type2, typeName2, seq2) <- mixedDateTimeTypes
    } yield ((type1, typeName1, seq1), (type2, typeName2, seq2))) {
    case ((_, type1, seq1), (_, type2, seq2)) =>
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          withView("accumulation1", "accumulation2", "unioned") {
            setupMixedTypeAccumulation(seq1, seq2)
          }
        },
        condition = "INCOMPATIBLE_COLUMN_TYPE",
        parameters = Map(
          "tableOrdinalNumber" -> "second",
          "columnOrdinalNumber" -> "first",
          "dataType2" -> ("\"STRUCT<sketch: BINARY NOT NULL, maxItemsTracked: INT NOT NULL, " +
            "itemDataType: " + type1 + ", itemDataTypeDDL: STRING NOT NULL>\""),
          "operator" -> "UNION",
          "hint" -> "",
          "dataType1" -> ("\"STRUCT<sketch: BINARY NOT NULL, maxItemsTracked: INT NOT NULL, " +
            "itemDataType: " + type2 + ", itemDataTypeDDL: STRING NOT NULL>\"")
        ),
        queryContext = Array(
          ExpectedContext(
            "SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2", 0, 68))
      )
  }

  gridTest("SPARK-52798: number vs string - fail at combine")(mixedNumberTypes) {
    case (type1, _, seq1) =>
      withView("accumulation1", "accumulation2", "unioned") {
        setupMixedTypeAccumulation(
          seq1, Seq("'a'", "'b'", "'c'", "'c'", "'c'", "'c'", "'d'", "'d'"))
        checkError(
          exception = intercept[SparkRuntimeException] {
            sql("SELECT approx_top_k_combine(acc, 30) as com FROM unioned;").collect()
          },
          condition = "APPROX_TOP_K_SKETCH_TYPE_NOT_MATCH",
          parameters = Map("type1" -> toSQLType(type1), "type2" -> toSQLType(StringType))
        )
      }
  }

  gridTest("SPARK-52798: number vs boolean - fail at UNION")(mixedNumberTypes) {
    case (_, type1, seq1) =>
      val seq2 = Seq("(true)", "(true)", "(false)", "(false)")
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          withView("accumulation1", "accumulation2", "unioned") {
            setupMixedTypeAccumulation(seq1, seq2)
          }
        },
        condition = "INCOMPATIBLE_COLUMN_TYPE",
        parameters = Map(
          "tableOrdinalNumber" -> "second",
          "columnOrdinalNumber" -> "first",
          "dataType2" -> ("\"STRUCT<sketch: BINARY NOT NULL, maxItemsTracked: INT NOT NULL, " +
            "itemDataType: " + type1 + ", itemDataTypeDDL: STRING NOT NULL>\""),
          "operator" -> "UNION",
          "hint" -> "",
          "dataType1" -> ("\"STRUCT<sketch: BINARY NOT NULL, maxItemsTracked: INT NOT NULL, " +
            "itemDataType: BOOLEAN, itemDataTypeDDL: STRING NOT NULL>\"")
        ),
        queryContext = Array(
          ExpectedContext(
            "SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2", 0, 68))
      )
  }

  gridTest("SPARK-52798: datetime vs string - fail at combine")(mixedDateTimeTypes) {
    case (type1, _, seq1) =>
      withView("accumulation1", "accumulation2", "unioned") {
        setupMixedTypeAccumulation(
          seq1, Seq("'a'", "'b'", "'c'", "'c'", "'c'", "'c'", "'d'", "'d'"))
        checkError(
          exception = intercept[SparkRuntimeException] {
            sql("SELECT approx_top_k_combine(acc, 30) as com FROM unioned;").collect()
          },
          condition = "APPROX_TOP_K_SKETCH_TYPE_NOT_MATCH",
          parameters = Map("type1" -> toSQLType(type1), "type2" -> toSQLType(StringType))
        )
      }
  }

  gridTest("SPARK-52798: datetime vs boolean - fail at UNION")(mixedDateTimeTypes) {
    case (_, type1, seq1) =>
      val seq2 = Seq("(true)", "(true)", "(false)", "(false)")
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          withView("accumulation1", "accumulation2", "unioned") {
            setupMixedTypeAccumulation(seq1, seq2)
          }
        },
        condition = "INCOMPATIBLE_COLUMN_TYPE",
        parameters = Map(
          "tableOrdinalNumber" -> "second",
          "columnOrdinalNumber" -> "first",
          "dataType2" -> ("\"STRUCT<sketch: BINARY NOT NULL, maxItemsTracked: INT NOT NULL, " +
            "itemDataType: " + type1 + ", itemDataTypeDDL: STRING NOT NULL>\""),
          "operator" -> "UNION",
          "hint" -> "",
          "dataType1" -> ("\"STRUCT<sketch: BINARY NOT NULL, maxItemsTracked: INT NOT NULL, " +
            "itemDataType: BOOLEAN, itemDataTypeDDL: STRING NOT NULL>\"")
        ),
        queryContext = Array(
          ExpectedContext(
            "SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2", 0, 68))
      )
  }

  test("SPARK-52798: string vs boolean - fail at combine") {
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      val seq1 = Seq("'a'", "'b'", "'c'", "'c'", "'c'", "'c'", "'d'", "'d'")
      val seq2 = Seq("(true)", "(true)", "(false)", "(false)")
      withView("accumulation1", "accumulation2", "unioned") {
        setupMixedTypeAccumulation(seq1, seq2)
        checkError(
          exception = intercept[SparkRuntimeException] {
            sql("SELECT approx_top_k_combine(acc, 30) as com FROM unioned;").collect()
          },
          condition = "APPROX_TOP_K_SKETCH_TYPE_NOT_MATCH",
          parameters = Map("type1" -> toSQLType(StringType), "type2" -> toSQLType(BooleanType))
        )
      }
    }
  }

  test("SPARK-52798: combine more than 2 sketches with specified size") {
    withView("accumulation1", "accumulation2", "accumulation3", "unioned", "combined") {
      sql(s"SELECT approx_top_k_accumulate(expr, 10) as acc " +
        "FROM VALUES (0), (0), (0), (1), (1), (2), (2) AS tab(expr);")
        .createOrReplaceTempView("accumulation1")

      sql(s"SELECT approx_top_k_accumulate(expr, 10) as acc " +
        "FROM VALUES (1), (1), (2), (2), (3), (3), (4) AS tab(expr);")
        .createOrReplaceTempView("accumulation2")

      sql(s"SELECT approx_top_k_accumulate(expr, 20) as acc " +
        "FROM VALUES (2), (2), (3), (3), (3), (4), (5) AS tab(expr);")
        .createOrReplaceTempView("accumulation3")

      sql("SELECT acc from accumulation1 UNION ALL " +
        "SELECT acc FROM accumulation2 UNION ALL " +
        "SELECT acc FROM accumulation3")
        .createOrReplaceTempView("unioned")

      sql("SELECT approx_top_k_combine(acc, 30) as com FROM unioned")
        .createOrReplaceTempView("combined")

      val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
      checkAnswer(est, Row(Seq(Row(2, 6), Row(3, 5), Row(1, 4), Row(0, 3), Row(4, 2))))
    }
  }

  test("SPARK-52798: combine more than 2 sketches without specified size") {
    withView("accumulation1", "accumulation2", "accumulation3", "unioned") {
      sql(s"SELECT approx_top_k_accumulate(expr, 10) as acc " +
        "FROM VALUES (0), (0), (0), (1), (1), (2), (2) AS tab(expr);")
        .createOrReplaceTempView("accumulation1")

      sql(s"SELECT approx_top_k_accumulate(expr, 10) as acc " +
        "FROM VALUES (1), (1), (2), (2), (3), (3), (4) AS tab(expr);")
        .createOrReplaceTempView("accumulation2")

      sql(s"SELECT approx_top_k_accumulate(expr, 20) as acc " +
        "FROM VALUES (2), (2), (3), (3), (3), (4), (5) AS tab(expr);")
        .createOrReplaceTempView("accumulation3")

      sql("SELECT acc from accumulation1 UNION ALL " +
        "SELECT acc FROM accumulation2 UNION ALL " +
        "SELECT acc FROM accumulation3")
        .createOrReplaceTempView("unioned")

      checkError(
        exception = intercept[SparkRuntimeException] {
          sql("SELECT approx_top_k_combine(acc) as com FROM unioned").collect()
        },
        condition = "APPROX_TOP_K_SKETCH_SIZE_NOT_MATCH",
        parameters = Map("size1" -> "10", "size2" -> "20")
      )
    }
  }

  test("SPARK-53960: combine and estimate count NULL values") {
    withView("accumulation1", "accumulation2", "unioned", "combined") {
      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES 'a', 'a', 'b', NULL, NULL AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation1")

      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES 'b', 'b', NULL, NULL AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation2")

      sql("SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2")
        .createOrReplaceTempView("unioned")

      sql("SELECT approx_top_k_combine(acc, 20) as com FROM unioned")
        .createOrReplaceTempView("combined")

      val est = sql("SELECT approx_top_k_estimate(com, 2) FROM combined")
      checkAnswer(est, Row(Seq(Row(null, 4), Row("b", 3))))
    }
  }

  test("SPARK-53960: combine with a sketch of all nulls") {
    withView("accumulation1", "accumulation2", "unioned", "combined") {
      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES cast(NULL AS INT), cast(NULL AS INT), cast(NULL AS INT)
          |AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation1")

      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES 1, 1, 2, 2 AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation2")

      sql("SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2")
        .createOrReplaceTempView("unioned")

      sql("SELECT approx_top_k_combine(acc, 20) as com FROM unioned")
        .createOrReplaceTempView("combined")

      val est = sql("SELECT approx_top_k_estimate(com) FROM combined")
      checkAnswer(est, Row(Seq(Row(null, 3), Row(2, 2), Row(1, 2))))
    }
  }

  test("SPARK-53960: combine sketches with nulls from more than 2 sketches") {
    withView("accumulation1", "accumulation2", "accumulation3", "unioned", "combined") {
      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES 0, 0, 0, 1, 1, NULL AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation1")

      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES NULL, 1, 1, 2, 2, NULL AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation2")

      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES 2, 3, 3, NULL AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation3")

      sql(
        """SELECT acc from accumulation1 UNION ALL
          |SELECT acc FROM accumulation2 UNION ALL
          |SELECT acc FROM accumulation3""".stripMargin)
        .createOrReplaceTempView("unioned")

      sql("SELECT approx_top_k_combine(acc, 30) as com FROM unioned")
        .createOrReplaceTempView("combined")

      val est = sql("SELECT approx_top_k_estimate(com, 2) FROM combined")
      checkAnswer(est, Row(Seq(Row(1, 4), Row(null, 4))))
    }
  }

  test("SPARK-53960: combine 2 sketches with all nulls") {
    withView("accumulation1", "accumulation2", "unioned", "combined") {
      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES cast(NULL AS INT), cast(NULL AS INT), cast(NULL AS INT)
          |AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation1")

      sql(
        """SELECT approx_top_k_accumulate(expr, 10) as acc
          |FROM VALUES cast(NULL AS INT), cast(NULL AS INT)
          |AS tab(expr)""".stripMargin)
        .createOrReplaceTempView("accumulation2")

      sql("SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2")
        .createOrReplaceTempView("unioned")

      sql("SELECT approx_top_k_combine(acc, 20) as com FROM unioned")
        .createOrReplaceTempView("combined")

      val est = sql("SELECT approx_top_k_estimate(com) FROM combined")
      checkAnswer(est, Row(Seq(Row(null, 5))))
    }
  }
}
