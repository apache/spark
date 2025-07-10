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
import java.time.{LocalDateTime, LocalTime}

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimeType}


class ApproxTopKSuite extends QueryTest
                      with SharedSparkSession {

  import testImplicits._

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

  test("SPARK-52515: test of Integer type") {
    val res = sql(
      "SELECT approx_top_k(expr) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);"
    )
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2), Row(1, 2), Row(2, 1), Row(3, 1))))
  }

  test("SPARK-52515: test of String type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2)" +
        "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    checkAnswer(res, Row(Seq(Row("c", 4), Row("d", 2))))
  }

  test("SPARK-52515: test of Boolean type") {
    Seq(true, true, false, true, true, false, false).toDF("expr").createOrReplaceTempView("t_bool")
    val res = sql("SELECT approx_top_k(expr, 1) FROM t_bool;")
    checkAnswer(res, Row(Seq(Row(true, 4))))
  }

  test("SPARK-52515: test of Byte type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS BYTE), cast(0 AS BYTE), cast(1 AS BYTE), cast(1 AS BYTE), " +
        "cast(2 AS BYTE), cast(3 AS BYTE), cast(4 AS BYTE), cast(4 AS BYTE) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-52515: test of Short type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS SHORT), cast(0 AS SHORT), cast(1 AS SHORT), cast(1 AS SHORT), " +
        "cast(2 AS SHORT), cast(3 AS SHORT), cast(4 AS SHORT), cast(4 AS SHORT) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-52515: test of Long type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS LONG), cast(0 AS LONG), cast(1 AS LONG), cast(1 AS LONG), " +
        "cast(2 AS LONG), cast(3 AS LONG), cast(4 AS LONG), cast(4 AS LONG) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-52515: test of Float type") {
    val res = sql(
      "SELECT approx_top_k(expr) " +
        "FROM VALUES cast(0.0 AS FLOAT), cast(0.0 AS FLOAT), " +
        "cast(1.0 AS FLOAT), cast(1.0 AS FLOAT), " +
        "cast(2.0 AS FLOAT), cast(3.0 AS FLOAT), " +
        "cast(4.0 AS FLOAT), cast(4.0 AS FLOAT) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0.0, 2), Row(1.0, 2), Row(4.0, 2), Row(2.0, 1), Row(3.0, 1))))
  }

  test("SPARK-52515: test of Double type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0.0 AS DOUBLE), cast(0.0 AS DOUBLE), " +
        "cast(1.0 AS DOUBLE), cast(1.0 AS DOUBLE), " +
        "cast(2.0 AS DOUBLE), cast(3.0 AS DOUBLE), " +
        "cast(4.0 AS DOUBLE), cast(4.0 AS DOUBLE) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0.0, 2), Row(4.0, 2))))
  }

  test("SPARK-52515: test of Date type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast('2023-01-01' AS DATE), cast('2023-01-01' AS DATE), " +
        "cast('2023-01-02' AS DATE), cast('2023-01-02' AS DATE), " +
        "cast('2023-01-03' AS DATE), cast('2023-01-04' AS DATE), " +
        "cast('2023-01-05' AS DATE), cast('2023-01-05' AS DATE) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(Date.valueOf("2023-01-02"), 2), Row(Date.valueOf("2023-01-01"), 2))))
  }

  test("SPARK-52515: test of Timestamp type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast('2023-01-01 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-01 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-02 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-02 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-03 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-04 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-05 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-05 00:00:00' AS TIMESTAMP) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(Timestamp.valueOf("2023-01-02 00:00:00"), 2),
        Row(Timestamp.valueOf("2023-01-05 00:00:00"), 2))))
  }

  test("SPARK-52515: test of Timestamp_ntz type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES TIMESTAMP_NTZ'2023-01-01 00:00:00', " +
        "TIMESTAMP_NTZ'2023-01-01 00:00:00', " +
        "TIMESTAMP_NTZ'2023-01-02 00:00:00', " +
        "TIMESTAMP_NTZ'2023-01-02 00:00:00', " +
        "TIMESTAMP_NTZ'2023-01-03 00:00:00', " +
        "TIMESTAMP_NTZ'2023-01-04 00:00:00', " +
        "TIMESTAMP_NTZ'2023-01-05 00:00:00', " +
        "TIMESTAMP_NTZ'2023-01-05 00:00:00' AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(LocalDateTime.of(2023, 1, 5, 0, 0), 2),
        Row(LocalDateTime.of(2023, 1, 1, 0, 0), 2))))
  }

  test("SPARK-52515: test of Decimal type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES (0.0), (0.0), (0.0) ,(1.0), (1.0), (2.0), (3.0), (4.0) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.0"), 3), Row(new java.math.BigDecimal("1.0"), 2))))
  }

  test("SPARK-52515: test of Decimal(4, 1) type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES CAST(0.0 AS DECIMAL(4, 1)), CAST(0.0 AS DECIMAL(4, 1)), " +
        "CAST(0.0 AS DECIMAL(4, 1)), CAST(1.0 AS DECIMAL(4, 1)), " +
        "CAST(1.0 AS DECIMAL(4, 1)), CAST(2.0 AS DECIMAL(4, 1)), " +
        "CAST(3.0 AS DECIMAL(4, 1)), CAST(4.0 AS DECIMAL(4, 1)) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.0"), 3), Row(new java.math.BigDecimal("1.0"), 2))))
  }

  test("SPARK-52515: test of Decimal(10, 2) type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES CAST(0.0 AS DECIMAL(10, 2)), CAST(0.0 AS DECIMAL(10, 2)), " +
        "CAST(0.0 AS DECIMAL(10, 2)), CAST(1.0 AS DECIMAL(10, 2)), " +
        "CAST(1.0 AS DECIMAL(10, 2)), CAST(2.0 AS DECIMAL(10, 2)), " +
        "CAST(3.0 AS DECIMAL(10, 2)), CAST(4.0 AS DECIMAL(10, 2)) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.00"), 3), Row(new java.math.BigDecimal("1.00"), 2))))
  }

  test("SPARK-52515: test of Decimal(20, 3) type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES CAST(0.0 AS DECIMAL(20, 3)), CAST(0.0 AS DECIMAL(20, 3)), " +
        "CAST(0.0 AS DECIMAL(20, 3)), CAST(1.0 AS DECIMAL(20, 3)), " +
        "CAST(1.0 AS DECIMAL(20, 3)), CAST(2.0 AS DECIMAL(20, 3)), " +
        "CAST(3.0 AS DECIMAL(20, 3)), CAST(4.0 AS DECIMAL(20, 3)) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.000"), 3),
        Row(new java.math.BigDecimal("1.000"), 2))))
  }

  test("SPARK-52515: invalid k value") {
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

  test("SPARK-52515: invalid k value null") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, NULL) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NULL_ARG",
      parameters = Map("argName" -> "`k`")
    )
  }

  test("SPARK-52515: invalid maxItemsTracked value") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, 10, -1) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NON_POSITIVE_ARG",
      parameters = Map("argName" -> "`maxItemsTracked`", "argValue" -> "-1")
    )
  }

  test("SPARK-52515: invalid maxItemsTracked value null") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, 10, NULL) FROM VALUES (0), (1), (2) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_NULL_ARG",
      parameters = Map("argName" -> "`maxItemsTracked`")
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

  test("SPARK-52515: invalid maxItemsTracked > 1000000") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("SELECT approx_top_k(expr, 10, 1000001) FROM VALUES (0), (1) AS tab(expr);").collect()
      },
      condition = "APPROX_TOP_K_MAX_ITEMS_TRACKED_EXCEEDS_LIMIT",
      parameters = Map("maxItemsTracked" -> "1000001", "limit" -> "1000000")
    )
  }

  test("SPARK-52515: invalid item type array") {
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql("SELECT approx_top_k(expr) FROM VALUES array(1, 2), array(2, 3) AS tab(expr);")
      },
      condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
      parameters = Map(
        "sqlExpr" -> "\"approx_top_k(expr, 5, 10000)\"",
        "msg" -> "array columns are not supported",
        "hint" -> ""
      ),
      queryContext = Array(ExpectedContext("approx_top_k(expr)", 7, 24))
    )
  }

  test("SPARK-52515: invalid item type struct") {
    sql("SELECT struct(1, 2) AS expr").createOrReplaceTempView("struct_table")
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql("SELECT approx_top_k(expr) FROM struct_table;")
      },
      condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
      parameters = Map(
        "sqlExpr" -> "\"approx_top_k(expr, 5, 10000)\"",
        "msg" -> "struct columns are not supported",
        "hint" -> ""
      ),
      queryContext = Array(ExpectedContext("approx_top_k(expr)", 7, 24))
    )
  }

  test("SPARK-52515: invalid item type map") {
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql("SELECT approx_top_k(expr) FROM VALUES map('red', 1, 'green', 2) AS tab(expr);")
      },
      condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
      parameters = Map(
        "sqlExpr" -> "\"approx_top_k(expr, 5, 10000)\"",
        "msg" -> "map columns are not supported",
        "hint" -> ""
      ),
      queryContext = Array(ExpectedContext("approx_top_k(expr)", 7, 24))
    )
  }

  test("SPARK-52515: does not count NULL values") {
    val res = sql(
      "SELECT approx_top_k(expr, 2)" +
        "FROM VALUES 'a', 'a', 'b', 'b', 'b', NULL, NULL, NULL AS tab(expr);")
    checkAnswer(res, Row(Seq(Row("b", 3), Row("a", 2))))
  }

  test("SPARK-52626: Support group by Time column") {
    val ts1 = "15:00:00"
    val ts2 = "22:00:00"
    val localTime = Seq(ts1, ts1, ts2).map(LocalTime.parse)
    val df = localTime.toDF("t").groupBy("t").count().orderBy("t")
    val expectedSchema =
      new StructType().add(StructField("t", TimeType())).add("count", LongType, false)
    assert(df.schema == expectedSchema)
    checkAnswer(df, Seq(Row(LocalTime.parse(ts1), 2), Row(LocalTime.parse(ts2), 1)))
  }

  test("SPARK-52660: Support aggregation of Time column when codegen is split") {
    val res = sql(
      "SELECT max(expr), MIN(expr) " +
        "FROM VALUES TIME'22:01:00', " +
        "TIME'22:00:00', " +
        "TIME'15:00:00', " +
        "TIME'22:01:00', " +
        "TIME'13:22:01', " +
        "TIME'03:00:00', " +
        "TIME'22:00:00', " +
        "TIME'17:45:00' AS tab(expr);")
    checkAnswer(
      res,
      Row(LocalTime.of(22, 1, 0), LocalTime.of(3, 0, 0)))
  }

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

  test("SPARK-52588: invalid estimate if k is invalid") {
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

  test("SPARK-52588state: invalid estimate if state is not a struct") {
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql("SELECT approx_top_k_estimate(1, 5);")
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"approx_top_k_estimate(1, 5)\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"1\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRUCT\""),
      queryContext = Array(ExpectedContext("approx_top_k_estimate(1, 5)", 7, 33))
    )
  }

  test("SPARK-52588state: invalid estimate if state struct length is not 3") {
    val invalidState = "named_struct('sketch', X'01', 'itemDataType', CAST(NULL AS INT), " +
      "'maxItemsTracked', 10, 'invalidField', 1)"
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql(s"SELECT approx_top_k_estimate($invalidState, 5);")
      },
      condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
      parameters = Map(
        "sqlExpr" -> ("\"approx_top_k_estimate(named_struct(sketch, X'01', itemDataType, " +
          "CAST(NULL AS INT), maxItemsTracked, 10, invalidField, 1), 5)\""),
        "msg" -> ("State must be a struct with 3 fields. " +
          "Expected struct: struct<sketch:binary,itemDataType:any,maxItemsTracked:int>. " +
          "Got: struct<sketch:binary,itemDataType:int,maxItemsTracked:int,invalidField:int>"),
        "hint" -> ""),
      queryContext = Array(ExpectedContext(s"approx_top_k_estimate($invalidState, 5)", 7, 138))
    )
  }

  test("SPARK-52588state: invalid estimate if state struct does not have 'sketch' field") {
    val invalidState = "named_struct('notSketch', X'01', 'itemDataType', CAST(NULL AS INT), " +
      "'maxItemsTracked', 10)"
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql(s"SELECT approx_top_k_estimate($invalidState, 5);")
      },
      condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
      parameters = Map(
        "sqlExpr" -> ("\"approx_top_k_estimate(named_struct(notSketch, X'01', itemDataType, " +
          "CAST(NULL AS INT), maxItemsTracked, 10), 5)\""),
        "msg" -> "State struct must contain a field named 'sketch'.",
        "hint" -> ""
      ),
      queryContext = Array(ExpectedContext(s"approx_top_k_estimate($invalidState, 5)", 7, 122))
    )

  }

  test("SPARK-checkStruct") {
    sql("SELECT array(expr) as acc FROM VALUES 0, 0, 1 AS tab(expr);")
      .createOrReplaceTempView("aaa")
    sql("SELECT approx_top_k_estimate(acc) FROM aaa;").show(false)
  }

  test("SPARK-checkStruct2") {
    sql("SELECT struct('Spark', 5) as acc;")
      .createOrReplaceTempView("aaa")
    sql("SELECT approx_top_k_estimate(acc) FROM aaa;").show(false)
  }

}
