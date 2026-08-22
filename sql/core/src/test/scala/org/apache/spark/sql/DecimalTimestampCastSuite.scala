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

import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}

class DecimalTimestampCastSuite extends QueryTest with SharedSparkSession {

  private val codegenModes = Seq(
    Seq(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY"),
    Seq(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN"))

  private def decimalDataFrame(
      values: Seq[String],
      dataType: DecimalType = DecimalType(20, 0)): DataFrame = {
    val rows = values.map(value => Row(new java.math.BigDecimal(value)))
    val schema = StructType(StructField("value", dataType, nullable = false) :: Nil)
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  test("SPARK-58217: decimal to timestamp overflow in SQL execution") {
    withTempView("decimal_values") {
      decimalDataFrame(Seq("99999999999999999999", "-99999999999999999999", "1"))
        .createOrReplaceTempView("decimal_values")

      codegenModes.foreach { mode =>
        withSQLConf((Seq(SQLConf.ANSI_ENABLED.key -> "false") ++ mode): _*) {
          checkAnswer(
            spark.sql("""
              SELECT
                CAST(value AS TIMESTAMP) IS NULL AS is_null,
                CAST(CAST(value AS TIMESTAMP) AS LONG) AS seconds
              FROM decimal_values
            """),
            Row(true, null) :: Row(true, null) :: Row(false, 1L) :: Nil)
        }
      }
    }
  }

  test("SPARK-58217: decimal to timestamp overflow in ANSI SQL execution") {
    withTempView("decimal_values") {
      decimalDataFrame(Seq("99999999999999999999", "-99999999999999999999"))
        .createOrReplaceTempView("decimal_values")

      codegenModes.foreach { mode =>
        withSQLConf((Seq(SQLConf.ANSI_ENABLED.key -> "true") ++ mode): _*) {
          Seq(
            "99999999999999999999" -> "99999999999999999999BD",
            "-99999999999999999999" -> "-99999999999999999999BD").foreach {
            case (value, formattedValue) =>
              val exception = intercept[SparkArithmeticException] {
                spark.sql(s"""
                  SELECT CAST(value AS TIMESTAMP)
                  FROM decimal_values
                  WHERE value = CAST($value AS DECIMAL(20, 0))
                """).collect()
              }
              checkError(
                exception = exception,
                condition = "CAST_OVERFLOW",
                parameters = Map(
                  "value" -> formattedValue,
                  "sourceType" -> "\"DECIMAL(20,0)\"",
                  "targetType" -> "\"TIMESTAMP\"",
                  "ansiConfig" -> "\"spark.sql.ansi.enabled\""),
                sqlState = "22003")
          }
        }
      }
    }
  }

  test("SPARK-58217: decimal to timestamp fractional Long boundaries") {
    withTempView("decimal_boundaries") {
      decimalDataFrame(
        Seq(
          "9223372036854.7758075",
          "-9223372036854.7758085",
          "9223372036854.7758080",
          "-9223372036854.7758090"),
        DecimalType(20, 7)).createOrReplaceTempView("decimal_boundaries")

      codegenModes.foreach { mode =>
        withSQLConf((Seq(SQLConf.ANSI_ENABLED.key -> "false") ++ mode): _*) {
          checkAnswer(
            spark.sql("""
              SELECT unix_micros(CAST(value AS TIMESTAMP))
              FROM decimal_boundaries
            """),
            Row(Long.MaxValue) :: Row(Long.MinValue) :: Row(null) :: Row(null) :: Nil)
        }
      }
    }
  }
}
