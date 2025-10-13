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

import org.apache.spark.sql.functions.{bitmap_and_agg, bitmap_bit_position, bitmap_bucket_number, bitmap_construct_agg, bitmap_count, bitmap_or_agg, col, expr, hex, lit, substring, to_binary}
import org.apache.spark.sql.test.SharedSparkSession

class BitmapExpressionsQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("bitmap_construct_agg") {
    val table = "bitmaps_table"
    withTable(table) {
      (0 until 10000).toDF("id").selectExpr("100 * cast(id / 2 as int) col")
        .createOrReplaceTempView(table)

      val expected = spark.sql(
        s"""
           | select count (distinct col) c from $table
           |""".stripMargin).collect()

      val df = spark.sql(
        s"""
          | select sum(c) from (
          |   select bitmap_bucket_number(col) bn,
          |   bitmap_count(bitmap_construct_agg(bitmap_bit_position(col))) c
          |   from $table
          |   group by 1
          | )
          |""".stripMargin)
      checkAnswer(df, expected)
    }

    val df = Seq(1, 2, 3).toDF("a")
    checkAnswer(
      df.selectExpr("substring(hex(bitmap_construct_agg(bitmap_bit_position(a))), 0, 6)"),
      Seq(Row("070000"))
    )
    checkAnswer(
      df.select(substring(hex(bitmap_construct_agg(bitmap_bit_position(col("a")))), 0, 6)),
      Seq(Row("070000"))
    )
  }

  test("grouping bitmap_construct_agg") {
    val table = "bitmaps_table"
    withTable(table) {
      (0 until 10000).toDF("id").selectExpr(
        "(id % 4) part",
        "100 * cast(id / 8 as int) col")
        .createOrReplaceTempView(table)

      val expected = spark.sql(
        s"""
           | select part, count (distinct col) c from $table group by 1 order by 1
           |""".stripMargin).collect()

      val df = spark.sql(
        s"""
           | select part, sum(c) from (
           |   select part, bitmap_bucket_number(col) bn,
           |   bitmap_count(bitmap_construct_agg(bitmap_bit_position(col))) c
           |   from $table group by 1, 2 order by 1, 2
           | ) group by 1 order by 1
           |""".stripMargin)
      checkAnswer(df, expected)
    }
  }

  test("precomputed bitmaps") {
    val table = "bitmaps_table"
    val precomputed = "precomputed_table"
    withTable(table) {
      withTable(precomputed) {
        (0 until 10000).toDF("id").selectExpr(
          "(id % 4) part1",
          "((id + 7) % 3) part2",
          "100 * cast(id / 17 as int) col")
          .createOrReplaceTempView(table)
        spark.sql(
          s"""
             | select part1, part2, bitmap_bucket_number(col) bn,
             | bitmap_construct_agg(bitmap_bit_position(col)) bm
             | from $table group by 1, 2, 3
             |""".stripMargin).createOrReplaceTempView(precomputed)

        // Compute over both partitions
        {
          val expected = spark.sql(
            s"""
               | select part1, part2, count (distinct col) c from $table group by 1, 2 order by 1, 2
               |""".stripMargin).collect()

          val df = spark.sql(
            s"""
               | select part1, part2, sum(bitmap_count(bm))
               | from $precomputed group by 1, 2 order by 1, 2
               |""".stripMargin)
          checkAnswer(df, expected)
        }

        // Compute over one of the partitions
        Seq("part1", "part2").foreach {
          case part =>
            val expected = spark.sql(
              s"""
                 | select $part, count (distinct col) c from $table group by 1 order by 1
                 |""".stripMargin).collect()

            val df = spark.sql(
              s"""
                 | select $part, sum(c) from (
                 |   select $part, bn, bitmap_count(bitmap_or_agg(bm)) c
                 |   from $precomputed group by 1, 2
                 | ) group by 1 order by 1
                 |""".stripMargin)
            checkAnswer(df, expected)
        }
      }
    }
  }

  test("bitmap functions with floats") {
    val table = "bitmaps_table"
    withTable(table) {
      (0 until 10000).toDF("id").selectExpr(
        "(id % 4) part",
        "100 * id + cast(id / 8.0 as float) col")
        .createOrReplaceTempView(table)

      val expected = spark.sql(
        s"""
           | select part, count (distinct col) c from $table group by 1 order by 1
           |""".stripMargin).collect()

      val df = spark.sql(
        s"""
           | select part, sum(c) from (
           |   select part, bitmap_bucket_number(col) bn,
           |   bitmap_count(bitmap_construct_agg(bitmap_bit_position(col))) c
           |   from $table group by 1, 2 order by 1, 2
           | ) group by 1 order by 1
           |""".stripMargin)
      checkAnswer(df, expected)
    }
  }

  test("bitmap_bit_position") {
    val df = Seq(123).toDF("a")
    checkAnswer(
      df.selectExpr("bitmap_bit_position(a)"),
      Seq(Row(122))
    )
    checkAnswer(
      df.select(bitmap_bit_position(col("a"))),
      Seq(Row(122))
    )
  }

  test("bitmap_bucket_number") {
    val df = Seq(123).toDF("a")
    checkAnswer(
      df.selectExpr("bitmap_bucket_number(a)"),
      Seq(Row(1))
    )
    checkAnswer(
      df.select(bitmap_bucket_number(col("a"))),
      Seq(Row(1))
    )
  }

  test("bitmap_count") {
    val df = Seq("FFFF").toDF("a")
    checkAnswer(
      df.selectExpr("bitmap_count(to_binary(a, 'hex'))"),
      Seq(Row(16))
    )
    checkAnswer(
      df.select(bitmap_count(to_binary(col("a"), lit("hex")))),
      Seq(Row(16))
    )
  }

  test("bitmap_or_agg") {
    val df = Seq("10", "20", "40").toDF("a")
    checkAnswer(
      df.selectExpr("substring(hex(bitmap_or_agg(to_binary(a, 'hex'))), 0, 6)"),
      Seq(Row("700000"))
    )
    checkAnswer(
      df.select(substring(hex(bitmap_or_agg(to_binary(col("a"), lit("hex")))), 0, 6)),
      Seq(Row("700000"))
    )
  }

  test("bitmap_and_agg") {
    // Test basic AND functionality: F0 & 70 & 30 = 30
    val df = Seq("F0", "70", "30").toDF("a")
    checkAnswer(
      df.selectExpr("substring(hex(bitmap_and_agg(to_binary(a, 'hex'))), 0, 6)"),
      Seq(Row("300000")))
    checkAnswer(
      df.select(substring(hex(bitmap_and_agg(to_binary(col("a"), lit("hex")))), 0, 6)),
      Seq(Row("300000")))

    // Test with all 1s - should return FF
    val df2 = Seq("FF", "FF", "FF").toDF("a")
    checkAnswer(
      df2.selectExpr("substring(hex(bitmap_and_agg(to_binary(a, 'hex'))), 0, 6)"),
      Seq(Row("FF0000")))

    // Test with mixed values - A0 & F0 & 80 = 80
    val df3 = Seq("A0", "F0", "80").toDF("a")
    checkAnswer(
      df3.selectExpr("substring(hex(bitmap_and_agg(to_binary(a, 'hex'))), 0, 6)"),
      Seq(Row("800000")))

    // Test with one zero - anything & 00 = 00
    val df4 = Seq("FF", "00", "FF").toDF("a")
    checkAnswer(
      df4.selectExpr("substring(hex(bitmap_and_agg(to_binary(a, 'hex'))), 0, 6)"),
      Seq(Row("000000")))

    // Test with binary values of different lengths - "FF" & "FFFF" & "FFFFFF" = "FF0000"
    val df5 = Seq("FF", "FFFF", "FFFFFF").toDF("a")
    checkAnswer(
      df5.selectExpr("substring(hex(bitmap_and_agg(to_binary(a, 'hex'))), 0, 6)"),
      Seq(Row("FF0000")))

    // Test empty result (no rows) - should return all 1s as AND identity
    val emptyDf = Seq.empty[String].toDF("a")
    checkAnswer(
      emptyDf.selectExpr("substring(hex(bitmap_and_agg(to_binary(a, 'hex'))), 0, 6)"),
      Seq(Row("FFFFFF")))

    val emptyDf2 = Seq.empty[(String, Int)].toDF("a", "b")
    checkAnswer(
      emptyDf2
        .selectExpr("bitmap_and_agg(to_binary(a, 'hex')) " +
          "filter (where b = 1) as and_agg")
        .select(substring(hex(col("and_agg")), 0, 6)),
      Seq(Row("FFFFFF")))

    // Test empty result (no rows) - should return empty DataFrame
    val emptyDf3 = Seq.empty[(String, Int, Int)].toDF("a", "b", "c")
    checkAnswer(
      emptyDf3
        .groupBy("c")
        .agg(expr("bitmap_and_agg(to_binary(a, 'hex')) " +
          "filter (where b = 1) as and_agg").alias("and_agg"))
        .select(substring(hex(col("and_agg")), 0, 6)),
      Seq())
  }

  test("bitmap_and_agg with complex bitmaps from bitmap_construct_agg") {
    val table = "bitmap_and_test_table"
    withTable(table) {
      // Create test data with overlapping bit positions
      spark.sql(s"""
           | CREATE TABLE $table (group_id INT, bit_pos LONG)
           | """.stripMargin)
      spark.sql(s"""
           | INSERT INTO $table VALUES
           | (1, 1), (1, 3), (1, 5),  -- Group 1: bits 1,3,5 set
           | (2, 1), (2, 2), (2, 3),  -- Group 2: bits 1,2,3 set
           | (3, 3), (3, 4), (3, 5)   -- Group 3: bits 3,4,5 set
           | """.stripMargin)
      // Each group should have their respective bit counts, but when we AND them together
      // we should get the intersection
      val intersectionResult = spark.sql(s"""
           | SELECT bitmap_count(
           |   bitmap_and_agg(group_bitmap)
           | ) as intersection_count
           | FROM (
           |   SELECT bitmap_construct_agg(bitmap_bit_position(bit_pos)) as group_bitmap
           |   FROM $table
           |   GROUP BY group_id
           | )
           | """.stripMargin)
      // The intersection should be 1 (only bit 3 is common)
      checkAnswer(intersectionResult, Seq(Row(1)))
    }
  }

  test("bitmap_count called with non-binary type") {
    val df = Seq(12).toDF("a")
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("bitmap_count(a)")
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"bitmap_count(a)\"",
        "paramIndex" -> "first",
        "requiredType" -> "\"BINARY\"",
        "inputSql" -> "\"a\"",
        "inputType" -> "\"INT\""
      ),
      context = ExpectedContext(
        fragment = "bitmap_count(a)",
        start = 0,
        stop = 14
      )
    )
  }

  test("bitmap_or_agg called with non-binary type") {
    val df = Seq(12).toDF("a")
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("bitmap_or_agg(a)")
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"bitmap_or_agg(a)\"",
        "paramIndex" -> "first",
        "requiredType" -> "\"BINARY\"",
        "inputSql" -> "\"a\"",
        "inputType" -> "\"INT\""
      ),
      context = ExpectedContext(
        fragment = "bitmap_or_agg(a)",
        start = 0,
        stop = 15
      )
    )
  }

  test("bitmap_and_agg called with non-binary type") {
    val df = Seq(12).toDF("a")
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("bitmap_and_agg(a)")
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"bitmap_and_agg(a)\"",
        "paramIndex" -> "first",
        "requiredType" -> "\"BINARY\"",
        "inputSql" -> "\"a\"",
        "inputType" -> "\"INT\""),
      context = ExpectedContext(fragment = "bitmap_and_agg(a)", start = 0, stop = 16))
  }
}
