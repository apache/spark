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
}
