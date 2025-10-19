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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.test.SharedSparkSession

class WatermarkClauseSuite extends AnalysisTest with SharedSparkSession {
  test("watermark") {
    // attribute reference vs expression

    // tableName, attribute reference, alias
    assertEqual(
      """
        |SELECT *
        |FROM testData
        |WATERMARK ts DELAY OF INTERVAL 10 seconds AS tbl
        |WHERE a > 1
        |""".stripMargin,
      table("testData")
        .as("tbl")
        .watermark(
          UnresolvedAttribute("ts"),
          IntervalUtils.fromIntervalString("INTERVAL 10 seconds"))
        .where($"a" > 1)
        .select(UnresolvedStar(None))
    )

    // tableName, expression, alias
    assertEqual(
      """
        |SELECT *
        |FROM testData
        |WATERMARK timestamp_seconds(value) AS eventTime DELAY OF INTERVAL 10 seconds AS tbl
        |WHERE a > 1
        |""".stripMargin,
      table("testData")
        .as("tbl")
        .watermark(
          Alias(
            UnresolvedFunction(
              Seq("timestamp_seconds"), Seq(UnresolvedAttribute("value")), isDistinct = false),
            "eventTime")(),
          IntervalUtils.fromIntervalString("INTERVAL 10 seconds"))
        .where($"a" > 1)
        .select(UnresolvedStar(None))
    )

    // aliasedQuery
    assertEqual(
      """
        |SELECT *
        |FROM
        |(
        |    SELECT *
        |    FROM testData
        |)
        |WATERMARK ts DELAY OF INTERVAL 10 seconds AS tbl
        |WHERE a > 1
        |""".stripMargin,
      table("testData")
        .select(UnresolvedStar(None))
        .as("tbl")
        .watermark(
          UnresolvedAttribute("ts"),
          IntervalUtils.fromIntervalString("INTERVAL 10 seconds"))
        .where($"a" > 1)
        .select(UnresolvedStar(None))
    )

    // subquery
    assertEqual(
      """
        |SELECT key, time
        |FROM
        |(
        |    SELECT key, time
        |    FROM
        |    testData
        |    WATERMARK timestamp_seconds(ts) AS time DELAY OF INTERVAL 10 seconds
        |)
        |AS tbl
        |WHERE key = 'a'
        |""".stripMargin,
      table("testData")
        .watermark(
          Alias(
            UnresolvedFunction(
              Seq("timestamp_seconds"), Seq(UnresolvedAttribute("ts")), isDistinct = false),
            "time")(),
          IntervalUtils.fromIntervalString("INTERVAL 10 seconds"))
        .select($"key", $"time")
        .as("tbl")
        .where($"key" === "a")
        .select($"key", $"time")
    )

    // aliasedRelation
    val src1 = UnresolvedRelation(TableIdentifier("src1")).as("s1")
    val src2 = UnresolvedRelation(TableIdentifier("src2")).as("s2")
    assertEqual(
      """
        |SELECT *
        |FROM
        |(src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id)
        |WATERMARK ts DELAY OF INTERVAL 10 seconds AS dst
        |WHERE a > 1
        |""".stripMargin,
      src1.join(src2, Inner, Option($"s1.id" === $"s2.id"))
        .watermark(
          UnresolvedAttribute("ts"),
          IntervalUtils.fromIntervalString("INTERVAL 10 seconds"))
        .as("dst")
        .where($"a" > 1)
        .select(UnresolvedStar(None))
    )

    // table valued function
    assertEqual(
      """
        |SELECT *
        |FROM
        |mock_tvf(1, 'a')
        |WATERMARK ts DELAY OF INTERVAL 10 seconds AS dst
        |WHERE a > 1
        |""".stripMargin,
      UnresolvedTableValuedFunction("mock_tvf", Seq(Literal(1), Literal("a")), Seq.empty[String])
        .watermark(
          UnresolvedAttribute("ts"),
          IntervalUtils.fromIntervalString("INTERVAL 10 seconds"))
        .as("dst")
        .where($"a" > 1)
        .select(UnresolvedStar(None))
    )

    // inline table (not allowed)
    intercept(
      """
        |SELECT *
        |FROM
        |VALUES (1, 1), (2, 2)
        |WATERMARK ts DELAY OF INTERVAL 10 seconds AS dst
        |WHERE a > 1
        |""".stripMargin,
      "Syntax error at or near 'ts'")
  }
}
