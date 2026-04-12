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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Regression tests for ResolveEventTimeWatermark in HiveSessionStateBuilder.
 *
 * SPARK-53687 introduced UnresolvedEventTimeWatermark / ResolveEventTimeWatermark but only
 * added the rule to BaseSessionStateBuilder, missing HiveSessionStateBuilder (used in all
 * Databricks production environments). Without the rule, UnresolvedEventTimeWatermark
 * (output=Nil) persists through analysis, breaking any SQL query that uses the WATERMARK clause.
 *
 * Uses TestHiveSingleton to go through HiveSessionStateBuilder, matching production behavior.
 */
class WatermarkColumnResolutionSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  private def createTestTables(): Unit = {
    sql("CREATE TABLE IF NOT EXISTS wm_signup_details " +
      "(updated_at TIMESTAMP, consumer_profile_id INT) USING PARQUET")
    sql("CREATE TABLE IF NOT EXISTS wm_profiles " +
      "(updated_at TIMESTAMP, id INT) USING PARQUET")
    sql("CREATE TABLE IF NOT EXISTS wm_events " +
      "(ts TIMESTAMP, a INT) USING PARQUET")
  }

  private def analyzeStreamSQL(query: String): Unit = {
    val plan = spark.sessionState.sqlParser.parsePlan(query)
    val analyzed = spark.sessionState.analyzer.execute(plan)
    spark.sessionState.analyzer.checkAnalysis(analyzed)
  }

  test("STREAM JOIN with WATERMARK and alias resolves columns correctly") {
    createTestTables()
    analyzeStreamSQL(
      """
        |SELECT *
        |FROM
        |  STREAM(wm_signup_details) WATERMARK updated_at DELAY OF INTERVAL 60 SECONDS d
        |JOIN
        |  STREAM(wm_profiles) WATERMARK updated_at DELAY OF INTERVAL 60 SECONDS p
        |ON d.consumer_profile_id = p.id
        |  AND d.updated_at BETWEEN p.updated_at - INTERVAL '24 hours' AND p.updated_at
        |""".stripMargin)
  }

  test("STREAM with WATERMARK and alias resolves columns in WHERE") {
    createTestTables()
    analyzeStreamSQL(
      """
        |SELECT *
        |FROM STREAM(wm_events) WATERMARK ts DELAY OF INTERVAL 10 SECONDS t
        |WHERE t.a > 1
        |""".stripMargin)
  }

  test("STREAM with WATERMARK and alias resolves columns in ORDER BY") {
    createTestTables()
    analyzeStreamSQL(
      """
        |SELECT *
        |FROM STREAM(wm_events) WATERMARK ts DELAY OF INTERVAL 10 SECONDS d
        |ORDER BY d.a
        |""".stripMargin)
  }

  test("STREAM with computed WATERMARK expression resolves correctly") {
    createTestTables()
    analyzeStreamSQL(
      """
        |SELECT *
        |FROM STREAM(wm_events) WATERMARK CAST(ts AS TIMESTAMP) AS wm_time
        |  DELAY OF INTERVAL 10 SECONDS d
        |WHERE d.a > 1
        |""".stripMargin)
  }
}
