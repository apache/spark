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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.CalendarInterval

class CollationAggregationSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def assertUsesHashAggregate(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(exists(plan)(_.isInstanceOf[HashAggregateExec]), plan.toString)
    assert(!exists(plan)(_.isInstanceOf[SortAggregateExec]), plan.toString)
  }

  private def assertUsesObjectHashAggregate(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(exists(plan)(_.isInstanceOf[ObjectHashAggregateExec]), plan.toString)
    assert(!exists(plan)(_.isInstanceOf[SortAggregateExec]), plan.toString)
  }

  private def assertUsesSortAggregate(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(exists(plan)(_.isInstanceOf[SortAggregateExec]), plan.toString)
    assert(!exists(plan)(_.isInstanceOf[HashAggregateExec]), plan.toString)
    assert(!exists(plan)(_.isInstanceOf[ObjectHashAggregateExec]), plan.toString)
  }

  private def generatedCode(df: DataFrame): String = {
    flatMap(df.queryExecution.executedPlan) {
      case stage: WholeStageCodegenExec => Seq(stage.doCodeGen()._2.body)
      case _ => Nil
    }.mkString("\n")
  }

  test("HashAggregateExec groups collated string keys") {
    val testCases = Seq(
      "UTF8_LCASE" -> Seq("hello", "HELLO", "HeLlO"),
      "UNICODE_CI" -> Seq("hello", "HELLO", "HeLlO"),
      "UTF8_BINARY_RTRIM" -> Seq("hello", "hello ", "hello  "))

    testCases.foreach { case (collation, spellings) =>
      withTempView("collated_keys") {
        val values = spellings.zipWithIndex
          .map { case (key, index) => s"('$key', ${index + 1})" }
          .mkString(", ")
        sql(
          s"""
             |SELECT CAST(key AS STRING COLLATE $collation) AS key, value
             |FROM VALUES $values, (NULL, 4) AS data(key, value)
             |""".stripMargin).createOrReplaceTempView("collated_keys")

        val result = sql(
          """
            |SELECT LOWER(RTRIM(key)) AS normalized_key, SUM(value) AS total
            |FROM collated_keys
            |GROUP BY key
            |ORDER BY normalized_key NULLS LAST
            |""".stripMargin)
        assertUsesHashAggregate(result)
        checkAnswer(result, Seq(Row("hello", 6L), Row(null, 4L)))
      }
    }
  }

  test("HashAggregateExec preserves binary semantics for stable sibling keys") {
    withTempView("interval_keys") {
      Seq(
        ("hello", new CalendarInterval(1, 2, 3), 1),
        ("HELLO", new CalendarInterval(1, 2, 3), 2))
        .toDF("key", "calendar_interval", "value")
        .selectExpr(
          "CAST(key AS STRING COLLATE UTF8_LCASE) AS key",
          "calendar_interval",
          "value")
        .repartition(1)
        .createOrReplaceTempView("interval_keys")

      withSQLConf(
        SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString) {
        val result = sql(
          """
            |SELECT LOWER(key) AS normalized_key, SUM(value) AS total
            |FROM interval_keys
            |GROUP BY key, calendar_interval
            |""".stripMargin)
        assertUsesHashAggregate(result)
        checkAnswer(result, Row("hello", 3L))

        val objectResult = sql(
          """
            |SELECT LOWER(key) AS normalized_key, ARRAY_SORT(COLLECT_LIST(value)) AS values
            |FROM interval_keys
            |GROUP BY key, calendar_interval
            |""".stripMargin)
        assertUsesObjectHashAggregate(objectResult)
        checkAnswer(objectResult, Row("hello", Seq(1, 2)))
      }
    }

    withTempView("geometry_keys") {
      sql(
        """
          |SELECT
          |  CAST(key AS STRING COLLATE UTF8_LCASE) AS key,
          |  ST_GeomFromWKB(wkb) AS geometry,
          |  value
          |FROM VALUES
          |  ('hello', X'0101000000000000000000F03F0000000000000040', 1),
          |  ('HELLO', X'0101000000000000000000F03F0000000000000040', 2)
          |AS data(key, wkb, value)
          |""".stripMargin)
        .repartition(1)
        .createOrReplaceTempView("geometry_keys")

      val result = sql(
        """
          |SELECT LOWER(key) AS normalized_key, SUM(value) AS total
          |FROM geometry_keys
          |GROUP BY key, geometry
          |""".stripMargin)
      assertUsesHashAggregate(result)
      checkAnswer(result, Row("hello", 3L))
    }
  }

  test("nested and distinct collated grouping works across partial and final aggregation") {
    withTempView("nested_keys") {
      sql(
        """
          |SELECT id, CAST(key AS STRING COLLATE UTF8_LCASE) AS key, bucket, value
          |FROM VALUES
          |  (1, 'alpha', 0, 10),
          |  (2, 'ALPHA', 0, 20),
          |  (3, 'beta', 1, 30),
          |  (4, 'BETA', 1, 40),
          |  (5, NULL, 2, 50),
          |  (6, NULL, 2, 60)
          |AS data(id, key, bucket, value)
          |""".stripMargin)
        .repartition(4)
        .createOrReplaceTempView("nested_keys")

      val distinctKeys = sql(
        """
          |SELECT LOWER(key) AS normalized_key
          |FROM (SELECT DISTINCT key FROM nested_keys)
          |ORDER BY normalized_key NULLS LAST
          |""".stripMargin)
      assertUsesHashAggregate(distinctKeys)
      checkAnswer(distinctKeys, Seq(Row("alpha"), Row("beta"), Row(null)))

      val result = sql(
        """
          |SELECT
          |  LOWER(array_key[0]) AS array_key,
          |  LOWER(struct_key.s) AS struct_key,
          |  bucket,
          |  distinct_ids,
          |  total
          |FROM (
          |  SELECT
          |    ARRAY(key) AS array_key,
          |    NAMED_STRUCT('s', key) AS struct_key,
          |    bucket,
          |    COUNT(DISTINCT id) AS distinct_ids,
          |    SUM(value) AS total
          |  FROM nested_keys
          |  GROUP BY ARRAY(key), NAMED_STRUCT('s', key), bucket
          |)
          |ORDER BY bucket
          |""".stripMargin)
      assertUsesHashAggregate(result)
      checkAnswer(result, Seq(
        Row("alpha", "alpha", 0, 2L, 30L),
        Row("beta", "beta", 1, 2L, 70L),
        Row(null, null, 2, 2L, 110L)))
    }
  }

  test("ObjectHashAggregateExec groups collated keys before and after sort fallback") {
    withTempView("object_keys") {
      sql(
        """
          |SELECT CAST(key AS STRING COLLATE UTF8_LCASE) AS key, value
          |FROM VALUES
          |  ('hello', 1), ('world', 4),
          |  ('HELLO', 2), ('WORLD', 5),
          |  ('HeLlO', 3), ('WoRlD', 6)
          |AS data(key, value)
          |""".stripMargin).createOrReplaceTempView("object_keys")

      Seq(Int.MaxValue.toString, "1").foreach { fallbackThreshold =>
        withSQLConf(
          SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> fallbackThreshold) {
          val result = sql(
            """
              |SELECT
              |  LOWER(key) AS normalized_key,
              |  ARRAY_SORT(COLLECT_LIST(value)) AS collected_list,
              |  ARRAY_SORT(COLLECT_SET(value)) AS collected_set
              |FROM object_keys
              |GROUP BY key
              |ORDER BY normalized_key
              |""".stripMargin)
          assertUsesObjectHashAggregate(result)
          checkAnswer(result, Seq(
            Row("hello", Seq(1, 2, 3), Seq(1, 2, 3)),
            Row("world", Seq(4, 5, 6), Seq(4, 5, 6))))

          if (fallbackThreshold == "1") {
            assert(exists(result.queryExecution.executedPlan) {
              case aggregate: ObjectHashAggregateExec =>
                aggregate.metrics("numTasksFallBacked").value > 0
              case _ => false
            })
          }
        }
      }
    }
  }

  test("HashAggregateExec spill merges collation-equivalent keys") {
    withTempView("spill_keys") {
      sql(
        """
          |SELECT CAST(key AS STRING COLLATE UTF8_LCASE_RTRIM) AS key, value
          |FROM VALUES
          |  ('hello', 1), ('world', 4),
          |  ('HELLO ', 2), ('WORLD ', 5),
          |  ('HeLlO  ', 3), ('WoRlD  ', 6)
          |AS data(key, value)
          |""".stripMargin).createOrReplaceTempView("spill_keys")

      Seq(true, false).foreach { wholeStage =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "0, 1") {
          withClue(s"wholeStage=$wholeStage: ") {
            val result = sql(
              """
                |SELECT LOWER(RTRIM(key)) AS normalized_key, SUM(value) AS total
                |FROM spill_keys
                |GROUP BY key
                |ORDER BY normalized_key
                |""".stripMargin)
            assertUsesHashAggregate(result)
            checkAnswer(result, Seq(Row("hello", 6L), Row("world", 15L)))
            assert(exists(result.queryExecution.executedPlan) {
              case aggregate: HashAggregateExec =>
                aggregate.metrics("numTasksFallBacked").value > 0
              case _ => false
            })
          }
        }
      }
    }
  }

  test("two-level aggregation remains enabled only for binary-stable grouping keys") {
    withSQLConf(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "true") {
      val binary = sql(
        """
          |SELECT key, SUM(value)
          |FROM VALUES ('a', 1), ('a', 2), ('b', 3) AS data(key, value)
          |GROUP BY key
          |ORDER BY key
          |""".stripMargin)
      assertUsesHashAggregate(binary)
      checkAnswer(binary, Seq(Row("a", 3L), Row("b", 3L)))
      val binaryCode = generatedCode(binary)
      assert(binaryCode.contains("FastHashMap"))

      val collated = sql(
        """
          |SELECT LOWER(key) AS normalized_key, SUM(value)
          |FROM (
          |  SELECT CAST(key AS STRING COLLATE UTF8_LCASE) AS key, value
          |  FROM VALUES ('a', 1), ('A', 2) AS data(key, value)
          |)
          |GROUP BY key
          |""".stripMargin)
      assertUsesHashAggregate(collated)
      checkAnswer(collated, Row("a", 3L))
      val collatedCode = generatedCode(collated)
      assert(!collatedCode.contains("FastHashMap"))
      assert(collatedCode.contains("getAggregationBufferFromUnsafeRow"))
    }
  }

  test("SortAggregateExec groups collated keys when hash aggregation is disabled") {
    withSQLConf(
      SQLConf.USE_HASH_AGG.key -> "false",
      SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
      val result = sql(
        """
          |SELECT LOWER(key) AS normalized_key, SUM(value) AS total
          |FROM (
          |  SELECT CAST(key AS STRING COLLATE UTF8_LCASE) AS key, value
          |  FROM VALUES
          |    ('hello', 1), ('HELLO', 2),
          |    ('world', 4), ('WORLD', 5)
          |  AS data(key, value)
          |)
          |GROUP BY key
          |ORDER BY normalized_key
          |""".stripMargin)
      assertUsesSortAggregate(result)
      checkAnswer(result, Seq(Row("hello", 3L), Row("world", 9L)))
    }
  }
}
