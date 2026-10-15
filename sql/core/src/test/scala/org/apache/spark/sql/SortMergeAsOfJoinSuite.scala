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
import java.time.{Duration, Instant, LocalDateTime}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.plans.{Backward, Forward}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.SortMergeAsOfJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class SortMergeAsOfJoinSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.unset(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key)
    super.afterAll()
  }

  def prepareForAsOfJoin(): (classic.DataFrame, classic.DataFrame) = {
    val schema1 = StructType(
      StructField("a", IntegerType, false) ::
        StructField("b", StringType, false) ::
        StructField("left_val", StringType, false) :: Nil)
    val rowSeq1: List[Row] = List(
      Row(1, "x", "a"), Row(5, "y", "b"), Row(10, "z", "c"))
    val df1 = spark.createDataFrame(rowSeq1.asJava, schema1)

    val schema2 = StructType(
      StructField("a", IntegerType) ::
        StructField("b", StringType) ::
        StructField("right_val", IntegerType) :: Nil)
    val rowSeq2: List[Row] = List(
      Row(1, "v", 1), Row(2, "w", 2), Row(3, "x", 3),
      Row(6, "y", 6), Row(7, "z", 7))
    val df2 = spark.createDataFrame(rowSeq2.asJava, schema2)

    (df1, df2)
  }

  // Like checkAnswer, but also asserts the right-side buffer spilled (spillSize > 0).
  private def checkAnswerAndSpill(df: classic.DataFrame, expectedAnswer: Seq[Row]): Unit = {
    checkAnswer(df, expectedAnswer)
    val op = collectFirst(df.queryExecution.executedPlan) {
      case s: SortMergeAsOfJoinExec => s
    }
    assert(op.isDefined,
      s"Expected SortMergeAsOfJoinExec in plan:\n${df.queryExecution.executedPlan}")
    assert(op.get.metrics("spillSize").value > 0,
      s"Expected the right-side buffer to spill (spillSize > 0), " +
        s"got ${op.get.metrics("spillSize").value}")
  }

  /**
   * Rewrites each as-of comparison with the right side first, as an optimizer rule may:
   * `l.ts >= r.ts` becomes `r.ts <= l.ts`. It only flips a comparison that starts with a left
   * side column, so running it again changes nothing.
   */
  private object PutRightSideFirst extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      case j: AsOfJoin =>
        def fromLeft(e: Expression): Boolean =
          e.references.nonEmpty && e.references.subsetOf(j.left.outputSet)
        j.copy(asOfCondition = j.asOfCondition.transform {
          case GreaterThanOrEqual(l, r) if fromLeft(l) => LessThanOrEqual(r, l)
          case GreaterThan(l, r) if fromLeft(l) => LessThan(r, l)
          case LessThanOrEqual(l, r) if fromLeft(l) => GreaterThanOrEqual(r, l)
          case LessThan(l, r) if fromLeft(l) => GreaterThan(r, l)
        })
    }
  }

  private def withRightSideFirst(f: => Unit): Unit = {
    val saved = spark.experimental.extraOptimizations
    spark.experimental.extraOptimizations = saved :+ PutRightSideFirst
    try f finally spark.experimental.extraOptimizations = saved
  }

  private def withAsOfJoinAnalyzer(singlePass: Boolean)(f: => Unit): Unit = {
    withSQLConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
      f
    }
  }

  // Checks the "value" of the right row that a nearest join on DOUBLE keys matches.
  private def checkNearestDoubleTag(
      leftKey: Double,
      rightRows: Seq[Row],
      expected: String): Unit = {
    val left = spark.createDataFrame(
      List(Row(leftKey)).asJava, StructType(StructField("ts", DoubleType) :: Nil))
    val right = spark.createDataFrame(
      rightRows.asJava,
      StructType(StructField("ts", DoubleType) :: StructField("value", StringType) :: Nil))

    withClue(s"left key $leftKey: ") {
      checkAnswer(
        left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "nearest").select(right.col("value")),
        Seq(Row(expected)))
    }
  }

  test("uses SortMergeAsOfJoinExec physical operator") {
    val (df1, df2) = prepareForAsOfJoin()
    val result = df1.joinAsOf(
      df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
      joinType = "inner", tolerance = null,
      allowExactMatches = true, direction = "backward")
    val plan = result.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) {
      case _: SortMergeAsOfJoinExec => true
    }.nonEmpty, s"Expected SortMergeAsOfJoinExec in plan:\n$plan")
  }

  test("backward join - simple") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 3, "x", 3),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("backward join - usingColumns") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("backward join - left outer") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", null, null, null),
        Row(5, "y", "b", null, null, null),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("forward join") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "forward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 6, "y", 6),
        Row(10, "z", "c", null, null, null)
      ).filter(_.get(3) != null) // inner join: no match for 10
    )
  }

  test("nearest join") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 6, "y", 6),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("keeps the requested direction when a rewrite puts the right side first") {
    val schema = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val left = spark.createDataFrame(List(Row(10, "a")).asJava, schema)
    // The tie at ts=5 catches a backward join run as nearest: backward keeps the last row (r2),
    // nearest the first (r1). Forward and nearest both return r3, so only the `op.direction`
    // check catches a forward join run as nearest. One partition fixes the order within a tie.
    val right = spark.createDataFrame(
      List(Row(5, "r1"), Row(5, "r2"), Row(15, "r3"), Row(15, "r4")).asJava, schema)
      .coalesce(1)
    withRightSideFirst {
      Seq(
        ("backward", Backward, Row(10, "a", 5, "r2")),
        ("forward", Forward, Row(10, "a", 15, "r3"))
      ).foreach { case (directionName, direction, expected) =>
        val joined = left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = directionName)
        checkAnswer(joined, expected :: Nil)
        val op = collectFirst(joined.queryExecution.executedPlan) {
          case s: SortMergeAsOfJoinExec => s
        }.get
        val rightSideFirst = op.asOfCondition match {
          case c: BinaryComparison => c.left.references.subsetOf(op.right.outputSet)
          case _ => false
        }
        assert(rightSideFirst, s"Expected the right side first in ${op.asOfCondition}")
        assert(op.direction === direction)
      }
    }
  }

  test("nearest join scans past duplicate predecessors") {
    val leftSchema = StructType(StructField("ts", IntegerType) :: Nil)
    val rightSchema = StructType(
      StructField("ts", IntegerType) :: StructField("value", StringType) :: Nil)
    val left = spark.createDataFrame(List(Row(10)).asJava, leftSchema)
    val right = spark.createDataFrame(
      List(Row(6, "first"), Row(6, "second"), Row(9, "closest"), Row(11, "later")).asJava,
      rightSchema)

    checkAnswer(
      left.joinAsOf(
        right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest"),
      Seq(Row(10, 9, "closest")))
  }

  test("nearest join compares integer gaps without overflow") {
    val leftSchema = StructType(StructField("ts", IntegerType) :: Nil)
    val rightSchema = StructType(
      StructField("ts", IntegerType) :: StructField("value", StringType) :: Nil)

    Seq("false", "true").foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        val left = spark.createDataFrame(List(Row(-1000000000)).asJava, leftSchema)
        val right = spark.createDataFrame(
          List(Row(-1000000001, "near"), Row(1500000000, "far")).asJava,
          rightSchema)

        checkAnswer(
          left.joinAsOf(
            right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
            joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = "nearest"),
          Seq(Row(-1000000000, -1000000001, "near")))
      }
    }
  }

  test("nearest join compares long gaps without overflow") {
    val leftSchema = StructType(StructField("ts", LongType) :: Nil)
    val rightSchema = StructType(
      StructField("ts", LongType) :: StructField("value", StringType) :: Nil)

    Seq("false", "true").foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        val left = spark.createDataFrame(List(Row(-1L)).asJava, leftSchema)
        val right = spark.createDataFrame(
          List(Row(-2L, "near"), Row(Long.MaxValue, "far")).asJava,
          rightSchema)

        checkAnswer(
          left.joinAsOf(
            right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
            joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = "nearest"),
          Seq(Row(-1L, -2L, "near")))
      }
    }
  }

  test("nearest join keeps its only integer candidate when the gap overflows") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val left = spark.createDataFrame(
        List(Row(-1000000000)).asJava,
        StructType(StructField("ts", IntegerType) :: Nil))
      val right = spark.createDataFrame(
        List(Row(1500000000)).asJava,
        StructType(StructField("ts", IntegerType) :: Nil))

      checkAnswer(
        left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "nearest"),
        Seq(Row(-1000000000, 1500000000)))
    }
  }

  test("nearest join compares elapsed time across a DST transition") {
    withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
        SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false") {
      val leftSchema = StructType(StructField("ts", TimestampType) :: Nil)
      val rightSchema = StructType(
        StructField("ts", TimestampType) :: StructField("value", StringType) :: Nil)
      // The predecessor is closer in elapsed time (7 vs 10 minutes), but farther by wall clock.
      val leftTime = Timestamp.from(Instant.parse("2024-11-03T09:05:00Z"))
      val beforeTime = Timestamp.from(Instant.parse("2024-11-03T08:58:00Z"))
      val afterTime = Timestamp.from(Instant.parse("2024-11-03T09:15:00Z"))
      val left = spark.createDataFrame(List(Row(leftTime)).asJava, leftSchema)
      val right = spark.createDataFrame(
        List(Row(beforeTime, "near"), Row(afterTime, "far")).asJava,
        rightSchema)

      checkAnswer(
        left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "nearest"),
        Seq(Row(leftTime, beforeTime, "near")))
    }
  }

  Seq("backward", "forward", "nearest").foreach { direction =>
    test(s"$direction join rejects LTZ left and NTZ right keys") {
      val left = spark.createDataFrame(
        List(Row(Timestamp.from(Instant.parse("2024-03-10T10:05:00Z")))).asJava,
        new StructType().add("ts", TimestampType)
      )
      val right = spark.createDataFrame(
        List(Row(LocalDateTime.parse("2024-03-10T03:00:00"), "near")).asJava,
        new StructType().add("ts", TimestampNTZType).add("value", StringType)
      )

      val e = intercept[AnalysisException] {
        left.joinAsOf(
          other = right, leftAsOf = left.col("ts"), rightAsOf = right.col("ts"),
          usingColumns = Seq.empty, joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = direction
        ).collect()
      }
      checkErrorMatchPVals(
        exception = e,
        condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
        parameters = Map(
          "sqlExpr" -> ".+",
          "msg" -> ("Sort-merge as-of joins do not support mixing " +
            "TIMESTAMP_LTZ and TIMESTAMP_NTZ keys"),
          "hint" -> "")
      )
    }
  }

  gridTest("timestamp keys respect explicit user casts")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false") {
        val schema = new StructType().add("ltz", TimestampType).add("ntz", TimestampNTZType)
        val left = spark.createDataFrame(
          List(Row(
            Timestamp.from(Instant.parse("2024-11-03T08:30:00Z")),
            LocalDateTime.parse("2024-11-03T01:30:00"))).asJava,
          schema)
        val right = spark.createDataFrame(
          List(
            Row(Timestamp.from(Instant.parse("2024-11-03T08:28:00Z")),
              LocalDateTime.parse("2024-11-03T01:28:00"), "before"),
            Row(Timestamp.from(Instant.parse("2024-11-03T08:35:00Z")),
              LocalDateTime.parse("2024-11-03T01:35:00"), "after")).asJava,
          schema.add("value", StringType))

        for {
          direction <- Seq("backward", "forward", "nearest")
          (source, target, targetType) <- Seq(
            ("ltz", "ntz", TimestampNTZType), ("ntz", "ltz", TimestampType))
        } {
          def join(): classic.DataFrame = left.joinAsOf(
            other = right, leftAsOf = left.col(source).cast(targetType),
            rightAsOf = right.col(target), usingColumns = Seq.empty, joinType = "inner",
            tolerance = null, allowExactMatches = true, direction = direction
          ).select("value")

          val fallback = withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
            join().collect().toSeq
          }
          val joined = join()
          QueryTest.checkAnswer(df = joined, expectedAnswer = fallback, checkToRDD = false)
          assert(collectFirst(joined.queryExecution.executedPlan) {
            case s: SortMergeAsOfJoinExec => s
          }.isDefined)

          checkErrorMatchPVals(
            exception = intercept[AnalysisException] {
              left.joinAsOf(
                other = right, leftAsOf = left.col(source).cast(targetType),
                rightAsOf = right.col(source), usingColumns = Seq.empty, joinType = "inner",
                tolerance = null, allowExactMatches = true, direction = direction
              ).queryExecution.analyzed
            },
            condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
            parameters = Map(
              "sqlExpr" -> ".+",
              "msg" -> ("Sort-merge as-of joins do not support mixing " +
                "TIMESTAMP_LTZ and TIMESTAMP_NTZ keys"),
              "hint" -> "")
          )
        }
      }
    }
  }

  gridTest("mixed timestamp joins use fallback after enabling sort-merge")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false") {
        val leftTime = LocalDateTime.parse("2024-11-03T01:30:00")
        val left = spark.createDataFrame(
          List(Row(leftTime, Timestamp.from(Instant.parse("2024-11-03T08:30:00Z")))).asJava,
          new StructType().add("ntz", TimestampNTZType).add("ltz", TimestampType))
        val right = spark.createDataFrame(
          List(
            Row(Timestamp.from(Instant.parse("2024-11-03T08:10:00Z")), "far"),
            Row(Timestamp.from(Instant.parse("2024-11-03T09:20:00Z")), "ineligible"),
            Row(Timestamp.from(Instant.parse("2024-11-03T08:25:00Z")), "near")).asJava,
          new StructType().add("ts", TimestampType).add("value", StringType))

        withTempView("asof_before_config_change") {
          for {
            (leftKey, rightKey) <- Seq(
              (left.col("ntz"), right.col("ts")),
              (left.col("ltz").cast(TimestampNTZType), right.col("ts")),
              (functions.lit(leftTime), right.col("ts")),
              (functions.lit(Timestamp.from(Instant.parse("2024-11-03T08:30:00Z"))),
                right.col("ts").cast(TimestampNTZType)))
            throughView <- Seq(false, true)
          } {
            def join(): classic.DataFrame = left.joinAsOf(
              other = right, leftAsOf = leftKey, rightAsOf = rightKey,
              usingColumns = Seq.empty, joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = "backward"
            ).select("value")

            val (analyzed, expected) =
              withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
                val expected = join().collect().toSeq
                assert(expected == Seq(Row("near")))
                val joined = join()
                val analyzed = if (throughView) {
                  // Single-pass analysis does not support the view registration command.
                  withAsOfJoinAnalyzer(singlePass = false) {
                    joined.createOrReplaceTempView("asof_before_config_change")
                  }
                  spark.table("asof_before_config_change")
                } else {
                  joined
                }
                assert(analyzed.queryExecution.analyzed.resolved)
                (analyzed, expected)
              }

            QueryTest.checkAnswer(df = analyzed, expectedAnswer = expected, checkToRDD = false)
            assert(collectFirst(analyzed.queryExecution.executedPlan) {
              case s: SortMergeAsOfJoinExec => s
            }.isEmpty)
          }
        }
      }
    }
  }

  gridTest("supported keys use sort-merge after enabling the config")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      val schema = new StructType().add("id", IntegerType)
        .add("ltz", TimestampType).add("ntz", TimestampNTZType)
      val left = spark.createDataFrame(
        List(Row(10, Timestamp.from(Instant.parse("2024-01-01T10:00:00Z")),
          LocalDateTime.parse("2024-01-01T10:00:00"))).asJava,
        schema)
      val right = spark.createDataFrame(
        List(Row(9, Timestamp.from(Instant.parse("2024-01-01T09:00:00Z")),
          LocalDateTime.parse("2024-01-01T09:00:00"), "match")).asJava,
        schema.add("value", StringType))

      Seq("id", "ltz", "ntz").foreach { key =>
        val joined = withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
          left.joinAsOf(
            other = right, leftAsOf = left.col(key), rightAsOf = right.col(key),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = "nearest"
          ).select("value")
        }
        QueryTest.checkAnswer(df = joined, expectedAnswer = Seq(Row("match")), checkToRDD = false)
        assert(collectFirst(joined.queryExecution.executedPlan) {
          case s: SortMergeAsOfJoinExec => s
        }.isDefined)
      }
    }
  }

  gridTest("rejects mixed timestamp literal keys before constant folding")(Seq(false, true)) {
      singlePass =>
    withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
        SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
      val literalInput = spark.range(1).toDF()
      val literalKey = functions.lit(Timestamp.from(Instant.parse("2024-11-03T09:10:00Z")))
      val columnInput = spark.createDataFrame(
        List(
          Row(LocalDateTime.parse("2024-11-03T01:05:00")),
          Row(LocalDateTime.parse("2024-11-03T01:20:00"))).asJava,
        new StructType().add("ts", TimestampNTZType)
      )

      for {
        direction <- Seq("nearest", "backward", "forward")
        literalOnLeft <- Seq(true, false)
        tolerance <- Seq(null, functions.lit(Duration.ofHours(2)))
      } {
        val (left, right, leftKey, rightKey) = if (literalOnLeft) {
          (literalInput, columnInput, literalKey, columnInput.col("ts"))
        } else {
          (columnInput, literalInput, columnInput.col("ts"), literalKey)
        }
        withClue(s"direction=$direction, literalOnLeft=$literalOnLeft, tolerance=$tolerance: ") {
          checkErrorMatchPVals(
            exception = intercept[AnalysisException] {
              left.joinAsOf(
                other = right, leftAsOf = leftKey, rightAsOf = rightKey,
                usingColumns = Seq.empty, joinType = "inner", tolerance = tolerance,
                allowExactMatches = true, direction = direction
              ).queryExecution.analyzed
            },
            condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
            parameters = Map(
              "sqlExpr" -> ".+",
              "msg" -> ("Sort-merge as-of joins do not support mixing " +
                "TIMESTAMP_LTZ and TIMESTAMP_NTZ keys"),
              "hint" -> "")
          )
        }
      }

      withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
        val fallback = literalInput.joinAsOf(
          other = columnInput, leftAsOf = literalKey, rightAsOf = columnInput.col("ts"),
          usingColumns = Seq.empty, joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "nearest"
        )
        assert(fallback.queryExecution.analyzed.resolved)
      }
    }
  }

  gridTest("rejects mixed timestamp keys with STRING and DATE casts")(Seq(false, true)) {
      singlePass =>
    withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
        SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
      val left = spark.createDataFrame(
        List(Row("2024-11-03 01:15:00", Date.valueOf("2024-11-03"))).asJava,
        new StructType().add("stringKey", StringType).add("dateKey", DateType)
      )
      val right = spark.createDataFrame(
        List(
          Row(Timestamp.from(Instant.parse("2024-11-03T08:05:00Z")), "far"),
          Row(Timestamp.from(Instant.parse("2024-11-03T09:10:00Z")), "ineligible"),
          Row(Timestamp.from(Instant.parse("2024-11-03T08:12:00Z")), "near")).asJava,
        new StructType().add("ts", TimestampType).add("value", StringType)
      )

      for {
        key <- Seq("stringKey", "dateKey")
        direction <- Seq("backward", "forward", "nearest")
      } {
        withClue(s"key=$key, direction=$direction: ") {
          checkErrorMatchPVals(
            exception = intercept[AnalysisException] {
              left.joinAsOf(
                other = right, leftAsOf = left.col(key).cast(TimestampNTZType),
                rightAsOf = right.col("ts"), usingColumns = Seq.empty, joinType = "inner",
                tolerance = null, allowExactMatches = true, direction = direction
              ).queryExecution.analyzed
            },
            condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
            parameters = Map(
              "sqlExpr" -> ".+",
              "msg" -> ("Sort-merge as-of joins do not support mixing " +
                "TIMESTAMP_LTZ and TIMESTAMP_NTZ keys"),
              "hint" -> "")
          )
        }
      }
    }
  }

  Seq("backward", "forward", "nearest").foreach { direction =>
    test(s"$direction join compares nanosecond gaps") {
      withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
        val leftTime = Instant.parse("2020-01-01T00:00:00.000001000Z")
        val beforeTime = Instant.parse("2020-01-01T00:00:00.000000100Z")
        val afterTime = Instant.parse("2020-01-01T00:00:00.000001800Z")
        val left = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(leftTime))),
          new StructType().add("ts", TimestampLTZNanosType(9)))
        val right = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(
            Row(beforeTime, "before"), Row(afterTime, "after"))),
          new StructType().add("ts", TimestampLTZNanosType(9)).add("value", StringType))
        val expected = if (direction == "backward") {
          Row(leftTime, beforeTime, "before")
        } else {
          Row(leftTime, afterTime, "after")
        }

        checkAnswer(
          left.joinAsOf(
            other = right, leftAsOf = left.col("ts"), rightAsOf = right.col("ts"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = direction
          ),
          Seq(expected)
        )
      }
    }
  }

  Seq("backward", "forward", "nearest").foreach { direction =>
    test(s"$direction join compares timestamp without time zone nanoseconds") {
      withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
        val leftTime = LocalDateTime.parse("2020-01-01T00:00:00.000001000")
        val beforeTime = LocalDateTime.parse("2020-01-01T00:00:00.000000100")
        val afterTime = LocalDateTime.parse("2020-01-01T00:00:00.000001800")
        val left = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(leftTime))),
          new StructType().add("ts", TimestampNTZNanosType(9)))
        val right = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(
            Row(beforeTime, "before"), Row(afterTime, "after"))),
          new StructType().add("ts", TimestampNTZNanosType(9)).add("value", StringType))
        val expected = if (direction == "backward") {
          Row(leftTime, beforeTime, "before")
        } else {
          Row(leftTime, afterTime, "after")
        }

        checkAnswer(
          left.joinAsOf(
            other = right, leftAsOf = left.col("ts"), rightAsOf = right.col("ts"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = direction
          ),
          Seq(expected)
        )
      }
    }
  }

  test("nearest timestamp join with legacy intervals handles one and two candidates") {
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      val left = spark.sql("SELECT TIMESTAMP '2024-01-02 00:00:00' AS k")
      val oneRight = spark.sql(
        "SELECT TIMESTAMP '2024-01-01 00:00:00' AS k, 'near' AS tag")
      val twoRight = spark.sql(
        "SELECT TIMESTAMP '2024-01-01 00:00:00' AS k, 'near' AS tag " +
          "UNION ALL SELECT TIMESTAMP '2024-01-04 00:00:00' AS k, 'far' AS tag")

      Seq(oneRight, twoRight).foreach { right =>
        checkAnswer(
          left.joinAsOf(
            right, left.col("k"), right.col("k"), usingColumns = Seq.empty,
            joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = "nearest").select(right.col("tag")),
          Seq(Row("near")))
      }
    }
  }

  test("nearest DATE join with legacy intervals handles one and two candidates") {
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      val left = spark.sql("SELECT DATE '2024-01-02' AS k")
      val oneRight = spark.sql("SELECT DATE '2024-01-01' AS k, 'near' AS tag")
      val twoRight = spark.sql(
        "SELECT DATE '2024-01-01' AS k, 'near' AS tag " +
          "UNION ALL SELECT DATE '2024-01-04' AS k, 'far' AS tag")

      Seq(oneRight, twoRight).foreach { right =>
        checkAnswer(
          left.joinAsOf(
            right, left.col("k"), right.col("k"), usingColumns = Seq.empty,
            joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = "nearest").select(right.col("tag")),
          Seq(Row("near")))
      }
    }
  }

  Seq(
    ("DECIMAL(38, 0)", "-99999999999999999999999999999999999998",
      "-99999999999999999999999999999999999999", "2"),
    ("INTERVAL YEAR TO MONTH", "INTERVAL '-100' MONTH",
      "INTERVAL '-101' MONTH", "INTERVAL '178956970-7' YEAR TO MONTH"),
    ("INTERVAL DAY TO SECOND", "INTERVAL '-100' DAY",
      "INTERVAL '-101' DAY", "INTERVAL '106751991' DAY")
  ).foreach { case (dataType, leftValue, nearValue, farValue) =>
    test(s"nearest $dataType keeps a near row before an overflowing far row") {
      Seq(false, true).foreach { ansiEnabled =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
          val left = spark.sql(s"SELECT CAST($leftValue AS $dataType) AS k")
          val right = spark.sql(
            s"SELECT CAST($nearValue AS $dataType) AS k, 'near' AS tag " +
              s"UNION ALL SELECT CAST($farValue AS $dataType) AS k, 'far' AS tag")

          checkAnswer(
            left.joinAsOf(
              right, left.col("k"), right.col("k"), usingColumns = Seq.empty,
              joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = "nearest").select(right.col("tag")),
            Seq(Row("near")))
        }
      }
    }
  }

  test("backward join - tolerance = 1") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner",
        tolerance = functions.lit(1),
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(10, "z", "c", null, null, null)
      ).filter(_.get(3) != null)
    )
  }

  test("backward join - allowExactMatches = false") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = false, direction = "backward"),
      Seq(
        // left.a=1: no right row with a < 1 -> no match
        // left.a=5: right.a=3 (3 < 5) -> match
        Row(5, "y", "b", 3, "x", 3),
        // left.a=10: right.a=7 (7 < 10) -> match
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("empty left side") {
    val (_, df2) = prepareForAsOfJoin()
    val emptyDf = spark.createDataFrame(
      java.util.Collections.emptyList[Row](),
      StructType(
        StructField("a", IntegerType, false) ::
          StructField("b", StringType, false) ::
          StructField("left_val", StringType, false) :: Nil))
    checkAnswer(
      emptyDf.joinAsOf(
        df2, emptyDf.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq.empty
    )
  }

  test("empty right side") {
    val (df1, _) = prepareForAsOfJoin()
    val emptyDf = spark.createDataFrame(
      java.util.Collections.emptyList[Row](),
      StructType(
        StructField("a", IntegerType) ::
          StructField("b", StringType) ::
          StructField("right_val", IntegerType) :: Nil))
    // Inner join: no matches possible
    checkAnswer(
      df1.joinAsOf(
        emptyDf, df1.col("a"), emptyDf.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq.empty
    )
    // Left outer: all left rows with null right
    checkAnswer(
      df1.joinAsOf(
        emptyDf, df1.col("a"), emptyDf.col("a"), usingColumns = Seq.empty,
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", null, null, null),
        Row(5, "y", "b", null, null, null),
        Row(10, "z", "c", null, null, null)
      )
    )
  }

  test("null as-of keys") {
    val schema1 = StructType(
      StructField("a", IntegerType, true) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("a", IntegerType, true) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(null, "x"), Row(3, "y"), Row(7, "z")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(1, "a"), Row(null, "b"), Row(5, "c")).asJava, schema2)
    // Null as-of keys should not match anything (as-of condition
    // evaluates to null for null inputs)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(null, "x", null, null),
        Row(3, "y", 1, "a"),
        Row(7, "z", 5, "c")
      )
    )
  }

  test("multiple rows with same equi-key") {
    val schema1 = StructType(
      StructField("grp", StringType) ::
        StructField("ts", IntegerType) :: Nil)
    val schema2 = StructType(
      StructField("grp", StringType) ::
        StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(
        Row("A", 5), Row("A", 10), Row("A", 15),
        Row("B", 3), Row("B", 8)
      ).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(
        Row("A", 2, "a1"), Row("A", 7, "a2"), Row("A", 12, "a3"),
        Row("B", 1, "b1"), Row("B", 6, "b2"), Row("B", 10, "b3")
      ).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq("grp"),
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row("A", 5, "A", 2, "a1"),
        Row("A", 10, "A", 7, "a2"),
        Row("A", 15, "A", 12, "a3"),
        Row("B", 3, "B", 1, "b1"),
        Row("B", 8, "B", 6, "b2")
      )
    )
  }

  test("long type as-of key") {
    val schema1 = StructType(
      StructField("ts", LongType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", LongType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(100L, "a"), Row(200L, "b"), Row(300L, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(50L, "x"), Row(150L, "y"), Row(250L, "z")).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(100L, "a", 50L, "x"),
        Row(200L, "b", 150L, "y"),
        Row(300L, "c", 250L, "z")
      )
    )
  }

  test("double type as-of key") {
    val schema1 = StructType(
      StructField("ts", DoubleType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", DoubleType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(1.5, "a"), Row(3.0, "b"), Row(5.5, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(1.0, "x"), Row(2.5, "y"), Row(4.0, "z")).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1.5, "a", 1.0, "x"),
        Row(3.0, "b", 2.5, "y"),
        Row(5.5, "c", 4.0, "z")
      )
    )
  }

  for {
    (leftType, rightType) <- Seq(
      ("TIMESTAMP", "TIMESTAMP"), ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ"), ("DATE", "DATE"),
      ("DATE", "TIMESTAMP"), ("TIMESTAMP", "DATE"),
      ("DATE", "TIMESTAMP_NTZ"), ("TIMESTAMP_NTZ", "DATE"))
    (direction, expected) <- Seq(
      ("backward", "before"), ("forward", "after"), ("nearest", "before"))
  } {
    val keyTypes = if (leftType == rightType) leftType else s"$leftType and $rightType"
    test(s"$direction join with $keyTypes keys") {
      Seq("false", "true").foreach { legacyInterval =>
        withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> legacyInterval) {
          val left = spark.sql(s"SELECT CAST('2024-01-02' AS $leftType) AS k")
          val right = spark.sql(
            Seq(
              "2023-12-31" -> "far before", "2024-01-01" -> "before",
              "2024-01-04" -> "after", "2024-01-05" -> "far after"
            ).map { case (key, tag) =>
              s"SELECT CAST('$key' AS $rightType) AS k, '$tag' AS tag"
            }.mkString(" UNION ALL ")
          )
          val result = left.joinAsOf(
            other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = direction
          ).select(right.col("tag"))

          checkAnswer(result, Seq(Row(expected)))
          assert(collectFirst(result.queryExecution.executedPlan) {
            case s: SortMergeAsOfJoinExec => s
          }.isDefined, s"Expected SortMergeAsOfJoinExec in plan:\n" +
            result.queryExecution.executedPlan)
        }
      }
    }
  }

  for {
    keyType <- Seq("DATE", "TIMESTAMP", "TIMESTAMP_NTZ")
    direction <- Seq("backward", "forward", "nearest")
  } {
    test(s"$direction join with $keyType keys, interval tolerance and exact matches") {
      for {
        legacyInterval <- Seq(false, true)
        allowExactMatches <- Seq(false, true)
      } {
        withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> legacyInterval.toString) {
          val left = spark.sql(
            s"""SELECT case_id, CAST('2024-01-10' AS $keyType) AS k
               |FROM VALUES ('inside'), ('boundary'), ('outside'), ('exact') AS t(case_id)
               |""".stripMargin
          )
          val right = spark.sql(
            s"""SELECT case_id, CAST(k AS $keyType) AS k, tag
               |FROM VALUES
               |  ('inside', '2024-01-07', 'far before'),
               |  ('inside', '2024-01-09', 'before'),
               |  ('inside', '2024-01-11', 'after'),
               |  ('inside', '2024-01-13', 'far after'),
               |  ('boundary', '2024-01-08', 'before'),
               |  ('boundary', '2024-01-12', 'after'),
               |  ('outside', '2024-01-07', 'before'),
               |  ('outside', '2024-01-13', 'after'),
               |  ('exact', '2024-01-09', 'before'),
               |  ('exact', '2024-01-10', 'exact'),
               |  ('exact', '2024-01-11', 'after')
               |AS t(case_id, k, tag)
               |""".stripMargin
          )
          val result = left.joinAsOf(
            other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
            usingColumns = Seq("case_id"), joinType = "leftouter",
            tolerance = functions.lit(Duration.ofDays(2)),
            allowExactMatches = allowExactMatches, direction = direction
          ).select(left.col("case_id"), right.col("tag"))
          val closest = if (direction == "forward") "after" else "before"

          withClue(s"legacyInterval=$legacyInterval, allowExactMatches=$allowExactMatches: ") {
            checkAnswer(
              result,
              Seq(
                Row("inside", closest),
                Row("boundary", if (allowExactMatches) closest else null),
                Row("outside", null),
                Row("exact", if (allowExactMatches) "exact" else closest)
              )
            )
          }
        }
      }
    }
  }

  test("conf disabled falls back to correlated subquery rewrite") {
    val (df1, df2) = prepareForAsOfJoin()
    withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
      val result = df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward")
      val plan = result.queryExecution.executedPlan
      assert(collectWithSubqueries(plan) {
        case _: SortMergeAsOfJoinExec => true
      }.isEmpty, "Should NOT use SortMergeAsOfJoinExec when conf is disabled")
      // Results should still be correct
      checkAnswer(result, Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 3, "x", 3),
        Row(10, "z", "c", 7, "z", 7)
      ))
    }
  }

  test("self join") {
    val schema = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df = spark.createDataFrame(
      List(Row(1, "a"), Row(3, "b"), Row(5, "c")).asJava, schema)
    checkAnswer(
      df.joinAsOf(
        df, df.col("ts"), df.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "a", 1, "a"),
        Row(3, "b", 3, "b"),
        Row(5, "c", 5, "c")
      )
    )
  }

  test("no equi-key - all rows in single partition") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(2, "a"), Row(5, "b"), Row(9, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(1, "x"), Row(4, "y"), Row(7, "z")).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(2, "a", 1, "x"),
        Row(5, "b", 4, "y"),
        Row(9, "c", 7, "z")
      )
    )
  }

  test("forward join - left outer with no match") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(1, "a"), Row(5, "b"), Row(10, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(3, "x"), Row(7, "y")).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "forward"),
      Seq(
        Row(1, "a", 3, "x"),
        Row(5, "b", 7, "y"),
        Row(10, "c", null, null) // no right row >= 10
      )
    )
  }

  test("forward join - tolerance") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(1, "a"), Row(5, "b"), Row(10, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(2, "x"), Row(7, "y"), Row(15, "z")).asJava, schema2)
    // tolerance = 3: only match if right.ts <= left.ts + 3
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner",
        tolerance = functions.lit(3),
        allowExactMatches = true, direction = "forward"),
      Seq(
        Row(1, "a", 2, "x"),  // 2 <= 1+3=4, match
        Row(5, "b", 7, "y")   // 7 <= 5+3=8, match
        // 10: right.ts=15, 15 > 10+3=13, no match
      )
    )
  }

  test("nearest join - tolerance") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(10, "a"), Row(20, "b")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(5, "x"), Row(8, "w"), Row(11, "v"), Row(12, "y"), Row(25, "z")).asJava, schema2)
    // tolerance = 3: only match if |left.ts - right.ts| <= 3. For left.ts=10 the closest row
    // (11) is neither the first (8) nor the last (12) row in range, so a forward or backward
    // scan would pick a different row.
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "leftouter",
        tolerance = functions.lit(3),
        allowExactMatches = true, direction = "nearest"),
      Seq(
        Row(10, "a", 11, "v"),  // |10-11|=1 is the closest in [7, 13]
        Row(20, "b", null, null) // |20-25|=5 > 3, no match
      )
    )
  }

  test("nearest join - equidistant right rows") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(10, "a")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(8, "x"), Row(12, "y")).asJava, schema2)
    // Both are distance 2 from left.ts=10. On an equal gap, the row before the left key wins.
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner",
        tolerance = functions.lit(5),
        allowExactMatches = true, direction = "nearest"),
      Row(10, "a", 8, "x") :: Nil)
  }

  test("nearest join keeps the closest floating-point predecessor") {
    // Both gaps round to the DOUBLE 1.0e20, but the later predecessor is closer.
    checkNearestDoubleTag(1.0e20, Seq(Row(1.0, "far"), Row(2.0, "near")), "near")
  }

  test("nearest join compares floating-point gaps exactly") {
    // Both gaps round to the DOUBLE 1.0e20, but the successor is closer.
    checkNearestDoubleTag(1.0, Seq(Row(-1.0e20, "far"), Row(1.0e20, "near")), "near")
  }

  test("nearest join with NaN and infinite floating-point keys") {
    // NaN sorts above every other value. An exact match wins, although Inf - Inf is NaN.
    val inf = Double.PositiveInfinity
    val nan = Double.NaN
    checkNearestDoubleTag(inf, Seq(Row(1.0, "far"), Row(inf, "near")), "near")
    checkNearestDoubleTag(nan, Seq(Row(5.0, "far"), Row(nan, "near")), "near")
    // A gap to an infinite key is infinite, and a gap to NaN is larger still.
    checkNearestDoubleTag(1.0, Seq(Row(-inf, "far"), Row(3.0, "near")), "near")
    checkNearestDoubleTag(1.0, Seq(Row(0.0, "near"), Row(inf, "far")), "near")
    checkNearestDoubleTag(1.0, Seq(Row(0.0, "near"), Row(nan, "far")), "near")
    checkNearestDoubleTag(1.0, Seq(Row(-inf, "near"), Row(nan, "far")), "near")
    checkNearestDoubleTag(inf, Seq(Row(1.0, "near"), Row(nan, "far")), "near")
    // Two infinite gaps are equal, so the row before wins.
    checkNearestDoubleTag(1.0, Seq(Row(-inf, "near"), Row(inf, "far")), "near")
    // The finite gap of 2 * Double.MaxValue does not fit in a DOUBLE, but is still finite.
    checkNearestDoubleTag(
      -Double.MaxValue, Seq(Row(-inf, "far"), Row(Double.MaxValue, "near")), "near")
    // A single candidate wins, whatever its gap.
    checkNearestDoubleTag(-inf, Seq(Row(nan, "near")), "near")
    checkNearestDoubleTag(nan, Seq(Row(-inf, "near")), "near")
  }

  gridTest("nearest join rejects STRING keys during analysis")(Seq(false, true)) { singlePass =>
    // With ANSI on, analysis already rejects the subtraction of two strings.
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        val left = spark.sql("SELECT '50' AS k")
        val right = spark.sql("SELECT '10' AS k, 'ten' AS tag")

        def join(): classic.DataFrame = left.joinAsOf(
          other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
          usingColumns = Seq.empty, joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "nearest"
        )

        checkError(
          exception = intercept[AnalysisException] {
            join().queryExecution.analyzed
          },
          condition = "AS_OF_JOIN.UNSUPPORTED_NEAREST_KEY_TYPE",
          parameters = Map("dataType" -> "\"STRING\"")
        )

        val analyzed = withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
          QueryTest.checkAnswer(
            df = join(), expectedAnswer = Seq(Row("50", "10", "ten")), checkToRDD = false)
          join()
        }
        QueryTest.checkAnswer(
          df = analyzed, expectedAnswer = Seq(Row("50", "10", "ten")), checkToRDD = false)
        assert(collectFirst(analyzed.queryExecution.executedPlan) {
          case s: SortMergeAsOfJoinExec => s
        }.isEmpty)
      }
    }
  }

  test("nearest join accepts STRING keys cast to numbers") {
    val left = spark.sql("SELECT '50' AS k")
    val right = spark.sql(
      "SELECT '9' AS k, 'nine' AS tag UNION ALL SELECT '10' AS k, 'ten' AS tag " +
        "UNION ALL SELECT '100' AS k, 'hundred' AS tag")

    checkAnswer(
      left.joinAsOf(
        right, left.col("k").cast("double"), right.col("k").cast("double"),
        usingColumns = Seq.empty, joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest").select(right.col("tag")),
      Seq(Row("ten")))
  }

  test("nearest join with swapped as-of columns") {
    val left = spark.sql("SELECT 10 AS lk")
    val right = spark.sql("SELECT 7 AS rk, 'far' AS tag UNION ALL SELECT 12 AS rk, 'near' AS tag")

    // The left as-of key comes from the right side, and the right one from the left side.
    checkAnswer(
      left.joinAsOf(
        right, right.col("rk"), left.col("lk"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest").select(right.col("tag")),
      Seq(Row("near")))
  }

  test("nearest join with a LONG constant key against an INT column") {
    // The optimizer rewrites 2147483647L > CAST(k AS BIGINT) to NOT (k = 2147483647).
    val left = spark.sql("SELECT 1 AS id")
    val right = spark.sql(
      "SELECT 2147483644 AS k, 'far' AS tag UNION ALL SELECT 2147483646 AS k, 'near' AS tag")

    checkAnswer(
      left.joinAsOf(
        right, functions.lit(Int.MaxValue.toLong), right.col("k"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest").select(right.col("tag")),
      Seq(Row("near")))
  }

  test("nearest join with a DECIMAL constant key against an INT column") {
    // The analyzer rewrites 5.4BD > k to 6 > k, which has the same result for an INT k.
    val left = spark.sql("SELECT 1 AS id")
    val right = spark.sql("SELECT 5 AS k, 'near' AS tag UNION ALL SELECT 6 AS k, 'far' AS tag")

    checkAnswer(
      left.joinAsOf(
        right, functions.lit(new java.math.BigDecimal("5.4")), right.col("k"),
        usingColumns = Seq.empty, joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest").select(right.col("tag")),
      Seq(Row("near")))
  }

  test("nearest join with DECIMAL keys of different scales") {
    val left = spark.sql("SELECT CAST(1 AS DECIMAL(10, 2)) AS k")
    val right = spark.sql(
      "SELECT CAST(0.5 AS DECIMAL(20, 4)) AS k, 'far' AS tag " +
        "UNION ALL SELECT CAST(1.25 AS DECIMAL(20, 4)) AS k, 'near' AS tag")

    checkAnswer(
      left.joinAsOf(
        right, left.col("k"), right.col("k"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest").select(right.col("tag")),
      Seq(Row("near")))
  }

  test("nearest join compares scaled decimal gaps exactly") {
    // DECIMAL(38, 10) - DECIMAL(38, 10) is DECIMAL(38, 9), so both gaps round to 0.
    withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> "true") {
      val left = spark.sql("SELECT CAST(0 AS DECIMAL(38, 10)) AS k")
      val right = spark.sql(
        "SELECT CAST(-0.0000000003 AS DECIMAL(38, 10)) AS k, 'far' AS tag " +
          "UNION ALL SELECT CAST(0.0000000002 AS DECIMAL(38, 10)) AS k, 'near' AS tag")

      checkAnswer(
        left.joinAsOf(
          right, left.col("k"), right.col("k"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "nearest").select(right.col("tag")),
        Seq(Row("near")))
    }
  }

  test("nearest join scans past duplicate floating-point predecessors") {
    val leftSchema = StructType(StructField("ts", DoubleType) :: Nil)
    val rightSchema = StructType(
      StructField("ts", DoubleType) :: StructField("value", StringType) :: Nil)
    val left = spark.createDataFrame(List(Row(10.0)).asJava, leftSchema)
    val right = spark.createDataFrame(
      List(
        Row(6.0, "first"), Row(6.0, "second"), Row(9.0, "closest"), Row(11.5, "later")
      ).asJava,
      rightSchema)

    checkAnswer(
      left.joinAsOf(
        right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest"),
      Seq(Row(10.0, 9.0, "closest")))
  }

  test("forward join - allowExactMatches = false") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = false, direction = "forward"),
      Seq(
        // left.a=1: right.a must be > 1 -> right.a=2
        Row(1, "x", "a", 2, "w", 2),
        // left.a=5: right.a must be > 5 -> right.a=6
        Row(5, "y", "b", 6, "y", 6),
        // left.a=10: no right.a > 10
        Row(10, "z", "c", null, null, null)
      ).filter(_.get(3) != null)
    )
  }

  test("nearest join - allowExactMatches = false") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(5, "a"), Row(10, "b")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(5, "x"), Row(8, "y"), Row(10, "z")).asJava, schema2)
    // allowExactMatches=false: exact matches excluded
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = false, direction = "nearest"),
      Seq(
        // left.ts=5: exclude right.ts=5, nearest is 8 (distance 3)
        Row(5, "a", 8, "y"),
        // left.ts=10: exclude right.ts=10, nearest is 8 (distance 2)
        Row(10, "b", 8, "y")
      )
    )
  }

  test("nearest join - allowExactMatches = false with right rows bracketing left") {
    // Regression test: right rows on both sides of the exact match point.
    // The operator must not early-terminate when the as-of condition is
    // false at the interior point (right == left).
    val schema = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(10, "a")).asJava, schema)
    val df2 = spark.createDataFrame(
      List(Row(1, "x"), Row(10, "y"), Row(11, "z")).asJava, schema)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = false, direction = "nearest"),
      // distance to 11 is 1, distance to 1 is 9. Correct answer is 11.
      Seq(Row(10, "a", 11, "z"))
    )
  }

  test("nearest join - null as-of keys") {
    val schema = StructType(
      StructField("ts", IntegerType, nullable = true) ::
        StructField("value", StringType) :: Nil)
    val left = spark.createDataFrame(
      List(Row(null, "missing"), Row(10, "found")).asJava, schema)
    val right = spark.createDataFrame(
      List(Row(null, "null"), Row(7, "earlier"), Row(12, "later")).asJava, schema)

    checkAnswer(
      left.joinAsOf(
        right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "nearest"),
      Seq(Row(null, "missing", null, null), Row(10, "found", 12, "later")))
  }

  test("null equi-keys with EqualTo condition") {
    // NULL = NULL -> NULL under EqualTo semantics, so null-keyed rows
    // should NOT match each other.
    val schema1 = StructType(
      StructField("grp", IntegerType, nullable = true) ::
        StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("grp", IntegerType, nullable = true) ::
        StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(null, 5, "a"), Row(1, 5, "b"), Row(null, 10, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(null, 3, "x"), Row(1, 4, "y"), Row(null, 8, "z")).asJava, schema2)
    // Inner join: null equi-keys should produce no match
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq("grp"),
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        // grp=1, left.ts=5: right.ts=4 (4 <= 5) -> match
        Row(1, 5, "b", 1, 4, "y")
        // grp=null rows: no match (EqualTo semantics)
      )
    )
    // Left outer: null equi-keys should emit null right
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq("grp"),
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(null, 5, "a", null, null, null),
        Row(1, 5, "b", 1, 4, "y"),
        Row(null, 10, "c", null, null, null)
      )
    )
  }

  test("null-safe equi-key (<=>) in ON matches null keys, unlike EqualTo") {
    // <=> is a residual (empty equi-keys), so null keys match, unlike EqualTo.
    val schema1 = StructType(
      StructField("grp", IntegerType, nullable = true) ::
        StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("grp", IntegerType, nullable = true) ::
        StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(null, 5, "a"), Row(1, 5, "b"), Row(null, 10, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(null, 3, "x"), Row(1, 4, "y"), Row(null, 8, "z")).asJava, schema2)
    val joined = df1.joinAsOf(
      df2, df1.col("ts"), df2.col("ts"),
      joinExprs = df1.col("grp") <=> df2.col("grp"),
      joinType = "inner", tolerance = null,
      allowExactMatches = true, direction = "backward")
    checkAnswer(
      joined,
      Seq(
        Row(null, 5, "a", null, 3, "x"),
        Row(1, 5, "b", 1, 4, "y"),
        Row(null, 10, "c", null, 8, "z")
      )
    )
    val plan = joined.queryExecution.executedPlan
    val asOfExecs = collectWithSubqueries(plan) { case j: SortMergeAsOfJoinExec => j }
    assert(asOfExecs.length == 1, s"expected one SortMergeAsOfJoinExec in:\n$plan")
    assert(asOfExecs.head.leftKeys.isEmpty && asOfExecs.head.rightKeys.isEmpty,
      s"<=> must be a residual, so equi-keys must be empty, got ${asOfExecs.head}")
  }

  test("residual condition via joinExprs") {
    // Test that pair-correlated residual predicates are routed into the
    // scanner's residualCondition (not a post-join FilterExec).
    val schema1 = StructType(
      StructField("grp", StringType) ::
        StructField("ts", IntegerType) ::
        StructField("amount", IntegerType) :: Nil)
    val schema2 = StructType(
      StructField("grp", StringType) ::
        StructField("ts", IntegerType) ::
        StructField("threshold", IntegerType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row("A", 10, 50), Row("A", 20, 30)).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(
        Row("A", 5, 40), Row("A", 8, 60), Row("A", 15, 25)
      ).asJava, schema2)
    // joinExprs includes equi-key (grp) + residual (amount > threshold).
    // The strategy splits into equi-key + residual inside the scanner.
    // Backward join with residual amount > threshold:
    //   left(A,10,50): scan right ts<=10: (8,60) 50>60? No; (5,40) 50>40? Yes -> match (5,40)
    //   left(A,20,30): scan right ts<=20: (15,25) 30>25? Yes -> match (15,25)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"),
        joinExprs = (df1.col("grp") === df2.col("grp")) &&
          (df1.col("amount") > df2.col("threshold")),
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row("A", 10, 50, "A", 5, 40),
        Row("A", 20, 30, "A", 15, 25)
      )
    )
  }

  test("forward join - residual rejects every row within the tolerance") {
    // For left (5, 10), ts 6 is within the tolerance but fails amount > threshold. ts 9 passes
    // the residual but is past the tolerance, so it gets no match. Left (4, 30) matches ts 6.
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("amount", IntegerType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("threshold", IntegerType) :: Nil)
    val df1 = spark.createDataFrame(List(Row(4, 30), Row(5, 10)).asJava, schema1)
    val df2 = spark.createDataFrame(List(Row(6, 20), Row(9, 1)).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"),
        joinExprs = df1.col("amount") > df2.col("threshold"),
        joinType = "leftouter", tolerance = functions.lit(2),
        allowExactMatches = true, direction = "forward"),
      Seq(Row(4, 30, 6, 20), Row(5, 10, null, null)))
  }

  test("forward join - picks the closest row when right - left overflows") {
    // Int.MaxValue - (-100) overflows INT, so a distance-based scan throws or picks far.
    val schema1 = StructType(StructField("ts", IntegerType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("tag", StringType) :: Nil)
    val df1 = spark.createDataFrame(List(Row(-100)).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(-50, "near"), Row(Int.MaxValue, "far")).asJava, schema2)
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        Seq(true, false).foreach { allowExactMatches =>
          checkAnswer(
            df1.joinAsOf(
              df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
              joinType = "inner", tolerance = null,
              allowExactMatches = allowExactMatches, direction = "forward"),
            Seq(Row(-100, -50, "near")))
        }
      }
    }
  }

  test("nearest join applies eligibility on both sides after spilling") {
    withSQLConf(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      val leftSchema = StructType(
        StructField("ts", IntegerType) :: StructField("min_score", IntegerType) :: Nil)
      val rightSchema = StructType(
        StructField("ts", IntegerType) :: StructField("score", IntegerType) ::
          StructField("value", StringType) :: Nil)
      val left = spark.createDataFrame(
        List(Row(10, 1), Row(20, 1)).asJava, leftSchema)
      val right = spark.createDataFrame(
        List(
          Row(8, 1, "before"), Row(9, 0, "rejected before"),
          Row(10, 1, "excluded exact"), Row(11, 0, "rejected after"),
          Row(12, 1, "after"), Row(14, 1, "far before"),
          Row(18, 0, "rejected before"), Row(20, 1, "excluded exact"),
          Row(21, 0, "rejected after"), Row(22, 1, "closest after")).asJava,
        rightSchema)

      checkAnswerAndSpill(
        left.joinAsOf(
          right, left.col("ts"), right.col("ts"),
          joinExprs = left.col("min_score") <= right.col("score"),
          joinType = "inner", tolerance = functions.lit(7),
          allowExactMatches = false, direction = "nearest"),
        Seq(Row(10, 1, 8, 1, "before"), Row(20, 1, 22, 1, "closest after")))
    }
  }

  test("backward join - spill to disk") {
    // Force spill by setting in-memory threshold to 1 row.
    // Verifies that ExternalAppendOnlyUnsafeRowArray's spill path
    // produces the same results as the in-memory path.
    withSQLConf(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      val (df1, df2) = prepareForAsOfJoin()
      // No equi-key (bufferAllRight path)
      checkAnswerAndSpill(
        df1.joinAsOf(
          df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "backward"),
        Seq(
          Row(1, "x", "a", 1, "v", 1),
          Row(5, "y", "b", 3, "x", 3),
          Row(10, "z", "c", 7, "z", 7)
        )
      )
      // Equi-key (bufferRightGroup): at most one right row per group, so it does not spill.
      checkAnswer(
        df1.joinAsOf(
          df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "backward"),
        Seq(
          Row(10, "z", "c", 7, "z", 7)
        )
      )
    }
  }

  test("forward join - spill to disk") {
    withSQLConf(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      val (df1, df2) = prepareForAsOfJoin()
      // No equi-key (bufferAllRight path)
      checkAnswerAndSpill(
        df1.joinAsOf(
          df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "forward"),
        Seq(
          Row(1, "x", "a", 1, "v", 1),
          Row(5, "y", "b", 6, "y", 6)
        )
      )
      // With equi-key (bufferRightGroup path) - use data with multiple
      // right rows per group to actually exceed the spill threshold.
      val schema1 = StructType(
        StructField("grp", StringType) ::
          StructField("ts", IntegerType) :: Nil)
      val schema2 = StructType(
        StructField("grp", StringType) ::
          StructField("ts", IntegerType) ::
          StructField("val", StringType) :: Nil)
      val left = spark.createDataFrame(
        List(Row("A", 5), Row("A", 10)).asJava, schema1)
      val right = spark.createDataFrame(
        List(Row("A", 6, "a"), Row("A", 8, "b"), Row("A", 12, "c")).asJava, schema2)
      checkAnswerAndSpill(
        left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq("grp"),
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "forward"),
        Seq(
          Row("A", 5, "A", 6, "a"),
          Row("A", 10, "A", 12, "c")
        )
      )
    }
  }

  test("nearest join - spill to disk") {
    withSQLConf(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      val (df1, df2) = prepareForAsOfJoin()
      // No equi-key (bufferAllRight path)
      checkAnswerAndSpill(
        df1.joinAsOf(
          df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "nearest"),
        Seq(
          Row(1, "x", "a", 1, "v", 1),
          Row(5, "y", "b", 6, "y", 6),
          Row(10, "z", "c", 7, "z", 7)
        )
      )
      // With equi-key (bufferRightGroup path)
      val schema1 = StructType(
        StructField("grp", StringType) ::
          StructField("ts", IntegerType) :: Nil)
      val schema2 = StructType(
        StructField("grp", StringType) ::
          StructField("ts", IntegerType) ::
          StructField("val", StringType) :: Nil)
      val left = spark.createDataFrame(
        List(Row("A", 5), Row("A", 10)).asJava, schema1)
      val right = spark.createDataFrame(
        List(Row("A", 3, "a"), Row("A", 7, "b"), Row("A", 12, "c")).asJava, schema2)
      checkAnswerAndSpill(
        left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq("grp"),
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "nearest"),
        Seq(
          Row("A", 5, "A", 3, "a"),
          Row("A", 10, "A", 12, "c")
        )
      )
    }
  }

  test("left outer join - spill to disk") {
    withSQLConf(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      val (df1, df2) = prepareForAsOfJoin()
      checkAnswerAndSpill(
        df1.joinAsOf(
          df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "leftouter", tolerance = null,
          allowExactMatches = true, direction = "backward"),
        Seq(
          Row(1, "x", "a", 1, "v", 1),
          Row(5, "y", "b", 3, "x", 3),
          Row(10, "z", "c", 7, "z", 7)
        )
      )
    }
  }

  test("backward join - in-memory group followed by a spill-backed one") {
    // `clear()` drops the spillable backing store between equi-key groups, so one scanner can
    // see an in-memory group (whose buffer iterator yields the distinct stored rows) and then a
    // spill-backed one (whose iterator re-points a single UnsafeRow on every next(), so that a
    // retained match has to be copied out). Group "A" has one right row and stays in memory,
    // group "B" has three and spills; in "B" the match is followed by a non-matching row, so a
    // match that was not copied out would be clobbered before it is emitted.
    withSQLConf(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      val leftSchema = StructType(
        StructField("grp", StringType) ::
          StructField("ts", IntegerType) :: Nil)
      val rightSchema = StructType(
        StructField("grp", StringType) ::
          StructField("ts", IntegerType) ::
          StructField("right_val", StringType) :: Nil)
      // Values of differing lengths so a clobbered match stands out in the answer.
      val bestVal = "b" * 40
      val left = spark.createDataFrame(
        List(Row("A", 8), Row("B", 8)).asJava, leftSchema)
      val right = spark.createDataFrame(
        List(Row("A", 3, "aa"), Row("B", 1, "x"), Row("B", 5, bestVal), Row("B", 12, "y")).asJava,
        rightSchema)
      checkAnswerAndSpill(
        left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq("grp"),
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = "backward"),
        Seq(
          Row("A", 8, "A", 3, "aa"),
          Row("B", 8, "B", 5, bestVal)
        )
      )
    }
  }
}
