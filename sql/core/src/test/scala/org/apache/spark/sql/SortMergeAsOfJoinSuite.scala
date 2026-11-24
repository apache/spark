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

import java.lang.management.ManagementFactory
import java.sql.Timestamp
import java.time.{Duration, Instant, LocalDateTime}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.util.Random

import com.sun.management.UnixOperatingSystemMXBean

import org.apache.spark.{SparkException, SparkThrowable, TaskContext}
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Backward, Forward, Nearest}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.InMemoryCatalog
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, WholeStageCodegenExec}
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

  // Like checkAnswer, but also asserts the right-side buffer spilled (spillSize > 0). Runs with
  // the ASOF join codegen off and on, so both paths must handle the spilled buffer, except for a
  // nearest join, which runs the interpreted scan both times.
  private def checkAnswerAndSpill(df: => classic.DataFrame, expectedAnswer: Seq[Row]): Unit = {
    withAsOfJoinCodegenOffAndOn { codegen =>
      val query = df
      checkAnswer(query, expectedAnswer)
      val op = assertAsOfJoinCodegen(query, codegen)
      assert(op.metrics("spillSize").value > 0,
        s"Expected the right-side buffer to spill (spillSize > 0), " +
          s"got ${op.metrics("spillSize").value}")
      assert(op.metrics("numAsOfCursorTasks").value == 0,
        "Expected the buffered scan, but a task used the cursor scan")
    }
  }

  // Whether `join` runs in a codegen stage, and not only in a stage below it.
  private def isGenerated(plan: SparkPlan, join: SortMergeAsOfJoinExec): Boolean = {
    def inStage(node: SparkPlan): Boolean = node match {
      case j: SortMergeAsOfJoinExec => j eq join
      case _: InputAdapter => false
      case other => other.children.exists(inStage)
    }
    collect(plan) { case stage: WholeStageCodegenExec if inStage(stage.child) => stage }.nonEmpty
  }

  /**
   * Like checkAnswer, but also asserts that the join used the cursor scan, with the ASOF join
   * codegen off and on. A right group spills from its second row, so a cursor scan that
   * buffered rows would show a spill.
   */
  private def checkCursorScan(df: => classic.DataFrame, expectedAnswer: Seq[Row]): Unit = {
    withAsOfJoinCodegenOffAndOn { codegen =>
      withSQLConf(
          SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
          SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
        val query = df
        checkAnswer(query, expectedAnswer)
        val op = assertAsOfJoinCodegen(query, codegen)
        assert(op.metrics("numAsOfCursorTasks").value > 0, "Expected the cursor scan")
        assert(op.metrics("spillSize").value == 0, "The cursor scan buffered right rows")
      }
    }
  }

  /** Checks `df` against the expected rows with the buffered scan, then with the cursor scan. */
  private def checkBothScans(df: => classic.DataFrame, expectedAnswer: Seq[Row]): Unit = {
    withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> "false") {
      withAsOfJoinCodegenOffAndOn { codegen =>
        val query = df
        checkAnswer(query, expectedAnswer)
        val op = assertAsOfJoinCodegen(query, codegen)
        assert(op.metrics("numAsOfCursorTasks").value == 0, "Expected the buffered scan")
      }
    }
    checkCursorScan(df, expectedAnswer)
  }

  /**
   * Runs `f` with the ASOF join codegen off, then on, and passes it the setting. Build each
   * DataFrame inside `f`, since a DataFrame keeps the plan it got first.
   */
  private def withAsOfJoinCodegenOffAndOn(f: Boolean => Unit): Unit = {
    Seq(false, true).foreach { codegen =>
      withClue(s"ASOF join codegen $codegen: ") {
        withSQLConf(SQLConf.ENABLE_SORT_MERGE_AS_OF_JOIN_CODEGEN.key -> codegen.toString) {
          f(codegen)
        }
      }
    }
  }

  /**
   * Asserts that `df`, after it ran, has a sort-merge ASOF join, and that the join ran generated
   * exactly when the codegen is on and the join is not nearest. Returns the join.
   */
  private def assertAsOfJoinCodegen(df: DataFrame, codegen: Boolean): SortMergeAsOfJoinExec = {
    val plan = df.queryExecution.executedPlan
    val op = collectFirst(plan) { case s: SortMergeAsOfJoinExec => s }
    assert(op.isDefined, s"Expected SortMergeAsOfJoinExec in plan:\n$plan")
    assert(isGenerated(plan, op.get) == (codegen && op.get.direction != Nearest),
      s"Unexpected ASOF join codegen in:\n$plan")
    op.get
  }

  /**
   * Like checkAnswer, with the ASOF join codegen off and on. Also asserts that the plan has the
   * join, and that it ran generated exactly when the codegen is on and the join is not nearest.
   * Single-pass analysis needs `checkToRDD = false`.
   */
  private def checkAnswerCodegenOffAndOn(
      df: => DataFrame,
      expectedAnswer: Seq[Row],
      checkToRDD: Boolean = true): Unit = {
    withAsOfJoinCodegenOffAndOn { codegen =>
      val query = df
      if (checkToRDD) {
        checkAnswer(query, expectedAnswer)
      } else {
        QueryTest.checkAnswer(df = query, expectedAnswer = expectedAnswer, checkToRDD = false)
      }
      assertAsOfJoinCodegen(query, codegen)
    }
  }

  /**
   * Checks `df` with the ASOF join codegen off and on, with InferFiltersFromConstraints excluded
   * so the rows with a NULL ON key reach the join. Also asserts that every input row reaches it.
   */
  private def checkAnswerWithNullOnKeys(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName) {
      val qe = df.queryExecution
      def inputs(plan: LogicalPlan): Seq[LogicalPlan] =
        plan.collectFirst { case j: AsOfJoin => j.children }.get
      def numRows(plan: LogicalPlan): Long = classic.Dataset.ofRows(spark, plan).count()
      inputs(qe.analyzed).zip(inputs(qe.optimizedPlan)).foreach { case (input, joinInput) =>
        assert(numRows(joinInput) == numRows(input),
          s"Expected every input row to reach the as-of join in:\n${qe.optimizedPlan}")
      }
      checkAnswerCodegenOffAndOn(df, expectedAnswer)
    }
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

  private def withRightSideFirst(f: => Unit): Unit = withExtraOptimization(PutRightSideFirst)(f)

  /**
   * Makes the tolerance bound of each backward or forward as-of join inclusive or strict, so a
   * test can check both forms, whatever form `allowExactMatches` gives it.
   */
  private case class WithToleranceBound(inclusive: Boolean) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      case j: AsOfJoin if j.direction != Nearest =>
        j.copy(asOfCondition = j.asOfCondition match {
          case And(comparison, GreaterThan(right, limit)) if inclusive =>
            And(comparison, GreaterThanOrEqual(right, limit))
          case And(comparison, GreaterThanOrEqual(right, limit)) if !inclusive =>
            And(comparison, GreaterThan(right, limit))
          case And(comparison, LessThan(right, limit)) if inclusive =>
            And(comparison, LessThanOrEqual(right, limit))
          case And(comparison, LessThanOrEqual(right, limit)) if !inclusive =>
            And(comparison, LessThan(right, limit))
          case other => other
        })
    }
  }

  private def withExtraOptimization(rule: Rule[LogicalPlan])(f: => Unit): Unit = {
    val saved = spark.experimental.extraOptimizations
    spark.experimental.extraOptimizations = saved :+ rule
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

  test("SPARK-61756: the interpreted join releases the sorters of its children early") {
    // The right side has keys above every left key, so the join stops reading it before its end.
    // It must release the right sorter once it has produced its last row, instead of holding its
    // memory until the task completes. Read the join to its end in a task, and check that the
    // task holds no execution memory afterwards.
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val trades = spark.range(0, 100).selectExpr("id % 5 AS k", "id AS ts")
      val quotes = spark.range(0, 1000).selectExpr("id % 10 AS k", "id AS ts")
      // A nearest join has no generated scan, so it runs the interpreted one.
      val df = trades.joinAsOf(
        quotes, trades.col("ts"), quotes.col("ts"), usingColumns = Seq("k"),
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest")
      val memory = df.queryExecution.toRdd.mapPartitions { iter =>
        iter.foreach(_ => ())
        Iterator(TaskContext.get().taskMemoryManager().getMemoryConsumptionForThisTask)
      }.collect().toSeq
      // Checked after the run: with AQE, the plan is final only then.
      val plan = df.queryExecution.executedPlan
      val join = collectFirst(plan) { case j: SortMergeAsOfJoinExec => j }.get
      assert(!isGenerated(plan, join), plan)
      assert(memory.forall(_ == 0L), s"$memory, $plan")
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
    checkCursorScan(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkCursorScan(
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
        withClue(s"$directionName: ") {
          withAsOfJoinCodegenOffAndOn { codegen =>
            val joined = left.joinAsOf(
              right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
              joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = directionName)
            checkAnswer(joined, expected :: Nil)
            val op = assertAsOfJoinCodegen(joined, codegen)
            val rightSideFirst = op.asOfCondition match {
              case c: BinaryComparison => c.left.references.subsetOf(op.right.outputSet)
              case _ => false
            }
            assert(rightSideFirst, s"Expected the right side first in ${op.asOfCondition}")
            assert(op.direction === direction)
          }
        }
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

  gridTest("mixed timestamp keys use LTZ for sort-merge")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false") {
        for {
          ansi <- Seq(false, true)
          spring <- Seq(false, true)
          ltzOnLeft <- Seq(false, true)
        } {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
            val (leftLtz, leftNtz, beforeLtz, beforeNtz, afterLtz, afterNtz) = if (spring) {
              ("2024-03-10T10:05:00Z", "2024-03-10T03:05:00",
                "2024-03-10T10:00:00Z", "2024-03-10T03:00:00",
                "2024-03-10T10:15:00Z", "2024-03-10T03:15:00")
            } else {
              ("2024-11-03T08:30:00Z", "2024-11-03T01:30:00",
                "2024-11-03T08:25:00Z", "2024-11-03T01:25:00",
                "2024-11-03T08:35:00Z", "2024-11-03T01:35:00")
            }
            val left = spark.createDataFrame(
              List(Row(Timestamp.from(Instant.parse(leftLtz)),
                LocalDateTime.parse(leftNtz))).asJava,
              new StructType().add("ltz", TimestampType).add("ntz", TimestampNTZType))
            val right = spark.createDataFrame(
              List(
                Row(Timestamp.from(Instant.parse(afterLtz)),
                  LocalDateTime.parse(afterNtz), "after"),
                Row(Timestamp.from(Instant.parse(beforeLtz)),
                  LocalDateTime.parse(beforeNtz), "before"),
                Row(Timestamp.from(Instant.parse(
                  if (spring) "2024-03-10T10:50:00Z" else "2024-11-03T09:20:00Z")),
                  LocalDateTime.parse(
                    if (spring) "2024-03-10T02:50:00" else "2024-11-03T02:20:00"),
                  "far after")).asJava,
              new StructType().add("ltz", TimestampType).add("ntz", TimestampNTZType)
                .add("tag", StringType))
            val leftKey = left.col(if (ltzOnLeft) "ltz" else "ntz")
            val rightKey = right.col(if (ltzOnLeft) "ntz" else "ltz")

            for (direction <- Seq("backward", "forward", "nearest")) {
              withAsOfJoinCodegenOffAndOn { codegen =>
                val joined = left.joinAsOf(
                  other = right, leftAsOf = leftKey, rightAsOf = rightKey,
                  usingColumns = Seq.empty, joinType = "inner", tolerance = null,
                  allowExactMatches = true, direction = direction
                ).select("tag")
                val join =
                  joined.queryExecution.analyzed.collectFirst { case j: AsOfJoin => j }.get
                assert(join.leftSortExprs.head.dataType ==
                  (if (ltzOnLeft || direction == "nearest") TimestampType else TimestampNTZType))
                assert(join.rightSortExprs.head.dataType == TimestampType)
                val expected = if (direction == "forward") "after" else "before"
                QueryTest.checkAnswer(
                  df = joined, expectedAnswer = Seq(Row(expected)), checkToRDD = false)
                val physicalJoin = assertAsOfJoinCodegen(joined, codegen)
                assert(
                  physicalJoin.leftSortExprs.head.dataType == join.leftSortExprs.head.dataType)
                assert(physicalJoin.rightSortExprs.head.dataType == TimestampType)
              }
            }
          }
        }
      }
    }
  }

  gridTest("mixed timestamp composite keys use comparison ordering")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        val left = spark.sql(
          "SELECT TIMESTAMP_LTZ '2024-03-10 10:05:00Z' AS ts, 1 AS n")
          .selectExpr("ts", "n", "named_struct('ts', ts, 'n', n) AS k")
        val right = spark.sql(
          """SELECT ts, CAST(n AS BIGINT) AS n, tag FROM VALUES
            |  (TIMESTAMP_NTZ '2024-03-10 02:50:00', 1, 'far'),
            |  (TIMESTAMP_NTZ '2024-03-10 03:00:00', 1, 'before'),
            |  (TIMESTAMP_NTZ '2024-03-10 03:15:00', 1, 'after') AS t(ts, n, tag)
            |""".stripMargin)
          .selectExpr("ts", "n", "tag", "named_struct('ts', ts, 'n', n) AS k")
        for {
          shape <- Seq("tuple", "struct", "array", "nested")
          direction <- Seq("backward", "forward")
        } {
          def key(df: DataFrame): Column = shape match {
            case "tuple" => functions.struct(df.col("ts"), df.col("n"))
            case "struct" => df.col("k")
            case "array" => functions.array(df.col("ts"))
            case "nested" => functions.struct(functions.array(df.col("k")))
          }
          def joined: DataFrame = left.joinAsOf(
            other = right, leftAsOf = key(left), rightAsOf = key(right),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = direction
          ).select("tag")
          val expected = if (direction == "backward") "before" else "after"
          checkAnswerCodegenOffAndOn(joined, Seq(Row(expected)), checkToRDD = false)
        }
      }
    }
  }

  gridTest("mixed timestamp sort keys preserve the analysis time zone")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        val left = spark.sql("SELECT TIMESTAMP '2024-03-10 10:55:00Z' AS lk")
        val right = spark.sql(
          """SELECT * FROM VALUES
            |  (TIMESTAMP_NTZ '2024-03-10 02:50:00', 'near'),
            |  (TIMESTAMP_NTZ '2024-03-10 03:00:00', 'far') AS t(rk, tag)
            |""".stripMargin)
        withAsOfJoinCodegenOffAndOn { codegen =>
          val joined = left.joinAsOf(
            other = right, leftAsOf = left.col("lk"), rightAsOf = right.col("rk"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = "backward"
          ).select("tag")
          assert(joined.queryExecution.analyzed.resolved)

          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
            QueryTest.checkAnswer(
              df = joined, expectedAnswer = Seq(Row("near")), checkToRDD = false)
            assertAsOfJoinCodegen(joined, codegen)
          }
        }
      }
    }
  }

  gridTest("timestamp keys respect explicit user casts")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false") {
        val schema = new StructType().add("ltz", TimestampType).add("ntz", TimestampNTZType)
        val left = spark.createDataFrame(
          List(Row(
            Timestamp.from(Instant.parse("2024-11-03T09:30:00Z")),
            LocalDateTime.parse("2024-11-03T01:30:00"))).asJava,
          schema)
        val right = spark.createDataFrame(
          List(
            Row(Timestamp.from(Instant.parse("2024-11-03T08:28:00Z")),
              LocalDateTime.parse("2024-11-03T01:28:00"), "before"),
            Row(Timestamp.from(Instant.parse("2024-11-03T08:35:00Z")),
              LocalDateTime.parse("2024-11-03T01:35:00"), "after")).asJava,
          schema.add("tag", StringType))

        for {
          direction <- Seq("backward", "forward", "nearest")
          (source, target, targetType) <- Seq(
            ("ltz", "ntz", TimestampNTZType), ("ntz", "ltz", TimestampType))
        } {
          def joined: DataFrame = left.joinAsOf(
            other = right, leftAsOf = left.col(source).cast(targetType),
            rightAsOf = right.col(target), usingColumns = Seq.empty, joinType = "inner",
            tolerance = null, allowExactMatches = true, direction = direction
          ).select("tag")
          val join = joined.queryExecution.analyzed.collectFirst { case j: AsOfJoin => j }.get
          assert((join.leftSortExprs ++ join.rightSortExprs).forall(_.dataType == targetType))
          val expected = if (direction == "forward") "after" else "before"
          checkAnswerCodegenOffAndOn(joined, Seq(Row(expected)), checkToRDD = false)
        }
      }
    }
  }

  gridTest("sort-merge selection survives disabling the config after analysis")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false") {
        val left = spark.sql(
          "SELECT 10 AS id, TIMESTAMP_NTZ '2024-11-03 01:30:00' AS ts")
        val right = spark.sql(
          """SELECT * FROM VALUES
            |  (9, TIMESTAMP '2024-11-03 08:25:00Z', 'before'),
            |  (12, TIMESTAMP '2024-11-03 08:37:00Z', 'after'),
            |  (30, TIMESTAMP '2024-11-03 09:20:00Z', 'far after') AS t(id, ts, tag)
            |""".stripMargin)
        for {
          key <- Seq("id", "ts")
          direction <- Seq("backward", "forward", "nearest")
        } {
          withAsOfJoinCodegenOffAndOn { codegen =>
            val joined = left.joinAsOf(
              other = right, leftAsOf = left.col(key), rightAsOf = right.col(key),
              usingColumns = Seq.empty, joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = direction
            ).select("tag")
            val join = joined.queryExecution.analyzed.collectFirst { case j: AsOfJoin => j }.get
            assert(join.requiresSortMergeAsOfJoin)
            val expected = if (direction == "forward") "after" else "before"
            withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
              QueryTest.checkAnswer(
                df = joined, expectedAnswer = Seq(Row(expected)), checkToRDD = false)
              assertAsOfJoinCodegen(joined, codegen)
            }
          }
        }
      }
    }
  }

  gridTest("fallback selection survives enabling sort-merge after analysis")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false",
          SQLConf.ANSI_ENABLED.key -> "true") {
        val left = spark.sql("SELECT 10 AS k")
        val right = spark.sql("SELECT * FROM VALUES (9, 'before'), (12, 'after') AS t(k, tag)")
        for (direction <- Seq("backward", "forward", "nearest")) {
          withTempView("asof_fallback_view") {
            val joined = left.joinAsOf(
              other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
              usingColumns = Seq.empty, joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = direction
            ).select("tag")
            val join = joined.queryExecution.analyzed.collectFirst { case j: AsOfJoin => j }.get
            assert(!join.requiresSortMergeAsOfJoin)
            // The single-pass analyzer does not resolve CREATE VIEW commands.
            withAsOfJoinAnalyzer(singlePass = false) {
              joined.createOrReplaceTempView("asof_fallback_view")
            }
            withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true") {
              for (df <- Seq(joined, spark.table("asof_fallback_view"))) {
                val expected = if (direction == "forward") "after" else "before"
                QueryTest.checkAnswer(
                  df = df, expectedAnswer = Seq(Row(expected)), checkToRDD = false)
                assert(!df.queryExecution.optimizedPlan.exists(_.isInstanceOf[AsOfJoin]))
                assert(collectFirst(df.queryExecution.executedPlan) {
                  case s: SortMergeAsOfJoinExec => s
                }.isEmpty)
              }
            }
          }
        }
      }
    }
  }

  gridTest("sort-merge keys preserve analysis case sensitivity")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.CASE_SENSITIVE.key -> "false") {
        val left = spark.sql("SELECT named_struct('A', CAST(10 AS INT)) AS lk")
        val right = spark.sql(
          "SELECT named_struct('a', CAST(9 AS BIGINT)) AS rk, 'before' AS tag")
        withAsOfJoinCodegenOffAndOn { codegen =>
          val joined = left.joinAsOf(
            other = right, leftAsOf = left.col("lk"), rightAsOf = right.col("rk"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = "backward"
          ).select("tag")
          assert(joined.queryExecution.analyzed.resolved)

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            QueryTest.checkAnswer(
              df = joined, expectedAnswer = Seq(Row("before")), checkToRDD = false)
            assertAsOfJoinCodegen(joined, codegen)
          }
        }
      }
    }
  }

  gridTest("numeric keys use comparison ordering")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      for {
        ansi <- Seq(false, true)
        (leftType, rightType) <- Seq(
          ("BIGINT", "FLOAT"), ("DECIMAL(12, 2)", "DECIMAL(8, 4)"),
          ("INT", "DECIMAL(12, 2)"), ("STRING", "INT"))
      } {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          val left = spark.sql(s"SELECT CAST(50 AS $leftType) AS k")
          val right = spark.sql(
            s"""SELECT CAST(k AS $rightType) AS k, tag
               |FROM VALUES (100, 'far after'), (9, 'far before'),
               |  (49, 'before'), (52, 'after') AS t(k, tag)
               |""".stripMargin)
          for (direction <- Seq("backward", "forward", "nearest")) {
            def joined: DataFrame = left.joinAsOf(
              other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
              usingColumns = Seq.empty, joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = direction
            ).select("tag")
            val expected = if (direction == "forward") "after" else "before"
            checkAnswerCodegenOffAndOn(joined, Seq(Row(expected)), checkToRDD = false)
          }
        }
      }
    }
  }

  gridTest("numeric keys preserve ANSI comparison precision")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.ANSI_ENABLED.key -> "true") {
        val left = spark.sql("SELECT 16777219L AS k")
        val right = spark.sql(
          """SELECT CAST(k AS FLOAT) AS k, tag
            |FROM VALUES (16777218, 'before'), (16777220, 'after') AS t(k, tag)
            |""".stripMargin)
        for (direction <- Seq("backward", "forward", "nearest")) {
          def joined: DataFrame = left.joinAsOf(
            other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = direction
          ).select("tag")
          val join = joined.queryExecution.analyzed.collectFirst { case j: AsOfJoin => j }.get
          if (direction == "nearest") {
            assert(join.leftSortExprs.head.dataType == DoubleType)
            assert(join.rightSortExprs.head.dataType == DoubleType)
          } else {
            assert(join.leftSortExprs.head.dataType == LongType)
            assert(join.rightSortExprs.head.dataType == FloatType)
          }
          val expected = if (direction == "forward") "after" else "before"
          checkAnswerCodegenOffAndOn(joined, Seq(Row(expected)), checkToRDD = false)
        }
      }
    }
  }

  gridTest("tolerance types are checked before evaluation")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
        val left = spark.sql("SELECT TIMESTAMP '2024-01-02 00:00:00' AS k")
        val right = spark.sql("SELECT TIMESTAMP '2024-01-01 00:00:00' AS k")
        checkErrorMatchPVals(
          exception = intercept[AnalysisException] {
            left.joinAsOf(
              other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
              usingColumns = Seq.empty, joinType = "inner",
              tolerance = functions.expr("INTERVAL 2 DAYS"),
              allowExactMatches = true, direction = "backward"
            )
          },
          condition = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
          parameters = Map(
            "functionName" -> "`>=`", "dataType" -> "\"INTERVAL\"", "sqlExpr" -> ".+"
          )
        )
      }
    }
  }

  gridTest("tolerance and key errors have the same precedence")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true") {
        val left = spark.sql("SELECT '5' AS lk")
        val right = spark.sql("SELECT '4' AS rk")

        def join(leftKey: Column, direction: String): DataFrame = {
          left.joinAsOf(
            other = right, leftAsOf = leftKey, rightAsOf = right.col("rk"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = functions.lit(-1),
            allowExactMatches = true, direction = direction
          )
        }

        for ((leftKey, direction) <- Seq(
            (functions.lit(5), "backward"), (left.col("lk"), "backward"),
            (left.col("lk"), "nearest"))) {
          checkError(
            exception = intercept[AnalysisException] {
              join(leftKey, direction).queryExecution.analyzed
            },
            condition = "AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE",
            parameters = Map.empty
          )
        }

        val unresolvedKeyError = intercept[AnalysisException] {
          join(functions.col("missing"), "backward").queryExecution.analyzed
        }
        assert(unresolvedKeyError.getCondition == "UNRESOLVED_COLUMN.WITH_SUGGESTION")
      }
    }
  }

  gridTest("backward and forward joins support TIME keys")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.TIME_TYPE_ENABLED.key -> "true") {
        val left = spark.sql("SELECT TIME '10:00:00' AS k")
        val right = spark.sql(
          """SELECT * FROM VALUES
            |  (TIME '10:02:00', 'after'), (TIME '09:59:00', 'before'),
            |  (TIME '10:00:00', 'exact') AS t(k, tag)
            |""".stripMargin)
        for {
          direction <- Seq("backward", "forward")
          exact <- Seq(false, true)
        } {
          def joined: DataFrame = left.joinAsOf(
            other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
            usingColumns = Seq.empty, joinType = "inner",
            tolerance = functions.lit(Duration.ofMinutes(3)),
            allowExactMatches = exact, direction = direction
          ).select("tag")
          val expected = if (exact) "exact" else if (direction == "forward") "after" else "before"
          checkAnswerCodegenOffAndOn(joined, Seq(Row(expected)), checkToRDD = false)
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

        checkAnswerCodegenOffAndOn(
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

        checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
      df1.joinAsOf(
        emptyDf, df1.col("a"), emptyDf.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq.empty
    )
    // Left outer: all left rows with null right
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
          def result: DataFrame = left.joinAsOf(
            other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = direction
          ).select(right.col("tag"))

          checkAnswerCodegenOffAndOn(result, Seq(Row(expected)))
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
          def result: DataFrame = left.joinAsOf(
            other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
            usingColumns = Seq("case_id"), joinType = "leftouter",
            tolerance = functions.lit(Duration.ofDays(2)),
            allowExactMatches = allowExactMatches, direction = direction
          ).select(left.col("case_id"), right.col("tag"))
          val closest = if (direction == "forward") "after" else "before"

          withClue(s"legacyInterval=$legacyInterval, allowExactMatches=$allowExactMatches: ") {
            checkAnswerCodegenOffAndOn(
              result,
              Seq(
                Row("inside", closest),
                Row("boundary", closest),
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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

  test("tolerance joins use the cursor scan and match the buffered scan") {
    // Key 'a' has right rows at 10, 20, 30, 38, 40, 42 and 50, and the tolerance is 3. Each left
    // row of key 'a' is inside, at or past the tolerance from its closest right row, or matches
    // one exactly. Key 'b' has no right rows, 'c' has no left rows, and 'd' has only a NULL
    // right as-of key.
    val left = spark.sql(
      """SELECT * FROM VALUES
        |(1, 'a', 8), (2, 'a', 12), (3, 'a', 23), (4, 'a', 27), (5, 'a', 29), (6, 'a', 34),
        |(7, 'a', 40), (8, 'a', 47), (9, 'a', 53), (10, 'a', NULL), (11, 'b', 5), (12, 'd', 5),
        |(13, NULL, 10)
        |AS t(id, k, ts)""".stripMargin)
    val right = spark.sql(
      """SELECT * FROM VALUES
        |('a', NULL, 'a_null'), ('a', 10, 'r10'), ('a', 20, 'r20'), ('a', 30, 'r30'),
        |('a', 38, 'r38'), ('a', 40, 'r40'), ('a', 42, 'r42'), ('a', 50, 'r50'), ('c', 1, 'c1'),
        |('d', NULL, 'd_null'), (NULL, 10, 'null key')
        |AS t(k, ts, tag)""".stripMargin)
    // The matches of left rows 1 to 9, by id.
    def matches(direction: String, exact: Boolean, inclusive: Boolean): Map[Int, String] = {
      val (inside, atTolerance, exactMatch) = direction match {
        // 12 is 2 after 10. 23 and 53 are 3 after 20 and 50. 40 is 2 after 38.
        case "backward" => (Map(2 -> "r10"), Map(3 -> "r20", 9 -> "r50"), "r38")
        // 8 and 29 are 2 and 1 before 10 and 30. 27 and 47 are 3 before 30 and 50. 40 is 2
        // before 42. 23 keeps 30 as its closest row but fails the bound, so 27 checks the kept
        // row again.
        case _ => (Map(1 -> "r10", 5 -> "r30"), Map(4 -> "r30", 8 -> "r50"), "r42")
      }
      inside ++ (if (inclusive) atTolerance else Map.empty) +
        (7 -> (if (exact) "r40" else exactMatch))
    }
    for {
      direction <- Seq("backward", "forward")
      allowExactMatches <- Seq(true, false)
      inclusive <- Seq(true, false)
      joinType <- Seq("inner", "leftouter")
      ansi <- Seq(true, false)
    } {
      val found = matches(direction, allowExactMatches, inclusive)
      val expected = (1 to 13).flatMap { id =>
        found.get(id) match {
          case Some(tag) => Some(Row(id, tag))
          case None if joinType == "leftouter" => Some(Row(id, null))
          case None => None
        }
      }
      withClue(s"$direction, allowExactMatches $allowExactMatches, inclusive bound " +
          s"$inclusive, $joinType, ANSI $ansi: ") {
        // Without InferFiltersFromConstraints, the rows with a NULL key reach the join.
        withSQLConf(
            SQLConf.ANSI_ENABLED.key -> ansi.toString,
            SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName) {
          withExtraOptimization(WithToleranceBound(inclusive)) {
            checkBothScans(
              left.joinAsOf(
                right, left.col("ts"), right.col("ts"), usingColumns = Seq("k"),
                joinType = joinType, tolerance = functions.lit(3),
                allowExactMatches = allowExactMatches, direction = direction
              ).select(left.col("id"), right.col("tag")),
              expected)
          }
        }
      }
    }
  }

  test("tolerance joins on BIGINT, DOUBLE, DATE, TIMESTAMP and TIMESTAMP_NTZ keys use the " +
      "cursor scan") {
    // The left keys are 10 and 20 and the right keys 7, 13, 16 and 24, in the unit of the
    // tolerance 3. So 10 matches 7 backward and 13 forward, both at the tolerance, and 20 is 4
    // from its closest right keys, past the tolerance.
    Seq(
      ("CAST(n AS BIGINT)", functions.lit(3L)),
      ("CAST(n AS DOUBLE)", functions.lit(3.0)),
      ("date_add(DATE'2024-01-01', n)", functions.expr("INTERVAL '3' DAY")),
      // A whole number of days: backward subtracts it with date_sub, forward adds it.
      ("date_add(DATE'2024-01-01', n)", functions.lit(3)),
      ("add_months(DATE'2024-01-01', n)", functions.expr("INTERVAL '3' MONTH")),
      ("TIMESTAMP'2024-01-01 00:00:00' + make_dt_interval(n)", functions.lit(Duration.ofDays(3))),
      ("TIMESTAMP_NTZ'2024-01-01 00:00:00' + make_dt_interval(n)",
        functions.lit(Duration.ofDays(3))),
      ("TIMESTAMP'2024-01-01 00:00:00' + make_ym_interval(0, n)",
        functions.expr("INTERVAL '3' MONTH"))
    ).foreach { case (key, tolerance) =>
      val left = spark.sql(s"SELECT n AS id, $key AS ts FROM VALUES (10), (20) AS t(n)")
      val right =
        spark.sql(s"SELECT n AS tag, $key AS ts FROM VALUES (7), (13), (16), (24) AS t(n)")
      Seq("backward" -> 7, "forward" -> 13).foreach { case (direction, tag) =>
        withClue(s"$key, $direction: ") {
          checkBothScans(
            left.joinAsOf(
              right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
              joinType = "leftouter", tolerance = tolerance,
              allowExactMatches = true, direction = direction
            ).select(left.col("id"), right.col("tag")),
            Seq(Row(10, tag), Row(20, null)))
        }
      }
    }
  }

  test("a tolerance join on a Scala UDF key uses the cursor scan") {
    // The sort, the comparison and the limit all evaluate the UDF of the left column. The left
    // keys are 10, 20 and 40, and the tolerance is 3.
    val times10 = functions.udf((ts: Int) => ts * 10)
    val left = spark.sql("SELECT * FROM VALUES (1, 1), (2, 2), (3, 4) AS t(id, ts)")
    val right = spark.sql(
      "SELECT * FROM VALUES (9, 'r9'), (20, 'r20'), (31, 'r31'), (42, 'r42') AS t(ts, tag)")
    Seq(
      // 10 is 1 after 9, and 40 is 9 after 31.
      "backward" -> Seq(Row(1, "r9"), Row(2, "r20"), Row(3, null)),
      // 10 is 10 before 20, and 40 is 2 before 42.
      "forward" -> Seq(Row(1, null), Row(2, "r20"), Row(3, "r42"))
    ).foreach { case (direction, expected) =>
      withClue(s"$direction: ") {
        checkBothScans(
          left.joinAsOf(
            right, times10(left.col("ts")), right.col("ts"), usingColumns = Seq.empty,
            joinType = "leftouter", tolerance = functions.lit(3),
            allowExactMatches = true, direction = direction
          ).select(left.col("id"), right.col("tag")),
          expected)
      }
    }
  }

  test("the cursor scan evaluates a computed as-of key no more often than the buffered scan") {
    // Each ON key has one left row and one right row, which passes the comparison. So the
    // buffered scan reads each key once in the comparison, and the left key again in a
    // tolerance limit.
    val countLeft = functions.udf { (ts: Int) => AsOfKeyEvaluations.left.incrementAndGet(); ts }
    val countRight = functions.udf { (ts: Int) => AsOfKeyEvaluations.right.incrementAndGet(); ts }
    val left = spark.range(1, 21).selectExpr("id AS k", "CAST(id * 10 AS INT) AS ts")
    for {
      (direction, offset) <- Seq("backward" -> -1, "forward" -> 1)
      tolerance <- Seq(None, Some(3))
      codegen <- Seq(false, true)
    } {
      val right = spark.range(1, 21)
        .selectExpr("id AS k", s"CAST(id * 10 + $offset AS INT) AS ts", "id AS tag")
      // A tolerance takes the cursor scan only for a right key that is a column.
      val rightKey = if (tolerance.isEmpty) countRight(right.col("ts")) else right.col("ts")
      def evaluations(cursorScan: Boolean): (Long, Long) = {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> cursorScan.toString,
            SQLConf.ENABLE_SORT_MERGE_AS_OF_JOIN_CODEGEN.key -> codegen.toString,
            // Without InferFiltersFromConstraints, no filter below the join reads the keys.
            SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName) {
          val df = left.joinAsOf(
            right, countLeft(left.col("ts")), rightKey,
            joinExprs = left.col("k") === right.col("k"), joinType = "inner",
            tolerance = tolerance.map(functions.lit).orNull,
            allowExactMatches = true, direction = direction
          ).select(left.col("k"), right.col("tag"))
          AsOfKeyEvaluations.left.set(0)
          AsOfKeyEvaluations.right.set(0)
          val rows = df.collect()
          val counts = (AsOfKeyEvaluations.left.get, AsOfKeyEvaluations.right.get)
          assert(rows.map(row => (row.getLong(0), row.getLong(1))).sorted.toSeq ==
            (1L to 20L).map(k => (k, k)))
          val op = assertAsOfJoinCodegen(df, codegen)
          assert((op.metrics("numAsOfCursorTasks").value > 0) == cursorScan)
          counts
        }
      }
      withClue(s"$direction, tolerance $tolerance, codegen $codegen: ") {
        val (bufferedLeft, bufferedRight) = evaluations(cursorScan = false)
        val (cursorLeft, cursorRight) = evaluations(cursorScan = true)
        assert(bufferedLeft > 0 && (tolerance.nonEmpty || bufferedRight > 0))
        assert(cursorLeft <= bufferedLeft, s"left key: cursor $cursorLeft, buffered $bufferedLeft")
        assert(cursorRight <= bufferedRight,
          s"right key: cursor $cursorRight, buffered $bufferedRight")
      }
    }
  }

  test("in a group with several left rows, the cursor scan evaluates a computed as-of key " +
      "once per row it compares") {
    // One group with left keys 10, 11 and 25. Backward, the right keys are 9 and 24, and each
    // left row compares the right row that the cursor waits at. Forward, they are 12 and 30, and
    // left rows 11 and 25 check the kept right row 12 again. The test compares the two scans on
    // the same plan, so evaluations outside the join, such as in the sort below it, cancel out.
    val countLeft = functions.udf { (ts: Int) => AsOfKeyEvaluations.left.incrementAndGet(); ts }
    val countRight = functions.udf { (ts: Int) => AsOfKeyEvaluations.right.incrementAndGet(); ts }
    val left = spark.sql("SELECT * FROM VALUES (10), (11), (25) AS t(ts)")
    Seq(
      // (direction, right keys, tolerance, matched tags, and how many fewer times the cursor scan
      // evaluates the (left, right) keys than the buffered scan)
      // Buffered: each left row compares 9 and 24, both keys each time, so (6, 6). Cursor: 9 and
      // 24, then 24, then 24, and one left key per left row, so (3, 4).
      ("backward", Seq(9, 24), None, Seq(9, 9, 24), (3, 2)),
      // Buffered: 10 and 11 compare 12, and 25 compares 12 and 30, so (4, 4). Cursor: 12; the
      // kept 12; the kept 12, then 30, so (3, 4).
      ("forward", Seq(12, 30), None, Seq(12, 12, 30), (1, 0)),
      // With a tolerance the right key is a column. The limit reads the left key again on each
      // right row that passes the comparison, or on the closest row. Buffered: 2, 2 and 2
      // compares, and 1, 1 and 2 limits, so 10. Cursor: one key and one limit per left row, so 6.
      ("backward", Seq(9, 24), Some(3), Seq(9, 9, 24), (4, 0)),
      // Buffered: 1, 1 and 2 compares, and 1, 1 and 1 limits, so 7. Cursor: 6. 25 is 5 before 30.
      ("forward", Seq(12, 30), Some(3), Seq(12, 12, -1), (1, 0))
    ).foreach { case (direction, rightKeys, tolerance, tags, fewerEvaluations) =>
      val right = spark.sql(
        s"SELECT ts, ts AS tag FROM VALUES ${rightKeys.map(k => s"($k)").mkString(", ")} AS t(ts)")
      val rightKey = if (tolerance.isEmpty) countRight(right.col("ts")) else right.col("ts")
      val expected = Seq(10, 11, 25).zip(tags).map { case (ts, tag) =>
        Row(ts, if (tag < 0) null else tag)
      }
      // The (left, right) key evaluations of one scan.
      def evaluations(cursorScan: Boolean, codegen: Boolean): (Long, Long) =
        withClue(s"$direction, tolerance $tolerance, codegen $codegen, cursor $cursorScan: ") {
          withSQLConf(
              SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> cursorScan.toString,
              SQLConf.ENABLE_SORT_MERGE_AS_OF_JOIN_CODEGEN.key -> codegen.toString,
              SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName) {
            val df = left.joinAsOf(
              right, countLeft(left.col("ts")), rightKey, usingColumns = Seq.empty,
              joinType = "leftouter", tolerance = tolerance.map(functions.lit).orNull,
              allowExactMatches = true, direction = direction
            ).select(left.col("ts"), right.col("tag"))
            AsOfKeyEvaluations.left.set(0)
            AsOfKeyEvaluations.right.set(0)
            val rows = df.collect().toSeq
            val counts = (AsOfKeyEvaluations.left.get, AsOfKeyEvaluations.right.get)
            QueryTest.sameRows(expected, rows).foreach(error => fail(error))
            val op = assertAsOfJoinCodegen(df, codegen)
            assert((op.metrics("numAsOfCursorTasks").value > 0) == cursorScan)
            counts
          }
        }
      for (codegen <- Seq(false, true)) {
        val (bufferedLeft, bufferedRight) = evaluations(cursorScan = false, codegen)
        val (cursorLeft, cursorRight) = evaluations(cursorScan = true, codegen)
        assert((bufferedLeft - cursorLeft, bufferedRight - cursorRight) == fewerEvaluations,
          s"$direction, tolerance $tolerance, codegen $codegen: buffered " +
            s"($bufferedLeft, $bufferedRight), cursor ($cursorLeft, $cursorRight)")
      }
    }
  }

  test("tolerance joins without ON keys use the cursor scan") {
    val left = spark.sql("SELECT * FROM VALUES (1, 8), (2, 12), (3, 23), (4, 40) AS t(id, ts)")
    val right = spark.sql(
      """SELECT * FROM VALUES (NULL, 'r_null'), (10, 'r10'), (15, 'r15'), (20, 'r20'), (42, 'r42')
        |AS t(ts, tag)""".stripMargin)
    // 23 is 3 after 20 and 12 is 3 before 15, so a strict bound drops them.
    Seq(
      ("backward", true, Seq(Row(1, null), Row(2, "r10"), Row(3, "r20"), Row(4, null))),
      ("backward", false, Seq(Row(1, null), Row(2, "r10"), Row(3, null), Row(4, null))),
      ("forward", true, Seq(Row(1, "r10"), Row(2, "r15"), Row(3, null), Row(4, "r42"))),
      ("forward", false, Seq(Row(1, "r10"), Row(2, null), Row(3, null), Row(4, "r42")))
    ).foreach { case (direction, inclusive, expected) =>
      withClue(s"$direction, inclusive bound $inclusive: ") {
        withExtraOptimization(WithToleranceBound(inclusive)) {
          checkBothScans(
            left.joinAsOf(
              right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
              joinType = "leftouter", tolerance = functions.lit(3),
              allowExactMatches = true, direction = direction
            ).select(left.col("id"), right.col("tag")),
            expected)
        }
      }
    }
  }

  test("tolerance joins match a plain join on random rows with the cursor scan off and on") {
    // Right as-of keys are unique within each ON key, so a plain join with MAX or MIN over
    // (as-of key, tag) finds the one closest right row.
    val leftSchema = new StructType().add("id", IntegerType).add("k", IntegerType)
      .add("ts", IntegerType)
    val rightSchema = new StructType().add("k", IntegerType).add("ts", IntegerType)
      .add("tag", StringType)
    (1 to 2).foreach { seed =>
      val random = new Random(seed)
      def orNull(value: Int): Any = if (random.nextInt(10) == 0) null else value
      // Left ON keys are 0 to 7 and right ON keys 2 to 9, so keys 0, 1, 8 and 9 have one side.
      val leftRows = (0 until 200).map { id =>
        Row(id, orNull(random.nextInt(8)), orNull(random.nextInt(100)))
      }
      val rightRows = (2 until 10).flatMap { k =>
        random.shuffle((0 until 100).toList).take(30).map(ts => Row(k, ts, s"q$k-$ts"))
      } ++ Seq(Row(null, 5, "NULL ON key"), Row(3, null, "NULL as-of key"))
      val left = spark.createDataFrame(spark.sparkContext.parallelize(leftRows), leftSchema)
      val right = spark.createDataFrame(spark.sparkContext.parallelize(rightRows), rightSchema)
      withTempView("t", "q") {
        left.createOrReplaceTempView("t")
        right.createOrReplaceTempView("q")
        for {
          direction <- Seq("backward", "forward")
          allowExactMatches <- Seq(true, false)
          inclusive <- Seq(true, false)
          tolerance <- Seq(0, 5)
        } {
          val base = (direction, allowExactMatches) match {
            case ("backward", true) => ">="
            case ("backward", false) => ">"
            case (_, true) => "<="
            case _ => "<"
          }
          val (closest, bound) = (direction, inclusive) match {
            case ("backward", true) => ("MAX", s"q.ts >= t.ts - $tolerance")
            case ("backward", false) => ("MAX", s"q.ts > t.ts - $tolerance")
            case (_, true) => ("MIN", s"q.ts <= t.ts + $tolerance")
            case _ => ("MIN", s"q.ts < t.ts + $tolerance")
          }
          val expected = sql(
            s"""
               |SELECT t.id, $closest(named_struct('ts', q.ts, 'tag', q.tag)).tag
               |FROM t LEFT JOIN q ON t.k = q.k AND t.ts $base q.ts AND $bound
               |GROUP BY t.id
               |""".stripMargin).collect().toSeq
          // With tolerance 0, no right row passes unless the bound and the match both allow an
          // equal key. The optimizer can then fold a condition such as l < r AND r <= l + 0
          // into false, which has no keys to move a cursor, so the scan is not checked then.
          val canMatch = tolerance > 0 || (allowExactMatches && inclusive)
          if (!canMatch) assert(expected.forall(_.isNullAt(1)), expected)
          // The default spill thresholds: a one-row threshold makes the buffered scan open one
          // spill file per right row of the group for every left row.
          for (cursorScan <- Seq(false, true)) {
            withClue(s"seed $seed, $direction, allowExactMatches $allowExactMatches, " +
                s"inclusive bound $inclusive, tolerance $tolerance, cursor scan $cursorScan: ") {
              withSQLConf(
                  SQLConf.SHUFFLE_PARTITIONS.key -> "1",
                  SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> cursorScan.toString,
                  SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName) {
                withExtraOptimization(WithToleranceBound(inclusive)) {
                  withAsOfJoinCodegenOffAndOn { codegen =>
                    val df = left.joinAsOf(
                      right, left.col("ts"), right.col("ts"), usingColumns = Seq("k"),
                      joinType = "leftouter", tolerance = functions.lit(tolerance),
                      allowExactMatches = allowExactMatches, direction = direction
                    ).select(left.col("id"), right.col("tag"))
                    checkAnswer(df, expected)
                    val op = assertAsOfJoinCodegen(df, codegen)
                    if (canMatch) {
                      assert((op.metrics("numAsOfCursorTasks").value > 0) == cursorScan,
                        "Unexpected scan: the cursor scan runs exactly when it is on")
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("nearest joins with a tolerance keep the buffered scan") {
    val left = spark.sql("SELECT * FROM VALUES (10), (20) AS t(ts)")
    val right = spark.sql("SELECT * FROM VALUES (8, 'r8'), (11, 'r11'), (25, 'r25') AS t(ts, tag)")
    val df = left.joinAsOf(
      right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
      joinType = "leftouter", tolerance = functions.lit(3),
      allowExactMatches = true, direction = "nearest"
    ).select(left.col("ts"), right.col("tag"))
    checkAnswer(df, Seq(Row(10, "r11"), Row(20, null)))
    val op = assertAsOfJoinCodegen(df, codegen = false)
    assert(op.metrics("numAsOfCursorTasks").value == 0, "Expected the buffered scan")
  }

  test("a tolerance join with a tolerance bound in ON too keeps the buffered scan") {
    // Trade 10 is 3 after quote 7: inside the tolerance of 5, but outside the ON bound of 2.
    // Trade 20 is 1 after quote 19, inside both.
    val left = spark.sql("SELECT * FROM VALUES (10), (20) AS t(ts)")
    val right = spark.sql("SELECT * FROM VALUES (7, 'r7'), (19, 'r19') AS t(ts, tag)")
    def df: classic.DataFrame = left.joinAsOf(
      right, left.col("ts"), right.col("ts"),
      joinExprs = right.col("ts") >= left.col("ts") - 2,
      joinType = "leftouter", tolerance = functions.lit(5),
      allowExactMatches = true, direction = "backward"
    ).select(left.col("ts"), right.col("tag"))
    withAsOfJoinCodegenOffAndOn { codegen =>
      val query = df
      checkAnswer(query, Seq(Row(10, null), Row(20, "r19")))
      val op = assertAsOfJoinCodegen(query, codegen)
      assert(op.condition.isDefined, s"Expected the ON bound as the residual condition in:\n$op")
      assert(op.metrics("numAsOfCursorTasks").value == 0, "Expected the buffered scan")
    }
  }

  test("a tolerance bound given only in joinExprs uses the cursor scan") {
    // The bound is the residual condition. Backward, 10 is 1 after 9, 20 is 1 after 19, and 30
    // is 5 after 25. Forward, 10 is 2 before 12, 20 is 5 before 25, and 30 is 1 before 31.
    val left = spark.sql("SELECT * FROM VALUES (10), (20), (30) AS t(ts)")
    val right = spark.sql(
      "SELECT * FROM VALUES (9, 'r9'), (12, 'r12'), (19, 'r19'), (25, 'r25'), (31, 'r31') " +
        "AS t(ts, tag)")
    Seq(
      ("backward", right.col("ts") >= left.col("ts") - 2,
        Seq(Row(10, "r9"), Row(20, "r19"), Row(30, null))),
      ("forward", right.col("ts") <= left.col("ts") + 2,
        Seq(Row(10, "r12"), Row(20, null), Row(30, "r31")))
    ).foreach { case (direction, bound, expected) =>
      withClue(s"$direction: ") {
        def df: classic.DataFrame = left.joinAsOf(
          right, left.col("ts"), right.col("ts"), joinExprs = bound,
          joinType = "leftouter", tolerance = null,
          allowExactMatches = true, direction = direction
        ).select(left.col("ts"), right.col("tag"))
        val join = collectFirst(df.queryExecution.sparkPlan) { case j: SortMergeAsOfJoinExec => j }
        assert(join.exists(_.condition.isDefined),
          s"Expected the bound as the residual condition in:\n${df.queryExecution.sparkPlan}")
        checkBothScans(df, expected)
      }
    }
  }

  test("a tolerance of +Infinity on DOUBLE keys returns the same rows with either scan") {
    // The limit l - Infinity is NaN for l = +Infinity, and l + Infinity is NaN for l = -Infinity.
    // Spark orders NaN above +Infinity, in the sort and in the comparisons, so a NaN limit still
    // passes the right rows at one end of the order.
    val left = spark.createDataFrame(
      Seq(Row(1, Double.PositiveInfinity), Row(2, Double.NegativeInfinity), Row(3, Double.NaN),
        Row(4, 2.0)).asJava,
      new StructType().add("id", IntegerType).add("ts", DoubleType))
    val right = spark.createDataFrame(
      Seq(Row(Double.NegativeInfinity, "ninf"), Row(1.0, "one"),
        Row(Double.PositiveInfinity, "pinf"), Row(Double.NaN, "nan")).asJava,
      new StructType().add("ts", DoubleType).add("tag", StringType))
    Seq(
      "backward" -> Seq(null, "ninf", "nan", "one"),
      "forward" -> Seq("pinf", "ninf", "nan", "pinf")
    ).foreach { case (direction, tags) =>
      withClue(s"$direction: ") {
        checkBothScans(
          left.joinAsOf(
            right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
            joinType = "leftouter", tolerance = functions.lit(Double.PositiveInfinity),
            allowExactMatches = true, direction = direction
          ).select(left.col("id"), right.col("tag")),
          tags.zipWithIndex.map { case (tag, i) => Row(i + 1, tag) })
      }
    }
  }

  test("a tolerance limit that overflows fails both scans under ANSI and matches nothing " +
      "without ANSI") {
    val schema = new StructType().add("k", StringType).add("ts", LongType)
    def rows(values: Any*): classic.DataFrame = spark.createDataFrame(
      values.map(value => Row("A", value)).asJava, schema)
    // Forward adds the tolerance to the left key, and backward subtracts it.
    Seq(
      ("forward", Long.MaxValue - 1, Long.MaxValue),
      ("backward", Long.MinValue + 1, Long.MinValue)
    ).foreach { case (direction, leftTs, rightTs) =>
      val left = rows(leftTs)
      def join(right: classic.DataFrame): classic.DataFrame = left.joinAsOf(
        right, left.col("ts"), right.col("ts"), usingColumns = Seq("k"),
        joinType = "leftouter", tolerance = functions.lit(10L),
        allowExactMatches = true, direction = direction
      ).select(left.col("ts"), right.col("ts"))
      withClue(s"$direction: ") {
        // The right row passes the comparison, so both scans evaluate the limit.
        for {
          cursorScan <- Seq(false, true)
          codegen <- Seq(false, true)
        } {
          withClue(s"cursor scan $cursorScan, codegen $codegen: ") {
            withSQLConf(
                SQLConf.ANSI_ENABLED.key -> "true",
                SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> cursorScan.toString,
                SQLConf.ENABLE_SORT_MERGE_AS_OF_JOIN_CODEGEN.key -> codegen.toString) {
              val e = intercept[Exception](join(rows(rightTs)).collect())
              val causes = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
              assert(causes.exists {
                case error: SparkThrowable => error.getCondition == "ARITHMETIC_OVERFLOW"
                case _ => false
              }, e)
            }
          }
        }
        // Without ANSI, the limit wraps around, so the right row is past it.
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
          checkBothScans(join(rows(rightTs)), Seq(Row(leftTs, null)))
        }
        // No right row passes the comparison, so neither scan evaluates the limit. The NULL
        // right key ends the bound before the limit. Without InferFiltersFromConstraints, the
        // NULL right key reaches the join.
        val farSide = if (direction == "forward") 0L else Long.MaxValue
        withSQLConf(
            SQLConf.ANSI_ENABLED.key -> "true",
            SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName) {
          checkBothScans(join(rows(null, farSide)), Seq(Row(leftTs, null)))
        }
      }
    }
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
    withAsOfJoinAnalyzer(singlePass) {
      for {
        ansi <- Seq(false, true)
        sortMerge <- Seq(false, true)
      } {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> sortMerge.toString,
            SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          val left = spark.sql("SELECT '50' AS k")
          val right = spark.sql(
            "SELECT * FROM VALUES ('9', 'nine'), ('10', 'ten'), ('100', 'hundred') AS t(k, tag)")
          val error = intercept[AnalysisException] {
            left.joinAsOf(
              other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
              usingColumns = Seq.empty, joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = "nearest"
            ).queryExecution.analyzed
          }
          checkError(
            exception = error,
            condition = "AS_OF_JOIN.UNSUPPORTED_NEAREST_KEY_TYPE",
            parameters = Map("dataType" -> "\"STRING\"")
          )
          assert(!error.getMessage.contains("sort-merge"))
          val castKeys = left.joinAsOf(
            other = right, leftAsOf = left.col("k").cast("double"),
            rightAsOf = right.col("k").cast("double"), usingColumns = Seq.empty,
            joinType = "inner", tolerance = null, allowExactMatches = true, direction = "nearest"
          ).select("tag")
          QueryTest.checkAnswer(df = castKeys, expectedAnswer = Seq(Row("ten")), checkToRDD = false)
        }
      }
    }
  }

  gridTest("nearest join rejects unsupported sort-merge key types")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      for {
        key <- Seq("true", "X'01'", "array(1)", "named_struct('a', 1)", "TIME '10:00:00'")
      } {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
            SQLConf.TIME_TYPE_ENABLED.key -> "true") {
          val left = spark.sql(s"SELECT $key AS k")
          val right = spark.sql(s"SELECT $key AS k")
          checkError(
            exception = intercept[AnalysisException] {
              left.joinAsOf(
                other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
                usingColumns = Seq.empty, joinType = "inner", tolerance = null,
                allowExactMatches = true, direction = "nearest"
              ).queryExecution.analyzed
            },
            condition = "AS_OF_JOIN.UNSUPPORTED_NEAREST_KEY_TYPE",
            parameters = Map("dataType" -> s"\"${left.schema("k").dataType.sql}\"")
          )
        }
      }
    }
  }

  gridTest("nearest join with untyped null keys")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true") {
        val left = spark.sql("SELECT NULL AS k, 1 AS id")
        val right = spark.sql("SELECT NULL AS k, 'right' AS tag")
        val joined = left.joinAsOf(
          other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
          usingColumns = Seq.empty, joinType = "leftouter", tolerance = null,
          allowExactMatches = true, direction = "nearest"
        ).select("id", "tag")
        QueryTest.checkAnswer(
          df = joined, expectedAnswer = Seq(Row(1, null)), checkToRDD = false)
      }
    }
  }

  gridTest("backward and forward joins sort STRING keys as text")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      for (ansi <- Seq(false, true)) {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          val left = spark.sql("SELECT * FROM VALUES ('50'), ('k'), ('u') AS t(k)")
          val right = spark.sql(
            "SELECT * FROM VALUES ('p'), ('100'), ('h'), ('10'), ('9') AS t(k)")
          for (direction <- Seq("backward", "forward")) {
            def joined: DataFrame = left.joinAsOf(
              other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
              usingColumns = Seq.empty, joinType = "leftouter", tolerance = null,
              allowExactMatches = true, direction = direction
            )
            val expected = if (direction == "backward") {
              Seq(Row("50", "100"), Row("k", "h"), Row("u", "p"))
            } else {
              Seq(Row("50", "9"), Row("k", "p"), Row("u", null))
            }
            checkAnswerCodegenOffAndOn(joined, expected, checkToRDD = false)
          }
        }
      }
    }
  }

  gridTest("sort-merge joins reject a tolerance on STRING keys")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      for {
        ansi <- Seq(false, true)
        collation <- Seq("", " COLLATE UTF8_LCASE")
      } {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          val left = spark.sql(s"SELECT '50'$collation AS lk")
          val right = spark.sql(
            s"SELECT k$collation AS rk FROM VALUES ('41'), ('5'), ('50') AS t(k)")
          for (direction <- Seq("backward", "forward")) {
            checkError(
              exception = intercept[AnalysisException] {
                left.joinAsOf(
                  other = right, leftAsOf = left.col("lk"), rightAsOf = right.col("rk"),
                  usingColumns = Seq.empty, joinType = "inner", tolerance = functions.lit(10),
                  allowExactMatches = true, direction = direction
                ).queryExecution.analyzed
              },
              condition = "AS_OF_JOIN.UNSUPPORTED_TOLERANCE_KEY_TYPE",
              parameters = Map("dataType" -> s""""STRING$collation"""")
            )
            def castKeys: DataFrame = left.joinAsOf(
              other = right, leftAsOf = left.col("lk").cast("int"),
              rightAsOf = right.col("rk").cast("int"), usingColumns = Seq.empty,
              joinType = "inner", tolerance = functions.lit(10),
              allowExactMatches = true, direction = direction
            ).select("rk")
            checkAnswerCodegenOffAndOn(castKeys, Seq(Row("50")), checkToRDD = false)
          }
        }
      }
    }
  }

  gridTest("join rejects mixed STRING collations")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      for (ansi <- Seq(false, true)) {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          val left = spark.sql("SELECT 'z' AS lk")
          val right = spark.sql("SELECT * FROM VALUES ('Z'), ('a') AS t(rk)")
          for (direction <- Seq("backward", "forward", "nearest")) {
            checkError(
              exception = intercept[AnalysisException] {
                left.joinAsOf(
                  other = right, leftAsOf = functions.collate(left.col("lk"), "UTF8_LCASE"),
                  rightAsOf = right.col("rk"),
                  usingColumns = Seq.empty, joinType = "inner", tolerance = null,
                  allowExactMatches = true, direction = direction
                ).queryExecution.analyzed
              },
              condition = "AS_OF_JOIN.UNSUPPORTED_KEY_TYPE",
              parameters = Map("type1" -> "\"STRING COLLATE UTF8_LCASE\"", "type2" -> "\"STRING\"")
            )
          }
        }
      }
    }
  }

  gridTest("backward and forward joins use the STRING keys' collation")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      for (ansi <- Seq(false, true)) {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          val left = spark.sql("SELECT 'z' COLLATE UTF8_LCASE AS lk")
          val right = spark.sql(
            "SELECT rk COLLATE UTF8_LCASE AS rk FROM VALUES ('Z'), ('a') AS t(rk)")
          for {
            direction <- Seq("backward", "forward")
            allowExactMatches <- Seq(false, true)
          } {
            def joined: DataFrame = left.joinAsOf(
              other = right, leftAsOf = left.col("lk"), rightAsOf = right.col("rk"),
              usingColumns = Seq.empty, joinType = "leftouter", tolerance = null,
              allowExactMatches = allowExactMatches, direction = direction
            ).select("rk")
            val expected = if (allowExactMatches) {
              "Z"
            } else if (direction == "backward") {
              "a"
            } else {
              null
            }
            checkAnswerCodegenOffAndOn(joined, Seq(Row(expected)), checkToRDD = false)
          }
        }
      }
    }
  }

  gridTest("join keys can be reversed")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true") {
        val left = spark.sql("SELECT 10 AS lk")
        val right = spark.sql(
          "SELECT * FROM VALUES (7, 'far'), (9, 'before'), (12, 'after') AS t(rk, tag)")
        for ((direction, expected) <- Seq(
            "backward" -> "after", "forward" -> "before", "nearest" -> "before")) {
          def reversed: DataFrame = left.joinAsOf(
            other = right, leftAsOf = right.col("rk"), rightAsOf = left.col("lk"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = direction
          ).select("tag")
          checkAnswerCodegenOffAndOn(reversed, Seq(Row(expected)), checkToRDD = false)
        }
      }
    }
  }

  gridTest("reversed join keys respect the tolerance")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true") {
        val left = spark.sql("SELECT * FROM VALUES (10), (20) AS t(lk)")
        val right = spark.sql(
          """SELECT * FROM VALUES
            |(7, 'far before'), (9, 'before'), (11, 'after'), (13, 'far after'),
            |(18, 'outside before'), (22, 'outside after') AS t(rk, tag)
            |""".stripMargin)
        for ((direction, expected) <- Seq("backward" -> "after", "forward" -> "before")) {
          def reversed: DataFrame = left.joinAsOf(
            other = right, leftAsOf = right.col("rk"), rightAsOf = left.col("lk"),
            usingColumns = Seq.empty, joinType = "leftouter", tolerance = functions.lit(1),
            allowExactMatches = true, direction = direction
          ).select("lk", "tag")
          checkAnswerCodegenOffAndOn(
            reversed, Seq(Row(10, expected), Row(20, null)), checkToRDD = false)
        }
      }
    }
  }

  test("a tolerance join on a computed right key keeps the buffered scan, which reads that key " +
      "for a NULL left key") {
    // For a NULL left key, l >= r is NULL, not FALSE, so the buffered scan goes on to the bound
    // r >= l - 10. That reads r.v * 1000, which overflows under ANSI. The cursor scan returns
    // before it reads any right key. One right row per key, so the sort below never compares
    // and never evaluates the right key either.
    val left = spark.sql("SELECT * FROM VALUES ('B', CAST(NULL AS BIGINT)) AS t(k, v)")
    def join(rightValue: Long, direction: String): classic.DataFrame = {
      val right = spark.sql(s"SELECT * FROM VALUES ('B', ${rightValue}L) AS t(k, v)")
      left.joinAsOf(
        right, left.col("v"), right.col("v") * 1000, usingColumns = Seq("k"),
        joinType = "left", tolerance = functions.lit(10L),
        allowExactMatches = true, direction = direction)
    }
    // Without InferFiltersFromConstraints, nothing below the join reads the right key.
    withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName) {
      for (direction <- Seq("backward", "forward")) {
        withClue(s"$direction: ") {
          for {
            cursorScan <- Seq(false, true)
            codegen <- Seq(false, true)
          } {
            withClue(s"cursor scan $cursorScan, codegen $codegen: ") {
              withSQLConf(
                  SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> cursorScan.toString,
                  SQLConf.ENABLE_SORT_MERGE_AS_OF_JOIN_CODEGEN.key -> codegen.toString) {
                val e = intercept[Exception](join(Long.MaxValue, direction).collect())
                val causes = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
                assert(causes.exists {
                  case error: SparkThrowable => error.getCondition == "ARITHMETIC_OVERFLOW"
                  case _ => false
                }, e)
              }
            }
          }
          // A right key that does not overflow: the cursor scan is on, and not used.
          withAsOfJoinCodegenOffAndOn { codegen =>
            val df = join(1L, direction)
            checkAnswer(df, Row("B", null, null, null) :: Nil)
            val op = assertAsOfJoinCodegen(df, codegen)
            assert(op.metrics("numAsOfCursorTasks").value == 0,
              s"Expected the buffered scan:\n${df.queryExecution.executedPlan}")
          }
        }
      }
    }
  }

  test("reversed join keys keep the buffered scan, which skips a failing left key") {
    // The reversed condition reads the right key first and stops at its NULL, so v * 1000 never
    // overflows. The cursor reads the left key first, so it would fail the query under ANSI.
    withSQLConf(
        SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true") {
      val left = spark.sql("SELECT * FROM VALUES ('B', 9223372036854775807L) AS t(k, v)")
      val right = spark.sql("SELECT * FROM VALUES ('B', CAST(NULL AS BIGINT)) AS t(k, v)")
      for (direction <- Seq("backward", "forward")) {
        withClue(s"$direction: ") {
          withAsOfJoinCodegenOffAndOn { codegen =>
            val reversed = left.joinAsOf(
              other = right, leftAsOf = right.col("v"), rightAsOf = left.col("v") * 1000,
              usingColumns = Seq("k"), joinType = "left", tolerance = null,
              allowExactMatches = true, direction = direction)
            checkAnswer(reversed, Row("B", Long.MaxValue, null, null) :: Nil)
            val op = assertAsOfJoinCodegen(reversed, codegen)
            assert(op.metrics("numAsOfCursorTasks").value == 0,
              s"Expected the buffered scan:\n${reversed.queryExecution.executedPlan}")
          }
        }
      }
    }
  }

  gridTest("join keys can reference hidden USING columns")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true") {
        val parsed = spark.sessionState.sqlParser.parsePlan(
          """SELECT l.id, r.tag
            |FROM ((VALUES (1, 10), (2, 20)) AS l(id, k)
            |  LEFT JOIN (VALUES (10)) AS lk(k) USING (k))
            |LEFT ASOF JOIN (
            |  (VALUES (9, 'before'), (12, 'after'), (30, 'hidden')) AS r(k, tag)
            |  LEFT JOIN (VALUES (9), (12)) AS rk(k) USING (k))
            |  MATCH_CONDITION (lk.k >= rk.k)
            |""".stripMargin)
        for (direction <- Seq("backward", "forward", "nearest")) {
          // Keep the USING inputs unresolved when constructing the DataFrame ASOF node.
          val plan = parsed.transform {
            case join: AsOfJoin =>
              AsOfJoin(
                left = join.left, right = join.right,
                leftAsOf = join.matchLeftOperand.get, rightAsOf = join.matchRightOperand.get,
                condition = None, joinType = join.joinType, tolerance = None,
                allowExactMatches = true, direction = AsOfJoinDirection(direction)
              )
          }
          def joined: DataFrame = classic.Dataset.ofRows(spark, plan)
          val expected = if (direction == "forward") "after" else "before"
          checkAnswerCodegenOffAndOn(
            joined, Seq(Row(1, expected), Row(2, null)), checkToRDD = false)
        }
      }
    }
  }

  gridTest("join rejects constant keys before numeric coercion")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      for (ansi <- Seq(false, true)) {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> ansi.toString,
            SQLConf.LITERAL_PICK_MINIMUM_PRECISION.key -> "true",
            SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.key -> "false") {
          val left = spark.sql("SELECT CAST(1E-30 AS DECIMAL(38, 30)) AS lk")
          val right = spark.sql("SELECT CAST(1E-30 AS DECIMAL(38, 30)) AS rk")
          for {
            direction <- Seq("backward", "forward", "nearest")
            (leftKey, rightKey, refs1, refs2) <- Seq(
              (left.col("lk"), functions.lit(0), "\"lk\"", "\"0\""),
              (functions.lit(0), right.col("rk"), "\"0\"", "\"rk\""),
              (left.col("lk"), functions.lit(0) + functions.lit(1),
                "\"lk\"", "\"(0 + 1)\""))
          } {
            checkError(
              exception = intercept[AnalysisException] {
                left.joinAsOf(
                  other = right, leftAsOf = leftKey, rightAsOf = rightKey,
                  usingColumns = Seq.empty, joinType = "inner", tolerance = null,
                  allowExactMatches = false, direction = direction
                ).queryExecution.analyzed
              },
              condition = "AS_OF_JOIN.KEY_REFERENCE",
              parameters = Map("refs1" -> refs1, "refs2" -> refs2)
            )
          }
        }
      }
    }
  }

  gridTest("join key errors describe DataFrame keys")(Seq(false, true)) { singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
          SQLConf.ANSI_ENABLED.key -> "true") {
        val left = spark.sql("SELECT 10 AS lk")
        val right = spark.sql("SELECT 9 AS rk")
        for (direction <- Seq("backward", "forward", "nearest")) {
          for ((leftKey, rightKey, refs1, refs2) <- Seq(
              (left.col("lk") + right.col("rk"), right.col("rk"),
                "\"(lk + rk)\"", "\"rk\""),
              (left.col("lk"), left.col("lk"), "\"lk\"", "\"lk\""))) {
            val error = intercept[AnalysisException] {
              left.joinAsOf(
                other = right, leftAsOf = leftKey, rightAsOf = rightKey,
                usingColumns = Seq.empty, joinType = "inner", tolerance = null,
                allowExactMatches = true, direction = direction
              ).queryExecution.analyzed
            }
            checkError(
              exception = error,
              condition = "AS_OF_JOIN.KEY_REFERENCE",
              parameters = Map("refs1" -> refs1, "refs2" -> refs2)
            )
            assert(!error.getMessage.contains("MATCH_CONDITION"))
          }

          for ((leftKey, invalidExpression) <- Seq(
              (left.col("lk") + functions.rand(), ".*rand.*"),
              (left.col("lk") + functions.expr("(SELECT 1)"), ".*scalarsubquery.*"))) {
            val error = intercept[AnalysisException] {
              left.joinAsOf(
                other = right, leftAsOf = leftKey, rightAsOf = right.col("rk"),
                usingColumns = Seq.empty, joinType = "inner", tolerance = null,
                allowExactMatches = true, direction = direction
              ).queryExecution.analyzed
            }
            checkErrorMatchPVals(
              exception = error,
              condition = "AS_OF_JOIN.INVALID_KEY_EXPRESSION",
              parameters = Map("expr" -> invalidExpression)
            )
            assert(!error.getMessage.contains("MATCH_CONDITION"))
          }
        }
      }
    }
  }

  /**
   * Writes a left input of one parquet file, and a right input of a smaller and a bigger file
   * with one row each. The left file size is between them, nearer the smaller one. Passes `f`
   * the two paths.
   */
  private def withFileSizeInputs(f: (String, String) => Unit): Unit = {
    withTempPath { dir =>
      val leftPath = s"${dir.getCanonicalPath}/left"
      val rightPath = s"${dir.getCanonicalPath}/right"
      // SHA-256 digests do not compress, so the number of digests sets the file size.
      def text(digests: Int): String = "concat_ws('', transform(" +
        s"sequence(1, $digests), x -> sha2(CAST(id * 1000 + x AS STRING), 256))) AS text"
      spark.range(0, 3).selectExpr("id AS lid", text(20))
        .coalesce(1).write.parquet(leftPath)
      spark.range(100, 101).selectExpr("id AS rid", text(1))
        .coalesce(1).write.parquet(rightPath)
      spark.range(200, 201).selectExpr("id AS rid", text(600))
        .coalesce(1).write.mode("append").parquet(rightPath)

      val leftSize = spark.read.parquet(leftPath).select("_metadata.file_size").head().getLong(0)
      val rightSizes = spark.read.parquet(rightPath)
        .select("rid", "_metadata.file_size").collect().map(r => r.getLong(0) -> r.getLong(1))
        .toMap
      assert(rightSizes(100L) < leftSize && leftSize < rightSizes(200L) &&
        leftSize - rightSizes(100L) < rightSizes(200L) - leftSize,
        s"Unexpected file sizes: left $leftSize, right $rightSizes")
      f(leftPath, rightPath)
    }
  }

  test("join keys can reference file metadata columns") {
    withFileSizeInputs { (leftPath, rightPath) =>
      for ((direction, rid) <- Seq("backward" -> 100L, "forward" -> 200L, "nearest" -> 100L)) {
        withClue(s"$direction: ") {
          val expected = Seq(Row(0L, rid), Row(1L, rid), Row(2L, rid))
          checkAnswerCodegenOffAndOn({
            val left = spark.read.parquet(leftPath)
            val right = spark.read.parquet(rightPath)
            left.joinAsOf(
              right, left.col("_metadata.file_size"), right.col("_metadata.file_size"),
              usingColumns = Seq.empty, joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = direction
            ).select(left.col("lid"), right.col("rid"))
          }, expected)

          // The same join with the metadata fields selected into regular columns first.
          val left = spark.read.parquet(leftPath)
            .select(functions.col("lid"), functions.col("_metadata.file_size").as("lk"))
          val right = spark.read.parquet(rightPath)
            .select(functions.col("rid"), functions.col("_metadata.file_size").as("rk"))
          checkAnswer(
            left.joinAsOf(
              right, left.col("lk"), right.col("rk"), usingColumns = Seq.empty,
              joinType = "inner", tolerance = null,
              allowExactMatches = true, direction = direction
            ).select("lid", "rid"),
            expected)
        }
      }
    }
  }

  // The (left rid, right rid) matches of a self join of the right input of `withFileSizeInputs`
  // on file size, without exact matches.
  private val fileSizeSelfJoinMatches = Seq(
    "backward" -> Seq(Row(200L, 100L)),
    "forward" -> Seq(Row(100L, 200L)),
    "nearest" -> Seq(Row(100L, 200L), Row(200L, 100L)))

  test("self join keys can reference file metadata columns through aliases") {
    withFileSizeInputs { (_, path) =>
      val df = spark.read.parquet(path)
      for ((direction, expected) <- fileSizeSelfJoinMatches) {
        withClue(s"$direction: ") {
          checkAnswerCodegenOffAndOn(
            df.as("l").joinAsOf(
              df.as("r"), functions.col("l._metadata.file_size"),
              functions.col("r._metadata.file_size"), usingColumns = Seq.empty,
              joinType = "inner", tolerance = null,
              allowExactMatches = false, direction = direction
            ).select("l.rid", "r.rid"),
            expected)
        }
      }
    }
  }

  test("self join keys can reference file metadata columns without aliases") {
    withFileSizeInputs { (_, path) =>
      def fileSize(df: DataFrame): Column = df.col("_metadata.file_size")
      // The same key, with the metadata column in a lambda body.
      def fileSizeInLambda(df: DataFrame): Column = functions.aggregate(
        functions.array(functions.lit(0L)), functions.lit(0L), (acc, x) => acc + x + fileSize(df))
      def selfJoin(direction: String)(
          other: classic.DataFrame => classic.DataFrame, key: DataFrame => Column): DataFrame = {
        val df = spark.read.parquet(path)
        df.joinAsOf(
          other(df), key(df), key(df), usingColumns = Seq.empty, joinType = "inner",
          tolerance = null, allowExactMatches = false, direction = direction
        ).toDF("lrid", "ltext", "rrid", "rtext").select("lrid", "rrid")
      }
      for ((direction, expected) <- fileSizeSelfJoinMatches) {
        withClue(s"$direction: ") {
          checkAnswerCodegenOffAndOn(selfJoin(direction)(df => df, fileSize), expected)
          checkAnswerCodegenOffAndOn(
            selfJoin(direction)(_.filter(functions.col("rid") > 0), fileSize), expected)
          checkAnswerCodegenOffAndOn(selfJoin(direction)(df => df, fileSizeInLambda), expected)
        }
      }
    }
  }

  test("join keys over file metadata columns must each reference one input") {
    withFileSizeInputs { (leftPath, rightPath) =>
      val left = spark.read.parquet(leftPath)
      val right = spark.read.parquet(rightPath)
      val metadataKey = "\"_metadata.file_size AS file_size\""
      for {
        direction <- Seq("backward", "forward", "nearest")
        (input, other, leftKey, rightKey, refs1) <- Seq(
          (left, right, left.col("_metadata.file_size") + right.col("rid"),
            right.col("_metadata.file_size"), "\"(_metadata.file_size AS file_size + rid)\""),
          (left, right, left.col("_metadata.file_size"), left.col("_metadata.file_size"),
            metadataKey))
      } {
        checkError(
          exception = intercept[AnalysisException] {
            input.joinAsOf(
              other, leftKey, rightKey, usingColumns = Seq.empty, joinType = "inner",
              tolerance = null, allowExactMatches = true, direction = direction
            ).queryExecution.analyzed
          },
          condition = "AS_OF_JOIN.KEY_REFERENCE",
          parameters = Map("refs1" -> refs1, "refs2" -> metadataKey))
      }
    }
  }

  test("join keys can reference DSv2 metadata columns") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryCatalog].getName) {
      withTable("testcat.asof_left", "testcat.asof_right") {
        // One write task per table, so the `index` metadata column numbers the rows in order.
        spark.sql("SELECT * FROM VALUES (1, 'a'), (1, 'b'), (2, 'c') AS t(k, lid)")
          .coalesce(1).writeTo("testcat.asof_left").create()
        spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(k, rid)")
          .coalesce(1).writeTo("testcat.asof_right").create()
        checkAnswer(spark.table("testcat.asof_left").select("lid", "index"),
          Seq(Row("a", 0), Row("b", 1), Row("c", 2)))
        checkAnswer(spark.table("testcat.asof_right").select("rid", "index"),
          Seq(Row("x", 0), Row("y", 1)))

        def join(usingColumns: Seq[String], joinType: String, direction: String): DataFrame = {
          val left = spark.table("testcat.asof_left")
          val right = spark.table("testcat.asof_right")
          left.joinAsOf(
            right, left.col("index"), right.col("index"), usingColumns = usingColumns,
            joinType = joinType, tolerance = null,
            allowExactMatches = true, direction = direction
          ).select(left.col("lid"), right.col("rid"))
        }
        for ((direction, expected) <- Seq(
            "backward" -> Seq(Row("a", "x"), Row("b", "y"), Row("c", "y")),
            "forward" -> Seq(Row("a", "x"), Row("b", "y")),
            "nearest" -> Seq(Row("a", "x"), Row("b", "y"), Row("c", "y")))) {
          withClue(s"$direction: ") {
            checkAnswerCodegenOffAndOn(join(Nil, "inner", direction), expected)
          }
        }
        // No right row has an index at or after the last left row.
        checkAnswerCodegenOffAndOn(
          join(Nil, "leftouter", "forward"), Seq(Row("a", "x"), Row("b", "y"), Row("c", null)))
        // The ON key limits each left row to the right rows with the same `k`.
        checkAnswerCodegenOffAndOn(
          join(Seq("k"), "inner", "backward"), Seq(Row("a", "x"), Row("b", "x"), Row("c", "y")))
      }
    }
  }

  /** Writes a DSv2 table `testcat.asof_self` with ids x, y and z at indexes 0, 1 and 2. */
  private def withIndexedTable(f: => Unit): Unit = {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryCatalog].getName) {
      withTable("testcat.asof_self") {
        // One write task, so the `index` metadata column numbers the rows in order.
        spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y'), (3, 'z') AS t(k, id)")
          .coalesce(1).writeTo("testcat.asof_self").create()
        checkAnswer(spark.table("testcat.asof_self").select("id", "index"),
          Seq(Row("x", 0), Row("y", 1), Row("z", 2)))
        f
      }
    }
  }

  /** Joins `testcat.asof_self` with `other(t)`, a plan over that table, on `index`, unaliased. */
  private def indexSelfJoin(
      direction: String,
      allowExactMatches: Boolean,
      other: classic.DataFrame => classic.DataFrame = df => df): DataFrame = {
    val t = spark.table("testcat.asof_self")
    t.joinAsOf(
      other(t), t.col("index"), t.col("index"), usingColumns = Seq.empty, joinType = "inner",
      tolerance = null, allowExactMatches = allowExactMatches, direction = direction
    ).toDF("lk", "lid", "rk", "rid").select("lid", "rid")
  }

  test("self join keys can reference DSv2 metadata columns without aliases") {
    withIndexedTable {
      for ((direction, expected) <- Seq(
          "backward" -> Seq(Row("y", "x"), Row("z", "y")),
          "forward" -> Seq(Row("x", "y"), Row("y", "z")),
          // On equal gaps, nearest keeps the earlier row, so y gets x.
          "nearest" -> Seq(Row("x", "y"), Row("y", "x"), Row("z", "y")))) {
        withClue(s"$direction: ") {
          val t = spark.table("testcat.asof_self")
          checkAnswerCodegenOffAndOn(
            t.as("l").joinAsOf(
              t.as("r"), functions.col("l.index"), functions.col("r.index"),
              usingColumns = Seq.empty, joinType = "inner", tolerance = null,
              allowExactMatches = false, direction = direction
            ).select("l.id", "r.id"),
            expected)
          checkAnswerCodegenOffAndOn(indexSelfJoin(direction, allowExactMatches = false), expected)
          checkAnswerCodegenOffAndOn(
            indexSelfJoin(direction, allowExactMatches = false, _.filter(functions.col("k") > 0)),
            expected)
        }
      }
    }
  }

  test("unaliased DSv2 metadata self join returns the same rows with sort-merge on and off") {
    withIndexedTable {
      for ((direction, allowExactMatches, expected) <- Seq(
          ("backward", false, Seq(Row("y", "x"), Row("z", "y"))),
          ("forward", false, Seq(Row("x", "y"), Row("y", "z"))),
          // Exact matches: y is as near to x as to z, and the two plans pick different rows then.
          ("nearest", true, Seq(Row("x", "x"), Row("y", "y"), Row("z", "z"))))) {
        withClue(s"$direction: ") {
          checkAnswerCodegenOffAndOn(indexSelfJoin(direction, allowExactMatches), expected)
          withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
            val joined = indexSelfJoin(direction, allowExactMatches)
            checkAnswer(joined, expected)
            assert(collectWithSubqueries(joined.queryExecution.executedPlan) {
              case j: SortMergeAsOfJoinExec => j
            }.isEmpty, "Expected the correlated subquery rewrite")
          }
        }
      }
    }
  }

  test("SQL MATCH_CONDITION does not accept file metadata operands yet") {
    withFileSizeInputs { (leftPath, rightPath) =>
      // A known limit: SQL operands skip metadata columns, so both analyzers accept the same ones.
      withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true") {
        checkError(
          exception = intercept[AnalysisException] {
            spark.sql(s"SELECT l.lid, r.rid FROM parquet.`$leftPath` l " +
              s"ASOF JOIN parquet.`$rightPath` r " +
              "MATCH_CONDITION (l._metadata.file_size >= r._metadata.file_size)")
          },
          condition = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
          parameters = Map(
            "refs1" -> "\"_metadata.file_size AS file_size\"",
            "refs2" -> "\"_metadata.file_size AS file_size\""))
      }
    }
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

  gridTest("nearest join rejects numeric constant keys against an INT column")(Seq(
      (2147483647L, "\"2147483647\""),
      (new java.math.BigDecimal("5.4"), "\"5.4\""))) { case (key, expectedRef) =>
    val left = spark.sql("SELECT 1 AS id")
    val right = spark.sql("SELECT 1 AS k")

    checkError(
      exception = intercept[AnalysisException] {
        left.joinAsOf(
          other = right, leftAsOf = functions.lit(key),
          rightAsOf = right.col("k"), usingColumns = Seq.empty, joinType = "inner",
          tolerance = null, allowExactMatches = true, direction = "nearest"
        ).queryExecution.analyzed
      },
      condition = "AS_OF_JOIN.KEY_REFERENCE",
      parameters = Map("refs1" -> expectedRef, "refs2" -> "\"k\"")
    )
  }

  gridTest("nearest join preserves decimal key values and explicit casts")(Seq(false, true)) {
      singlePass =>
    withAsOfJoinAnalyzer(singlePass) {
      for {
        ansi <- Seq(false, true)
        integralRight <- Seq(false, true)
      } {
        withSQLConf(
            SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> ansi.toString,
            SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> "true",
            SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.key -> "false") {
          val left = spark.createDataFrame(
            List(Row(new java.math.BigDecimal("1.4"))).asJava,
            new StructType().add("k", DecimalType(38, 30)))
          val right = spark.createDataFrame(
            (if (integralRight) {
              List(Row(0, "before"), Row(2, "after"))
            } else {
              List(Row(new java.math.BigDecimal("0"), "before"),
                Row(new java.math.BigDecimal("2"), "after"))
            }).asJava,
            new StructType().add("k", if (integralRight) IntegerType else DecimalType(38, 0))
              .add("tag", StringType))
          val joined = left.joinAsOf(
            other = right, leftAsOf = left.col("k"), rightAsOf = right.col("k"),
            usingColumns = Seq.empty, joinType = "inner", tolerance = null,
            allowExactMatches = true, direction = "nearest"
          ).select("tag")
          QueryTest.checkAnswer(df = joined, expectedAnswer = Seq(Row("after")), checkToRDD = false)

          val castKeys = left.joinAsOf(
            other = right, leftAsOf = left.col("k").cast(DecimalType(38, 0)),
            rightAsOf = right.col("k"), usingColumns = Seq.empty, joinType = "inner",
            tolerance = null, allowExactMatches = true, direction = "nearest"
          ).select("tag")
          QueryTest.checkAnswer(
            df = castKeys, expectedAnswer = Seq(Row("before")), checkToRDD = false)
        }
      }
    }
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerWithNullOnKeys(
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
    checkAnswerWithNullOnKeys(
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

  test("nearest join never matches a NULL right equi-key against a NOT NULL left key") {
    // The left key is NOT NULL and the right key nullable. The NULL right key sorts first, so a
    // key compare that read it as 0 would match left key 0 and stop before the rows of key -1.
    val left = spark.createDataFrame(
      List(Row(-1, 10), Row(0, 10)).asJava,
      StructType(
        StructField("k", IntegerType, nullable = false) ::
          StructField("ts", IntegerType, nullable = false) :: Nil))
    val right = spark.createDataFrame(
      List(Row(null, 1, "null key"), Row(-1, 2, "minus one")).asJava,
      StructType(
        StructField("k", IntegerType) ::
          StructField("ts", IntegerType) ::
          StructField("tag", StringType) :: Nil))
    // One partition: with several, the NULL right row could land apart from the left rows and
    // never be compared.
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      checkAnswerWithNullOnKeys(
        left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq("k"),
          joinType = "leftouter", tolerance = null,
          allowExactMatches = true, direction = "nearest")
          .select(left.col("k"), right.col("tag")),
        Row(-1, "minus one") :: Row(0, null) :: Nil)
    }
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
    withAsOfJoinCodegenOffAndOn { codegen =>
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
      assertAsOfJoinCodegen(joined, codegen)
      val plan = joined.queryExecution.executedPlan
      val asOfExecs = collectWithSubqueries(plan) { case j: SortMergeAsOfJoinExec => j }
      assert(asOfExecs.length == 1, s"expected one SortMergeAsOfJoinExec in:\n$plan")
      assert(asOfExecs.head.leftKeys.isEmpty && asOfExecs.head.rightKeys.isEmpty,
        s"<=> must be a residual, so equi-keys must be empty, got ${asOfExecs.head}")
    }
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
    checkAnswerCodegenOffAndOn(
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
    checkAnswerCodegenOffAndOn(
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
          checkAnswerCodegenOffAndOn(
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
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1",
      // The cursor scan never buffers, so turn it off to test the spill.
      SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> "false") {
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
      checkAnswerCodegenOffAndOn(
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
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1",
      // The cursor scan never buffers, so turn it off to test the spill.
      SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> "false") {
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
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1",
      // The cursor scan never buffers, so turn it off to test the spill.
      SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> "false") {
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
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1",
      // The cursor scan never buffers, so turn it off to test the spill.
      SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> "false") {
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

  Seq("backward", "forward").foreach { direction =>
    test(s"generated $direction scan closes the spill readers of a group it leaves early") {
      assume(ManagementFactory.getOperatingSystemMXBean.isInstanceOf[UnixOperatingSystemMXBean],
        "needs a JVM that counts its open files")
      withSQLConf(
          SQLConf.ENABLE_SORT_MERGE_AS_OF_JOIN_CODEGEN.key -> "true",
          SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
          SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "10",
          // The cursor scan never buffers, so turn it off to test the spill.
          SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> "false") {
        // One right group of 50 rows that spills every 10 rows. Each left key k matches the right
        // key k, so every scan stops with the later spill files of the group still unread.
        val left = spark.range(1, 17).selectExpr("CAST(id AS INT) AS ts")
        val right = spark.range(50).selectExpr("CAST(id AS INT) AS ts")
        val df = left.joinAsOf(
          right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = null,
          allowExactMatches = true, direction = direction)
        // Counts inside the task, since the task closes every spill reader when it ends.
        val results = df.queryExecution.toRdd.mapPartitions { rows =>
          val bean = ManagementFactory.getOperatingSystemMXBean
            .asInstanceOf[UnixOperatingSystemMXBean]
          val before = bean.getOpenFileDescriptorCount
          val matches = rows.map(row => (row.getInt(0), row.getInt(1))).toList
          Iterator.single((matches, bean.getOpenFileDescriptorCount - before))
        }.collect()
        val plan = df.queryExecution.executedPlan
        val join = collectFirst(plan) { case j: SortMergeAsOfJoinExec => j }
        assert(join.exists(isGenerated(plan, _)), s"Expected the generated scan in:\n$plan")
        assert(join.get.metrics("spillSize").value > 0, "Expected the right group to spill")
        assert(results.length == 1, "Expected one task, since there is no ON key")
        val (matches, openedFiles) = results.head
        assert(matches == (1 to 16).map(k => (k, k)).toList)
        assert(openedFiles < 8, s"the scan left $openedFiles more files open")
      }
    }
  }

  // (grp, ts, v, tag) rows for the differential test below. Both sides have ties on `ts` with
  // different tags, NULL as-of keys, a NULL equi-key and a group that the other side lacks. The
  // left keys fall before, on, between and after the right keys of their group.
  private def directionTestSides(): (classic.DataFrame, classic.DataFrame) = {
    val schema = StructType(
      StructField("grp", StringType) ::
        StructField("ts", IntegerType) ::
        StructField("v", IntegerType) ::
        StructField("tag", StringType) :: Nil)
    val leftRows = List(
      Row("A", 1, 5, "l1"), Row("A", 5, 5, "l2"), Row("A", 5, 1, "l3"), Row("A", null, 5, "l4"),
      Row("A", 9, 5, "l5"), Row("A", 20, 5, "l6"), Row("B", 4, 5, "l7"), Row("C", 4, 5, "l8"),
      Row(null, 5, 5, "l9"))
    val rightRows = List(
      Row("A", 2, 1, "r1"), Row("A", 5, 9, "r2"), Row("A", 5, 3, "r3"), Row("A", null, 1, "r4"),
      Row("A", 7, 3, "r5"), Row("A", 7, 7, "r6"), Row("A", 12, 1, "r7"), Row("B", 1, 1, "r8"),
      Row("B", 4, 6, "r9"), Row("D", 4, 1, "r10"), Row(null, 5, 1, "r11"))
    // One partition per side fixes the order within a tie.
    (spark.createDataFrame(leftRows.asJava, schema).coalesce(1),
      spark.createDataFrame(rightRows.asJava, schema).coalesce(1))
  }

  // A nearest join has no generated scan to compare with.
  for {
    direction <- Seq("backward", "forward")
    joinType <- Seq("inner", "left")
  } {
    test(s"$direction $joinType join returns the same rows with the ASOF join codegen off and on") {
      val leftRowCount = directionTestSides()._1.count()
      var matchedRows = 0L
      for {
        equiKey <- Seq(false, true)
        residual <- Seq(false, true)
        tolerance <- Seq(None, Some(2))
        allowExactMatches <- Seq(false, true)
        // This residual condition is not a tolerance bound, so it keeps the buffered scan, and
        // the cursor scan config changes nothing there.
        cursorScan <- if (residual) Seq(false) else Seq(false, true)
      } {
        withClue(s"equi-key $equiKey, residual $residual, tolerance $tolerance, " +
            s"exact matches $allowExactMatches, cursor scan $cursorScan: ") {
          withSQLConf(
              SQLConf.SHUFFLE_PARTITIONS.key -> "1",
              SQLConf.SORT_MERGE_AS_OF_JOIN_CURSOR_SCAN_ENABLED.key -> cursorScan.toString) {
            def joined: DataFrame = {
              val (left, right) = directionTestSides()
              val conditions =
                (if (equiKey) Seq(left.col("grp") === right.col("grp")) else Nil) ++
                  (if (residual) Seq(left.col("v") >= right.col("v")) else Nil)
              left.joinAsOf(
                right, left.col("ts"), right.col("ts"),
                joinExprs = conditions.reduceOption(_ && _).orNull,
                joinType = joinType, tolerance = tolerance.map(functions.lit).orNull,
                allowExactMatches = allowExactMatches, direction = direction)
            }
            // The interpreted scan gives the expected rows of the generated one.
            var interpreted: Seq[Row] = null
            withAsOfJoinCodegenOffAndOn { codegen =>
              val query = joined
              if (codegen) {
                checkAnswer(query, interpreted)
              } else {
                interpreted = query.collect().toSeq
              }
              val op = assertAsOfJoinCodegen(query, codegen)
              assert((op.metrics("numAsOfCursorTasks").value > 0) == cursorScan,
                "Unexpected scan: the cursor scan runs exactly when it is on")
            }
            if (joinType == "left") assert(interpreted.length == leftRowCount)
            matchedRows += interpreted.count(!_.isNullAt(7))
          }
        }
      }
      assert(matchedRows > 0, "no case matched any row")
    }
  }

  test("a nearest join built with the codegen on has no generated scan") {
    val (left, right) = directionTestSides()
    val joined = left.joinAsOf(
      right, left.col("ts"), right.col("ts"), usingColumns = Seq.empty,
      joinType = "inner", tolerance = null, allowExactMatches = true, direction = "nearest")
    val planned = joined.queryExecution.sparkPlan.collectFirst {
      case j: SortMergeAsOfJoinExec => j
    }.get
    // The planner already passes the config on. Set it here so the test does not depend on it.
    val join = planned.copy(codegenEnabled = true)
    assert(join.direction == Nearest)
    assert(!join.supportCodegen)
    // A stage built by hand around the join must fail, not run the scan of another direction.
    checkError(
      exception = intercept[SparkException] {
        WholeStageCodegenExec(join)(codegenStageId = 0).doCodeGen()
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "The Nearest AS-OF join has no ordered scan"))
  }
}

/** Counts the calls of the Scala UDF as-of keys of a test. Local tasks run in this JVM. */
private object AsOfKeyEvaluations {
  val left = new AtomicLong()
  val right = new AtomicLong()
}
