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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, NullAwareHashPartitioning}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StructType}

class ExistenceJoinSuite extends SharedSparkSession {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder, toRichColumn}

  private val EnsureRequirements = new EnsureRequirements()

  private lazy val left = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, 2.0),
      Row(1, 2.0),
      Row(2, 1.0),
      Row(2, 1.0),
      Row(3, 3.0),
      Row(null, null),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("a", IntegerType).add("b", DoubleType))

  private lazy val right = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(2, 3.0),
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(null, null),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  private lazy val rightUniqueKey = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  private lazy val singleConditionEQ = EqualTo(left.col("a").expr, right.col("c").expr)

  private lazy val composedConditionEQ = {
    And(EqualTo(left.col("a").expr, right.col("c").expr),
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  private lazy val composedConditionNEQ = {
    And(LessThan(left.col("a").expr, right.col("c").expr),
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  test("ordinary left anti equi-join spreads NULL keys in shuffle partitioning") {
    val nullableLeft = Seq(
      (Integer.valueOf(1), "left-1"),
      (Integer.valueOf(2), "left-2"),
      (null.asInstanceOf[Integer], "left-null-1"),
      (null.asInstanceOf[Integer], "left-null-2")).toDF("k", "lv")
    val nullableRight = Seq(
      (Integer.valueOf(1), "right-1"),
      (null.asInstanceOf[Integer], "right-null")).toDF("k", "rv")
    val joinCondition = EqualTo(nullableLeft("k").expr, nullableRight("k").expr)
    val join = Join(nullableLeft.logicalPlan, nullableRight.logicalPlan,
      LeftAnti, Some(joinCondition), JoinHint.NONE)

    val (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =
      ExtractEquiJoinKeys.unapply(join).getOrElse(fail("Failed to extract equi-join keys"))
    val expectedAnswer = Seq(
      Row(2, "left-2"),
      Row(null, "left-null-1"),
      Row(null, "left-null-2"))

    def checkJoin(createJoin: (SparkPlan, SparkPlan) => SparkPlan): Unit = {
      val plan = EnsureRequirements.apply(createJoin(
        nullableLeft.queryExecution.sparkPlan,
        nullableRight.queryExecution.sparkPlan))
      val partitionings = plan.collect {
        case exchange: ShuffleExchangeExec => exchange.outputPartitioning
      }
      assert(partitionings.size == 2)
      assert(partitionings.forall(_.isInstanceOf[NullAwareHashPartitioning]))

      checkAnswer2(
        nullableLeft,
        nullableRight,
        (left: SparkPlan, right: SparkPlan) => EnsureRequirements.apply(createJoin(left, right)),
        expectedAnswer,
        sortAnswers = true)
    }

    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key -> "true") {
      checkJoin((left, right) =>
        SortMergeJoinExec(leftKeys, rightKeys, LeftAnti, boundCondition, left, right))
      checkJoin((left, right) =>
        ShuffledHashJoinExec(
          leftKeys, rightKeys, LeftAnti, BuildRight, boundCondition, left, right))
    }
  }

  test("ordinary left anti equi-join keeps hash partitioning when null-aware shuffle is disabled") {
    val nullableLeft = Seq(
      (Integer.valueOf(1), "left-1"),
      (null.asInstanceOf[Integer], "left-null")).toDF("k", "lv")
    val nullableRight = Seq(
      (Integer.valueOf(1), "right-1"),
      (null.asInstanceOf[Integer], "right-null")).toDF("k", "rv")
    val joinCondition = EqualTo(nullableLeft("k").expr, nullableRight("k").expr)
    val join = Join(nullableLeft.logicalPlan, nullableRight.logicalPlan,
      LeftAnti, Some(joinCondition), JoinHint.NONE)
    val (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =
      ExtractEquiJoinKeys.unapply(join).getOrElse(fail("Failed to extract equi-join keys"))

    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      val plan = EnsureRequirements.apply(
        SortMergeJoinExec(leftKeys, rightKeys, LeftAnti, boundCondition,
          nullableLeft.queryExecution.sparkPlan, nullableRight.queryExecution.sparkPlan))
      val partitionings = plan.collect {
        case exchange: ShuffleExchangeExec => exchange.outputPartitioning
      }
      assert(partitionings.size == 2)
      assert(partitionings.forall(_.isInstanceOf[HashPartitioning]))
    }
  }

  test("ordinary left anti equi-join keeps hash partitioning for non-nullable join keys") {
    val nonNullableLeft = spark.range(3).toDF("k")
    val nonNullableRight = spark.range(3).toDF("k")
    val joinCondition = EqualTo(nonNullableLeft("k").expr, nonNullableRight("k").expr)
    val join = Join(nonNullableLeft.logicalPlan, nonNullableRight.logicalPlan,
      LeftAnti, Some(joinCondition), JoinHint.NONE)
    val (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =
      ExtractEquiJoinKeys.unapply(join).getOrElse(fail("Failed to extract equi-join keys"))

    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key -> "true") {
      val plan = EnsureRequirements.apply(
        SortMergeJoinExec(leftKeys, rightKeys, LeftAnti, boundCondition,
          nonNullableLeft.queryExecution.sparkPlan, nonNullableRight.queryExecution.sparkPlan))
      val partitionings = plan.collect {
        case exchange: ShuffleExchangeExec => exchange.outputPartitioning
      }
      assert(partitionings.size == 2)
      assert(partitionings.forall(_.isInstanceOf[HashPartitioning]))
    }
  }

  // Condition: a = c (equi-key) AND b < 3.0 (left-only) AND d < 4.0 (right-only)
  private lazy val leftOnlyResidualCondition = {
    And(
      And(
        EqualTo(left.col("a").expr, right.col("c").expr),
        LessThan(left.col("b").expr, Literal(3.0))),
      LessThan(right.col("d").expr, Literal(4.0)))
  }

  // Condition: a = c (equi-key) AND d < 4.0 (right-only)
  private lazy val rightOnlyResidualCondition = {
    And(
      EqualTo(left.col("a").expr, right.col("c").expr),
      LessThan(right.col("d").expr, Literal(4.0)))
  }

  protected def testWithSplitStreamedSideCondOnAndOff(
      testName: String)(f: String => Unit): Unit = {
    Seq("false", "true").foreach { configValue =>
      testWithWholeStageCodegenOnAndOff(
        s"$testName (splitStreamedSideJoinCondition=$configValue)") { _ =>
        withSQLConf(
          SQLConf.SPLIT_STREAMED_SIDE_JOIN_CONDITION.key -> configValue) {
          f(configValue)
        }
      }
    }
  }

  // Note: the input dataframes and expression must be evaluated lazily because
  // the SQLContext should be used only within a test to keep SQL tests stable
  private def testExistenceJoin(
      testName: String,
      joinType: JoinType,
      leftRows: => DataFrame,
      rightRows: => DataFrame,
      condition: => Option[Expression],
      expectedAnswer: Seq[Row]): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan,
        Inner, condition, JoinHint.NONE)
      ExtractEquiJoinKeys.unapply(join)
    }

    val existsAttr = AttributeReference("exists", BooleanType, false)()
    val leftSemiPlus = ExistenceJoin(existsAttr)
    def createLeftSemiPlusJoin(join: SparkPlan): SparkPlan = {
      joinType match {
        case LeftSemi =>
          val output = join.output.dropRight(1)
          ProjectExec(output, FilterExec(existsAttr, join))
        case LeftAnti =>
          val output = join.output.dropRight(1)
          ProjectExec(output, FilterExec(Not(existsAttr), join))
        case _ =>
          // Only LeftSemi/LeftAnti can be expressed by filtering an ExistenceJoin result.
          join
      }
    }

    val testSemiPlusWrapper = joinType == LeftSemi || joinType == LeftAnti

    testWithSplitStreamedSideCondOnAndOff(s"$testName using ShuffledHashJoin") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements.apply(
              ShuffledHashJoinExec(
                leftKeys, rightKeys, joinType, BuildRight, boundCondition, left, right)),
            expectedAnswer,
            sortAnswers = true)
          if (testSemiPlusWrapper) {
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              EnsureRequirements.apply(
                createLeftSemiPlusJoin(ShuffledHashJoinExec(
                  leftKeys, rightKeys, leftSemiPlus, BuildRight, boundCondition, left, right))),
              expectedAnswer,
              sortAnswers = true)
          }
        }
      }
    }

    testWithSplitStreamedSideCondOnAndOff(s"$testName using BroadcastHashJoin") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements.apply(
              BroadcastHashJoinExec(
                leftKeys, rightKeys, joinType, BuildRight, boundCondition, left, right)),
            expectedAnswer,
            sortAnswers = true)
          if (testSemiPlusWrapper) {
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              EnsureRequirements.apply(
                createLeftSemiPlusJoin(BroadcastHashJoinExec(
                  leftKeys, rightKeys, leftSemiPlus, BuildRight, boundCondition, left, right))),
              expectedAnswer,
              sortAnswers = true)
          }
        }
      }
    }

    testWithSplitStreamedSideCondOnAndOff(s"$testName using SortMergeJoin") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements.apply(
              SortMergeJoinExec(leftKeys, rightKeys, joinType, boundCondition, left, right)),
            expectedAnswer,
            sortAnswers = true)
          if (testSemiPlusWrapper) {
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              EnsureRequirements.apply(
                createLeftSemiPlusJoin(SortMergeJoinExec(
                  leftKeys, rightKeys, leftSemiPlus, boundCondition, left, right))),
              expectedAnswer,
              sortAnswers = true)
          }
        }
      }
    }

    Seq("false", "true").foreach { configValue =>
      test(s"$testName using BroadcastNestedLoopJoin build left" +
        s" (splitStreamedSideJoinCondition=$configValue)") {
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1",
          SQLConf.SPLIT_STREAMED_SIDE_JOIN_CONDITION.key -> configValue) {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements.apply(
              BroadcastNestedLoopJoinExec(left, right, BuildLeft, joinType, condition)),
            expectedAnswer,
            sortAnswers = true)
          if (testSemiPlusWrapper) {
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              EnsureRequirements.apply(
                createLeftSemiPlusJoin(BroadcastNestedLoopJoinExec(
                  left, right, BuildLeft, leftSemiPlus, condition))),
              expectedAnswer,
              sortAnswers = true)
          }
        }
      }
    }

    testWithSplitStreamedSideCondOnAndOff(
      s"$testName using BroadcastNestedLoopJoin build right") { _ =>
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          EnsureRequirements.apply(
            BroadcastNestedLoopJoinExec(left, right, BuildRight, joinType, condition)),
          expectedAnswer,
          sortAnswers = true)
        if (testSemiPlusWrapper) {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements.apply(
              createLeftSemiPlusJoin(BroadcastNestedLoopJoinExec(
                left, right, BuildRight, leftSemiPlus, condition))),
            expectedAnswer,
            sortAnswers = true)
        }
      }
    }
  }

  testExistenceJoin(
    "test no condition with non-empty right side for left semi join",
    LeftSemi,
    left,
    right,
    None,
    Seq(Row(1, 2.0), Row(1, 2.0), Row(2, 1.0), Row(2, 1.0), Row(3, 3.0), Row(null, null),
      Row(null, 5.0), Row(6, null)))

  testExistenceJoin(
    "test no condition with empty right side for left semi join",
    LeftSemi,
    left,
    spark.emptyDataFrame,
    None,
    Seq.empty)

  testExistenceJoin(
    "test single condition (equal) for left semi join",
    LeftSemi,
    left,
    right,
    Some(singleConditionEQ),
    Seq(Row(2, 1.0), Row(2, 1.0), Row(3, 3.0), Row(6, null)))

  testExistenceJoin(
    "test single unique condition (equal) for left semi join",
    LeftSemi,
    left,
    right.select(right.col("c")).distinct(), /* Trigger BHJs and SHJs unique key code path! */
    Some(singleConditionEQ),
    Seq(Row(2, 1.0), Row(2, 1.0), Row(3, 3.0), Row(6, null)))

  testExistenceJoin(
    "test composed condition (equal & non-equal) for left semi join",
    LeftSemi,
    left,
    right,
    Some(composedConditionEQ),
    Seq(Row(2, 1.0), Row(2, 1.0)))

  testExistenceJoin(
    "test composed condition (both non-equal) for left semi join",
    LeftSemi,
    left,
    right,
    Some(composedConditionNEQ),
    Seq(Row(1, 2.0), Row(1, 2.0), Row(2, 1.0), Row(2, 1.0)))

  testExistenceJoin(
    "test no condition with non-empty right side for left anti join",
    LeftAnti,
    left,
    right,
    None,
    Seq.empty)

  testExistenceJoin(
    "test no condition with empty right side for left anti join",
    LeftAnti,
    left,
    spark.emptyDataFrame,
    None,
    Seq(Row(1, 2.0), Row(1, 2.0), Row(2, 1.0), Row(2, 1.0), Row(3, 3.0), Row(null, null),
      Row(null, 5.0), Row(6, null)))

  testExistenceJoin(
    "test single condition (equal) for left anti join",
    LeftAnti,
    left,
    right,
    Some(singleConditionEQ),
    Seq(Row(1, 2.0), Row(1, 2.0), Row(null, null), Row(null, 5.0)))


  testExistenceJoin(
    "test single unique condition (equal) for left anti join",
    LeftAnti,
    left,
    right.select(right.col("c")).distinct(), /* Trigger BHJs and SHJs unique key code path! */
    Some(singleConditionEQ),
    Seq(Row(1, 2.0), Row(1, 2.0), Row(null, null), Row(null, 5.0)))

  testExistenceJoin(
    "test composed condition (equal & non-equal) test for left anti join",
    LeftAnti,
    left,
    right,
    Some(composedConditionEQ),
    Seq(Row(1, 2.0), Row(1, 2.0), Row(3, 3.0), Row(6, null), Row(null, 5.0), Row(null, null)))

  testExistenceJoin(
    "test composed condition (both non-equal) for left anti join",
    LeftAnti,
    left,
    right,
    Some(composedConditionNEQ),
    Seq(Row(3, 3.0), Row(6, null), Row(null, 5.0), Row(null, null)))

  testExistenceJoin(
    "test composed unique condition (both non-equal) for left anti join",
    LeftAnti,
    left,
    rightUniqueKey,
    Some(And(EqualTo(left.col("a").expr, rightUniqueKey.col("c").expr),
      LessThan(left.col("b").expr, rightUniqueKey.col("d").expr))),
    Seq(Row(1, 2.0), Row(1, 2.0), Row(3, 3.0), Row(null, null), Row(null, 5.0), Row(6, null)))

  // ---- Tests for streamed-side-only residual predicate hoisting ----

  // LeftAnti: rows where b >= 3.0 OR no right match with d < 4.0
  // (1, 2.0): no c=1 match -> emitted
  // (2, 1.0): match exists -> dropped
  // (3, 3.0): b=3.0 >= 3.0 -> emitted
  // (null, null): null key -> emitted
  // (null, 5.0): b=5.0 >= 3.0 -> emitted
  // (6, null): b=null -> emitted
  testExistenceJoin(
    "test left-only residual condition for left anti join",
    LeftAnti,
    left,
    right,
    Some(leftOnlyResidualCondition),
    Seq(Row(1, 2.0), Row(1, 2.0), Row(3, 3.0), Row(null, null), Row(null, 5.0), Row(6, null)))

  // LeftOuter: rows where b < 3.0 and right match with d < 4.0 get matched; others emitted as
  // null-padded. Equi-matches: (2,1.0)-(2,3.0), (3,3.0)-(3,2.0), (6,null)-(6,null).
  // For (2,1.0): b<3.0 true, right d=3.0<4.0 true -> matched output (2,1.0,2,3.0).
  //   Both sides contain two rows with the matching key, so the Cartesian product emits 4 rows.
  // For (3,3.0): b<3.0 false -> emitted as (3,3.0,null,null)
  // For (6,null): b<3.0 null -> emitted as (6,null,null,null)
  // For (1,2.0): no equi-match -> emitted as (1,2.0,null,null)
  // Null keys are emitted as null-padded.
  testExistenceJoin(
    "test left-only residual condition for left outer join",
    LeftOuter,
    left,
    right,
    Some(leftOnlyResidualCondition),
    Seq(
      Row(1, 2.0, null, null),
      Row(1, 2.0, null, null),
      Row(2, 1.0, 2, 3.0),
      Row(2, 1.0, 2, 3.0),
      Row(2, 1.0, 2, 3.0),
      Row(2, 1.0, 2, 3.0),
      Row(3, 3.0, null, null),
      Row(null, null, null, null),
      Row(null, 5.0, null, null),
      Row(6, null, null, null)))

  // ExistenceJoin with the same left-only residual condition: exists=true only for (2,1.0).
  testExistenceJoin(
    "test left-only residual condition for existence join",
    ExistenceJoin(AttributeReference("exists", BooleanType, false)()),
    left,
    right,
    Some(leftOnlyResidualCondition),
    Seq(
      Row(1, 2.0, false),
      Row(1, 2.0, false),
      Row(2, 1.0, true),
      Row(2, 1.0, true),
      Row(3, 3.0, false),
      Row(null, null, false),
      Row(null, 5.0, false),
      Row(6, null, false)))

}
