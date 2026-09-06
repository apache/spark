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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.DirectShufflePartitionID
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.{SinglePartition, _}
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.execution.{BinaryExecNode, DummySparkPlan, LeafExecNode, SafeForKWayMerge, SortExec, UnaryExecNode}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, GroupPartitionsExec}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.python.FlatMapCoGroupsInPandasExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class EnsureRequirementsSuite extends SharedSparkSession {
  private val exprA = AttributeReference("a", IntegerType)()
  private val exprB = AttributeReference("b", IntegerType)()
  private val exprC = AttributeReference("c", IntegerType)()
  private val exprD = AttributeReference("d", IntegerType)()
  // Filler attributes, only ever used to mean "not a cluster key".
  private val exprX = AttributeReference("x", IntegerType)()
  private val exprY = AttributeReference("y", IntegerType)()
  private val exprZ = AttributeReference("z", IntegerType)()

  private val EnsureRequirements = new EnsureRequirements()

  test("reorder should handle PartitioningCollection") {
    val plan1 = DummySparkPlan(
      outputPartitioning = PartitioningCollection(Seq(
        HashPartitioning(exprA :: exprB :: Nil, 5),
        HashPartitioning(exprA :: Nil, 5))))
    val plan2 = DummySparkPlan()

    // Test PartitioningCollection on the left side of join.
    val smjExec1 = SortMergeJoinExec(
      exprB :: exprA :: Nil, exprA :: exprB :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec1) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB))
        assert(rightKeys === Seq(exprB, exprA))
      case other => fail(other.toString)
    }

    // Test PartitioningCollection on the right side of join.
    val smjExec2 = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprB :: exprA :: Nil, Inner, None, plan2, plan1)
    EnsureRequirements.apply(smjExec2) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprA))
        assert(rightKeys === Seq(exprA, exprB))
      case other => fail(other.toString)
    }

    // Both sides are PartitioningCollection, but left side cannot be reordered to match
    // and it should fall back to the right side.
    val smjExec3 = SortMergeJoinExec(
      exprD :: exprC :: Nil, exprB :: exprA :: Nil, Inner, None, plan1, plan1)
    EnsureRequirements.apply(smjExec3) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _), _) =>
        assert(leftKeys === Seq(exprC, exprD))
        assert(rightKeys === Seq(exprA, exprB))
      case other => fail(other.toString)
    }
  }

  test("reorder should handle KeyedPartitioning") {
    // partitioning on the left
    val plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(Seq(years(exprA), bucket(4, exprB), days(exprC)), Seq.empty)
    )
    val plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(Seq(years(exprB), bucket(4, exprA), days(exprD)), Seq.empty)
    )
    val smjExec = SortMergeJoinExec(
      exprB :: exprC :: exprA :: Nil, exprA :: exprD :: exprB :: Nil,
      Inner, None, plan1, plan2
    )
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: KeyedPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: KeyedPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB, exprC))
        assert(rightKeys === Seq(exprB, exprA, exprD))
      case other => fail(other.toString)
    }

    // partitioning on the right
    val plan3 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(Seq(bucket(4, exprD), days(exprA), years(exprC)), Seq.empty)
    )
    val smjExec2 = SortMergeJoinExec(
      exprB :: exprD :: exprC :: Nil, exprA :: exprC :: exprD :: Nil,
      Inner, None, plan1, plan3
    )
    EnsureRequirements.apply(smjExec2) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _),
      SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _), _) =>
        assert(leftKeys === Seq(exprC, exprB, exprD))
        assert(rightKeys === Seq(exprD, exprA, exprC))
      case other => fail(other.toString)
    }
  }

  test("reorder should fallback to the other side partitioning") {
    val plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: exprC :: Nil, 5))
    val plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprB :: exprC :: Nil, 5))

    // Test fallback to the right side, which has HashPartitioning.
    val smjExec1 = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprC :: exprB :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec1) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprA))
        assert(rightKeys === Seq(exprB, exprC))
      case other => fail(other.toString)
    }

    // Test fallback to the right side, which has PartitioningCollection.
    val plan3 = DummySparkPlan(
      outputPartitioning = PartitioningCollection(Seq(HashPartitioning(exprB :: exprC :: Nil, 5))))
    val smjExec2 = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprC :: exprB :: Nil, Inner, None, plan1, plan3)
    EnsureRequirements.apply(smjExec2) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprA))
        assert(rightKeys === Seq(exprB, exprC))
      case other => fail(other.toString)
    }

    // The right side has HashPartitioning, so it is matched first, but no reordering match is
    // found, and it should fall back to the left side, which has a PartitioningCollection.
    val smjExec3 = SortMergeJoinExec(
      exprC :: exprB :: Nil, exprA :: exprB :: Nil, Inner, None, plan3, plan1)
    EnsureRequirements.apply(smjExec3) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprC))
        assert(rightKeys === Seq(exprB, exprA))
      case other => fail(other.toString)
    }
  }

  test("SPARK-35675: EnsureRequirements remove shuffle should respect PartitioningCollection") {
    import testImplicits._
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df1 = Seq((1, 2)).toDF("c1", "c2")
      val df2 = Seq((1, 3)).toDF("c3", "c4")
      val res = df1.join(df2, $"c1" === $"c3").repartition($"c1")
      assert(res.queryExecution.executedPlan.collect {
        case s: ShuffleExchangeLike => s
      }.size == 2)
    }
  }

  private def applyEnsureRequirementsWithSubsetKeys(plan: SparkPlan): SparkPlan = {
    var res: SparkPlan = null
    withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
      res = EnsureRequirements.apply(plan)
    }
    res
  }

  test("Successful compatibility check with HashShuffleSpec") {
    val plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: Nil, 5))
    val plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprB :: Nil, 5))

    var smjExec = SortMergeJoinExec(
      exprA :: Nil, exprB :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA))
        assert(rightKeys === Seq(exprB))
      case other => fail(other.toString)
    }

    smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprB :: exprC :: Nil, Inner, None, plan1, plan2)
    // By default we can't eliminate shuffles if the partitions keys are subset of join keys.
    assert(EnsureRequirements.apply(smjExec)
      .collect { case s: ShuffleExchangeLike => s }.length == 2)
    // with the config set, it should also work if both partition keys are subset of their
    // corresponding cluster keys
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB))
        assert(rightKeys === Seq(exprB, exprC))
      case other => fail(other.toString)
    }

    smjExec = SortMergeJoinExec(
      exprB :: exprA :: Nil, exprC :: exprB :: Nil, Inner, None, plan1, plan2)
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprA))
        assert(rightKeys === Seq(exprC, exprB))
      case other => fail(other.toString)
    }
  }

  test("Successful compatibility check with HashShuffleSpec and duplicate keys") {
    var plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: Nil, 5))
    var plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprC :: Nil, 5))
    var smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB, exprB))
        assert(rightKeys === Seq(exprA, exprC, exprC))
      case other => fail(other.toString)
    }

    plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: exprA :: Nil, 5))
    plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprC :: exprA :: Nil, 5))
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB, exprB))
        assert(rightKeys === Seq(exprA, exprC, exprC))
      case other => fail(other.toString)
    }

    plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: exprA :: Nil, 5))
    plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprC :: exprA :: Nil, 5))
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprD :: Nil, Inner, None, plan1, plan2)
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB, exprB))
        assert(rightKeys === Seq(exprA, exprC, exprD))
      case other => fail(other.toString)
    }

    plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: Nil, 5))
    plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprC :: Nil, 5))
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB, exprB))
        assert(rightKeys === Seq(exprA, exprC, exprC))
      case other => fail(other.toString)
    }
  }

  test("incompatible & repartitioning with HashShuffleSpec") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> 5.toString) {
      var plan1 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprA :: Nil, 10))
      var plan2 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprD :: Nil, 5))
      var smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(p: HashPartitioning, _, _, _, _), _), _) =>
          assert(leftKeys === Seq(exprA, exprB))
          assert(rightKeys === Seq(exprC, exprD))
          assert(p.expressions == Seq(exprC))
        case other => fail(other.toString)
      }

      // RHS has more partitions so should be chosen
      plan1 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprA :: Nil, 5))
      plan2 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprD :: Nil, 10))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(p: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
          assert(leftKeys === Seq(exprA, exprB))
          assert(rightKeys === Seq(exprC, exprD))
          assert(p.expressions == Seq(exprB))
        case other => fail(other.toString)
      }

      // If both sides have the same # of partitions, should pick the first one from left
      plan1 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprA :: Nil, 5))
      plan2 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprD :: Nil, 5))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(p: HashPartitioning, _, _, _, _), _), _) =>
          assert(leftKeys === Seq(exprA, exprB))
          assert(rightKeys === Seq(exprC, exprD))
          assert(p.expressions == Seq(exprC))
        case other => fail(other.toString)
      }
    }
  }

  test("Incompatible & repartitioning with HashShuffleSpec and duplicate keys") {
    var plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: exprA :: Nil, 10))
    var plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprC :: exprB :: Nil, 5))
    var smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
      SortExec(_, _, ShuffleExchangeExec(p: HashPartitioning, _, _, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB, exprB))
        assert(rightKeys === Seq(exprA, exprC, exprC))
        assert(p.expressions == Seq(exprA, exprC, exprA))
      case other => fail(other.toString)
    }

    plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: exprA :: Nil, 10))
    plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprC :: exprB :: Nil, 5))
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprD :: Nil, Inner, None, plan1, plan2)
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
      SortExec(_, _, ShuffleExchangeExec(p: HashPartitioning, _, _, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB, exprB))
        assert(rightKeys === Seq(exprA, exprC, exprD))
        assert(p.expressions == Seq(exprA, exprC, exprA))
      case other => fail(other.toString)
    }
  }

  test("Successful compatibility check with other specs") {
    var plan1 = DummySparkPlan(outputPartitioning = SinglePartition)
    var plan2 = DummySparkPlan(outputPartitioning = SinglePartition)
    var smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, SinglePartition, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, SinglePartition, _, _), _), _) =>
      case other => fail(other.toString)
    }

    plan1 = DummySparkPlan(outputPartitioning = SinglePartition)
    plan2 = DummySparkPlan(outputPartitioning = HashPartitioning(exprC :: exprD :: Nil, 1))
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, SinglePartition, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
      case other => fail(other.toString)
    }

    plan1 = DummySparkPlan(outputPartitioning = PartitioningCollection(Seq(
        HashPartitioning(Seq(exprA), 10), HashPartitioning(Seq(exprA, exprB), 10))))
    plan2 = DummySparkPlan(outputPartitioning = HashPartitioning(Seq(exprC, exprD), 10))
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
      case other => fail(other.toString)
    }
  }

  test("Incompatible & repartitioning with other specs") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> 5.toString) {

      // SinglePartition <-> RangePartitioning(10)
      // Only RHS should be shuffled and be converted to SinglePartition <-> SinglePartition
      var plan1 = DummySparkPlan(outputPartitioning = SinglePartition)
      var plan2 = DummySparkPlan(outputPartitioning = RangePartitioning(
        Seq(SortOrder.apply(exprC, Ascending, sameOrderExpressions = Seq.empty)), 10))
      var smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(left.numPartitions == 5)
          assert(right.numPartitions == 5)
        case other => fail(other.toString)
      }

      // HashPartitioning(10) <-> RangePartitioning(5)
      // Only RHS should be shuffled and be converted to
      //   HashPartitioning(10) <-> HashPartitioning(10)
      plan1 = DummySparkPlan(outputPartitioning = HashPartitioning(Seq(exprA, exprB), 10))
      plan2 = DummySparkPlan(outputPartitioning = RangePartitioning(
        Seq(SortOrder.apply(exprC, Ascending, sameOrderExpressions = Seq.empty)), 5))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, DummySparkPlan(_, _, left: HashPartitioning, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(left.numPartitions == 10)
          assert(right.numPartitions == 10)
          assert(right.expressions == Seq(exprC, exprD))
        case other => fail(other.toString)
      }

      // HashPartitioning(1) <-> RangePartitioning(10)
      // If the conf is not set, both sides should be shuffled and be converted to
      // HashPartitioning(5) <-> HashPartitioning(5)
      // If the conf is set, only RHS should be shuffled and be converted to
      // HashPartitioning(1) <-> HashPartitioning(1)
      plan1 = DummySparkPlan(outputPartitioning = HashPartitioning(Seq(exprA), 1))
      plan2 = DummySparkPlan(outputPartitioning = RangePartitioning(
        Seq(SortOrder.apply(exprC, Ascending, sameOrderExpressions = Seq.empty)), 10))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(left.numPartitions == 5)
          assert(left.expressions == Seq(exprA, exprB))
          assert(right.numPartitions == 5)
          assert(right.expressions == Seq(exprC, exprD))
        case other => fail(other.toString)
      }
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, DummySparkPlan(_, _, left: HashPartitioning, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(left.numPartitions == 1)
          assert(right.numPartitions == 1)
          assert(right.expressions == Seq(exprC))
        case other => fail(other.toString)
      }

      // RangePartitioning(1) <-> RangePartitioning(1)
      // Both sides should be shuffled and be converted to
      //   HashPartitioning(5) <-> HashPartitioning(5)
      plan1 = DummySparkPlan(outputPartitioning = RangePartitioning(
        Seq(SortOrder.apply(exprA, Ascending, sameOrderExpressions = Seq.empty)), 1))
      plan2 = DummySparkPlan(outputPartitioning = RangePartitioning(
        Seq(SortOrder.apply(exprD, Ascending, sameOrderExpressions = Seq.empty)), 1))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(left.numPartitions == conf.numShufflePartitions)
          assert(left.expressions == Seq(exprA, exprB))
          assert(right.numPartitions == conf.numShufflePartitions)
          assert(right.expressions == Seq(exprC, exprD))
        case other => fail(other.toString)
      }

      plan1 = DummySparkPlan(outputPartitioning = PartitioningCollection(Seq(
        HashPartitioning(Seq(exprA), 10), HashPartitioning(Seq(exprB), 10))))
      plan2 = DummySparkPlan(outputPartitioning = PartitioningCollection(Seq(
        HashPartitioning(Seq(exprC), 10), HashPartitioning(Seq(exprD), 10))))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: exprC :: exprD :: Nil, exprA :: exprB :: exprC :: exprD :: Nil,
        Inner, None, plan1, plan2)
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, DummySparkPlan(_, _, left: PartitioningCollection, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(left.numPartitions == 10)
          assert(right.numPartitions == 10)
          assert(right.expressions == Seq(exprA))
        case other => fail(other.toString)
      }

      plan1 = DummySparkPlan(outputPartitioning = PartitioningCollection(Seq(
        HashPartitioning(Seq(exprA), 10), HashPartitioning(Seq(exprB), 10))))
      plan2 = DummySparkPlan(outputPartitioning = PartitioningCollection(Seq(
        HashPartitioning(Seq(exprC), 20), HashPartitioning(Seq(exprD), 20))))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: exprC :: exprD :: Nil, exprA :: exprB :: exprC :: exprD :: Nil,
        Inner, None, plan1, plan2)
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, right: PartitioningCollection, _, _), _), _) =>
          assert(left.numPartitions == 20)
          assert(left.expressions == Seq(exprC))
          assert(right.numPartitions == 20)
        case other => fail(other.toString)
      }
    }
  }

  test("EnsureRequirements should respect spark.sql.shuffle.partitions") {
    val defaultNumPartitions = 10
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> defaultNumPartitions.toString) {

      // HashPartitioning(5) <-> HashPartitioning(5)
      // No shuffle should be inserted
      var plan1: SparkPlan = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprA :: exprB :: Nil, 5))
      var plan2: SparkPlan = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprC :: exprD :: Nil, 5))
      var smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, DummySparkPlan(_, _, left: HashPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, right: HashPartitioning, _, _), _), _) =>
          assert(left.expressions === Seq(exprA, exprB))
          assert(right.expressions === Seq(exprC, exprD))
        case other => fail(other.toString)
      }

      // HashPartitioning(6) <-> HashPartitioning(5)
      // Should shuffle RHS and convert to HashPartitioning(6) <-> HashPartitioning(6)
      plan1 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprA :: exprB :: Nil, 6))
      plan2 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprC :: exprD :: Nil, 5))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, DummySparkPlan(_, _, left: HashPartitioning, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(left.expressions === Seq(exprA, exprB))
          assert(right.expressions === Seq(exprC, exprD))
          assert(left.numPartitions == 6)
          assert(right.numPartitions == 6)
        case other => fail(other.toString)
      }

      // RangePartitioning(10) <-> HashPartitioning(5)
      // Should shuffle LHS and convert to HashPartitioning(5) <-> HashPartitioning(5)
      plan1 = DummySparkPlan(
        outputPartitioning = RangePartitioning(
          Seq(SortOrder.apply(exprA, Ascending, sameOrderExpressions = Seq.empty)), 10))
      plan2 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprC :: exprD :: Nil, 5))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, right: HashPartitioning, _, _), _), _) =>
          assert(left.expressions === Seq(exprA, exprB))
          assert(right.expressions === Seq(exprC, exprD))
          assert(left.numPartitions == 5)
          assert(right.numPartitions == 5)
        case other => fail(other.toString)
      }

      // SinglePartition <-> HashPartitioning(5)
      // Should shuffle LHS and convert to HashPartitioning(5) <-> HashPartitioning(5)
      plan1 = DummySparkPlan(outputPartitioning = SinglePartition)
      plan2 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprC :: exprD :: Nil, 5))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, right: HashPartitioning, _, _), _), _) =>
          assert(left.expressions === Seq(exprA, exprB))
          assert(right.expressions === Seq(exprC, exprD))
          assert(left.numPartitions == 5)
          assert(right.numPartitions == 5)
        case other => fail(other.toString)
      }

      // ShuffleExchange(7) <-> HashPartitioning(6)
      // Should shuffle LHS and convert to HashPartitioning(6) <-> HashPartitioning(6)
      plan1 = ShuffleExchangeExec(
        outputPartitioning = HashPartitioning(exprA :: exprB :: Nil, 7),
        child = DummySparkPlan())
      plan2 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprC :: exprD :: Nil, 6))
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, right: HashPartitioning, _, _), _), _) =>
          assert(left.expressions === Seq(exprA, exprB))
          assert(right.expressions === Seq(exprC, exprD))
          assert(left.numPartitions == 6)
          assert(right.numPartitions == 6)
        case other => fail(other.toString)
      }

      // ShuffleExchange(7) <-> ShuffleExchange(6)
      // Should consider `spark.sql.shuffle.partitions` and shuffle both sides, and
      // convert to HashPartitioning(10) <-> HashPartitioning(10)
      plan1 = ShuffleExchangeExec(
        outputPartitioning = HashPartitioning(exprA :: Nil, 7),
        child = DummySparkPlan())
      plan2 = ShuffleExchangeExec(
        outputPartitioning = HashPartitioning(exprC :: Nil, 6),
        child = DummySparkPlan())
      smjExec = SortMergeJoinExec(
        exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(left.expressions === Seq(exprA, exprB))
          assert(right.expressions === Seq(exprC, exprD))
          assert(left.numPartitions == conf.numShufflePartitions)
          assert(right.numPartitions == conf.numShufflePartitions)
        case other => fail(other.toString)
      }
    }
  }

  test("Respect spark.sql.shuffle.partitions with AQE") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> 8.toString,
      SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> 10.toString) {
      Seq(true, false).foreach { enable =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> s"$enable") {
          val plan1 = DummySparkPlan(
            outputPartitioning = HashPartitioning(exprA :: exprB :: Nil, 9))
          val plan2 = DummySparkPlan(
            outputPartitioning = UnknownPartitioning(8))
          val smjExec = SortMergeJoinExec(
            exprA :: exprB :: Nil, exprC :: exprD :: Nil, Inner, None, plan1, plan2)
          EnsureRequirements.apply(smjExec) match {
            case SortMergeJoinExec(leftKeys, rightKeys, _, _,
            SortExec(_, _, DummySparkPlan(_, _, left: HashPartitioning, _, _), _),
            SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
              assert(leftKeys === Seq(exprA, exprB))
              assert(rightKeys === Seq(exprC, exprD))
              assert(left.numPartitions == 9)
              assert(right.numPartitions == 9)
            case other => fail(other.toString)
          }
        }
      }
    }
  }

  test("SPARK-40703: shuffle for SinglePartitionShuffleSpec") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> 20.toString) {
      // We should re-shuffle the side with single partition when the other side is
      // `HashPartitioning` with shuffle node, and respect the minimum parallelism.
      var plan1: SparkPlan = ShuffleExchangeExec(
        outputPartitioning = HashPartitioning(exprA :: Nil, 10),
        DummySparkPlan())
      var plan2 = DummySparkPlan(outputPartitioning = SinglePartition)
      var smjExec = SortMergeJoinExec(exprA :: Nil, exprC :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(leftKeys === Seq(exprA))
          assert(rightKeys === Seq(exprC))
          assert(left.numPartitions == 20)
          assert(right.numPartitions == 20)
        case other => fail(other.toString)
      }

      // We should also re-shuffle the side with only a single partition even the other side does
      // not have `ShuffleExchange`, but just `HashPartitioning`. However in this case the minimum
      // shuffle parallelism will be ignored since we don't want to introduce extra shuffle.
      plan1 = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprA :: Nil, 10))
      plan2 = DummySparkPlan(outputPartitioning = SinglePartition)
      smjExec = SortMergeJoinExec(exprA :: Nil, exprC :: Nil, Inner, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
          assert(leftKeys === Seq(exprA))
          assert(rightKeys === Seq(exprC))
          assert(right.numPartitions == 10)
        case other => fail(other.toString)
      }
    }
  }

  test("SPARK-41986: Introduce shuffle on SinglePartition") {
    val filesMaxPartitionBytes = conf.filesMaxPartitionBytes
    withSQLConf(SQLConf.MAX_SINGLE_PARTITION_BYTES.key -> filesMaxPartitionBytes.toString) {
      Seq(filesMaxPartitionBytes, filesMaxPartitionBytes + 1).foreach { size =>
        val logicalPlan = StatsTestPlan(Nil, 1L, AttributeMap.empty, Some(size))
        val left = DummySparkPlan(outputPartitioning = SinglePartition)
        left.setLogicalLink(logicalPlan)
        val right = DummySparkPlan(outputPartitioning = SinglePartition)
        right.setLogicalLink(logicalPlan)
        val smjExec = SortMergeJoinExec(exprA :: Nil, exprC :: Nil, Inner, None, left, right)

        if (size <= filesMaxPartitionBytes) {
          EnsureRequirements.apply(smjExec) match {
            case SortMergeJoinExec(leftKeys, rightKeys, _, _,
            SortExec(_, _, _: DummySparkPlan, _),
            SortExec(_, _, _: DummySparkPlan, _), _) =>
              assert(leftKeys === Seq(exprA))
              assert(rightKeys === Seq(exprC))
            case other => fail(other.toString)
          }
        } else {
          EnsureRequirements.apply(smjExec) match {
            case SortMergeJoinExec(leftKeys, rightKeys, _, _,
            SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
            SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
              assert(leftKeys === Seq(exprA))
              assert(rightKeys === Seq(exprC))
              assert(left.numPartitions == 5)
              assert(right.numPartitions == 5)
            case other => fail(other.toString)
          }
        }
      }
    }
  }

  test("Check with KeyedPartitioning") {
    // simplest case: identity transforms
    var plan1 = new DummySparkPlanWithBatchScanChild(
      KeyedPartitioning(exprA :: exprB :: Nil, Seq.empty))
    var plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(exprA :: exprC :: Nil, Seq.empty))
    var smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprA :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
        assert(left.expressions === Seq(exprA, exprB))
        assert(right.expressions === Seq(exprA, exprC))
      case other => fail(other.toString)
    }

    // matching bucket transforms from both sides
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(bucket(4, exprA) :: bucket(16, exprB) :: Nil, Seq.empty)
    )
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(bucket(4, exprA) :: bucket(16, exprC) :: Nil, Seq.empty)
    )
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprA :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
        assert(left.expressions === Seq(bucket(4, exprA), bucket(16, exprB)))
        assert(right.expressions === Seq(bucket(4, exprA), bucket(16, exprC)))
      case other => fail(other.toString)
    }

    // partition collections
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(bucket(4, exprA) :: bucket(16, exprB) :: Nil, Seq.empty)
    )
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = PartitioningCollection.fromPartitionings(Seq(
        KeyedPartitioning(bucket(4, exprA) :: bucket(16, exprC) :: Nil, Seq.empty),
        KeyedPartitioning(bucket(4, exprA) :: bucket(16, exprC) :: Nil, Seq.empty))
      )
    )
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprA :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _), _) =>
        assert(left.expressions === Seq(bucket(4, exprA), bucket(16, exprB)))
      case other => fail(other.toString)
    }
    smjExec = SortMergeJoinExec(
      exprA :: exprC :: Nil, exprA :: exprB :: Nil, Inner, None, plan2, plan1)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
        assert(right.expressions === Seq(bucket(4, exprA), bucket(16, exprB)))
      case other => fail(other.toString)
    }

    // bucket + years transforms from both sides
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprA) :: years(exprB) :: Nil, Seq.empty)
    )
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprA) :: years(exprC) :: Nil, Seq.empty)
    )
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprA :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
        assert(left.expressions === Seq(bucket(4, exprA), years(exprB)))
        assert(right.expressions === Seq(bucket(4, exprA), years(exprC)))
      case other => fail(other.toString)
    }
  }

  test("KeyedPartitioning with subset of join keys") {
    var plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprB) :: years(exprC) :: Nil, Seq.empty)
    )
    var plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprC) :: years(exprB) :: Nil, Seq.empty)
    )

    // simple case: join key exprA is not covered by either side's partition keys, so by default
    // the coverage check of requireAllClusterKeysForCoPartition falls back to shuffle to avoid
    // joining on a partitioning coarser than the join keys
    var smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprC :: Nil, exprA :: exprC :: exprB :: Nil, Inner, None, plan1, plan2)
    assert(EnsureRequirements.apply(smjExec)
      .collect { case s: ShuffleExchangeLike => s }.length == 2)
    // with requireAllClusterKeysForCoPartition=false, SPJ is allowed
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
        assert(left.expressions === Seq(bucket(4, exprB), years(exprC)))
        assert(right.expressions === Seq(bucket(4, exprC), years(exprB)))
      case other => fail(other.toString)
    }

    // should also work with distributions with duplicated keys
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprA) :: years(exprB) :: Nil, Seq.empty)
    )
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprA) :: years(exprC) :: Nil, Seq.empty)
    )
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
        assert(left.expressions === Seq(bucket(4, exprA), years(exprB)))
        assert(right.expressions === Seq(bucket(4, exprA), years(exprC)))
      case other => fail(other.toString)
    }

    // both partitioning and distribution have duplicated keys
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(years(exprA) :: bucket(4, exprB) :: days(exprA) :: Nil, Seq.empty))
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(years(exprA) :: bucket(4, exprC) :: days(exprA) :: Nil, Seq.empty))
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
        assert(left.expressions === Seq(years(exprA), bucket(4, exprB), days(exprA)))
        assert(right.expressions === Seq(years(exprA), bucket(4, exprC), days(exprA)))
      case other => fail(other.toString)
    }

    // a column partitioned by more than one transform: partition expressions outnumber the
    // join keys, but every join key is covered, so SPJ is allowed with default configs
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprA :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
      SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
        assert(left.expressions === Seq(years(exprA), bucket(4, exprB), days(exprA)))
        assert(right.expressions === Seq(years(exprA), bucket(4, exprC), days(exprA)))
      case other => fail(other.toString)
    }

    // invalid case: partitioning key positions don't match
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprA) :: bucket(4, exprB) :: Nil, Seq.empty)
    )
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprB) :: bucket(4, exprC) :: Nil, Seq.empty)
    )

    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprC :: Nil, exprA :: exprB :: exprC :: Nil, Inner, None, plan1, plan2)
    applyEnsureRequirementsWithSubsetKeys(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
      SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
        assert(left.expressions === Seq(exprA, exprB, exprC))
        assert(right.expressions === Seq(exprA, exprB, exprC))
      case other => fail(other.toString)
    }

    // invalid case: different number of buckets (we don't support coalescing/repartitioning yet)
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprA) :: bucket(4, exprB) :: Nil, Seq.empty)
    )
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(bucket(4, exprA) :: bucket(8, exprC) :: Nil, Seq.empty)
    )
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
      SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
        assert(left.expressions === Seq(exprA, exprB, exprB))
        assert(right.expressions === Seq(exprA, exprC, exprC))
      case other => fail(other.toString)
    }

    // invalid case: partition key positions match but with different transforms
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(years(exprA) :: bucket(4, exprB) :: Nil, Seq.empty)
    )
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(days(exprA) :: bucket(4, exprC) :: Nil, Seq.empty)
    )
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
      SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
        assert(left.expressions === Seq(exprA, exprB, exprB))
        assert(right.expressions === Seq(exprA, exprC, exprC))
      case other => fail(other.toString)
    }


    // invalid case: multiple references in transform
    plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(years(exprA) :: buckets(4, Seq(exprB, exprC)) :: Nil, Seq.empty)
    )
    plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(years(exprA) :: buckets(4, Seq(exprB, exprC)) :: Nil, Seq.empty)
    )
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, ShuffleExchangeExec(left: HashPartitioning, _, _, _, _), _),
      SortExec(_, _, ShuffleExchangeExec(right: HashPartitioning, _, _, _, _), _), _) =>
        assert(left.expressions === Seq(exprA, exprB, exprB))
        assert(right.expressions === Seq(exprA, exprC, exprC))
      case other => fail(other.toString)
    }
  }

  test("KeyedPartitioning: duplicated join keys in hand-built plans do not block SPJ") {
    // Queries produce this key list only in unusual configurations: BooleanSimplification
    // normally dedups the conjunction, but it is an excludable rule
    // (spark.sql.optimizer.excludedRules), and EnsureRequirements must also stay robust
    // for hand-built or rewritten plans. The coverage check treats duplicated cluster
    // keys as covered, so SPJ is allowed with either config value.
    val plan1 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(bucket(4, exprA) :: bucket(4, exprB) :: Nil, Seq.empty))
    val plan2 = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(bucket(4, exprA) :: bucket(4, exprC) :: Nil, Seq.empty))
    val smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprB :: Nil, exprA :: exprC :: exprC :: Nil, Inner, None, plan1, plan2)
    Seq("true", "false").foreach { requireAllKeys =>
      withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> requireAllKeys) {
        EnsureRequirements.apply(smjExec) match {
          case SortMergeJoinExec(_, _, _, _,
            SortExec(_, _, DummySparkPlan(_, _, left: KeyedPartitioning, _, _), _),
            SortExec(_, _, DummySparkPlan(_, _, right: KeyedPartitioning, _, _), _), _) =>
            assert(left.expressions === Seq(bucket(4, exprA), bucket(4, exprB)))
            assert(right.expressions === Seq(bucket(4, exprA), bucket(4, exprC)))
          case other => fail(s"Expected no shuffle, but got: $other")
        }
      }
    }
  }

  test("SPARK-41413: check compatibility when partition values mismatch") {
    withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true") {
      val leftPartValues = Seq(Array[Any](1, 1), Array[Any](2, 2)).map(new GenericInternalRow(_))
      val rightPartValues = Seq(Array[Any](1, 1), Array[Any](2, 2), Array[Any](3, 3))
          .map(new GenericInternalRow(_))

      var plan1 = new DummySparkPlanWithBatchScanChild(
        outputPartitioning =
          KeyedPartitioning(bucket(4, exprB) :: bucket(8, exprC) :: Nil, leftPartValues)
      )
      var plan2 = new DummySparkPlanWithBatchScanChild(
        outputPartitioning =
          KeyedPartitioning(bucket(4, exprC) :: bucket(8, exprB) :: Nil, rightPartValues)
      )

      // simple case
      var smjExec = SortMergeJoinExec(
        exprA :: exprB :: exprC :: Nil, exprA :: exprC :: exprB :: Nil, Inner, None, plan1, plan2)
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
            SortExec(_, _,
              GroupPartitionsExec(DummySparkPlan(_, _, left: KeyedPartitioning, _, _),
                _, _, _, _, _), _),
            SortExec(_, _,
              GroupPartitionsExec(DummySparkPlan(_, _, right: KeyedPartitioning, _, _),
                _, _, _, _, _), _),
            _) =>
          assert(left.expressions === Seq(bucket(4, exprB), bucket(8, exprC)))
          assert(right.expressions === Seq(bucket(4, exprC), bucket(8, exprB)))
        case other => fail(other.toString)
      }

      // With partition collections
      plan1 = new DummySparkPlanWithBatchScanChild(outputPartitioning =
        PartitioningCollection.fromPartitionings(
          Seq(KeyedPartitioning(bucket(4, exprB) :: bucket(8, exprC) :: Nil, leftPartValues),
            KeyedPartitioning(bucket(4, exprB) :: bucket(8, exprC) :: Nil, leftPartValues))
        )
      )

      smjExec = SortMergeJoinExec(
        exprA :: exprB :: exprC :: Nil, exprA :: exprC :: exprB :: Nil, Inner, None, plan1, plan2)
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
            SortExec(_, _,
              GroupPartitionsExec(DummySparkPlan(_, _, left: PartitioningCollection, _, _),
                _, _, _, _, _), _),
            SortExec(_, _,
              GroupPartitionsExec(DummySparkPlan(_, _, right: KeyedPartitioning, _, _),
                _, _, _, _, _), _),
            _) =>
          assert(left.partitionings.length == 2)
          assert(left.partitionings.head.isInstanceOf[KeyedPartitioning])
          assert(left.partitionings.head.asInstanceOf[KeyedPartitioning].expressions ==
            Seq(bucket(4, exprB), bucket(8, exprC)))
          assert(right.expressions === Seq(bucket(4, exprC), bucket(8, exprB)))
        case other => fail(other.toString)
      }

      // Nested partition collections
      plan2 = new DummySparkPlanWithBatchScanChild(outputPartitioning =
        PartitioningCollection.fromPartitionings(
          Seq(
            PartitioningCollection.fromPartitionings(
              Seq(
                KeyedPartitioning(bucket(4, exprC) :: bucket(8, exprB) :: Nil, rightPartValues),
                KeyedPartitioning(bucket(4, exprC) :: bucket(8, exprB) :: Nil, rightPartValues))),
              PartitioningCollection.fromPartitionings(
                Seq(
                  KeyedPartitioning(bucket(4, exprC) :: bucket(8, exprB) :: Nil, rightPartValues),
                  KeyedPartitioning(bucket(4, exprC) :: bucket(8, exprB) :: Nil, rightPartValues)))
          )
        )
      )

      smjExec = SortMergeJoinExec(
        exprA :: exprB :: exprC :: Nil, exprA :: exprC :: exprB :: Nil, Inner, None, plan1, plan2)
      applyEnsureRequirementsWithSubsetKeys(smjExec) match {
        case SortMergeJoinExec(_, _, _, _,
            SortExec(_, _,
              GroupPartitionsExec(DummySparkPlan(_, _, left: PartitioningCollection, _, _),
                _, _, _, _, _), _),
            SortExec(_, _,
              GroupPartitionsExec(DummySparkPlan(_, _, right: PartitioningCollection, _, _),
                _, _, _, _, _), _),
            _) =>
          assert(left.partitionings.length == 2)
          assert(left.partitionings.head.isInstanceOf[KeyedPartitioning])
          assert(left.partitionings.head.asInstanceOf[KeyedPartitioning].expressions ==
              Seq(bucket(4, exprB), bucket(8, exprC)))
          assert(right.partitionings.length == 2)
          assert(right.partitionings.head.isInstanceOf[PartitioningCollection])
        case other => fail(other.toString)
      }
    }
  }

  test("SPARK-41471: shuffle right side when" +
    " spark.sql.sources.v2.bucketing.shuffle.enabled is true") {
    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {

      val a1 = AttributeReference("a1", IntegerType)()

      val partitionKeys = Seq(50, 51, 52).map(v => InternalRow.fromSeq(Seq(v)))
      val plan1 = new DummySparkPlanWithBatchScanChild(
        outputPartitioning = KeyedPartitioning(identity(a1) :: Nil, partitionKeys))
      val plan2 = DummySparkPlan(outputPartitioning = SinglePartition)

      val smjExec = ShuffledHashJoinExec(
        a1 :: Nil, a1 :: Nil, Inner, BuildRight, None, plan1, plan2)
      EnsureRequirements.apply(smjExec) match {
        case ShuffledHashJoinExec(_, _, _, _, _,
        DummySparkPlan(_, _, left: KeyedPartitioning, _, _),
        ShuffleExchangeExec(KeyedPartitioning(attrs, pks, _, _),
        DummySparkPlan(_, _, SinglePartition, _, _), _, _, _), _) =>
          assert(left.expressions == a1 :: Nil)
          assert(attrs == a1 :: Nil)
          assert(partitionKeys == pks.map(_.row))
        case other => fail(other.toString)
      }
    }
  }

  test("SPARK-42168: FlatMapCoGroupInPandas and Window function with differing key order") {
    val lKey = AttributeReference("key", IntegerType)()
    val lKey2 = AttributeReference("key2", IntegerType)()

    val rKey = AttributeReference("key", IntegerType)()
    val rKey2 = AttributeReference("key2", IntegerType)()
    val rValue = AttributeReference("value", IntegerType)()

    val left = DummySparkPlan()
    val right = WindowExec(
      Alias(
        WindowExpression(
          Sum(rValue).toAggregateExpression(),
          WindowSpecDefinition(
            Seq(rKey2, rKey),
            Nil,
            SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
          )
        ), "sum")() :: Nil,
      Seq(rKey2, rKey),
      Nil,
      DummySparkPlan()
    )

    val pythonUdf = PythonUDF("pyUDF", null,
      StructType(Seq(StructField("value", IntegerType))),
      Seq.empty,
      PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      true)

    val flapMapCoGroup = FlatMapCoGroupsInPandasExec(
      Seq(lKey, lKey2),
      Seq(rKey, rKey2),
      pythonUdf,
      AttributeReference("value", IntegerType)() :: Nil,
      left,
      right
    )

    val result = EnsureRequirements.apply(flapMapCoGroup)
    result match {
      case FlatMapCoGroupsInPandasExec(leftKeys, rightKeys, _, _,
        SortExec(leftOrder, false, _, _), SortExec(rightOrder, false, _, _)) =>
        assert(leftKeys === Seq(lKey, lKey2))
        assert(rightKeys === Seq(rKey, rKey2))
        assert(leftKeys.map(k => SortOrder(k, Ascending)) === leftOrder)
        assert(rightKeys.map(k => SortOrder(k, Ascending)) === rightOrder)
      case other => fail(other.toString)
    }
  }

  def bucket(numBuckets: Int, expr: Expression): TransformExpression = {
    TransformExpression(BucketFunction, Seq(expr), Some(numBuckets))
  }

  def buckets(numBuckets: Int, expr: Seq[Expression]): TransformExpression = {
    TransformExpression(BucketFunction, expr, Some(numBuckets))
  }

  test("ShufflePartitionIdPassThrough - avoid unnecessary shuffle when children are compatible") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      val passThrough_a_5 = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprA), 5)

      val leftPlan = DummySparkPlan(outputPartitioning = passThrough_a_5)
      val rightPlan = DummySparkPlan(outputPartitioning = passThrough_a_5)
      val join = SortMergeJoinExec(exprA :: Nil, exprA :: Nil, Inner, None, leftPlan, rightPlan)

      EnsureRequirements.apply(join) match {
        case SortMergeJoinExec(
            leftKeys,
            rightKeys,
            _,
            _,
            SortExec(_, _, DummySparkPlan(_, _, _: ShufflePartitionIdPassThrough, _, _), _),
            SortExec(_, _, DummySparkPlan(_, _, _: ShufflePartitionIdPassThrough, _, _), _),
            _
            ) =>
          assert(leftKeys === Seq(exprA))
          assert(rightKeys === Seq(exprA))
        case other => fail(s"We don't expect shuffle on either side, but got: $other")
      }
    }
  }

  test("ShufflePartitionIdPassThrough incompatibility - different partitions") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      // Different number of partitions - should add shuffles
      val leftPlan = DummySparkPlan(
        outputPartitioning = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprA), 5))
      val rightPlan = DummySparkPlan(
        outputPartitioning = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprB), 8))
      val join = SortMergeJoinExec(exprA :: Nil, exprB :: Nil, Inner, None, leftPlan, rightPlan)

      EnsureRequirements.apply(join) match {
        case SortMergeJoinExec(_, _, _, _,
          SortExec(_, _, ShuffleExchangeExec(p1: HashPartitioning, _, _, _, _), _),
          SortExec(_, _, ShuffleExchangeExec(p2: HashPartitioning, _, _, _, _), _), _) =>
          // Both sides should be shuffled to default partitions
          assert(p1.numPartitions == 10)
          assert(p2.numPartitions == 10)
          assert(p1.expressions == Seq(exprA))
          assert(p2.expressions == Seq(exprB))
        case other => fail(s"Expected shuffles on both sides, but got: $other")
      }
    }
  }

  test("ShufflePartitionIdPassThrough incompatibility - key position mismatch") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      // Key position mismatch - should add shuffles
      val leftPlan = DummySparkPlan(
        outputPartitioning = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprA), 5))
      val rightPlan = DummySparkPlan(
        outputPartitioning = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprC), 5))
      // Join on different keys than partitioning keys
      val join = SortMergeJoinExec(exprA :: exprB :: Nil, exprD :: exprC :: Nil, Inner, None,
        leftPlan, rightPlan)

      EnsureRequirements.apply(join) match {
        case SortMergeJoinExec(_, _, _, _,
          SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _),
          SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _), _) =>
          // Both sides shuffled due to key mismatch
        case other => fail(s"Expected shuffles on both sides, but got: $other")
      }
    }
  }

  test("ShufflePartitionIdPassThrough vs HashPartitioning - always shuffles") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      // ShufflePartitionIdPassThrough vs HashPartitioning - always adds shuffles
      val leftPlan = DummySparkPlan(
        outputPartitioning = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprA), 5))
      val rightPlan = DummySparkPlan(
        outputPartitioning = HashPartitioning(exprB :: Nil, 5))
      val join = SortMergeJoinExec(exprA :: Nil, exprB :: Nil, Inner, None, leftPlan, rightPlan)

      EnsureRequirements.apply(join) match {
        case SortMergeJoinExec(_, _, _, _,
          SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _),
          SortExec(_, _, _: DummySparkPlan, _), _) =>
          // Left side shuffled, right side kept as-is
        case other => fail(s"Expected shuffle on the left side, but got: $other")
      }
    }
  }

  test("ShufflePartitionIdPassThrough vs SinglePartition - shuffles added") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      // Even when compatible (numPartitions=1), shuffles added due to canCreatePartitioning=false
      val leftPlan = DummySparkPlan(
        outputPartitioning = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprA), 1))
      val rightPlan = DummySparkPlan(outputPartitioning = SinglePartition)
      val join = SortMergeJoinExec(exprA :: Nil, exprB :: Nil, Inner, None, leftPlan, rightPlan)

      EnsureRequirements.apply(join) match {
        case SortMergeJoinExec(_, _, _, _,
          SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _),
          SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _, _, _), _), _) =>
          // Both sides shuffled due to canCreatePartitioning = false
        case other => fail(s"Expected shuffles on both sides, but got: $other")
      }
    }
  }


  test("ShufflePartitionIdPassThrough - compatible with multiple clustering keys") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      val passThrough_a_5 = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprA), 5)
      val passThrough_b_5 = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprB), 5)

      // Both partitioned by exprA, joined on (exprA, exprB)
      // Should be compatible because exprA positions overlap
      val leftPlanA = DummySparkPlan(outputPartitioning = passThrough_a_5)
      val rightPlanA = DummySparkPlan(outputPartitioning = passThrough_a_5)
      val joinA = SortMergeJoinExec(exprA :: exprB :: Nil, exprA :: exprB :: Nil, Inner, None,
        leftPlanA, rightPlanA)

      EnsureRequirements.apply(joinA) match {
        case SortMergeJoinExec(
            leftKeys,
            rightKeys,
            _,
            _,
            SortExec(_, _, DummySparkPlan(_, _, _: ShufflePartitionIdPassThrough, _, _), _),
            SortExec(_, _, DummySparkPlan(_, _, _: ShufflePartitionIdPassThrough, _, _), _),
            _
            ) =>
          assert(leftKeys === Seq(exprA, exprB))
          assert(rightKeys === Seq(exprA, exprB))
        case other => fail(s"We don't expect shuffle on either side with multiple " +
          s"clustering keys, but got: $other")
      }

      // Both sides partitioned by exprB and join on (exprA, exprB)
      // Should be compatible because partition key exprB matches at position 1 in join keys
      val leftPlanB = DummySparkPlan(outputPartitioning = passThrough_b_5)
      val rightPlanB = DummySparkPlan(outputPartitioning = passThrough_b_5)
      val joinB = SortMergeJoinExec(exprA :: exprB :: Nil, exprA :: exprB :: Nil, Inner, None,
        leftPlanB, rightPlanB)

      EnsureRequirements.apply(joinB) match {
        case SortMergeJoinExec(
            leftKeys,
            rightKeys,
            _,
            _,
            SortExec(_, _, DummySparkPlan(_, _, _: ShufflePartitionIdPassThrough, _, _), _),
            SortExec(_, _, DummySparkPlan(_, _, _: ShufflePartitionIdPassThrough, _, _), _),
            _
            ) =>
          // No shuffles because exprB (partition key) appears at position 1 in join keys
          assert(leftKeys === Seq(exprA, exprB))
          assert(rightKeys === Seq(exprA, exprB))
        case other => fail(s"Expected no shuffles due to position overlap at position 1, " +
          s"but got: $other")
      }
    }
  }

  test("ShufflePartitionIdPassThrough - incompatible when partition key not in join keys") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      // Partitioned by exprA and exprB respectively, but joining on completely different keys
      // Should require shuffles because partition keys don't match join keys
      val leftPlan = DummySparkPlan(
        outputPartitioning = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprA), 5))
      val rightPlan = DummySparkPlan(
        outputPartitioning = ShufflePartitionIdPassThrough(DirectShufflePartitionID(exprB), 5))
      val join = SortMergeJoinExec(exprC :: Nil, exprD :: Nil, Inner, None, leftPlan, rightPlan)

      EnsureRequirements.apply(join) match {
        case SortMergeJoinExec(_, _, _, _,
          SortExec(_, _, ShuffleExchangeExec(p1: HashPartitioning, _, _, _, _), _),
          SortExec(_, _, ShuffleExchangeExec(p2: HashPartitioning, _, _, _, _), _), _) =>
          // Both sides should be shuffled because partition keys not in join keys
          assert(p1.numPartitions == 10)
          assert(p2.numPartitions == 10)
          assert(p1.expressions == Seq(exprC))
          assert(p2.expressions == Seq(exprD))
        case other => fail(s"Expected shuffles on both sides due to key mismatch, but got: $other")
      }
    }
  }

  def years(expr: Expression): TransformExpression = {
    TransformExpression(YearsFunction, Seq(expr))
  }

  def days(expr: Expression): TransformExpression = {
    TransformExpression(DaysFunction, Seq(expr))
  }

  private class DummySparkPlanWithBatchScanChild(outputPartitioning: Partitioning)
    extends DummySparkPlan(
      children = Seq(BatchScanExec(Seq.empty, null, Seq.empty, table = null)),
      outputPartitioning = outputPartitioning,
      requiredChildDistribution = Seq(UnspecifiedDistribution),
      requiredChildOrdering = Seq(Seq.empty)
    )

  test("SPARK-58968: a grouped KeyedPartitioning must still honour requiredNumPartitions") {
    val exprKey = AttributeReference("k", IntegerType)()
    // A grouped KeyedPartitioning with three distinct keys and three partitions.
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(Seq(exprKey), Seq(InternalRow(1), InternalRow(2), InternalRow(3))))
    // `requiredNumPartitions` is a hard requirement - `Partitioning.satisfies` refuses to satisfy a
    // distribution whose partition count differs, and neither `keysSatisfy` nor
    // `nonGroupedSatisfies` checks it on its own. A stateful streaming operator asks for this shape
    // through `StatefulOperatorPartitioning.getCompatibleDistribution`.
    val parent = parentRequiring(
      child,
      ClusteredDistribution(Seq(exprKey), requiredNumPartitions = Some(5)))

    val newChild = EnsureRequirements.apply(parent).children.head
    assert(newChild.isInstanceOf[ShuffleExchangeExec],
      s"expected a shuffle to reach 5 partitions, got ${newChild.getClass.getSimpleName}")
    assert(newChild.outputPartitioning.numPartitions == 5)
  }

  test("SPARK-58968: a KeyedPartitioning with no partition expressions is kept as it is") {
    // No in-tree producer builds one. The scan gates on `supportsExpressions`, and the projecting
    // and grouping nodes derive their expressions from an existing partitioning. But the guard
    // that keeps such a member is what stops it from falling to the shuffle branch, where
    // `UnspecifiedDistribution.createPartitioning` throws outright.
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(Seq.empty, Seq(InternalRow(), InternalRow())))
    val parent = parentRequiring(child, UnspecifiedDistribution)

    assert(EnsureRequirements.apply(parent).children.head eq child)
  }

  test("SPARK-58968: a partitioning that covers no operation key is shuffled, not projected") {
    val exprKey = AttributeReference("k", IntegerType)()
    // Nothing at `KeyedPartitioning` construction requires a partition expression to reference a
    // column. A DSv2 scan cannot report one that does not, because `supportsExpressions` refuses
    // it, but no other producer is held to that, so a reference-free expression is possible here.
    //
    // Such a partitioning has no attributes to check against the operation keys, which makes
    // `keysSatisfy` vacuously true, and it covers no position to project onto. Projecting to
    // no position at all would put every row on a single partition, so the child has to be
    // shuffled.
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(Seq(Literal(1)), Seq(InternalRow(1), InternalRow(2), InternalRow(3))))
    val parent = parentRequiring(child, ClusteredDistribution(Seq(exprKey)))

    val newChild = EnsureRequirements.apply(parent).children.head
    assert(newChild.isInstanceOf[ShuffleExchangeExec],
      s"expected a shuffle, got ${newChild.getClass.getSimpleName}")
    assert(groupPartitionsNodes(newChild).isEmpty)
  }

  test("SPARK-58968: no GroupPartitionsExec when a cluster key is the partition expression " +
      "itself") {
    val exprId = AttributeReference("id", IntegerType)()
    val exprTs = AttributeReference("ts", IntegerType)()
    val transform = years(exprTs)
    // Partitioned by (id, years(ts)) with two partitions sharing id = 1, so projecting to [id]
    // would merge them.
    val keys = Seq(InternalRow(1, 1), InternalRow(1, 2), InternalRow(2, 3))

    // Both cluster keys are covered, `id` at the reference level and `years(ts)` at the expression
    // level. The partitioning is exactly the clustering, so no projection is needed. Deriving the
    // positions from `KeyedShuffleSpec.keyPositions` alone would look up `years(ts)` by its
    // *reference* `ts`, which is not a cluster key, drop it, and coalesce for nothing. And under
    // `requireAllClusterKeys` the resulting `KeyedPartitioning([id])` would not even satisfy the
    // distribution the node was inserted for.
    Seq(true, false).foreach { requireAllClusterKeys =>
      val child = new DummySparkPlanWithBatchScanChild(
        outputPartitioning = KeyedPartitioning(Seq(exprId, transform), keys))
      val parent = parentRequiring(
        child,
        ClusteredDistribution(Seq(exprId, transform), requireAllClusterKeys))

      withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val newChild = EnsureRequirements.apply(parent).children.head
        // Note: do not interpolate the plan into an assertion message here. `BatchScanExec`'s table
        // is null in this fixture, so rendering the tree throws, and `assert`'s clue is by-value.
        assert(groupPartitionsNodes(newChild).isEmpty,
          s"requireAllClusterKeys=$requireAllClusterKeys: the partitioning already satisfies")
        assert(newChild.outputPartitioning.numPartitions == 3)
      }
    }
  }

  test("SPARK-58968: a transform position is kept when its reference is a cluster key") {
    // `clusterKeyPositions` keeps a position two ways. The other tests all keep plain attributes,
    // which the expression-level test matches on its own, so this is the one that turns on the
    // reference-level test: `bucket(4, a)` is not a cluster key, its reference `a` is.
    //
    // It is also the shape the soundness argument is about. `keysSatisfy`'s subset branch requires
    // every expression to have a single reference, so a kept expression is a function of one
    // cluster key, and coalescing on the projected keys cannot separate rows sharing that key.
    //
    // Two of the three buckets carry the same value, so projecting to position 0 merges them,
    // which is what keeps this out of the needs-no-node case.
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(
        Seq(bucket(4, exprA), exprB),
        Seq(InternalRow(1, 10), InternalRow(1, 20), InternalRow(2, 30))))
    val parent = parentRequiring(child, ClusteredDistribution(Seq(exprA)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      val gpes = groupPartitionsNodes(newChild)
      assert(gpes.map(_.joinKeyPositions) == Seq(Some(Seq(0))),
        "expected a node projecting to the bucket position, whose reference is the cluster key")
      assert(gpes.map(_.groupedPartitions.size) == Seq(2))
    }
  }

  test("SPARK-58968: a non-grouped KeyedPartitioning may still group when the resulting count " +
      "matches requiredNumPartitions") {
    val exprKey = AttributeReference("k", IntegerType)()
    // Three partitions and two distinct keys, so a plain (non-projecting) GroupPartitionsExec
    // produces two, which is exactly what the distribution asks for. The requirement has to be
    // checked against the count the node would produce, not against the count the partitioning has
    // now. Refusing to group here would cost a shuffle for nothing.
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(Seq(exprKey), Seq(InternalRow(1), InternalRow(1), InternalRow(2))))
    val parent = parentRequiring(
      child,
      ClusteredDistribution(Seq(exprKey), requiredNumPartitions = Some(2)))

    val newChild = EnsureRequirements.apply(parent).children.head
    assert(groupPartitionsNodes(newChild).size == 1,
      s"expected a GroupPartitionsExec, got ${newChild.getClass.getSimpleName}")
    assert(newChild.collect { case s: ShuffleExchangeExec => s }.isEmpty, "no shuffle needed")
    assert(newChild.outputPartitioning.numPartitions == 2)
  }

  test("SPARK-58968: a single-partition KeyedPartitioning under OrderedDistribution") {
    val exprKey = AttributeReference("k", IntegerType)()
    // One partition, so `partitionKeys.sliding(2)` yields a single window of size one. A
    // `case Seq(k1, k2)` lambda cannot match it and throws `MatchError` at planning time. A v2
    // table whose rows all share one partition value reports exactly this -
    // `DataSourceV2ScanExecBase` has no single-partition short-circuit.
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(Seq(exprKey), Seq(InternalRow(1))))
    val parent = parentRequiring(child, OrderedDistribution(Seq(SortOrder(exprKey, Ascending))))

    withSQLConf(SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      assert(groupPartitionsNodes(newChild).isEmpty,
        "a single partition is trivially sorted, so no node is needed")
      assert(newChild.outputPartitioning.numPartitions == 1)
    }
  }

  test("SPARK-58968: a projection whose resulting count matches requiredNumPartitions still " +
      "groups") {
    val exprN = AttributeReference("n", IntegerType)()
    val exprI = AttributeReference("i", IntegerType)()
    // Three partitions on (n, i), and projecting to the operation key [i] leaves two, which is
    // what the distribution asks for. It mirrors the test below, where the projected count misses
    // the requirement and we shuffle. Here it matches and the node is the right answer.
    val keys = Seq(InternalRow(1, 1), InternalRow(2, 1), InternalRow(3, 2))
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(Seq(exprN, exprI), keys))
    val parent = parentRequiring(
      child,
      ClusteredDistribution(Seq(exprI), requiredNumPartitions = Some(2)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      assert(groupPartitionsNodes(newChild).map(_.joinKeyPositions) ==
        Seq(Some(Seq(1))), "expected a node projecting to the operation key [i]")
      assert(newChild.collect { case s: ShuffleExchangeExec => s }.isEmpty, "no shuffle needed")
      assert(newChild.outputPartitioning.numPartitions == 2)
    }
  }

  test("SPARK-58968: requiredNumPartitions is honoured with allowKeysSubsetOfPartitionKeys off") {
    val exprKey = AttributeReference("k", IntegerType)()
    // Three partitions, two distinct keys, so a coalescing node would leave two - one short of the
    // requirement. `master` inserted that node and left the plan with the wrong partition count;
    // this path does not depend on the subset config, which is left at its default here.
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning =
        KeyedPartitioning(Seq(exprKey), Seq(InternalRow(1), InternalRow(1), InternalRow(2))))
    val parent = parentRequiring(
      child,
      ClusteredDistribution(Seq(exprKey), requiredNumPartitions = Some(3)))

    val newChild = EnsureRequirements.apply(parent).children.head
    assert(newChild.isInstanceOf[ShuffleExchangeExec],
      s"expected a shuffle, got ${newChild.getClass.getSimpleName}")
    assert(newChild.outputPartitioning.numPartitions == 3)
  }

  test("SPARK-58968: a projection that would break requiredNumPartitions falls back to a shuffle") {
    val exprN = AttributeReference("n", IntegerType)()
    val exprI = AttributeReference("i", IntegerType)()
    // Partitioned by (n, i) with three partitions. Projecting to the operation key [i] leaves two.
    val keys = Seq(InternalRow(1, 1), InternalRow(2, 1), InternalRow(3, 2))
    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(Seq(exprN, exprI), keys))
    // The partitioning matches `requiredNumPartitions` as it stands, but it needs a projection to
    // satisfy the clustering, and a `GroupPartitionsExec` derives its count from the keys it is
    // given, so it would produce two partitions and break the requirement the check just passed.
    // A shuffle is the only thing that gets both right.
    val parent = parentRequiring(
      child,
      ClusteredDistribution(Seq(exprI), requiredNumPartitions = Some(3)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      assert(newChild.isInstanceOf[ShuffleExchangeExec],
        s"expected a shuffle, got ${newChild.getClass.getSimpleName}")
      assert(newChild.outputPartitioning.numPartitions == 3)
    }
  }

  test("SPARK-58968: cogroup on a non-leading subset of the partition keys projects to that key") {
    val nL = AttributeReference("nL", IntegerType)()
    val iL = AttributeReference("iL", IntegerType)()
    val nR = AttributeReference("nR", IntegerType)()
    val iR = AttributeReference("iR", IntegerType)()
    // Partition keys are (n, i). Projecting to position 1 (= i, the cogroup key) leaves [1, 2],
    // projecting to position 0 (= n) would leave [1, 2, 3].
    val keys = Seq(InternalRow(1, 1), InternalRow(2, 1), InternalRow(3, 2))

    val left = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(Seq(nL, iL), keys))
    val right = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = KeyedPartitioning(Seq(nR, iR), keys))

    val pythonUdf = PythonUDF("pyUDF", null,
      StructType(Seq(StructField("value", IntegerType))),
      Seq.empty,
      PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      true)

    // FlatMapCoGroupsInPandasExec requires ClusteredDistribution on both children but is not a
    // ShuffledJoin, so the projection must come from the multi-child co-partitioning block, not
    // from an inline GroupPartitionsExec. Otherwise the positions get applied twice and the
    // second application indexes into the unprojected partition expressions.
    val cogroup = FlatMapCoGroupsInPandasExec(
      Seq(iL), Seq(iR), pythonUdf,
      AttributeReference("value", IntegerType)() :: Nil, left, right)

    withSQLConf(
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      val result = EnsureRequirements.apply(cogroup)
      val gpes = groupPartitionsNodes(result)
      assert(gpes.map(_.joinKeyPositions) == Seq(Some(Seq(1)), Some(Seq(1))),
        "both sides must be grouped on the cogroup key i, which is at position 1")
      assert(gpes.map(_.groupedPartitions.size) == Seq(2, 2))
      assert(result.children.map(_.outputPartitioning).forall {
        case k: KeyedPartitioning => k.expressions == Seq(iL) || k.expressions == Seq(iR)
        case _ => false
      }, "the reported partitioning must be on the cogroup key")
    }
  }

  test("SPARK-58968: a required count filters the candidates, it does not veto the winner") {
    // Projecting to [a] leaves 2 partitions, to [a, b] leaves 3, and the distribution asks for 2.
    // So only the narrower set can honour the count, and it is the one the containment prune would
    // drop and the ranking would lose. Testing the count on the winner instead, or pruning before
    // filtering, sends the whole child to a shuffle.
    val keys = Seq(
      InternalRow(1, 1, 1), InternalRow(1, 2, 2), InternalRow(2, 2, 3), InternalRow(2, 2, 4))
    val onlyA = KeyedPartitioning(Seq(exprA, exprX, exprY), keys).copy(isGrouped = false)
    val aAndB = onlyA.copy(expressions = Seq(exprA, exprB, exprY))

    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = PartitioningCollection(Seq(onlyA, aAndB)))
    val parent = parentRequiring(
      child,
      ClusteredDistribution(Seq(exprA, exprB), requiredNumPartitions = Some(2)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      assert(newChild.collect { case s: ShuffleExchangeExec => s }.isEmpty,
        "the candidate that honours the required count must not lose to the one that cannot")
      val gpes = groupPartitionsNodes(newChild)
      assert(gpes.map(_.joinKeyPositions) == Seq(Some(Seq(0))))
      assert(gpes.map(_.groupedPartitions.size) == Seq(2))
    }
  }

  test("SPARK-58968: a contained position set never wins, not even on a tie") {
    // The third key column is constant, so projecting to [a] and to [a, b] both leave 3 partitions.
    // `{0}` is contained in `{0, 1}` and is dropped for that reason alone, which settles the tie
    // toward the wider set, the one that still names `b`. Ranking by count alone would take
    // whichever the child reports first.
    val keys = Seq(InternalRow(1, 1, 7), InternalRow(2, 2, 7), InternalRow(3, 3, 7))
    val onlyA = KeyedPartitioning(Seq(exprA, exprX, exprY), keys).copy(isGrouped = false)
    val aAndB = onlyA.copy(expressions = Seq(exprA, exprB, exprY))

    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = PartitioningCollection(Seq(onlyA, aAndB)))
    val parent = parentRequiring(child, ClusteredDistribution(Seq(exprA, exprB)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      val gpes = groupPartitionsNodes(newChild)
      assert(gpes.map(_.joinKeyPositions) == Seq(Some(Seq(0, 1))),
        "the containing set must win the tie, so that the projection still names b")
      assert(gpes.map(_.groupedPartitions.size) == Seq(3))
    }
  }

  test("SPARK-58968: a narrower projection wins when it leaves more partitions") {
    // The first key column has 4 distinct values, the second and third 3 between them. So the
    // member covering position 0 alone leaves more partitions than the one covering positions 1 and
    // 2, and neither position set contains the other, so coverage would pick the wrong one.
    val keys = Seq(
      InternalRow(1, 9, 9), InternalRow(2, 9, 9), InternalRow(3, 8, 8), InternalRow(4, 7, 7))
    // `isGrouped = false` keeps both members out of the needs-no-node case, so both are candidates
    // and the ranking decides. The wider one comes first, so taking the widest gives positions
    // [1, 2] and 3 partitions instead.
    val onlyB = KeyedPartitioning(Seq(exprX, exprB, exprC), keys).copy(isGrouped = false)
    val onlyA = onlyB.copy(expressions = Seq(exprA, exprY, exprZ))

    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = PartitioningCollection(Seq(onlyB, onlyA)))
    val parent = parentRequiring(child, ClusteredDistribution(Seq(exprA, exprB, exprC)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      val gpes = groupPartitionsNodes(newChild)
      assert(gpes.map(_.joinKeyPositions) == Seq(Some(Seq(0))),
        "the projection leaving the most partitions must win over the one covering more keys")
      assert(gpes.map(_.groupedPartitions.size) == Seq(4))
    }
  }

  test("SPARK-58968: the candidate covering the most operation keys wins") {
    // Partition keys are (a, <second key column>, y). Projecting to the first position alone
    // leaves 2 partitions, projecting to the first two leaves 3.
    val keys = Seq(
      InternalRow(1, 1, 1), InternalRow(1, 1, 2), InternalRow(1, 2, 3), InternalRow(2, 2, 4))
    // Two members of one collection name the second key column differently. `x` is not a cluster
    // key and `b` is, so they disagree on how many operation keys they cover, one against two.
    // Only the wider one keeps partitions that share an `a` but differ in `b` apart, which is why
    // the widest coverage is picked rather than the first member. `copy` keeps the `partitionKeys`
    // reference `PartitioningCollection` requires its members to share.
    val onlyA = KeyedPartitioning(Seq(exprA, exprX, exprY), keys)
    val aAndB = onlyA.copy(expressions = Seq(exprA, exprB, exprY))

    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = PartitioningCollection(Seq(onlyA, aAndB)))
    val parent = parentRequiring(child, ClusteredDistribution(Seq(exprA, exprB)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      val gpes = groupPartitionsNodes(newChild)
      assert(gpes.map(_.joinKeyPositions) == Seq(Some(Seq(0, 1))),
        "the projection must come from the member covering both operation keys")
      assert(gpes.map(_.groupedPartitions.size) == Seq(3))
    }
  }

  test("SPARK-58968: the candidate whose projection leaves the most partitions wins") {
    // The first key column has 2 distinct values, the second 3. Both members below cover one
    // operation key, so the coverage prefilter keeps both and the projected counts decide.
    val keys = Seq(
      InternalRow(1, 5, 1), InternalRow(1, 6, 2), InternalRow(2, 7, 3), InternalRow(2, 7, 4))
    // The member leaving fewer partitions comes first, so taking the first, or the fewest, gives
    // position 0 and 2 partitions instead.
    val onlyA = KeyedPartitioning(Seq(exprA, exprY, exprZ), keys)
    val onlyB = onlyA.copy(expressions = Seq(exprX, exprB, exprZ))

    val child = new DummySparkPlanWithBatchScanChild(
      outputPartitioning = PartitioningCollection(Seq(onlyA, onlyB)))
    val parent = parentRequiring(child, ClusteredDistribution(Seq(exprA, exprB)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = EnsureRequirements.apply(parent).children.head
      val gpes = groupPartitionsNodes(newChild)
      assert(gpes.map(_.joinKeyPositions) == Seq(Some(Seq(1))),
        "the projection must come from the member leaving the most partitions")
      assert(gpes.map(_.groupedPartitions.size) == Seq(3))
    }
  }

  test("SPARK-58968: equally covering candidates follow the child's own order") {
    // Projecting to either of the first two key columns leaves 2 partitions, so the two members
    // below cover one operation key each and their projections are equally good. Nothing ranks
    // them. The choice is still visible in the plan, because they project to different keys, so it
    // follows the order the child reports its partitionings in.
    val keys = Seq(
      InternalRow(1, 5, 1), InternalRow(1, 6, 2), InternalRow(2, 5, 3), InternalRow(2, 6, 4))
    val onlyA = KeyedPartitioning(Seq(exprA, exprY, exprZ), keys)
    val onlyB = onlyA.copy(expressions = Seq(exprX, exprB, exprZ))

    Seq(Seq(onlyA, onlyB) -> Seq(0), Seq(onlyB, onlyA) -> Seq(1)).foreach {
      case (members, expectedPositions) =>
        val child = new DummySparkPlanWithBatchScanChild(
          outputPartitioning = PartitioningCollection(members))
        val parent = parentRequiring(child, ClusteredDistribution(Seq(exprA, exprB)))

        withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
          val newChild = EnsureRequirements.apply(parent).children.head
          val gpes = groupPartitionsNodes(newChild)
          assert(gpes.map(_.joinKeyPositions) == Seq(Some(expectedPositions)),
            s"the first of the two equally covering members must win, expected " +
              s"$expectedPositions")
          assert(gpes.map(_.groupedPartitions.size) == Seq(2))
        }
    }
  }

  test("SPARK-56549: tryEnableSortedMerge traversal continues through plain unary nodes") {
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val exprKey = AttributeReference("k", IntegerType)()
      val partitionKeys = Seq(InternalRow(1), InternalRow(2), InternalRow(1))
      val ordering = Seq(SortOrder(exprKey, Ascending))
      val leaf = DummyLeafSafeForKWayMerge(
        outputPartitioning = KeyedPartitioning(Seq(exprKey), partitionKeys),
        outputOrdering = ordering)
      val gpe = GroupPartitionsExec(leaf)

      // Baseline: GPE at root -- at least one alternative has sorted merge enabled.
      assert(EnsureRequirements.tryEnableSortedMerge(gpe).exists(anyGpeEnabled))
      // Plain unary wrapper (e.g. FilterExec): traversal continues and sorted merge is enabled.
      assert(EnsureRequirements.tryEnableSortedMerge(DummyPassthroughUnaryExec(gpe))
        .exists(anyGpeEnabled))
      // Two levels of plain unary wrappers: still enabled.
      assert(EnsureRequirements.tryEnableSortedMerge(
        DummyPassthroughUnaryExec(DummyPassthroughUnaryExec(gpe)))
        .exists(anyGpeEnabled))
    }
  }

  test("SPARK-56549: tryEnableSortedMerge traversal continues through binary nodes that " +
    "propagate ordering from one child (e.g. ShuffledHashJoinExec stream side)") {
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val exprKey = AttributeReference("k", IntegerType)()
      val partitionKeys = Seq(InternalRow(1), InternalRow(2), InternalRow(1))
      val ordering = Seq(SortOrder(exprKey, Ascending))
      val leaf = DummyLeafSafeForKWayMerge(
        outputPartitioning = KeyedPartitioning(Seq(exprKey), partitionKeys),
        outputOrdering = ordering)
      val gpe = GroupPartitionsExec(leaf)
      val otherChild = DummyLeafSafeForKWayMerge()

      // Binary node whose ordering comes from left child (GPE side): sorted merge enabled.
      assert(EnsureRequirements.tryEnableSortedMerge(DummyOrderFromLeftBinaryExec(gpe, otherChild))
        .exists(anyGpeEnabled))
      // Binary node with GPE only on the non-ordering (right) side: the binary node's
      // outputPartitioning = left.outputPartitioning carries no KeyedPartitioning, so the pruning
      // condition stops traversal at the root; no GPE is enabled.
      assert(!EnsureRequirements.tryEnableSortedMerge(DummyOrderFromLeftBinaryExec(otherChild, gpe))
        .exists(anyGpeEnabled))
    }
  }

  test("SPARK-56549: tryEnableSortedMerge traversal through binary nodes with " +
    "PartitioningCollection (KP from both children, e.g. SHJ InnerLike)") {
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val exprKey = AttributeReference("k", IntegerType)()
      val partitionKeys = Seq(InternalRow(1), InternalRow(2), InternalRow(1))
      val ordering = Seq(SortOrder(exprKey, Ascending))
      val leaf = DummyLeafSafeForKWayMerge(
        outputPartitioning = KeyedPartitioning(Seq(exprKey), partitionKeys),
        outputOrdering = ordering)
      val gpe = GroupPartitionsExec(leaf)
      val otherChild = DummyLeafSafeForKWayMerge(
        outputPartitioning = UnknownPartitioning(gpe.outputPartitioning.numPartitions))

      // GPE on ordering (left) side: sorted merge is enabled and the binary's outputOrdering
      // becomes non-empty.
      assert(EnsureRequirements.tryEnableSortedMerge(DummyBothKPBinaryExec(gpe, otherChild))
        .exists(p => anyGpeEnabled(p) && p.outputOrdering.nonEmpty))

      // GPE on non-ordering (right) side: the PartitioningCollection on the binary node includes
      // KP from the right child, so traversal enters the binary and sorted merge IS enabled on the
      // GPE. However, the binary's outputOrdering remains empty: it comes from the left (non-GPE)
      // child. The call site's find correctly rejects all such alternatives.
      assert(EnsureRequirements.tryEnableSortedMerge(DummyBothKPBinaryExec(otherChild, gpe))
        .exists(anyGpeEnabled))
      assert(!EnsureRequirements.tryEnableSortedMerge(DummyBothKPBinaryExec(otherChild, gpe))
        .exists(_.outputOrdering.nonEmpty))
    }
  }

  test("SPARK-56549: tryEnableSortedMerge traversal stops at SortExec and Exchange") {
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val exprKey = AttributeReference("k", IntegerType)()
      val partitionKeys = Seq(InternalRow(1), InternalRow(2), InternalRow(1))
      val ordering = Seq(SortOrder(exprKey, Ascending))
      val leaf = DummyLeafSafeForKWayMerge(
        outputPartitioning = KeyedPartitioning(Seq(exprKey), partitionKeys),
        outputOrdering = ordering)
      val gpe = GroupPartitionsExec(leaf)

      // SortExec: the pruning condition (!isInstanceOf[SortExec]) stops traversal, so the GPE
      // inside is not enabled in any alternative.
      assert(!EnsureRequirements.tryEnableSortedMerge(
        SortExec(ordering, global = false, child = gpe)).exists(anyGpeEnabled))
      // Exchange produces non-KeyedPartitioning output so the hasKeyedPartitioning half of the
      // pruning condition stops traversal; GPE inside is not enabled.
      assert(!EnsureRequirements.tryEnableSortedMerge(DummyExchangeExec(gpe)).exists(anyGpeEnabled))
      // Plain unary wrapper above a SortExec: traversal reaches the wrapper but stops at the
      // SortExec; GPE inside is still not enabled.
      assert(!EnsureRequirements.tryEnableSortedMerge(
        DummyPassthroughUnaryExec(SortExec(ordering, global = false, child = gpe)))
        .exists(anyGpeEnabled))
    }
  }

  private def anyGpeEnabled(plan: SparkPlan): Boolean =
    plan.collectFirst { case gpe: GroupPartitionsExec if gpe.enableSortedMerge => true }.isDefined

  /** A parent that requires `distribution` of its single `child` and no ordering. */
  private def parentRequiring(child: SparkPlan, distribution: Distribution): DummySparkPlan =
    DummySparkPlan(
      children = Seq(child),
      requiredChildDistribution = Seq(distribution),
      requiredChildOrdering = Seq(Seq.empty))

  private def groupPartitionsNodes(plan: SparkPlan): Seq[GroupPartitionsExec] =
    plan.collect { case g: GroupPartitionsExec => g }
}

private case class DummyLeafSafeForKWayMerge(
    override val outputOrdering: Seq[SortOrder] = Nil,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0)
  ) extends LeafExecNode with SafeForKWayMerge {
  override protected def doExecute(): RDD[InternalRow] = null
  override def output: Seq[Attribute] = Seq.empty
}

private case class DummyPassthroughUnaryExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override protected def doExecute(): RDD[InternalRow] = null
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

// Models a binary join whose output ordering comes from the left child (e.g. SHJ stream=left).
private case class DummyOrderFromLeftBinaryExec(left: SparkPlan, right: SparkPlan)
    extends BinaryExecNode {
  override def output: Seq[Attribute] = left.output ++ right.output
  override def outputOrdering: Seq[SortOrder] = left.outputOrdering
  override def outputPartitioning: Partitioning = left.outputPartitioning
  override protected def doExecute(): RDD[InternalRow] = null
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)
}

// Models a binary join whose outputPartitioning is a PartitioningCollection containing both
// children's partitionings (e.g. SHJ InnerLike), while outputOrdering still comes from the left
// child only.
private case class DummyBothKPBinaryExec(left: SparkPlan, right: SparkPlan)
    extends BinaryExecNode {
  override def output: Seq[Attribute] = left.output ++ right.output
  override def outputOrdering: Seq[SortOrder] = left.outputOrdering
  override def outputPartitioning: Partitioning =
    PartitioningCollection.fromPartitionings(Seq(left.outputPartitioning, right.outputPartitioning))
  override protected def doExecute(): RDD[InternalRow] = null
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)
}

// Exchange produces non-KeyedPartitioning output (UnknownPartitioning by default);
// do not override outputPartitioning or outputOrdering here.
private case class DummyExchangeExec(child: SparkPlan) extends Exchange {
  override def output: Seq[Attribute] = child.output
  override protected def doExecute(): RDD[InternalRow] = null
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
