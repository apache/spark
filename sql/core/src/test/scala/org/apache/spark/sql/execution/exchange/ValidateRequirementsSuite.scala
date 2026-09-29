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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, Literal, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, KeyedPartitioning, Partitioning, PartitioningCollection, REPEATS_GROUP, SinglePartition, SPREADS_SPLITS, UngroupingOrigin}
import org.apache.spark.sql.execution.{CoGroupExec, DummySparkPlan, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, ObjectType}

class ValidateRequirementsSuite extends SharedSparkSession {

  import testImplicits._

  private def testValidate(
      joinKeyIndices: Seq[Int],
      leftPartitionKeyIndices: Seq[Int],
      rightPartitionKeyIndices: Seq[Int],
      leftPartitionNum: Int,
      rightPartitionNum: Int,
      success: Boolean): Unit = {
    val table1 =
      spark.range(10).select($"id" + 1 as Symbol("a1"), $"id" + 2 as Symbol("b1"),
        $"id" + 3 as Symbol("c1")).queryExecution.executedPlan
    val table2 =
      spark.range(10).select($"id" + 1 as Symbol("a2"), $"id" + 2 as Symbol("b2"),
        $"id" + 3 as Symbol("c2")).queryExecution.executedPlan

    val leftKeys = joinKeyIndices.map(table1.output)
    val rightKeys = joinKeyIndices.map(table2.output)
    val leftPartitioning =
      HashPartitioning(leftPartitionKeyIndices.map(table1.output), leftPartitionNum)
    val rightPartitioning =
      HashPartitioning(rightPartitionKeyIndices.map(table2.output), rightPartitionNum)
    val left =
      SortExec(leftKeys.map(SortOrder(_, Ascending)), false,
        ShuffleExchangeExec(leftPartitioning, table1))
    val right =
      SortExec(rightKeys.map(SortOrder(_, Ascending)), false,
        ShuffleExchangeExec(rightPartitioning, table2))

    val plan = SortMergeJoinExec(leftKeys, rightKeys, Inner, None, left, right)
    assert(ValidateRequirements.validate(plan) == success, plan)
  }

  test("SMJ requirements satisfied with partial partition key") {
    testValidate(Seq(0, 1, 2), Seq(1), Seq(1), 5, 5, true)
  }

  test("SMJ requirements satisfied with different partition key order") {
    testValidate(Seq(0, 1, 2), Seq(2, 0, 1), Seq(2, 0, 1), 5, 5, true)
  }

  test("SMJ requirements not satisfied with unequal partition key order") {
    testValidate(Seq(0, 1, 2), Seq(1, 0), Seq(0, 1), 5, 5, false)
  }

  test("SMJ requirements not satisfied with unequal partition key length") {
    testValidate(Seq(0, 1, 2), Seq(1), Seq(1, 2), 5, 5, false)
  }

  test("SMJ requirements not satisfied with partition key missing from join key") {
    testValidate(Seq(1, 2), Seq(1, 0), Seq(1, 0), 5, 5, false)
  }

  test("SMJ requirements not satisfied with unequal partition number") {
    testValidate(Seq(0, 1, 2), Seq(0, 1, 2), Seq(0, 1, 2), 12, 10, false)
  }

  test("SMJ with HashPartitioning(1) and SinglePartition") {
    val table1 = spark.range(10).queryExecution.executedPlan
    val table2 = spark.range(10).queryExecution.executedPlan
    val leftPartitioning = HashPartitioning(table1.output, 1)
    val rightPartitioning = SinglePartition
    val left =
      SortExec(table1.output.map(SortOrder(_, Ascending)), false,
        ShuffleExchangeExec(leftPartitioning, table1))
    val right =
      SortExec(table2.output.map(SortOrder(_, Ascending)), false,
        ShuffleExchangeExec(rightPartitioning, table2))

    val plan = SortMergeJoinExec(table1.output, table2.output, Inner, None, left, right)
    assert(ValidateRequirements.validate(plan), plan)
  }

  private def testNestedJoin(
      joinKeyIndices1: Seq[(Int, Int)],
      joinKeyIndices2: Seq[(Int, Int)],
      partNums: Seq[Int],
      success: Boolean): Unit = {
    val table1 =
      spark.range(10).select($"id" + 1 as Symbol("a1"), $"id" + 2 as Symbol("b1"),
        $"id" + 3 as Symbol("c1")).queryExecution.executedPlan
    val table2 =
      spark.range(10).select($"id" + 1 as Symbol("a2"), $"id" + 2 as Symbol("b2"),
        $"id" + 3 as Symbol("c2")).queryExecution.executedPlan
    val table3 =
      spark.range(10).select($"id" + 1 as Symbol("a3"), $"id" + 2 as Symbol("b3"),
        $"id" + 3 as Symbol("c3")).queryExecution.executedPlan

    val key1 = joinKeyIndices1.map(_._1).map(table1.output)
    val key2 = joinKeyIndices1.map(_._2).map(table2.output)
    val key3 = joinKeyIndices2.map(_._1).map(table3.output)
    val key4 = joinKeyIndices2.map(_._2).map(table1.output ++ table2.output)
    val partitioning1 = HashPartitioning(key1, partNums(0))
    val partitioning2 = HashPartitioning(key2, partNums(1))
    val partitioning3 = HashPartitioning(key3, partNums(2))
    val joinRel1 =
      SortExec(key1.map(SortOrder(_, Ascending)), false, ShuffleExchangeExec(partitioning1, table1))
    val joinRel2 =
      SortExec(key2.map(SortOrder(_, Ascending)), false, ShuffleExchangeExec(partitioning2, table2))
    val joinRel3 =
      SortExec(key3.map(SortOrder(_, Ascending)), false, ShuffleExchangeExec(partitioning3, table3))

    val plan = SortMergeJoinExec(key3, key4, Inner, None,
      joinRel3, SortMergeJoinExec(key1, key2, Inner, None, joinRel1, joinRel2))
    assert(ValidateRequirements.validate(plan) == success, plan)
  }

  test("ValidateRequirements should work bottom up") {
    Seq(true, false).foreach { success =>
      testNestedJoin(Seq((0, 0)), Seq((0, 0)), Seq(5, if (success) 5 else 10, 5), success)
    }
  }

  test("PartitioningCollection exact match") {
    testNestedJoin(Seq((0, 0), (1, 1)), Seq((0, 0), (1, 1)), Seq(5, 5, 5), true)
    testNestedJoin(Seq((0, 0), (1, 1)), Seq((0, 3), (1, 4)), Seq(5, 5, 5), true)
  }

  test("PartitioningCollection mismatch with different order") {
    testNestedJoin(Seq((0, 0), (1, 1)), Seq((1, 1), (0, 0)), Seq(5, 5, 5), false)
    testNestedJoin(Seq((0, 0), (1, 1)), Seq((1, 4), (0, 3)), Seq(5, 5, 5), false)
  }

  test("PartitioningCollection mismatch with different set") {
    testNestedJoin(Seq((1, 1)), Seq((2, 2), (1, 1)), Seq(5, 5, 5), false)
    testNestedJoin(Seq((1, 1)), Seq((2, 5), (1, 4)), Seq(5, 5, 5), false)
  }

  test("PartitioningCollection mismatch with key missing from required") {
    testNestedJoin(Seq((2, 2), (1, 1)), Seq((2, 2)), Seq(5, 5, 5), false)
    testNestedJoin(Seq((2, 2), (1, 1)), Seq((2, 5)), Seq(5, 5, 5), false)
  }

  test("SPARK-59671: a co-partitioning operator judges keyed children by their pairing") {
    // The sides of a storage-partitioned join aligned for skew repeat their spread keys on purpose:
    // neither satisfies a clustering on its own, and each side's layout says where that ungrouping
    // comes from, so the pairing is what the operator reads.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val rows = Seq(InternalRow(1), InternalRow(1), InternalRow(2))
    def side(
        attr: AttributeReference,
        role: UngroupingOrigin,
        keys: Seq[InternalRow]): DummySparkPlan =
      DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(attr), keys).withLayout(_.copy(ungroupingOrigin = Some(role))))
    val left = side(a, SPREADS_SPLITS, rows)
    val right = side(b, REPEATS_GROUP, rows)
    val join = ShuffledHashJoinExec(Seq(a), Seq(b), Inner, BuildLeft, None, left, right)
    assert(ValidateRequirements.validate(join),
      s"a spread side and one that repeats the group pair, which is the requirement:\n$join")

    // The same key rows in another order do not pair: position by position is the contract.
    val off = side(b, REPEATS_GROUP, Seq(InternalRow(1), InternalRow(2), InternalRow(1)))
    assert(!ValidateRequirements.validate(join.copy(right = off)),
      "the same keys in a different order are not aligned")

    // Nor does a keyed side pair with one that never pairs: a hashed side matches neither the keys
    // nor the layout of a keyed one.
    val hashed = DummySparkPlan(outputPartitioning = HashPartitioning(Seq(b), 3))
    assert(!ValidateRequirements.validate(join.copy(right = hashed)),
      "a keyed side does not pair with a hashed one")

    // A side that does not say why it is ungrouped is one no producer built: it is waiting for the
    // node that would settle it, and a finished plan has none left to insert.
    val unsettled = DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(b), rows))
    assert(!ValidateRequirements.validate(join.copy(right = unsettled)),
      s"an ungrouped side is admitted only where a producer built one:\n$join")

    // Nor do two sides of the same role pair: an alignment is one of each, and two spreads leave a
    // key's rows split between them with nothing repeating the group.
    assert(!ValidateRequirements.validate(join.copy(right = side(b, SPREADS_SPLITS, rows))),
      "two sides that spread the same keys are not an alignment")

    // And a pair each side satisfies on its own is still refused when the sides do not line up,
    // which is the operator's requirement: the pairing, not the per-side answer.
    val leftKeys = DummySparkPlan(outputPartitioning =
      KeyedPartitioning(Seq(a), Seq(InternalRow(1), InternalRow(2), InternalRow(3))))
    val rightKeys = DummySparkPlan(outputPartitioning =
      KeyedPartitioning(Seq(b), Seq(InternalRow(4), InternalRow(5), InternalRow(6))))
    assert(leftKeys.outputPartitioning.satisfies(ClusteredDistribution(Seq(a))) &&
      rightKeys.outputPartitioning.satisfies(ClusteredDistribution(Seq(b))),
      "test setup: each side answers its own clustering")
    assert(!ValidateRequirements.validate(ShuffledHashJoinExec(
      Seq(a), Seq(b), Inner, BuildLeft, None, leftKeys, rightKeys)),
      "the sides do not line up, and nothing else the operator reads says otherwise")
  }

  test("SPARK-59671: a keyed pair is judged on the layouts the plan holds") {
    // Neither side's keys are deduped or re-sorted to make the pair line up: reading it through
    // `createShuffleSpec` would do both, so two sides whose layouts disagree as they stand would
    // answer as though they agreed.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val rows = Seq(InternalRow(1), InternalRow(1), InternalRow(2))
    def side(attr: AttributeReference, role: UngroupingOrigin,
        keys: Seq[InternalRow]): DummySparkPlan =
      DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(attr), keys).withLayout(_.copy(ungroupingOrigin = Some(role))))
    val left = side(a, SPREADS_SPLITS, rows)
    // The same keys in another order, which a projection onto distinct sorted keys would
    // normalize away.
    val off = side(b, REPEATS_GROUP, Seq(InternalRow(1), InternalRow(2), InternalRow(1)))
    assert(!ValidateRequirements.validate(ShuffledHashJoinExec(
      Seq(a), Seq(b), Inner, BuildLeft, None, left, off)),
      "the sides report different layouts, and nothing normalizes them")

    // And a count: the other side holds two partitions, and neither side has the node that would
    // make the two agree.
    val twoKeys = DummySparkPlan(outputPartitioning =
      KeyedPartitioning(Seq(b), Seq(InternalRow(1), InternalRow(2))))
    assert(!ValidateRequirements.validate(ShuffledHashJoinExec(
      Seq(a), Seq(b), Inner, BuildLeft, None, left, twoKeys)),
      "a child holding three partitions does not pair with one holding two")
  }

  test("SPARK-59671: a pair is judged on the key the operation clusters on") {
    // The subset permission applies where the operation's keys are a subset of the source's
    // partitioning keys: a side partitioned on `[a, b]` can serve an operator on `[a]`. Such a side
    // is judged on its own partitions under `[a]`, which is what its own key already gives it when
    // dropping `b` merges no partition, whether or not a node stands over it. No key is deduped and
    // none is re-sorted, so the pairs that line up are the ones whose keys line up as reported.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val d = AttributeReference("d", IntegerType)()
    withSQLConf(
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false") {
      def joinOnKeys(left: SparkPlan, right: SparkPlan): SparkPlan =
        ShuffledHashJoinExec(Seq(a), Seq(c), Inner, BuildLeft, None, left, right)

      // The projection's result, which is what such a plan reports from the side it grouped.
      val projected = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(a), Seq(InternalRow(1), InternalRow(2))))
      val projectedRight = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(c), Seq(InternalRow(1), InternalRow(2))))
      assert(ValidateRequirements.validate(joinOnKeys(projected, projectedRight)),
        "the layout the grouping node leaves is what the pair is judged on")

      // The source's keys, one step earlier: the second expression is the operation's to drop, and
      // what is left is the same pair, so it stands.
      val source = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(a, b), Seq(InternalRow(1, 1), InternalRow(2, 2))))
      val sourceRight = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(c, d), Seq(InternalRow(1, 1), InternalRow(2, 2))))
      assert(ValidateRequirements.validate(joinOnKeys(source, sourceRight)),
        s"the key the operation clusters on is what the side offers:\n$source")

      // And it is offered in the order it is reported, so a side whose keys run the other way is
      // not aligned: a spec that sorted them would call these two the same pair.
      val reversed = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(a, b), Seq(InternalRow(2, 2), InternalRow(1, 1))))
      assert(!ValidateRequirements.validate(joinOnKeys(reversed, sourceRight)),
        s"the keys are compared as reported, not as sorted:\n$reversed")
    }
  }

  test("SPARK-59671: a single clustered child still owes its own grouping") {
    // The pairing stands in for the per-child check only where children pair with each other.
    // An operator with a single clustered child, an aggregate over a join output say, is judged
    // per side: the ungrouped keyed layout does not satisfy it until a grouping stands under.
    val a = AttributeReference("a", IntegerType)()
    val child = DummySparkPlan(outputPartitioning =
      KeyedPartitioning(Seq(a), Seq(InternalRow(1), InternalRow(1))))
    val parent = DummySparkPlan(
      children = Seq(child),
      requiredChildDistribution = Seq(ClusteredDistribution(Seq(a))),
      requiredChildOrdering = Seq(Nil))
    assert(!ValidateRequirements.validate(parent), "an ungrouped child fails a lone clustered slot")
  }

  test("SPARK-59671: a partially clustered pair planned by the rule passes validation") {
    // Partially clustered distribution spreads a side, so its replicate side reports a
    // non-grouped layout whose keys repeat on purpose, and the validator's per-side check
    // refused such a pair. AQE validates a stage's whole candidate plan before accepting a
    // shuffle-read change, so the plan's stage took no coalescing either. The pairing takes
    // it: the spread is deliberate, and the two sides agree index by index.
    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
      val a = AttributeReference("a", IntegerType)()
      val b = AttributeReference("b", IntegerType)()
      val left = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(a), Seq(InternalRow(1), InternalRow(1), InternalRow(2))))
      val right = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(b), Seq(InternalRow(1), InternalRow(2))))
      val plan = new EnsureRequirements().apply(
        SortMergeJoinExec(Seq(a), Seq(b), Inner, None, left, right))
      val join = plan.collectFirst { case j: SortMergeJoinExec => j }
        .getOrElse(fail(s"expected the join back:\n${plan.treeString}"))
      def groupingAt(plan: SparkPlan): Option[GroupPartitionsExec] = plan match {
        case g: GroupPartitionsExec => Some(g)
        case s: SortExec if !s.global => groupingAt(s.child)
        case _ => None
      }
      val sideGroupings = join.children.flatMap(groupingAt)
      assert(sideGroupings.size == 2,
        s"test setup: both sides are aligned:\n${plan.treeString}")
      assert(sideGroupings.exists { g =>
        PartitioningCollection.representativeOf(g.outputPartitioning).exists(!_.isGrouped)
      }, s"test setup: a side keeps its splits, so its keys repeat:\n${plan.treeString}")
      assert(ValidateRequirements.validate(plan),
        s"a spread pair that agrees on its keys is accepted:\n${plan.treeString}")
    }
  }

  test("SPARK-59671: a collection of keyed members is judged by its pairing") {
    // A side that reports several keyed alternatives (a projection over a join keeps one per join
    // key column) is judged on the members the admission keeps: a side whose members all carry the
    // same origin for their ungrouped keys is a pair the producer aligns, and it holds up when the
    // member keyed on the join key is the one that lines up with the other side.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val rows = Seq(InternalRow(1), InternalRow(1), InternalRow(2))
    def keyed(attr: AttributeReference, role: UngroupingOrigin): KeyedPartitioning =
      KeyedPartitioning(Seq(attr), rows).withLayout(_.copy(ungroupingOrigin = Some(role)))
    def side(partitioning: Partitioning): DummySparkPlan =
      DummySparkPlan(outputPartitioning = partitioning)
    val bothAlternatives = side(PartitioningCollection.fromPartitionings(
      Seq(keyed(a, REPEATS_GROUP), keyed(b, REPEATS_GROUP))))
    assert(ValidateRequirements.validate(ShuffledHashJoinExec(
      Seq(a), Seq(a), Inner, BuildLeft, None, bothAlternatives, side(keyed(a, SPREADS_SPLITS)))),
      "the member keyed on the join key pairs with the other side")

    // The pairing still has to be there: a side whose members are keyed on something else offers
    // nothing to pair with.
    val wrongKeys = side(PartitioningCollection.fromPartitionings(
      Seq(keyed(b, REPEATS_GROUP), keyed(b, REPEATS_GROUP))))
    assert(!ValidateRequirements.validate(ShuffledHashJoinExec(
      Seq(a), Seq(a), Inner, BuildLeft, None, wrongKeys, side(keyed(a, SPREADS_SPLITS)))),
      "a side offering no member keyed on the join keys does not pair")
  }

  test("SPARK-59671: a multi-child clustered operator is judged on its children's pairing") {
    // Every operator whose children all owe a `ClusteredDistribution` reads one layout they have to
    // hold together, whichever operator it is: a cogroup zips corresponding partitions, so two
    // sides that each satisfy the distribution on their own are not enough.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val rows = Seq(InternalRow(1), InternalRow(1), InternalRow(2))
    def cogroupOf(left: SparkPlan, right: SparkPlan): SparkPlan = CoGroupExec(
      (key: Any, l: Iterator[Any], r: Iterator[Any]) => Nil,
      Literal(1), Literal(1), Literal(1), Seq(a), Seq(b), Seq(a), Seq(b), Nil, Nil,
      AttributeReference("obj", ObjectType(classOf[AnyRef]))(), left, right)
    def grouped(attr: AttributeReference, keys: Seq[Int]): DummySparkPlan = DummySparkPlan(
      outputOrdering = Seq(SortOrder(attr, Ascending)),
      outputPartitioning = KeyedPartitioning(Seq(attr), keys.map(InternalRow(_))))

    // A side that does not say why it is ungrouped is one no producer built, which is the whole of
    // what the validator can read: the operator kind is the producer's to check, and it is checked
    // where the alignment is built.
    val unsettled = DummySparkPlan(
      outputOrdering = Seq(SortOrder(a, Ascending)),
      outputPartitioning = KeyedPartitioning(Seq(a), rows))
    assert(!ValidateRequirements.validate(cogroupOf(unsettled, unsettled)),
      "an ungrouped side no producer built is refused, whoever reads it")

    // What a cogroup shares with a join is the mutual check on the layouts its sides report.
    def cogroup(aKeys: Seq[Int], bKeys: Seq[Int]): SparkPlan =
      cogroupOf(grouped(a, aKeys), grouped(b, bKeys))
    assert(ValidateRequirements.validate(cogroup(Seq(1, 2), Seq(1, 2))),
      "two sides holding the same grouped layout are read as they stand")
    assert(!ValidateRequirements.validate(cogroup(Seq(1, 2), Seq(1, 2, 3))),
      "a side holding three partitions does not pair with one holding two")
    assert(!ValidateRequirements.validate(cogroup(Seq(1, 2), Seq(2, 1))),
      "the same keys in another order are not aligned")
  }

  test("SPARK-59671: a side is judged on the member that pairs, whichever one it is") {
    // A side that reports several keyed alternatives offers all of them: the one keyed on the
    // join key is the one that lines up, and it need not be the first the side reports. Reading a
    // single member would refuse this pair, which the planner builds whenever a projection keeps
    // two key columns.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val rows = Seq(InternalRow(1), InternalRow(1), InternalRow(2))
    def keyed(attr: AttributeReference, role: UngroupingOrigin): KeyedPartitioning =
      KeyedPartitioning(Seq(attr), rows).withLayout(_.copy(ungroupingOrigin = Some(role)))
    val left = DummySparkPlan(outputPartitioning =
      PartitioningCollection.fromPartitionings(
        Seq(keyed(b, REPEATS_GROUP), keyed(a, REPEATS_GROUP))))
    val right = DummySparkPlan(outputPartitioning = keyed(a, SPREADS_SPLITS))
    assert(ValidateRequirements.validate(ShuffledHashJoinExec(
      Seq(a), Seq(a), Inner, BuildLeft, None, left, right)),
      "the second member is the one keyed on the join key, and it pairs")
  }

  test("SPARK-59671: the ordering requirement is asked on top of the pairing") {
    // A pair can line up and still owe its operator an ordering: the pairing covers the
    // distribution clause alone, so a sort-merge join over the aligned sides, with nothing
    // ordering them, is refused.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val rows = Seq(InternalRow(1), InternalRow(1), InternalRow(2))
    def side(attr: AttributeReference, role: UngroupingOrigin): DummySparkPlan =
      DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(attr), rows).withLayout(_.copy(ungroupingOrigin = Some(role))))
    val left = side(a, SPREADS_SPLITS)
    val right = side(b, REPEATS_GROUP)
    assert(left.outputOrdering.isEmpty && right.outputOrdering.isEmpty,
      "test setup: nothing orders the sides")
    assert(!ValidateRequirements.validate(
      SortMergeJoinExec(Seq(a), Seq(b), Inner, None, left, right)),
      "the sides pair, and still owe the join its ordering")
  }

  test("SPARK-59671: a collapsed pair is not admitted on its pairing alone") {
    // A layout whose keys were collapsed (a partition standing for several of the source's) serves
    // a clustering through a grouping node only where that grouping is permitted, and the
    // permission is a config. Without it nothing admits such a member, and a side offering nothing
    // has nothing to pair with.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    def collapsed(attr: AttributeReference): DummySparkPlan = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(attr),
        Seq(InternalRow(1), InternalRow(1), InternalRow(2)))
        .withLayout(_.copy(isCollapsed = true)))
    def pair: SparkPlan = ShuffledHashJoinExec(Seq(a), Seq(b), Inner, BuildLeft, None,
      collapsed(a), collapsed(b))

    // Whether a collapsed layout may be grouped is the producer's permission to apply, and it
    // applies it where the stamp goes: the validator reads no configuration, so the same pair
    // answers the same way either way.
    Seq("false", "true").foreach { subset =>
      withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> subset) {
        assert(!ValidateRequirements.validate(pair),
          s"a pair no producer built is refused whatever the permission says:\n$pair")
      }
    }
  }
}
