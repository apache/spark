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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, SortOrder, TransformExpression}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, KeyedPartitioning, KeyedShuffleSpec, KeyReducer, Partitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.KeyGroupedPartitioningSuiteBase
import org.apache.spark.sql.connector.catalog.functions.{BucketFunction, BucketReducer, Reducer}
import org.apache.spark.sql.connector.expressions.Expressions.identity
import org.apache.spark.sql.execution.{DummySparkPlan, LeafExecNode, SafeForKWayMerge}
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType}

class GroupPartitionsExecSuite
  extends KeyGroupedPartitioningSuiteBase with SQLMetricsTestUtils {

  private val exprA = AttributeReference("a", IntegerType)()
  private val exprB = AttributeReference("b", IntegerType)()
  private val exprC = AttributeReference("c", IntegerType)()

  private def row(a: Int): InternalRow = InternalRow.fromSeq(Seq(a))
  private def row(a: Int, b: Int): InternalRow = InternalRow.fromSeq(Seq(a, b))

  /** Reads one metric of a plan-graph node back from the status store values. */
  private def metricValue(
      metricValues: Map[Long, String], node: SparkPlanGraphNode, name: String): String = {
    val metric = node.metrics.find(_.name == name).getOrElse {
      val names = node.metrics.map(_.name).mkString(", ")
      fail(s"metric '$name' missing on ${node.name}: $names")
    }
    metricValues(metric.accumulatorId).replaceAll(",", "")
  }

  test("SPARK-59057: the output flag reports what this node's grouping merges") {
    // Keys [(1,1), (1,2), (2,1)] projected onto position 0 give [1, 1, 2]. The first two groups
    // cover keys the child held apart, the third does not.
    val keys = Seq(row(1, 1), row(1, 2), row(2, 1))
    def gpe(joinKeyPositions: Option[Seq[Int]],
        expected: Option[Seq[(InternalRowComparableWrapper, Int)]] = None,
        distribute: Boolean = false,
        childCollapsed: Boolean = false): KeyedPartitioning = {
      val childKp = KeyedPartitioning(Seq(exprA, exprB), keys).copy(isCollapsed = childCollapsed)
      GroupPartitionsExec(DummySparkPlan(outputPartitioning = childKp), joinKeyPositions,
        expected, distributePartitions = distribute)
        .outputPartitioning.asInstanceOf[KeyedPartitioning]
    }
    def keyOf(a: Int): InternalRowComparableWrapper =
      InternalRowComparableWrapper(row(a), Seq(exprA))

    assert(!gpe(None).isCollapsed,
      "no projection, so every group covers the one key it was built from")
    assert(gpe(Some(Seq(0))).isCollapsed, "keys (1,1) and (1,2) are merged into key 1")
    assert(gpe(None, childCollapsed = true).isCollapsed, "the child's flag is sticky")

    // The keys the join agreed on decide it. Keeping key 1 keeps the merge, keeping only key 2
    // does not, and that holds however the splits of a kept key are laid out afterwards. Key 1 has
    // two child splits, which is the split count `EnsureRequirements` derives for it.
    Seq(false, true).foreach { distribute =>
      assert(gpe(Some(Seq(0)), Some(Seq(keyOf(1) -> 2, keyOf(2) -> 1)), distribute).isCollapsed,
        s"distributePartitions=$distribute: the merged key 1 survives")
      assert(!gpe(Some(Seq(0)), Some(Seq(keyOf(2) -> 1)), distribute).isCollapsed,
        s"distributePartitions=$distribute: only key 2 survives, and it merges nothing")
    }
  }

  test("SPARK-59121: a node with no reducers keeps the child's reduced key marker") {
    // This node re-reports the child's expressions, projected to `joinKeyPositions`. A reduce that
    // happened below it has to survive that, or a further join above reads the reported transform
    // as if it still described the keys.
    val reducedExpr = TransformExpression(BucketFunction, Seq(exprA), Some(12))
      .reducedTogetherWith(TransformExpression(BucketFunction, Seq(exprA), Some(8)))
    val child = DummySparkPlan(outputPartitioning =
      KeyedPartitioning(Seq(reducedExpr, exprB), Seq(row(1, 10), row(2, 20), row(1, 30))))
    val gpe = GroupPartitionsExec(child, joinKeyPositions = Some(Seq(0)))

    assert(gpe.reducers.isEmpty, "test setup: this node reduces nothing itself")
    gpe.outputPartitioning match {
      case kp: KeyedPartitioning =>
        assert(kp.expressions === Seq(reducedExpr), "the projection keeps the marked expression")
        assert(!kp.expressionsDescribeKeys)
      case other => fail(s"Expected KeyedPartitioning, got $other")
    }

    // Same for a node that reduces the other position. The position it does not reduce passes
    // through, marker and all.
    val reducingGpe = GroupPartitionsExec(child, reducers = Some(Seq(None, Some(
      KeyReducer(BucketReducer(2), TransformExpression(BucketFunction, Seq(exprB), Some(2)))))))
    reducingGpe.outputPartitioning match {
      case kp: KeyedPartitioning =>
        assert(kp.expressions.head === reducedExpr, "the unreduced position keeps its marker")
        assert(!kp.expressionsDescribeKeys)
      case other => fail(s"Expected KeyedPartitioning, got $other")
    }
  }

  test("SPARK-56241: non-coalescing passes through child ordering unchanged") {
    // Each partition has a distinct key — no coalescing happens.
    val partitionKeys = Seq(row(1), row(2), row(3))
    val childOrdering = Seq(SortOrder(exprA, Ascending))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)
    val gpe = GroupPartitionsExec(child)

    assert(gpe.groupedPartitions.forall(_._2.size <= 1), "expected non-coalescing")
    assert(gpe.outputOrdering === childOrdering)
  }

  test("SPARK-58324: k-way merge ordering drops sameOrderExpressions") {
    // The child ordering carries sameOrderExpressions (planner metadata). The k-way merge
    // comparator only needs the sort key, so kWayMergeOrdering keeps child/direction/nullOrdering
    // but drops sameOrderExpressions, so LazyCodeGenOrdering does not serialize them with the RDD.
    val childOrdering = Seq(SortOrder(exprA, Ascending, Seq(exprB, exprC)))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), Seq(row(1), row(2), row(1))),
      outputOrdering = childOrdering)
    val gpe = GroupPartitionsExec(child)

    assert(child.outputOrdering.head.sameOrderExpressions.nonEmpty, "test setup")
    val merged = gpe.kWayMergeOrdering
    assert(merged.map(so => (so.child, so.direction)) === Seq((exprA, Ascending)))
    assert(merged.forall(_.sameOrderExpressions.isEmpty))
  }

  test("SPARK-56241: coalescing without reducers keeps key-expression orders from child") {
    // Key 1 appears on partitions 0 and 2, causing coalescing.
    val partitionKeys = Seq(row(1), row(2), row(1))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = Seq(SortOrder(exprA, Ascending)))
    val gpe = GroupPartitionsExec(child)

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    // With the config disabled (default), key-expression filtering is skipped.
    assert(gpe.outputOrdering === Nil)
    // When enabled, the key-expression order is preserved through coalescing.
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val ordering = gpe.outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.child === exprA)
      assert(ordering.head.direction === Ascending)
      assert(ordering.head.sameOrderExpressions.isEmpty)
    }
  }

  test("SPARK-56241: coalescing without reducers keeps one SortOrder per key expression") {
    // Multi-key partition: key (1,10) appears on partitions 0 and 2, causing coalescing.
    val partitionKeys = Seq(row(1, 10), row(2, 20), row(1, 10))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA, exprB), partitionKeys),
      outputOrdering = Seq(SortOrder(exprA, Ascending), SortOrder(exprB, Ascending)))
    val gpe = GroupPartitionsExec(child)

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    assert(gpe.outputOrdering === Nil)
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val ordering = gpe.outputOrdering
      assert(ordering.length === 2)
      assert(ordering.head.child === exprA)
      assert(ordering(1).child === exprB)
      assert(ordering.head.sameOrderExpressions.isEmpty)
      assert(ordering(1).sameOrderExpressions.isEmpty)
    }
  }

  test("SPARK-56241: coalescing join case preserves sameOrderExpressions from child") {
    // PartitioningCollection wraps two KeyedPartitionings (one per join side), sharing the same
    // partition keys. Key 1 coalesces partitions 0 and 2. The child (e.g. SortMergeJoinExec)
    // already carries sameOrderExpressions linking both sides' key expressions.
    val partitionKeys = Seq(row(1), row(2), row(1))
    val leftKP = KeyedPartitioning(Seq(exprA), partitionKeys)
    val rightKP = KeyedPartitioning(Seq(exprB), partitionKeys)
    val child = DummySparkPlan(
      outputPartitioning = PartitioningCollection.fromPartitionings(Seq(leftKP, rightKP)),
      outputOrdering = Seq(SortOrder(exprA, Ascending, sameOrderExpressions = Seq(exprB))))
    val gpe = GroupPartitionsExec(child)

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    assert(gpe.outputOrdering === Nil)
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val ordering = gpe.outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.child === exprA)
      assert(ordering.head.sameOrderExpressions === Seq(exprB))
    }
  }

  test("SPARK-56241: coalescing drops non-key sort orders from child") {
    // exprA is the partition key; exprC is a non-key sort order the child also reports
    // (e.g. a secondary sort within each partition). After coalescing, exprC ordering is lost
    // by concatenation, so only the exprA order should survive.
    val partitionKeys = Seq(row(1), row(2), row(1))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = Seq(SortOrder(exprA, Ascending), SortOrder(exprC, Ascending)))
    val gpe = GroupPartitionsExec(child)

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    assert(gpe.outputOrdering === Nil)
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val ordering = gpe.outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.child === exprA)
    }
  }

  test("SPARK-56241: coalescing with reducers returns empty ordering") {
    // When reducers are present, the original key expressions are not constant within the merged
    // partition, so outputOrdering falls back to the default (empty).
    val partitionKeys = Seq(row(1), row(2), row(1))
    val child = DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys))
    // reducers = Some(Seq(None)) - None element means identity reducer; the important thing is
    // that reducers.isDefined, which triggers the fallback.
    val gpe = GroupPartitionsExec(child, reducers = Some(Seq(None)))

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    assert(gpe.outputOrdering === Nil)
  }

  test("SPARK-59050: identity grouping without reducers keeps the child's marker") {
    // An identity grouping (output partition i holds exactly input partition i) keeps the
    // unknown-key hash-routing relationship intact, so the marker rides the transform.
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), Seq(row(1), row(2)))
        .copy(mayContainUnknownPartitionKeys = true))
    val out = GroupPartitionsExec(child)
      .outputPartitioning.asInstanceOf[KeyedPartitioning]
    assert(out.mayContainUnknownPartitionKeys, "the marker must survive identity grouping")
  }

  test("SPARK-59050: non-identity grouping without reducers drops the unknown-keyed claim") {
    // A regrouping that reorders or coalesces the partitions moves the rows an unknown-key claim
    // pins to hash(key) % numPartitions, so the claim cannot survive; clearing only the marker
    // would misreport the undeclared rows that remain.
    // Coalesce: keys [1, 2, 1] merge the two key-1 partitions.
    val coalesced = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), Seq(row(1), row(2), row(1)))
        .copy(mayContainUnknownPartitionKeys = true))
    GroupPartitionsExec(coalesced).outputPartitioning match {
      case u: UnknownPartitioning =>
        assert(u.numPartitions === 2, "the give-up count must match the physical partitions")
      case other =>
        fail(s"expected the unknown-keyed claim to be dropped on coalesce, got $other")
    }
    // Reorder: keys [2, 1] sort to [1, 2], swapping partitions 0 and 1.
    val reordered = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), Seq(row(2), row(1)))
        .copy(mayContainUnknownPartitionKeys = true))
    GroupPartitionsExec(reordered).outputPartitioning match {
      case u: UnknownPartitioning =>
        assert(u.numPartitions === 2, "the give-up count must match the physical partitions")
      case other =>
        fail(s"expected the unknown-keyed claim to be dropped on reorder, got $other")
    }
  }

  test("SPARK-59050: an identity grouping that shrinks the partition count drops the claim") {
    // `alignToExpectedKeys` emits only the expected keys, so a declared key the merged set
    // does not carry is never emitted. When the dropped key is trailing, every kept group
    // still reads as the identity, but the partition count shrank and the hash modulus with
    // it: the child's undeclared rows sit at hash(key) % 3 while the retained claim would
    // promise hash(key) % 2. The count check catches what the per-group check cannot.
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), Seq(row(1), row(2), row(3)))
        .copy(mayContainUnknownPartitionKeys = true))
    def keyOf(a: Int): InternalRowComparableWrapper =
      InternalRowComparableWrapper(row(a), Seq(exprA))
    val gpe = GroupPartitionsExec(child,
      expectedPartitionKeys = Some(Seq(keyOf(1) -> 1, keyOf(2) -> 1)))
    assert(gpe.groupedPartitions.size === 2, "the trailing key 3 is dropped")
    gpe.outputPartitioning match {
      case u: UnknownPartitioning =>
        assert(u.numPartitions === 2, "the give-up count must match the physical partitions")
      case other =>
        fail(s"expected the unknown-keyed claim to be dropped on a shrink, got $other")
    }
  }

  test("SPARK-59050: unknown-keyed child with reducers gives up the keyed claim at the real " +
    "count") {
    // The reducer instance of the give-up: a reduction regroups by the reduced keys, a
    // non-identity grouping, so the unknown-keyed claim is dropped at the physical grouped
    // count. The node must report `UnknownPartitioning` with its physical grouped count;
    // reporting zero partitions is what threw once a parent join built a
    // `PartitioningCollection` over both sides. (`areKeysCompatible` pairs a marked layout
    // only with same-function partners that need no reducer, so no planner path applies a
    // reducer to a marked layout; this pins the contract directly.)
    // The reducer must be real: it is the collapse of [1, 2, 3] to two groups that exercises
    // the regrouping the give-up answers. A reducer slot that rewrites no key is pinned by the
    // next test, where it is the slot itself, not a merge, that drops the claim.
    // Keys [1, 2, 3] reduced mod 2 give [1, 0, 1]: the node emits 2 grouped partitions where
    // the child had 3, and the give-up count has to be that physical 2.
    val mod2 = new Reducer[Int, Int] {
      override def reduce(v: Int): Int = v % 2
      override def resultType(): DataType = IntegerType
      override def displayName(): String = "mod2"
    }
    val reducer = KeyReducer(mod2, TransformExpression(BucketFunction, Seq(exprA), Some(2)))
    val partitionKeys = Seq(row(1), row(2), row(3))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys)
        .copy(mayContainUnknownPartitionKeys = true))
    val gpe = GroupPartitionsExec(child, reducers = Some(Seq(Some(reducer))))

    assert(gpe.groupedPartitions.size === 2, "mod 2 collapses keys 1 and 3")
    gpe.outputPartitioning match {
      case u: UnknownPartitioning =>
        assert(u.numPartitions === 2, "the give-up count must match the physical partitions")
      case other =>
        fail(s"expected the unknown-keyed claim to be dropped on reduction, got $other")
    }
    // A parent join merges the two sides' partitionings into a collection: a count of 0 fails
    // the uniform-numPartitions requirement, the planning throw reproduced; this call
    // throwing fails the test.
    val partner = KeyedPartitioning(Seq(exprB), Seq(row(1), row(2)))
    PartitioningCollection.fromPartitionings(Seq(gpe.outputPartitioning, partner))
  }

  test("SPARK-59050: a grouping that rewrites the declared keys drops the claim") {
    // `identityGrouping` also asks whether the grouping rewrote the keys: the claim the node
    // goes on to declare lives in the projected or reduced key space, while the child's
    // undeclared rows still sit at hash(originalKey) % numPartitions. A reducer slot, a
    // narrowing projection, or a reordering projection therefore gives up the claim even when
    // every group keeps its index and the count is unchanged. A conforming self-reducer cannot
    // rewrite a reachable key (its contract is r(f(x)) = f(x)), so the give-up there loses at
    // most an optimization; no planner path applies a reducer or a non-identity projection to
    // a marked layout, so these shapes are pinned here directly.
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA, exprB), Seq(row(1, 10), row(2, 20)))
        .copy(mayContainUnknownPartitionKeys = true))

    // Reducer slots: `Some(Seq(None, None))` leaves every key and every index unchanged.
    val reduced = GroupPartitionsExec(child, reducers = Some(Seq(None, None)))
    assert(reduced.groupedPartitions.size === 2)
    reduced.outputPartitioning match {
      case u: UnknownPartitioning =>
        assert(u.numPartitions === 2, "the give-up count must match the physical partitions")
      case other =>
        fail(s"expected the unknown-keyed claim to be dropped with reducer slots, got $other")
    }

    // Narrowing projection: position 0 of keys [(1, 10), (2, 20)] keeps both groups in order.
    val projected = GroupPartitionsExec(child, joinKeyPositions = Some(Seq(0)))
    assert(projected.groupedPartitions.size === 2)
    projected.outputPartitioning match {
      case u: UnknownPartitioning =>
        assert(u.numPartitions === 2, "the give-up count must match the physical partitions")
      case other =>
        fail("expected the unknown-keyed claim to be dropped on a narrowing projection, " +
          s"got $other")
    }

    // Reordering projection: positions Seq(1, 0) re-label every group into the swapped key
    // space while each group keeps its index and the count, so only the rewrite clause can
    // catch it. No producer of joinKeyPositions emits anything but ascending positions today;
    // this pins the predicate directly.
    val reordered = GroupPartitionsExec(child, joinKeyPositions = Some(Seq(1, 0)))
    assert(reordered.groupedPartitions.size === 2)
    assert(reordered.groupedPartitions.zipWithIndex.forall {
      case ((_, inputIndices), outputIndex) => inputIndices == Seq(outputIndex)
    }, "the reordering keeps every group at its index")
    reordered.outputPartitioning match {
      case u: UnknownPartitioning =>
        assert(u.numPartitions === 2, "the give-up count must match the physical partitions")
      case other =>
        fail("expected the unknown-keyed claim to be dropped on a reordering projection, " +
          s"got $other")
    }
  }

  test("SPARK-55715: enableSortedMerge with a child that is not SafeForKWayMerge falls back " +
      "to key-expression ordering") {
    // DummySparkPlan does not extend SafeForKWayMerge, so childIsSafeForKWayMerge = false and the
    // k-way merge is not feasible even with enableSortedMerge = true. outputOrdering must
    // therefore fall back to key-expression filtering (not return the full child ordering).
    val partitionKeys = Seq(row(1), row(2), row(1))
    val childOrdering = Seq(SortOrder(exprA, Ascending), SortOrder(exprC, Ascending))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)

    assert(!GroupPartitionsExec(child).groupedPartitions.forall(_._2.size <= 1),
      "expected coalescing")
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      // Even though enableSortedMerge = true, the child is not safe for k-way merge,
      // so only key-expression orders survive (non-key exprC is dropped).
      val ordering = GroupPartitionsExec(child, enableSortedMerge = true).outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.child === exprA)
    }
  }

  test("SPARK-55715: coalescing with enableSortedMerge = true returns full child ordering") {
    // Key 1 appears on partitions 0 and 2, causing coalescing. The child is a LeafExecNode so
    // childIsSafeForKWayMerge = true. With enableSortedMerge = true the node performs the k-way
    // merge, so the full child ordering (including the non-key exprC) must be returned, not just
    // the subset of key-expression orders.
    val partitionKeys = Seq(row(1), row(2), row(1))
    val childOrdering = Seq(SortOrder(exprA, Ascending), SortOrder(exprC, Ascending))
    val child = DummyLeafSparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)

    assert(!GroupPartitionsExec(child).groupedPartitions.forall(_._2.size <= 1),
      "expected coalescing")
    assert(GroupPartitionsExec(child, enableSortedMerge = true).outputOrdering === childOrdering)
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      // Without the flag there is no k-way merge, so only key-expression orders survive simple
      // concatenation and the non-key exprC is dropped.
      val ordering = GroupPartitionsExec(child).outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.child === exprA)
    }
  }

  test("SPARK-59279: enableSortedMerge decides the k-way merge, not the config") {
    // The config is the planner's input, and `enableSortedMerge` records what the planner decided
    // under it. Once the flag is set the config no longer matters, because the plan above was
    // built on the ordering the merge delivers.
    val partitionKeys = Seq(row(1), row(2), row(1))
    val childOrdering = Seq(SortOrder(exprA, Ascending), SortOrder(exprC, Ascending))
    val child = DummyLeafSparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)

    Seq(true, false).foreach { configEnabled =>
      withSQLConf(
          SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key ->
            configEnabled.toString) {
        val flagged = GroupPartitionsExec(child, enableSortedMerge = true)
        assert(flagged.outputOrdering === childOrdering,
          s"config=$configEnabled: the flag alone must keep the full ordering")
        val unflagged = GroupPartitionsExec(child)
        assert(unflagged.outputOrdering !== childOrdering,
          s"config=$configEnabled: without the flag there is no k-way merge to report, and the " +
            "config cannot supply one")
      }
    }
  }

  test("SPARK-56549: tryEnableSortedMerge returns Some when conditions are met") {
    val partitionKeys = Seq(row(1), row(2), row(1))
    val childOrdering = Seq(SortOrder(exprA, Ascending), SortOrder(exprC, Ascending))
    val child = DummyLeafSparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)
    val gpe = GroupPartitionsExec(child)

    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val result = gpe.tryEnableSortedMerge()
      assert(result.isDefined)
      assert(result.get.enableSortedMerge)
      assert(result.get.outputOrdering === childOrdering)
    }
  }

  test("SPARK-56549: tryEnableSortedMerge returns None when config is disabled") {
    val partitionKeys = Seq(row(1), row(2), row(1))
    val childOrdering = Seq(SortOrder(exprA, Ascending))
    val child = DummyLeafSparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)
    val gpe = GroupPartitionsExec(child)

    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "false") {
      assert(gpe.tryEnableSortedMerge().isEmpty)
    }
  }

  test("SPARK-56549: tryEnableSortedMerge returns None when child is not SafeForKWayMerge") {
    val partitionKeys = Seq(row(1), row(2), row(1))
    val childOrdering = Seq(SortOrder(exprA, Ascending))
    // DummySparkPlan does not extend SafeForKWayMerge
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)
    val gpe = GroupPartitionsExec(child)

    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      assert(gpe.tryEnableSortedMerge().isEmpty)
    }
  }

  test("SPARK-59027: createShuffleSpec subset-keys spec orders keys the same as this node's " +
      "grouping") {
    // With `allowKeysSubsetOfPartitionKeys`, `EnsureRequirements` may shuffle the other join side
    // onto the spec's projected keys while this side is re-grouped by a `GroupPartitionsExec`
    // carrying the spec's `joinKeyPositions`. The two key orders must agree (see
    // `KeyedPartitioning.groupedKeyRowOrdering`), or the sides are mis-aligned -- a planning-time
    // `PartitioningCollection` invariant failure for inner joins, silent wrong results for join
    // types that expose only one side's partitioning.
    // First-appearance order of the projected keys ([3], [1], [2]) differs from their sorted
    // order ([1], [2], [3]), so the assertion discriminates the sort each side uses.
    val partitionKeys = Seq(row(3, 30), row(1, 10), row(2, 20), row(1, 99))
    val partitioning = KeyedPartitioning(Seq(exprA, exprB), partitionKeys)

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val spec = partitioning.createShuffleSpec(ClusteredDistribution(Seq(exprA)))
        .asInstanceOf[KeyedShuffleSpec]
      assert(spec.joinKeyPositions === Some(Seq(0)))

      val gpe = GroupPartitionsExec(
        DummySparkPlan(outputPartitioning = partitioning),
        joinKeyPositions = spec.joinKeyPositions)
      assert(gpe.groupedPartitions.map(_._1) === spec.partitioning.partitionKeys)
    }
  }

  test("SPARK-56549: tryEnableSortedMerge returns None when no coalescing occurs") {
    val partitionKeys = Seq(row(1), row(2), row(3))
    val childOrdering = Seq(SortOrder(exprA, Ascending))
    val child = DummyLeafSparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)
    val gpe = GroupPartitionsExec(child)

    assert(gpe.groupedPartitions.forall(_._2.size <= 1), "expected non-coalescing")
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      assert(gpe.tryEnableSortedMerge().isEmpty)
    }
  }

  test("SPARK-59310: basic counts without alignment") {
    // Keys [1, 2, 1]: 3 input splits, key 1 coalesces partitions 0 and 2, 2 output partitions.
    val child = ExecutableKeyedLeaf(KeyedPartitioning(Seq(exprA), Seq(row(1), row(2), row(1))))
    val gpe = GroupPartitionsExec(child)
    gpe.execute()

    assert(gpe.metrics("numInputPartitions").value === 3)
    assert(gpe.metrics("numPartitions").value === 2)
    assert(gpe.metrics("numEmptyPartitions").value === 0)
    assert(gpe.metrics("numCoalescedPartitions").value === 1)
    assert(gpe.metrics("maxPartitionsPerGroup").value === 2)
    // Without expectedPartitionKeys there is no alignment, so its metrics stay unregistered.
    assert(!gpe.metrics.contains("numPrunedPartitions"))
    assert(!gpe.metrics.contains("numReplicatedPartitions"))
  }

  test("SPARK-59310: zero coalesced without duplicate keys") {
    val child = ExecutableKeyedLeaf(KeyedPartitioning(Seq(exprA), Seq(row(1), row(2), row(3))))
    val gpe = GroupPartitionsExec(child)
    gpe.execute()

    assert(gpe.metrics("numCoalescedPartitions").value === 0)
    assert(gpe.metrics("numPartitions").value === 3)
    assert(gpe.metrics("maxPartitionsPerGroup").value === 1)
  }

  test("SPARK-59310: distribute alignment pads and never replicates") {
    // Child splits: key 1 -> [0], key 2 -> [1, 2]. Expected: key 1 x1, key 2 x3, key 3 x1.
    // Key 2 spreads its 2 splits over 3 expected partitions (one empty pad) and key 3 has no
    // split (one more empty), so 5 output partitions with 2 empty and nothing coalesced.
    def keyOf(a: Int): InternalRowComparableWrapper =
      InternalRowComparableWrapper(row(a), Seq(exprA))
    val child = ExecutableKeyedLeaf(KeyedPartitioning(Seq(exprA), Seq(row(1), row(2), row(2))))
    val gpe = GroupPartitionsExec(child,
      expectedPartitionKeys = Some(Seq(keyOf(1) -> 1, keyOf(2) -> 3, keyOf(3) -> 1)),
      distributePartitions = true)
    gpe.execute()

    assert(gpe.metrics("numInputPartitions").value === 3)
    assert(gpe.metrics("numPartitions").value === 5)
    assert(gpe.metrics("numEmptyPartitions").value === 2)
    assert(gpe.metrics("numPrunedPartitions").value === 0)
    assert(gpe.metrics("numCoalescedPartitions").value === 0, "distribute never coalesces")
    assert(!gpe.metrics.contains("numReplicatedPartitions"), "distribute never replicates")
  }

  test("SPARK-59310: alignment prunes unmatched keys, pads missing ones") {
    // The expected keys carry key 1 and a key-3 slot the child does not hold, as an inner
    // join's intersection combined with the other side's layout would. The 2 splits of key 2
    // cannot produce join output and never enter the alignment; the missing key 3 pads two
    // empty output partitions, and being empty, replicates nothing despite its 2 slots.
    def keyOf(a: Int): InternalRowComparableWrapper =
      InternalRowComparableWrapper(row(a), Seq(exprA))
    val child = ExecutableKeyedLeaf(KeyedPartitioning(Seq(exprA), Seq(row(1), row(2), row(2))))
    val gpe = GroupPartitionsExec(child,
      expectedPartitionKeys = Some(Seq(keyOf(1) -> 1, keyOf(3) -> 2)))
    gpe.execute()

    assert(gpe.metrics("numInputPartitions").value === 3)
    assert(gpe.metrics("numPartitions").value === 3)
    assert(gpe.metrics("numPrunedPartitions").value === 2)
    assert(gpe.metrics("numEmptyPartitions").value === 2)
    assert(gpe.metrics("numCoalescedPartitions").value === 0)
    assert(gpe.metrics("maxPartitionsPerGroup").value === 1)
    assert(gpe.metrics("numReplicatedPartitions").value === 0,
      "empty groups replicate nothing, and the single-split key 1 has no copy")
  }

  test("SPARK-59310: replicate alignment counts the reads beyond the first") {
    // The other join side expects 2 partitions for key 1, so this side's splits for the key are
    // replicated to both: the slot beyond the first re-reads both splits, 2 extra input
    // partition reads. Each output partition also coalesces the 2 splits of the key.
    def keyOf(a: Int): InternalRowComparableWrapper =
      InternalRowComparableWrapper(row(a), Seq(exprA))
    val child = ExecutableKeyedLeaf(KeyedPartitioning(Seq(exprA), Seq(row(1), row(1))))
    val gpe = GroupPartitionsExec(child, expectedPartitionKeys = Some(Seq(keyOf(1) -> 2)))
    gpe.execute()

    assert(gpe.metrics("numInputPartitions").value === 2)
    assert(gpe.metrics("numPartitions").value === 2)
    assert(gpe.metrics("numReplicatedPartitions").value === 2)
    assert(gpe.metrics("numCoalescedPartitions").value === 2, "both copies merge the 2 splits")
    assert(gpe.metrics("maxPartitionsPerGroup").value === 2)
    assert(gpe.metrics("numEmptyPartitions").value === 0)
    assert(gpe.metrics("numPrunedPartitions").value === 0)
  }

  test("SPARK-59310: an inner join intersection prunes both sides") {
    withSQLConf(
        SQLConf.V2_BUCKETING_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable(s"testcat.ns.$items", s"testcat.ns.$purchases") {
        createTable(items, itemsColumns, Array(identity("id")))
        sql(s"INSERT INTO testcat.ns.$items VALUES " +
            s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
            s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
            s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
            s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
            s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
        createTable(purchases, purchasesColumns, Array(identity("item_id")))
        sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
            s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
            s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
            s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
            s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
            s"(4, 19.5, cast('2020-02-01' as timestamp))")

        // The sides hold different keys ({1,2,3} vs {1,2,4}), so grouping alone cannot align
        // them and the join pushes the inner intersection {1,2} down as the expected keys.
        // Each side then coalesces its duplicate-key splits (items merges two groups of two,
        // purchases one group of three) and prunes the one split of its unmatched key
        // (items key 3, purchases key 4); nothing is empty or replicated.
        // Both AQE arms: the query has no shuffle, so the executed nodes and their accumulator
        // ids are the same with and without AQE, and the reporting chain must work in both.
        Seq(false, true).foreach { aqeEnabled =>
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString) {
            val df = sql(s"SELECT i.id FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id")
            val previousExecutionIds = currentExecutionIds()
            checkAnswer(df, Seq.fill(6)(Row(1L)) ++ Seq.fill(2)(Row(2L)))
            val executionIds = currentExecutionIds().diff(previousExecutionIds)
            assert(executionIds.size === 1)
            val executionId = executionIds.head

            // The metrics must survive the full reporting path: set on the driver during
            // doExecute, posted to the listener bus, and readable back from the status store
            // the SQL UI renders.
            val metricValues = statusStore.executionMetrics(executionId)
            val groupNodes =
              statusStore.planGraph(executionId).nodes.filter(_.name == "GroupPartitions")
            assert(groupNodes.size === 2, "one GroupPartitionsExec per join side")
            groupNodes.foreach { node =>
              assert(metricValue(metricValues, node, "number of input partitions") === "5")
              assert(metricValue(metricValues, node, "number of partitions") === "2")
              assert(metricValue(metricValues, node, "number of empty partitions") === "0")
              assert(metricValue(metricValues, node, "number of pruned input partitions") === "1")
              assert(metricValue(metricValues, node,
                "number of replicated input partition reads") === "0",
                "no expected key carries multiple splits, so nothing is replicated")
            }
            assert(groupNodes.map(metricValue(metricValues, _, "number of coalesced partitions"))
              .sorted === Seq("1", "2"))
            assert(groupNodes.map(metricValue(metricValues, _, "max partitions per group"))
              .sorted === Seq("2", "3"))
          }
        }
      }
    }
  }

  test("SPARK-59310: a disjoint inner join prunes both sides to empty end to end") {
    withSQLConf(
        SQLConf.V2_BUCKETING_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(s"testcat.ns.$items", s"testcat.ns.$purchases") {
        createTable(items, itemsColumns, Array(identity("id")))
        sql(s"INSERT INTO testcat.ns.$items VALUES " +
            s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
            s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp))")
        createTable(purchases, purchasesColumns, Array(identity("item_id")))
        sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
            s"(3, 42.0, cast('2020-01-01' as timestamp)), " +
            s"(4, 44.0, cast('2020-01-15' as timestamp))")

        // The key sets {1, 2} and {3, 4} are disjoint, so the inner join's intersection is
        // empty: the alignment emits no output partition on either side, each side prunes both
        // of its inputs, and doExecute takes the empty-RDD branch. The metrics are sent before
        // that branch, so the total pruning still reaches the store.
        val df = sql(s"SELECT i.id FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
          "ON i.id = p.item_id")
        val previousExecutionIds = currentExecutionIds()
        checkAnswer(df, Nil)
        val executionIds = currentExecutionIds().diff(previousExecutionIds)
        assert(executionIds.size === 1)
        val executionId = executionIds.head

        val metricValues = statusStore.executionMetrics(executionId)
        val groupNodes =
          statusStore.planGraph(executionId).nodes.filter(_.name == "GroupPartitions")
        assert(groupNodes.size === 2, "one GroupPartitionsExec per join side")
        groupNodes.foreach { node =>
          assert(metricValue(metricValues, node, "number of input partitions") === "2")
          assert(metricValue(metricValues, node, "number of partitions") === "0")
          assert(metricValue(metricValues, node, "number of pruned input partitions") === "2")
          assert(metricValue(metricValues, node, "number of empty partitions") === "0")
          assert(metricValue(metricValues, node, "number of coalesced partitions") === "0")
          assert(metricValue(metricValues, node, "max partitions per group") === "0")
          assert(metricValue(metricValues, node,
            "number of replicated input partition reads") === "0")
        }
      }
    }
  }

  test("SPARK-59310: partial clustering replicates the smaller side") {
    withSQLConf(
        SQLConf.V2_BUCKETING_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(s"testcat.ns.$items", s"testcat.ns.$purchases") {
        createTable(items, itemsColumns, Array(identity("id")))
        sql(s"INSERT INTO testcat.ns.$items VALUES " +
            s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
            s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
            s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
            s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
        createTable(purchases, purchasesColumns, Array(identity("item_id")))
        sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
            s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
            s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
            s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
            s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
            s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
            s"(2, 22.0, cast('2020-01-03' as timestamp)), " +
            s"(3, 20.0, cast('2020-02-01' as timestamp))")

        // Partial clustering picks the side with fewer splits to replicate: items (4 splits,
        // key 1 x2, key 2 x1, key 3 x1) groups per key and copies each group into every
        // expected slot, while purchases (7 splits) keeps them, one per slot. The slots come
        // from purchases: key 1 x3, key 2 x3, key 3 x1. Items' extra reads: (3-1) x 2 splits
        // for key 1, (3-1) x 1 for key 2, none for key 3's single slot.
        val df = sql(s"SELECT i.id FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
          "ON i.id = p.item_id")
        val previousExecutionIds = currentExecutionIds()
        checkAnswer(df, Seq.fill(6)(Row(1L)) ++ Seq.fill(3)(Row(2L)) ++ Seq(Row(3L)))
        val executionIds = currentExecutionIds().diff(previousExecutionIds)
        assert(executionIds.size === 1)
        val executionId = executionIds.head

        val metricValues = statusStore.executionMetrics(executionId)
        val groupNodes =
          statusStore.planGraph(executionId).nodes.filter(_.name == "GroupPartitions")
        assert(groupNodes.size === 2, "one GroupPartitionsExec per join side")
        val byInputPartitions = groupNodes.map { node =>
          metricValue(metricValues, node, "number of input partitions") -> node
        }.toMap
        assert(byInputPartitions.keySet === Set("4", "7"))
        val replicatedSide = byInputPartitions("4")
        val distributeSide = byInputPartitions("7")
        Seq(replicatedSide, distributeSide).foreach { node =>
          assert(metricValue(metricValues, node, "number of partitions") === "7")
          assert(metricValue(metricValues, node, "number of empty partitions") === "0")
          assert(metricValue(metricValues, node, "number of pruned input partitions") === "0")
        }
        assert(metricValue(metricValues, replicatedSide,
          "number of replicated input partition reads") === "6")
        assert(!distributeSide.metrics.exists(
          _.name == "number of replicated input partition reads"),
          "the distribute side holds one split per slot and registers no replicated metric")
        assert(metricValue(metricValues, replicatedSide, "number of coalesced partitions") === "3",
          "the three copies of key 1's two-split group each merge their splits")
        assert(metricValue(metricValues, replicatedSide, "max partitions per group") === "2")
        assert(metricValue(metricValues, distributeSide, "number of coalesced partitions") === "0")
        assert(metricValue(metricValues, distributeSide, "max partitions per group") === "1")
      }
    }
  }

  private case class ExecutableKeyedLeaf(kp: KeyedPartitioning)
    extends LeafExecNode with SafeForKWayMerge {
    override def outputPartitioning: Partitioning = kp
    override def output: Seq[Attribute] = Seq(AttributeReference("a", IntegerType)())
    override protected def doExecute(): RDD[InternalRow] = {
      val n = kp.numPartitions
      sparkContext.parallelize(0 until n, n).map(i => InternalRow(i))
    }
  }
}

private case class DummyLeafSparkPlan(
    override val outputOrdering: Seq[SortOrder] = Nil,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0)
  ) extends LeafExecNode with SafeForKWayMerge {
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException
  override def output: Seq[Attribute] = Seq.empty
}
