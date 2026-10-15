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
package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, LessThan, Literal, SortOrder, TransformExpression}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, KeyedPartitioning, PartitioningCollection, REPLICATED_FOR_JOIN, SPLIT_FOR_JOIN, UngroupingOrigin}
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.KeyGroupedPartitioningSuiteBase
import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.connector.catalog.functions.BucketFunction
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{ShuffledJoin, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class OptimizeSkewedKeyGroupsSuite extends KeyGroupedPartitioningSuiteBase {

  test("SPARK-59436: a stacked grouping is not wrapped with a skewed grouping") {
    withSkewGroupSplit {
      val raw = DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprA),
        Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2))))
      val inner = GroupPartitionsExec(raw)
      val outer = GroupPartitionsExec(inner)
      val right = DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprB),
        Seq(InternalRow(1), InternalRow(2))))
      val join = SortMergeJoinExec(Seq(exprA), Seq(exprB), Inner, None,
        SortExec(Seq(SortOrder(exprA, Ascending)), global = false, outer),
        SortExec(Seq(SortOrder(exprB, Ascending)), global = false, right))
      assert(OptimizeSkewedKeyGroups().apply(join) == join)
      assert(!join.exists {
        case g: GroupPartitionsExec => g.expectedKeySpecs.exists(_.exists(_.skewed))
        case _ => false
      })
    }
  }

  test("SPARK-59436: skew optimization preserves the required final distribution") {
    withSkewGroupSplit {
      val join = SortMergeJoinExec(Seq(exprA), Seq(exprB), Inner, None,
        DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprA),
          Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)))),
        DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprB),
          Seq(InternalRow(1), InternalRow(2)))))
      val required = ClusteredDistribution(Seq(exprA))
      val original = EnsureRequirements().apply(join)
      assert(original.outputPartitioning.satisfies(required))
      val split = OptimizeSkewedKeyGroups().apply(original)
      assert(split.exists { case g: GroupPartitionsExec => g.isSkew; case _ => false })
      assert(ValidateRequirements.validate(split))
      assert(!split.outputPartitioning.satisfies(required))
      val retained = OptimizeSkewedKeyGroups(Some(required)).apply(original)
      assert(retained eq original)
      assert(ValidateRequirements.validate(retained, required))
    }
  }

  test("SPARK-59436: stage rollback preserves its original wrappers and exchange inputs") {
    withSkewGroupSplit {
      def join(): ShuffledJoin = SortMergeJoinExec(Seq(exprA), Seq(exprB), Inner, None,
        DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprA),
          Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)))),
        DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprB),
          Seq(InternalRow(1), InternalRow(2)))))
      val aligned = EnsureRequirements().apply(join())
      val consumer = DummySparkPlan(children = Seq(FilterExec(Literal(true), aligned)),
        requiredChildDistribution = Seq(ClusteredDistribution(Seq(exprA))),
        requiredChildOrdering = Seq(Nil))
      assert(OptimizeSkewedKeyGroups().apply(consumer) eq consumer,
        "a rejected candidate must return the original stage, including its wrappers")
      val exchange = ShuffleExchangeExec(
        ClusteredDistribution(Seq(exprA)).createPartitioning(5), aligned)
      val outer = DummySparkPlan(children = Seq(exchange, consumer),
        requiredChildDistribution = Seq(ClusteredDistribution(Seq(exprA)),
          org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution),
        requiredChildOrdering = Seq(Nil, Nil))
      val result = OptimizeSkewedKeyGroups().apply(outer)
      assert(result.children(1) eq consumer, "rollback must retain the original local branch")
      assert(result.children.head.exists {
        case g: GroupPartitionsExec => g.expectedKeySpecs.exists(_.exists(_.skewed))
        case _ => false
      }, "the independent exchange input must retain its skew optimization")
      assert(ValidateRequirements.validate(result))
      assert(OptimizeSkewedKeyGroups().apply(result) == result)
    }
  }

  test("SPARK-59436: disabled bucketing leaves the aligned plan unchanged") {
    withSkewGroupSplit {
      val plan = EnsureRequirements().apply(SortMergeJoinExec(
        Seq(exprA), Seq(exprB), Inner, None,
        DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprA),
          Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)))),
        DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprB),
          Seq(InternalRow(1), InternalRow(2))))))
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        assert(OptimizeSkewedKeyGroups().apply(plan) eq plan)
      }
      withSQLConf(SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "false") {
        assert(OptimizeSkewedKeyGroups().apply(plan) eq plan)
      }
    }
  }

  test("SPARK-59436: the split is the same on every pass") {
    // AQE re-plans the pair on every stage, so `EnsureRequirements` and this rule both run again
    // over what they left: a pass has to be a fixed point of the two, or the plan changes under
    // the rules that read it.
    def stable(plan: SparkPlan, what: String): Unit = {
      val first = OptimizeSkewedKeyGroups().apply(EnsureRequirements().apply(plan))
      val direct = OptimizeSkewedKeyGroups().apply(first)
      assert(first == direct, s"$what: applying the skew rule twice changed the plan")
      assert(ValidateRequirements.validate(first), s"$what: the result must satisfy requirements")
      val second = OptimizeSkewedKeyGroups().apply(EnsureRequirements().apply(first))
      assert(first == second,
        s"$what: a second pass changed the plan:\n--- first ---\n${first.treeString}" +
          s"\n--- second ---\n${second.treeString}")
    }
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val left = DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(a),
      (1 to 10).map(_ => InternalRow(1)) ++ Seq(InternalRow(2))))
    val right = DummySparkPlan(outputPartitioning =
      KeyedPartitioning(Seq(b), Seq(InternalRow(1), InternalRow(2))))
    val join = SortMergeJoinExec(Seq(a), Seq(b), Inner, None, left, right)

    withSQLConf(
        SQLConf.V2_BUCKETING_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
      stable(join, "the aligned pair")
      val parent = DummySparkPlan(children = Seq(join),
        requiredChildDistribution = Seq(ClusteredDistribution(Seq(a))),
        requiredChildOrdering = Seq(Nil))
      stable(parent, "a clustered consumer rolls the split back")
      withSQLConf(SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        stable(join, "the pair a partial clustering also spread")
      }
      val third = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(c), Seq(InternalRow(1), InternalRow(2))))
      stable(SortMergeJoinExec(Seq(a), Seq(c), Inner, None, join, third),
        "a second join over the first")
    }
  }


  private val exprA = AttributeReference("a", IntegerType)()
  private val exprB = AttributeReference("b", IntegerType)()

  private def planSkewJoin(plan: SparkPlan): SparkPlan = {
    val optimized = OptimizeSkewedKeyGroups().apply(EnsureRequirements().apply(plan))
    optimized.foreach {
      case join: ShuffledJoin => join.children.foreach { side =>
        def groupings(p: SparkPlan): Seq[GroupPartitionsExec] = p match {
          case g: GroupPartitionsExec => g +: groupings(g.child)
          case s: SortExec if !s.global => groupings(s.child)
          case _ => Nil
        }
        val nodes = groupings(side)
        if (nodes.exists(_.expectedKeySpecs.exists(_.exists(_.skewed)))) {
          assert(nodes.size == 1, s"a skewed join side must have exactly one GPE:\n$side")
        }
      }
      case _ =>
    }
    optimized
  }

  /** The roles the aligned side over `child` reports, one per partition. */
  private def originsOf(child: SparkPlan): Option[Seq[UngroupingOrigin]] =
    PartitioningCollection.representativeOf(child.outputPartitioning)
      .getOrElse(fail(s"expected a keyed layout in\n${child.treeString}"))
      .ungroupingOrigins

  private def splitsAndSpecs(child: SparkPlan): Seq[(Int, Boolean, Boolean)] = {
    val gpe = child.collectFirst { case g: GroupPartitionsExec => g }
      .getOrElse(fail(s"expected a GroupPartitionsExec in\n${child.treeString}"))
    gpe.expectedKeySpecs
      .getOrElse(fail(s"expected expectedPartitionKeys in\n${child.treeString}"))
      .map(e => (e.numSplits, e.distribute, e.skewed))
  }

  private def withSkewGroupSplit(f: => Unit): Unit = withSQLConf(
    SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
    SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
    // Each side uses its own median; the absolute threshold keeps small groups coalesced.
    SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
    SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0")(f)

  /**
   * Consistency checks for an aligned pair: neither side shuffles, each carries exactly one
   * `GroupPartitionsExec`, the two sides emit the same key sequence so the index pairing holds,
   * and each side reads every one of its input partitions exactly as often as its expected keys
   * reserve: once for a distributing or coalesced key, `numSplits` times for a replicated one.
   */
  private def checkAlignedPair(newLeft: SparkPlan, newRight: SparkPlan): Unit = {
    val gpes = Seq(newLeft, newRight).map { side =>
      assert(side.collect { case s: ShuffleExchangeExec => s }.isEmpty,
        s"the aligned side must not shuffle:\n${side.treeString}")
      val groupings = side.collect { case g: GroupPartitionsExec => g }
      assert(groupings.size === 1,
        s"expected exactly one GroupPartitionsExec:\n${side.treeString}")
      groupings.head
    }
    assert(gpes(0).groupedPartitions.map(_._1) === gpes(1).groupedPartitions.map(_._1),
      s"both sides must emit the same key sequence:\n${newLeft.treeString}\n${newRight.treeString}")
    gpes.foreach { gpe =>
      val childKeys = gpe.child.outputPartitioning.asInstanceOf[KeyedPartitioning].partitionKeys
      val reads = gpe.groupedPartitions.flatMap(_._2)
      // With reducers the expected keys live in the reduced key space while the child reports
      // raw keys, so the per-key own counts are not derivable here; the coverage and
      // within-slot uniqueness checks below still apply.
      if (!gpe.reducers.exists(_.exists(_.isDefined))) {
        val countsByKey = childKeys.groupBy(Predef.identity).view.mapValues(_.size).toMap
        // The stored specs are keyed by position: the alignment repeats each expected key across
        // its reserved slots, so the i-th run of distinct keys in the emitted sequence pairs with
        // the i-th specification.
        val distinctRuns = gpe.groupedPartitions.map(_._1).foldLeft(
          Vector.empty[InternalRowComparableWrapper]) { (runs, key) =>
          if (runs.lastOption.contains(key)) runs else runs :+ key
        }
        assert(distinctRuns.length === gpe.expectedKeySpecs.get.length,
          s"one spec per distinct run of emitted keys:\n${gpe.treeString}")
        val expectedReads = distinctRuns.zip(gpe.expectedKeySpecs.get).map { case (key, spec) =>
          val own = countsByKey.getOrElse(key, 0)
          if (spec.distribute) own else spec.numSplits * own
        }.sum
        assert(reads.size === expectedReads,
          s"input partitions must be read exactly as often as reserved:\n${gpe.treeString}")
      }
      assert(reads.distinct.sorted === (0 until childKeys.size).toList,
        s"every input partition must be read:\n${gpe.treeString}")
      gpe.groupedPartitions.foreach { case (_, group) =>
        assert(group.distinct.size === group.size,
          s"no partition may be read twice within one slot:\n${gpe.treeString}")
      }
    }
  }

  private def skewJoin(
      joinType: JoinType,
      leftKeys: Seq[InternalRow],
      rightKeys: Seq[InternalRow]): Seq[SparkPlan] =
    planSkewJoin(SortMergeJoinExec(Seq(exprA), Seq(exprB), joinType, None,
      DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprA), leftKeys)),
      DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprB), rightKeys)))).children

  test("SPARK-59436: compatible layouts split without pushdown or subset-key permission") {
    val columns = Array(Column.create("id", LongType))
    createTable("compatible_l", columns, Array(identity("id")))
    createTable("compatible_r", columns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.compatible_l VALUES " +
      (Seq.fill(10)("(1)") :+ "(2)").mkString(", "))
    sql("INSERT INTO testcat.ns.compatible_r VALUES (1), (2)")
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
      for (enabled <- Seq(false, true)) {
        withSQLConf(SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> enabled.toString) {
          val df = sql("SELECT /*+ MERGE(l), MERGE(r) */ l.id " +
            "FROM testcat.ns.compatible_l l JOIN testcat.ns.compatible_r r ON l.id = r.id")
          checkAnswer(df, Seq.fill(10)(Row(1L)) :+ Row(2L))
          val plan = df.queryExecution.executedPlan
          assert(collectAllShuffles(plan).isEmpty, plan.treeString)
          assert(collectAllGroupPartitions(plan).exists(_.isSkew) == enabled, plan.treeString)
          assert(ValidateRequirements.validate(plan))
        }
      }
    }
  }

  test("SPARK-59436: each side detects skew against its own median") {
    withSQLConf(
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "5.0") {
      val sparse = Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2), InternalRow(3))
      val uniform = (1 to 3).flatMap(key => Seq.fill(20)(InternalRow(key)))
      for ((left, right) <- Seq((sparse, uniform), (uniform, sparse))) {
        val sides = skewJoin(Inner, left, right)
        val distributed = if (left == sparse) sides.head else sides(1)
        val replicated = if (left == sparse) sides(1) else sides.head
        assert(splitsAndSpecs(distributed).head === (10, true, true))
        assert(splitsAndSpecs(replicated).head === (10, false, true))
        checkAlignedPair(sides.head, sides(1))
      }
      val even = skewJoin(Inner, uniform, uniform)
      assert(even.flatMap(collectAllGroupPartitions)
        .forall(_.expectedKeySpecs.forall(_.forall(!_.skewed))),
        "large but uniform groups must not be classified as skewed")
    }
  }

  test("SPARK-59436: skewed group split spreads the side holding more splits") {
    // Key 1 holds 10 of the left side's 11 splits against 1 on the right, and its group count
    // (10) exceeds its own-side median; key 2 is even. Only key 1 splits, over the left side
    // holding more of its splits, and the right replicates its group to each split.
    withSkewGroupSplit {
      val Seq(newLeft, newRight) = skewJoin(Inner,
        leftKeys = Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)),
        rightKeys = Seq(InternalRow(1), InternalRow(2)))
      assert(splitsAndSpecs(newLeft) === Seq((10, true, true), (1, true, false)))
      assert(splitsAndSpecs(newRight) === Seq((10, false, true), (1, false, false)))
      checkAlignedPair(newLeft, newRight)
    }
  }

  test("SPARK-59436: advisoryInputPartitionsPerOutputPartition chunks the spread of a skewed key") {
    // The left side's ten splits, more than the right's one, spread over ceil(10 / 3) = 4
    // output partitions instead of ten, and the right replicates its group 4 times instead
    // of 10.
    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0",
        SQLConf.V2_BUCKETING_ADVISORY_INPUT_PARTITIONS_PER_OUTPUT_PARTITION.key -> "3") {
      val Seq(newLeft, newRight) = skewJoin(Inner,
        leftKeys = Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)),
        rightKeys = Seq(InternalRow(1), InternalRow(2)))
      assert(splitsAndSpecs(newLeft) === Seq((4, true, true), (1, true, false)))
      assert(splitsAndSpecs(newRight) === Seq((4, false, true), (1, false, false)))
      checkAlignedPair(newLeft, newRight)
    }
  }

  test("SPARK-59436: an advisory at or above the split count leaves the skewed key coalesced") {
    // spread(n) = ceil(n / advisory) = 1: a spread landing on a single partition is the
    // unsplit layout, so the key is not marked skewed at all. An advisory this large is the
    // user declaring one task over all n splits acceptable.
    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0",
        SQLConf.V2_BUCKETING_ADVISORY_INPUT_PARTITIONS_PER_OUTPUT_PARTITION.key ->
          "100") {
      val Seq(newLeft, newRight) = skewJoin(Inner,
        leftKeys = Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)),
        rightKeys = Seq(InternalRow(1), InternalRow(2)))
      // The advisory of 100 spreads ten splits over one partition, the unsplit layout, so the key
      // is left unmarked and the pair falls to the plain shapes: `groupIfNeeded`'s bare wrap
      // coalesces the left's ten splits of key 1, the grouped right satisfies as is.
      val leftGpe = newLeft.collectFirst { case g: GroupPartitionsExec => g }
        .getOrElse(fail(s"expected the coalescing wrap in\n${newLeft.treeString}"))
      assert(leftGpe.expectedKeySpecs.isEmpty, "nothing is reserved, nothing is pushed")
      assert(leftGpe.outputPartitioning.numPartitions === 2,
        "key 1's ten splits coalesce into one partition")
      assert(newRight.collectFirst { case g: GroupPartitionsExec => g }.isEmpty,
        s"the grouped right side satisfies as is:\n${newRight.treeString}")
    }
  }

  test("SPARK-59436: skewed group split respects the join type's replication gate") {
    // A left outer join may not replicate the left side, so the left is the only side that
    // could distribute, and its single split of the skewed key is below the threshold,
    // so the per-side trigger of `OptimizeSkewedJoin` stands the split down: spreading the
    // small side would re-read the large one per slot while every task still reads it whole.
    withSkewGroupSplit {
      val Seq(newLeft, newRight) = skewJoin(LeftOuter,
        leftKeys = Seq(InternalRow(1), InternalRow(2)),
        rightKeys = Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)))
      // The stand-down leaves the pair to the plain shapes: the grouped left satisfies as is,
      // and `groupIfNeeded`'s bare wrap coalesces the right's ten splits of key 1.
      assert(newLeft.collectFirst { case g: GroupPartitionsExec => g }.isEmpty,
        s"the grouped left side satisfies as is:\n${newLeft.treeString}")
      val rightGpe = newRight.collectFirst { case g: GroupPartitionsExec => g }
        .getOrElse(fail(s"expected the coalescing wrap in\n${newRight.treeString}"))
      assert(rightGpe.expectedKeySpecs.isEmpty, "the stand-down reserves nothing")
      assert(rightGpe.outputPartitioning.numPartitions === 2,
        "key 1's ten splits coalesce into one partition")
      Seq(newLeft, newRight).foreach { side =>
        assert(side.collect { case s: ShuffleExchangeExec => s }.isEmpty,
          s"the pair aligns without a shuffle:\n${side.treeString}")
      }
    }
    // A full outer join may replicate neither side: nothing is reserved for the skewed key,
    // and the pair goes back in the plain shapes: the non-grouped side under a
    // `groupIfNeeded` coalescing wrap, the grouped side bare.
    withSkewGroupSplit {
      val Seq(newLeft, newRight) = skewJoin(FullOuter,
        leftKeys = Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)),
        rightKeys = Seq(InternalRow(1), InternalRow(2)))
      val leftGpe = newLeft.collectFirst { case g: GroupPartitionsExec => g }
        .getOrElse(fail(s"expected the coalescing wrap in\n${newLeft.treeString}"))
      assert(leftGpe.expectedKeySpecs.isEmpty, "nothing is reserved, nothing is pushed")
      assert(leftGpe.outputPartitioning.numPartitions === 2,
        "key 1's ten splits coalesce into one partition")
      assert(newRight.collectFirst { case g: GroupPartitionsExec => g }.isEmpty,
        s"the grouped right side satisfies as is:\n${newRight.treeString}")
      Seq(newLeft, newRight).foreach { side =>
        assert(side.collect { case s: ShuffleExchangeExec => s }.isEmpty,
          s"the pair aligns without a shuffle:\n${side.treeString}")
      }
    }
  }

  test("SPARK-59436: a skew split pairs per key, so a pair spreading opposite ways validates") {
    // The validator admits a deliberately ungrouped layout only through the pairing its
    // producer's stamp vouches for, and the stamp is held per partition: here the left holds key
    // 1's splits and the right key 2's, so each side spreads one key and repeats the other. A
    // single role per node cannot describe that, and a pair read as one role per side would be
    // refused as two spread sides; per key the roles are one of each, which is the soundness the
    // pairing asks for.
    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "0.5") {
      val plan = planSkewJoin(SortMergeJoinExec(Seq(exprA), Seq(exprB), Inner, None,
        DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprA),
          Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)) ++ Seq(InternalRow(3)))),
        DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprB),
          Seq(InternalRow(1)) ++ Seq.fill(10)(InternalRow(2)) ++ Seq(InternalRow(3))))))
      val join = plan.collectFirst { case j: SortMergeJoinExec => j }
        .getOrElse(fail(s"expected the join back:\n${plan.treeString}"))
      val Seq(newLeft, newRight) = join.children

      // Key 1 spreads on the left and key 2 on the right, so neither node routes one way, and
      // neither takes one role. Key 3's single slot is not skewed: it follows the side-wide
      // alignment, which for a pair the skew split alone built names the left the spread side.
      assert(splitsAndSpecs(newLeft) ===
        Seq((10, true, true), (10, false, true), (1, true, false)))
      assert(splitsAndSpecs(newRight) ===
        Seq((10, false, true), (10, true, true), (1, false, false)))
      checkAlignedPair(newLeft, newRight)
      assert(originsOf(newLeft) === Some(
        Seq.fill(10)(SPLIT_FOR_JOIN) ++ Seq.fill(10)(REPLICATED_FOR_JOIN) ++ Seq(SPLIT_FOR_JOIN)),
        "the left spreads key 1, repeats key 2, and takes the side-wide role for key 3")
      assert(originsOf(newRight) === Some(
        Seq.fill(10)(REPLICATED_FOR_JOIN) ++ Seq.fill(10)(SPLIT_FOR_JOIN) ++
          Seq(REPLICATED_FOR_JOIN)))
      assert(ValidateRequirements.validate(plan),
        s"a split pair that agrees on its keys is accepted:\n${plan.treeString}")
    }
  }

  test("SPARK-59436: skewed group split below the threshold groups every key") {
    // The same shape as the first split test with the feature off: the plain grouping wrap
    // `groupIfNeeded` adds coalesces key 1's ten splits into a single partition, the one
    // skewed join task the split exists to avoid, and nothing is pushed to align the pair.
    withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true") {
      val Seq(newLeft, newRight) = skewJoin(Inner,
        leftKeys = Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)),
        rightKeys = Seq(InternalRow(1), InternalRow(2)))
      val leftGpe = newLeft.collectFirst { case g: GroupPartitionsExec => g }
        .getOrElse(fail(s"expected a grouping wrap in\n${newLeft.treeString}"))
      assert(leftGpe.expectedKeySpecs.isEmpty, "no alignment is pushed without the split")
      assert(leftGpe.outputPartitioning.numPartitions === 2,
        "key 1's ten splits coalesce into one partition")
      assert(newLeft.collect { case g: GroupPartitionsExec => g }.size === 1,
        s"exactly one grouping on the left:\n${newLeft.treeString}")
      assert(newRight.collectFirst { case g: GroupPartitionsExec => g }.isEmpty,
        s"the grouped right side satisfies as is:\n${newRight.treeString}")
      Seq(newLeft, newRight).foreach { side =>
        assert(side.collect { case s: ShuffleExchangeExec => s }.isEmpty,
          s"the pair aligns without a shuffle:\n${side.treeString}")
      }
      // The coalescing wrap still reads every input partition exactly once.
      assert(leftGpe.groupedPartitions.flatMap(_._2).sorted === (0 until 11).toList,
        s"every left input partition is read once:\n${leftGpe.treeString}")
    }
  }

  test("SPARK-59436: skewed group split overrides the partially clustered side choice per key") {
    // With both features on, a non-skewed key keeps the partially clustered layout and the
    // skewed ones take the per-key split, including a flip of the side choice: the partially
    // clustered fallback replicates the side with fewer partitions (the right, 7 against 11)
    // and sizes every key from the left's own splits, so key 1, with one left split against
    // five right ones, would coalesce the right's five into a single partition. The per-key
    // choice makes the right, holding more of that key's splits, distribute them instead.
    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        // Each side uses its own median; the absolute threshold keeps small groups coalesced.
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "0.5") {
      val Seq(newLeft, newRight) = skewJoin(Inner,
        leftKeys = Seq(InternalRow(1)) ++ Seq.fill(9)(InternalRow(2)) ++ Seq(InternalRow(3)),
        rightKeys = Seq.fill(5)(InternalRow(1)) ++ Seq(InternalRow(2), InternalRow(3)))
      // Key 1 flips to the per-key choice: the right, holding 5 of its splits against the
      // left's 1, distributes them and the left replicates. Key 2 splits over the left, which
      // holds more of its splits, as the partial clustering already picked. Key 3 is below the
      // threshold and keeps the partially clustered layout: the left is the whole-side
      // distribute side.
      assert(splitsAndSpecs(newLeft) ===
        Seq((5, false, true), (9, true, true), (1, true, false)))
      assert(splitsAndSpecs(newRight) ===
        Seq((5, true, true), (9, false, true), (1, false, false)))
      checkAlignedPair(newLeft, newRight)
    }
  }

  test("SPARK-59436: skewed group split leaves an already compatible pair untouched") {
    // A pair with one input partition per key needs neither alignment groupings nor skew splits.
    withSkewGroupSplit {
      val keys = Seq(InternalRow(3), InternalRow(0), InternalRow(2), InternalRow(1))
      val Seq(newLeft, newRight) = skewJoin(Inner, leftKeys = keys, rightKeys = keys)
      Seq(newLeft, newRight).foreach { side =>
        assert(side.collect { case g: GroupPartitionsExec => g }.isEmpty,
          s"nothing to reserve, nothing inserted:\n${side.treeString}")
        assert(side.collect { case s: ShuffleExchangeExec => s }.isEmpty,
          s"the compatible pair must not shuffle:\n${side.treeString}")
      }
    }
  }

  test("SPARK-59436: skewed group split stands down over a marked child") {
    // A marked layout holds rows of undeclared keys that only its own routing keeps co-located,
    // so any grouping other than an identity one makes `GroupPartitionsExec` give up the keyed
    // partitioning; the plan above the join would then hash-shuffle a pair that aligned without
    // one. The marked side holds one partition per declared key, so it can only ever be the
    // replicating side of a split: the split stands down and the pair keeps the keys it has.
    withSkewGroupSplit {
      val markedLeft = DummySparkPlan(outputPartitioning =
        KeyedPartitioning(Seq(exprA), Seq(InternalRow(1), InternalRow(2)))
          .withLayout(_.copy(mayContainUnknownPartitionKeys = true)))
      val Seq(newLeft, newRight) = planSkewJoin(
        SortMergeJoinExec(Seq(exprA), Seq(exprB), Inner, None,
          markedLeft,
          DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprB),
            Seq.fill(5)(InternalRow(2)) ++ Seq(InternalRow(1)))))).children

      assert(newLeft.collectFirst { case g: GroupPartitionsExec => g }.isEmpty,
        s"the marked side must not be regrouped:\n${newLeft.treeString}")
      val rightGpe = newRight.collectFirst { case g: GroupPartitionsExec => g }
        .getOrElse(fail(s"expected the coalescing wrap in\n${newRight.treeString}"))
      assert(rightGpe.expectedKeySpecs.isEmpty,
        "the stand-down reserves nothing for the skewed key")
      Seq(newLeft, newRight).foreach { side =>
        assert(side.collect { case s: ShuffleExchangeExec => s }.isEmpty,
          s"the pair aligns without a shuffle:\n${side.treeString}")
      }
    }
  }

  test("SPARK-59436: a parent grouping over a split join stands the split down") {
    // The aggregate of a join -> agg shape clusters on the join's own keys, and a wrapper
    // between the two hands the join's layout through unchanged (the partial aggregate of a
    // partial-final pair, a projection). The grouping such a parent would add coalesces every
    // spread partition back into one consumer task, keeping the replication reads while losing
    // the parallelism they pay for. The rule rejects the candidate stage and retains the
    // original grouped alignment and its consumer requirements.
    withSkewGroupSplit {
      val aligned = skewJoin(Inner,
        leftKeys = Seq.fill(10)(InternalRow(1)) ++ Seq(InternalRow(2)),
        rightKeys = Seq(InternalRow(1), InternalRow(2)))
      val distribution = ClusteredDistribution(Seq(exprA))
      def parentOf(child: SparkPlan): DummySparkPlan = DummySparkPlan(
        children = Seq(child),
        requiredChildDistribution = Seq(distribution),
        requiredChildOrdering = Seq(Nil))
      val join = SortMergeJoinExec(Seq(exprA), Seq(exprB), Inner, None, aligned.head, aligned(1))
      // The real plan has the aggregate split into a partial and a final one, and the final
      // one's grouping lands on top of the partial, which hands the join's layout through. A
      // filter is the same shape: a unary node that reports its child's partitioning unchanged.
      Seq(
        join -> "the join itself",
        FilterExec(LessThan(exprA, Literal(1)), join) ->
          "the join under a unary node that hands its layout through")
        .foreach { case (child, shape) =>
        val newChild = planSkewJoin(parentOf(child)).children.head
        assert(newChild.outputPartitioning.satisfies(distribution),
          s"$shape: the parent's requirement must be met:\n${newChild.treeString}")
        assert(newChild.collect { case s: ShuffleExchangeExec => s }.isEmpty,
          s"$shape: and without a shuffle:\n${newChild.treeString}")
        assert(newChild.collect { case g: GroupPartitionsExec => g }
          .forall(g => !g.exists(_.isInstanceOf[SortMergeJoinExec])),
          s"$shape: no grouping may coalesce the join from above:\n${newChild.treeString}")
        val newJoin = newChild.collectFirst { case j: SortMergeJoinExec => j }
          .getOrElse(fail(s"$shape: expected the join back:\n${newChild.treeString}"))
        val sideSpecs = Seq(newJoin.left, newJoin.right).map { side =>
          side.collect { case g: GroupPartitionsExec => g }
            .flatMap(_.expectedKeySpecs.getOrElse(Nil))
        }
        assert(sideSpecs.flatten.forall(m => m.numSplits == 1 && !m.skewed),
          s"$shape: nothing stays split:\n${newChild.treeString}")
      }
    }
  }


  test("SPARK-59436: skewed group split stands down over reducers, and a re-run keeps them") {
    // A reducer regroups the keys the split counts would key on, so the optimization stands
    // down. The re-run is the arm that matters: both sides then report the same reduced keys, so
    // this pass derives no reducers, and a rewrite trusting only what it derived would clear the
    // node's own reducers and regroup the raw bucket(8) keys against the reduced bucket(4)
    // expected keys, dropping every group the two spaces disagree on (raw buckets 4 to 7).
    //
    // The bucket(8) side repeats bucket 0, so the counts report two input partitions for it over
    // a threshold of 1: deleting the stand-down splits that key, and the coalesced layout
    // asserted below is what the stand-down buys.
    val leftKeys = (0 until 8).map(InternalRow(_)) :+ InternalRow(0)
    val rightKeys = (0 until 4).map(InternalRow(_))
    def smj(children: Seq[SparkPlan]): SortMergeJoinExec =
      SortMergeJoinExec(Seq(exprA), Seq(exprB), Inner, None, children.head, children(1))
    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "1",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "0.5") {
      val firstPass = planSkewJoin(smj(Seq(
        DummySparkPlan(outputPartitioning =
          KeyedPartitioning(
            Seq(TransformExpression(BucketFunction, Seq(exprA), Some(8))), leftKeys)),
        DummySparkPlan(outputPartitioning =
          KeyedPartitioning(
            Seq(TransformExpression(BucketFunction, Seq(exprB), Some(4))), rightKeys))
      ))).children
      val secondPass = planSkewJoin(smj(firstPass)).children

      Seq(firstPass, secondPass).foreach { sides =>
        // The pair check pins the data consistency: all 9 raw left partitions survive the
        // reduced-key grouping. With the reducers cleared, raw buckets 4 to 7 would match no
        // reduced expected key and vanish.
        checkAlignedPair(sides.head, sides(1))
        val gpes = sides.map(_.collectFirst { case g: GroupPartitionsExec => g }.get)
        gpes.foreach { gpe =>
          assert(gpe.expectedKeySpecs.get.forall(e => e.numSplits == 1 && !e.skewed),
            s"the stand-down leaves every key coalesced:\n${gpe.treeString}")
        }
        // The reducing side must keep its reducers; the other side's keys are already the
        // reduced ones, so it carries no reducers to keep.
        assert(gpes.head.reducers.isDefined,
          s"the left reducers must survive:\n${gpes.head.treeString}")
      }
    }
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
  }

  /**
   * `items` partitioned by `(id, name)` holds eight splits under key 1 and one under key 2, plus
   * an unmatched key 3 when `withUnmatched`; `purchases` partitioned by `item_id` holds one split
   * per key. Key 1's group then towers over the median and the split fires on it alone, under
   * the same threshold and factor as the two tests above.
   */
  private def withSkewSplitTables(withUnmatched: Boolean)(body: => Unit): Unit = {
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      (1 to 8).map(n => s"(1, 'n$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))" +
      (if (withUnmatched) s", (3, 'dd', 12.0, cast('2020-01-01' as timestamp))" else ""))

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        // The expected keys counted below are the unfiltered merge's; pin the filter so a
        // change of its default cannot reshape them.
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0")(body)
  }

  test("SPARK-59436: shuffled hash joins share the skew alignment contract") {
    withSkewSplitTables(withUnmatched = false) {
      for (aqe <- Seq(false, true); codegen <- Seq(false, true)) {
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
          val df = sql(s"SELECT /*+ SHUFFLE_HASH(p) */ i.id, i.name, p.price " +
            s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id = p.item_id")
          checkAnswer(df, (1 to 8).map(n => Row(1L, s"n$n", 42.0)) :+ Row(2L, "cc", 11.0))
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          assert(plan.exists(_.isInstanceOf[
            org.apache.spark.sql.execution.joins.ShuffledHashJoinExec]), plan.treeString)
          assert(collectAllGroupPartitions(plan).count(_.isSkew) == 2, plan.treeString)
          assert(collectAllShuffles(plan).isEmpty, plan.treeString)
          assert(ValidateRequirements.validate(plan))
        }
      }
    }
  }

  test("SPARK-59436: a join with unique keyed inputs needs no grouping") {
    createTable("unique_l", Array(Column.create("id", LongType)), Array(identity("id")))
    createTable("unique_r", Array(Column.create("id", LongType)), Array(identity("id")))
    sql("INSERT INTO testcat.ns.unique_l VALUES (1), (2)")
    sql("INSERT INTO testcat.ns.unique_r VALUES (1), (2)")
    for (aqe <- Seq(false, true); enabled <- Seq(false, true)) {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
          SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> enabled.toString) {
        val df = sql("SELECT /*+ MERGE(l), MERGE(r) */ l.id " +
          "FROM testcat.ns.unique_l l JOIN testcat.ns.unique_r r ON l.id = r.id")
        checkAnswer(df, Seq(Row(1L), Row(2L)))
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(collectAllGroupPartitions(plan).isEmpty, plan.treeString)
        assert(collectAllShuffles(plan).isEmpty, plan.treeString)
        assert(ValidateRequirements.validate(plan))
      }
    }
  }

  test("SPARK-59436: an aggregate before the join preserves its grouping") {
    withSkewSplitTables(withUnmatched = false) {
      for (aqe <- Seq(false, true); enabled <- Seq(false, true)) {
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
            SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> enabled.toString) {
          val df = sql(s"SELECT /*+ MERGE(i), MERGE(p) */ i.id, i.n, p.price FROM " +
            s"(SELECT id, count(*) AS n FROM testcat.ns.$items GROUP BY id) i " +
            s"JOIN testcat.ns.$purchases p ON i.id = p.item_id")
          checkAnswer(df, Seq(Row(1L, 8L, 42.0), Row(2L, 1L, 11.0)))
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          val joins = collect(plan) { case j: ShuffledJoin => j }
          assert(joins.size == 1, plan.treeString)
          assert(joins.head.children.exists(_.exists(
            _.isInstanceOf[org.apache.spark.sql.execution.aggregate.BaseAggregateExec])),
            "the aggregate must remain below the join")
          assert(collectAllGroupPartitions(plan).nonEmpty, plan.treeString)
          assert(!collectAllGroupPartitions(plan).exists(_.isSkew), plan.treeString)
          assert(collectAllShuffles(plan).isEmpty, plan.treeString)
          assert(ValidateRequirements.validate(plan))
        }
      }
    }
  }

  test("SPARK-59436: a one-sided keyed shuffle remains an exchange boundary") {
    withSkewSplitTables(withUnmatched = false) {
      for (aqe <- Seq(false, true); enabled <- Seq(false, true)) {
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
            SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> enabled.toString) {
          val df = sql(s"SELECT /*+ MERGE(i), MERGE(r) */ i.id, i.name FROM " +
            s"testcat.ns.$items i JOIN range(1, 3) r ON i.id = r.id")
          checkAnswer(df, (1 to 8).map(n => Row(1L, s"n$n")) :+ Row(2L, "cc"))
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          assert(collectAllShuffles(plan).size == 1, plan.treeString)
          assert(collectAllGroupPartitions(plan).nonEmpty, plan.treeString)
          assert(!collectAllGroupPartitions(plan).exists(_.isSkew), plan.treeString)
          assert(ValidateRequirements.validate(plan))
        }
      }
    }
  }

  test("SPARK-59436: skewed group split spreads a many-splits key and replicates the other side") {
    // The join key `id` is coarser than the items side's `(id, name)` partitioning, so key 1
    // collects 8 input splits there against 1 on purchases; key 2 is even. With the split on,
    // the items side holding more of that key's splits keeps its 8 splits over 8 output
    // partitions while purchases replicates its group to each, and key 2 coalesces as usual.
    // The replicated reads must not change the answer.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      (1 to 8).map(n => s"(1, 'n$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        // Each side uses its own median; the absolute threshold keeps small groups coalesced.
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val plan = df.queryExecution.executedPlan

      val joins = collect(plan) { case j: ShuffledJoin => j }
      assert(joins.size == 1, s"expected one join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      assert(collectAllGroupPartitions(plan).size == 2,
        s"one aligned grouping per join side:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      val splitsAndSpecs = sides.map(_.flatMap { g =>
        g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
      })
      assert(splitsAndSpecs ==
          Seq(Seq((8, true, true), (1, true, false)), Seq((8, false, true), (1, false, false))),
        s"left/right splits, specs and skew marks: $splitsAndSpecs")

      val expected = (1 to 8).map(n => Row(1, s"n$n", 42.0)) ++ Seq(Row(2, "cc", 11.0))
      checkAnswer(df, expected)
      // The 8 replicas of the purchases group of key 1 re-read its single split 7 times beyond
      // the first pass; both sides report the one skewed key.
      assert(sides(1).head.metrics("numReplicatedPartitionReads").value === 7)
      sides.flatten.foreach { g =>
        assert(g.metrics("numSkewedKeyGroups").value === 1)
      }
    }
  }

  test("SPARK-59436: skewed group split survives adaptive re-planning") {
    // The same shape as the test above with AQE on: stage preparation hands whole trees back to
    // `EnsureRequirements`, and the re-runs must reproduce the first pass's split instead of
    // regrouping it away.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      (1 to 8).map(n => s"(1, 'n$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val expected = (1 to 8).map(n => Row(1, s"n$n", 42.0)) ++ Seq(Row(2, "cc", 11.0))
      checkAnswer(df, expected)
      val plan = stripAQEPlan(df.queryExecution.executedPlan)

      val joins = collect(plan) { case j: ShuffledJoin => j }
      assert(joins.size == 1, s"expected one join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      assert(collectAllGroupPartitions(plan).size == 2,
        s"one aligned grouping per join side:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      val splitsAndSpecs = sides.map(_.flatMap { g =>
        g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
      })
      assert(splitsAndSpecs ==
          Seq(Seq((8, true, true), (1, true, false)), Seq((8, false, true), (1, false, false))),
        s"left/right splits, specs and skew marks: $splitsAndSpecs")
      assert(sides(1).head.metrics("numReplicatedPartitionReads").value === 7)
      assert(ValidateRequirements.validate(joins.head),
        s"the split pair passes the validator's pairing check:\n$plan")
    }
  }

  test("SPARK-59436: skewed group split honors the advisory input partitions per slot") {
    // The eight splits of key 1 spread over ceil(8 / 3) = 3 output partitions in contiguous
    // chunks instead of eight, and purchases replicates its group 3 times instead of 8; the
    // answer is unchanged.
    withSkewSplitTables(withUnmatched = false) {
      withSQLConf(
          SQLConf.V2_BUCKETING_ADVISORY_INPUT_PARTITIONS_PER_OUTPUT_PARTITION.key ->
            "3") {
        val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
          s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
        val expected = (1 to 8).map(n => Row(1, s"n$n", 42.0)) ++ Seq(Row(2, "cc", 11.0))
        checkAnswer(df, expected)
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        val joins = collect(plan) { case j: ShuffledJoin => j }
        assert(joins.size == 1, s"expected one join in\n$plan")
        assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
        assert(collectAllGroupPartitions(plan).size == 2,
          s"one aligned grouping per join side:\n$plan")
        val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
        val splitsAndSpecs = sides.map(_.flatMap { g =>
          g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
        })
        assert(splitsAndSpecs ==
            Seq(Seq((3, true, true), (1, true, false)),
              Seq((3, false, true), (1, false, false))),
          s"left/right splits, specs and skew marks: $splitsAndSpecs")
        assert(sides(1).head.metrics("numReplicatedPartitionReads").value === 2)
        sides.flatten.foreach { g =>
          assert(g.metrics("numSkewedKeyGroups").value === 1)
        }
      }
    }
  }

  test("SPARK-59436: an aggregate on the join keys stands the skew split down") {
    // join -> agg on the join's own keys: the grouping that requirement would need over a
    // split pair coalesces every spread partition back into its one consumer task, so the
    // replication reads remain while the parallelism they pay for goes. The split stands down
    // by retaining the original stage. The join stays storage-partitioned, and the aggregate
    // consumes the grouped result with no regrouping of
    // its own. Its counts and sums must not see any replication either way.
    withSkewSplitTables(withUnmatched = false) {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, count(*), sum(p.price) " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id " +
        s"GROUP BY i.id")
      checkAnswer(df, Seq(Row(1, 8, 336.0), Row(2, 1, 11.0)))
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val joins = collect(plan) { case j: ShuffledJoin => j }
      assert(joins.size == 1, s"expected one join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      // Nothing regroups the join from above: the grouped layout answers the aggregate, and
      // only the side that held the spread keeps a plain coalescing wrap.
      val groupings = collectAllGroupPartitions(plan)
      assert(groupings.forall(g => !g.exists(_.isInstanceOf[ShuffledJoin])),
        s"no grouping may coalesce the split join from above:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      assert(sides.flatten.forall { g =>
        g.expectedKeySpecs.forall(_.forall(m => m.numSplits == 1 && !m.skewed))
      }, s"the alignment reserves one partition a key:\n$plan")
      assert(groupings.forall(g => !g.metrics.contains("numReplicatedPartitionReads")),
        s"no replication is left to pay for:\n$plan")
    }
  }


  test("SPARK-59436: a second grouping rolls back mixed skew optimization for the stage") {
    for (aqe <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        catalog.clearTables()
        // The lower join spreads each key on a different side. The upper alignment replaces its
        // own grouping rather than stacking another grouping directly over it.
        createTable(items, itemsColumns, Array(identity("id"), identity("name")))
        sql(s"INSERT INTO testcat.ns.$items VALUES " +
          (1 to 8).map(n => s"(1, 'n$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
            .mkString("", ", ", ", ") +
          s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

        // The mirror of the left side: one split for key 1 and eight for key 2.
        createTable(purchases, purchasesColumns, Array(identity("item_id"), identity("price")))
        sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
          s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
          (1 to 8).map(n => s"(2, $n.0, cast('2020-01-01' as timestamp))").mkString(", "))

        val others = "others"
        createTable(others,
          Array(Column.create("item_id", LongType), Column.create("tag", StringType)),
          Array(identity("item_id")))
        sql(s"INSERT INTO testcat.ns.$others VALUES (1, 'x'), (2, 'y')")

        withSQLConf(
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
            SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "false",
            // Each side uses its own median; the absolute threshold keeps small groups coalesced.
            SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
            SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "0.5") {
          val df = sql(s"SELECT /*+ MERGE(p), MERGE(o) */ i.id, i.name, p.price, o.tag " +
            s"FROM testcat.ns.$items i " +
            s"JOIN testcat.ns.$purchases p ON p.item_id = i.id " +
            s"JOIN testcat.ns.$others o ON o.item_id = i.id")
          val expected = (1 to 8).map(n => Row(1, s"n$n", 42.0, "x")) ++
            (1 to 8).map(n => Row(2, "cc", n.toDouble, "y"))
          checkAnswer(df, expected)

          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          val joins = collect(plan) { case j: ShuffledJoin => j }
          assert(joins.size == 2, s"expected two joins in\n$plan")
          assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
          val groupings = collectAllGroupPartitions(plan)
          assert(groupings.nonEmpty, plan.treeString)
          assert(groupings.forall(_.expectedKeySpecs.forall(_.forall(!_.skewed))), plan.treeString)
          assert(ValidateRequirements.validate(plan), plan.treeString)
        }
      }
    }
  }

  test("SPARK-59436: a split join leaves AQE's shuffle coalescing alone") {
    // AQE validates a stage's whole candidate plan before accepting a shuffle-read change, so
    // a split pair that failed `ValidateRequirements` at the join node used to keep an
    // unrelated shuffle in the same stage uncoalesced. The pairing is judged position by
    // position now, and the stage takes the coalesce.
    withSkewSplitTables(withUnmatched = false) {
      val df = sql(
        s"""
           |SELECT i.id, p.price
           |FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id
           |UNION ALL
           |SELECT count(*), cast(item_id % 2 AS DOUBLE)
           |FROM testcat.ns.$purchases GROUP BY item_id % 2
           |""".stripMargin)
      checkAnswer(df, (1 to 8).map(_ => Row(1, 42.0)) ++
        Seq(Row(2, 11.0), Row(1, 1.0), Row(1, 0.0)))
      val aqeReads = collect(df.queryExecution.executedPlan) { case r: AQEShuffleReadExec => r }
      assert(aqeReads.exists(_.hasCoalescedPartition),
        s"the aggregate's shuffle read must coalesce:\n${df.queryExecution.executedPlan}")
    }
  }

  test("SPARK-59436: skewed group split coexists with partially clustered distribution") {
    // Both features on: key 1 is skewed on the left and takes the per-key split;
    // key 2 is small on both sides and stays on
    // the partially clustered layout, which spreads the items side's 3 splits instead of
    // coalescing them. The purchases side replicates for both, 7 reads beyond the first for
    // key 1 and 2 for key 2.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      (1 to 8).map(n => s"(1, 'n$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(2, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'bb', 11.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 12.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        // Each side uses its own median; the absolute threshold keeps small groups coalesced.
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val expected = (1 to 8).map(n => Row(1, s"n$n", 42.0)) ++
        Seq(Row(2, "aa", 11.0), Row(2, "bb", 11.0), Row(2, "cc", 11.0))
      checkAnswer(df, expected)
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val joins = collect(plan) { case j: ShuffledJoin => j }
      assert(joins.size == 1, s"expected one join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      assert(collectAllGroupPartitions(plan).size == 2,
        s"one aligned grouping per join side:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      val splitsAndSpecs = sides.map(_.flatMap { g =>
        g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
      })
      assert(splitsAndSpecs ==
          Seq(Seq((8, true, true), (3, true, false)), Seq((8, false, true), (3, false, false))),
        s"left/right splits, specs and skew marks: $splitsAndSpecs")
      assert(sides(1).head.metrics("numReplicatedPartitionReads").value === 9)
      sides.flatten.foreach { g =>
        assert(g.metrics("numSkewedKeyGroups").value === 1)
      }
    }
  }

  test("SPARK-59436: skewed group split flips the partially clustered side per key") {
    // Both features on, and the per-key choice disagrees with the whole-side one: the partial
    // clustering distributes the items side (11 splits against 9), but key 1 holds 7 of its
    // splits on the purchases side against 1, so the split flips that key: purchases keeps
    // its 7 and items replicates its single split over them, while key 2 splits over the
    // items side as the partial clustering already picked and key 3 stays below the threshold.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'zz', 30.0, cast('2020-01-01' as timestamp)), " +
      (1 to 9).map(n => s"(2, 'm$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(3, 'q', 20.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      (1 to 7).map(n => s"(1, ${50.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
      s"(3, 13.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        // Each side uses its own median; the absolute threshold keeps small groups coalesced.
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "0.5") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val expected = (1 to 7).map(n => Row(1, "zz", 50.0 + n)) ++
        (1 to 9).map(n => Row(2, s"m$n", 11.0)) ++
        Seq(Row(3, "q", 13.0))
      checkAnswer(df, expected)
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val joins = collect(plan) { case j: ShuffledJoin => j }
      assert(joins.size == 1, s"expected one join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      assert(collectAllGroupPartitions(plan).size == 2,
        s"one aligned grouping per join side:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      val splitsAndSpecs = sides.map(_.flatMap { g =>
        g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
      })
      assert(splitsAndSpecs ==
          Seq(Seq((7, false, true), (9, true, true), (1, true, false)),
            Seq((7, true, true), (9, false, true), (1, false, false))),
        s"left/right splits, specs and skew marks: $splitsAndSpecs")
      // The items side replicates its key-1 split over 7 slots (6 reads beyond the first) and
      // the purchases side its key-2 split over 9 (8 beyond the first).
      assert(sides.head.head.metrics("numReplicatedPartitionReads").value === 6)
      assert(sides(1).head.metrics("numReplicatedPartitionReads").value === 8)
      sides.flatten.foreach { g =>
        assert(g.metrics("numSkewedKeyGroups").value === 2)
      }
    }
  }

  test("SPARK-59436: partially clustered and skew split survive adaptive re-planning") {
    // Both features on under AQE: the mixed layout, the per-key split on the skewed key and the
    // partially clustered spread on the other key, must be re-derived on every re-planning of
    // the join instead of being regrouped away.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      (1 to 8).map(n => s"(1, 'n$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(2, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'bb', 11.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 12.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "false",
        // Each side uses its own median; the absolute threshold keeps small groups coalesced.
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val expected = (1 to 8).map(n => Row(1, s"n$n", 42.0)) ++
        Seq(Row(2, "aa", 11.0), Row(2, "bb", 11.0), Row(2, "cc", 11.0))
      checkAnswer(df, expected)

      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val joins = collect(plan) { case j: ShuffledJoin => j }
      assert(joins.size == 1, s"expected one join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      assert(collectAllGroupPartitions(plan).size == 2,
        s"one aligned grouping per join side:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      val splitsAndSpecs = sides.map(_.flatMap { g =>
        g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
      })
      assert(splitsAndSpecs ==
          Seq(Seq((8, true, true), (3, true, false)), Seq((8, false, true), (3, false, false))),
        s"left/right splits, specs and skew marks: $splitsAndSpecs")
      assert(sides(1).head.metrics("numReplicatedPartitionReads").value === 9)
    }
  }

  test("SPARK-59436: skewed group split does not duplicate semi join rows") {
    // The right side's group reaches every left split as a replica; a left row matching in its
    // partition must come out exactly once. Replicating the left side of a semi join is what
    // would duplicate rows, and the join-type gate is what keeps the split on the other shape.
    withSkewSplitTables(withUnmatched = true) {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name " +
        s"FROM testcat.ns.$items i LEFT SEMI JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val expected = (1 to 8).map(n => Row(1, s"n$n")) ++ Seq(Row(2, "cc"))
      checkAnswer(df, expected)
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val joins = collect(plan) { case j: ShuffledJoin if j.joinType == LeftSemi => j }
      assert(joins.size == 1, s"expected one semi join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      assert(collectAllGroupPartitions(plan).size == 2,
        s"one aligned grouping per join side:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      val splitsAndSpecs = sides.map(_.flatMap { g =>
        g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
      })
      assert(splitsAndSpecs ==
          Seq(Seq((8, true, true), (1, true, false), (1, true, false)),
            Seq((8, false, true), (1, false, false), (1, false, false))),
        s"left/right splits, specs and skew marks: $splitsAndSpecs")
    }
  }

  test("SPARK-59436: skewed group split keeps left outer join rows and null padding") {
    // Left outer: every left row comes out exactly once, matched or null-padded. The unmatched
    // key 3 is held by the left side alone, so the right side replicates an empty group for it.
    withSkewSplitTables(withUnmatched = true) {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i LEFT OUTER JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val expected = (1 to 8).map(n => Row(1, s"n$n", 42.0)) ++
        Seq(Row(2, "cc", 11.0), Row(3, "dd", null))
      checkAnswer(df, expected)
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val joins = collect(plan) { case j: ShuffledJoin if j.joinType == LeftOuter => j }
      assert(joins.size == 1, s"expected one left outer join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      assert(collectAllGroupPartitions(plan).size == 2,
        s"one aligned grouping per join side:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      val splitsAndSpecs = sides.map(_.flatMap { g =>
        g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
      })
      assert(splitsAndSpecs ==
          Seq(Seq((8, true, true), (1, true, false), (1, true, false)),
            Seq((8, false, true), (1, false, false), (1, false, false))),
        s"left/right splits, specs and skew marks: $splitsAndSpecs")
    }
  }

  test("SPARK-59436: skewed group split stands down over a marked child end to end") {
    // A real plan reaches the marked shape: the unpartitioned side is shuffled onto `t`'s keyed
    // partitioning, which marks the shuffle output, and a left outer join reports its left
    // side's partitioning, so `j` keeps the marker into the second join. That join pairs it with
    // a side holding eight splits of one key, where the marked side can only be the replicating
    // one: replicating it makes its grouping a non-identity one and the node stops reporting the
    // keyed partitioning the join is planned on. The split stands down and the claim survives.
    val t = "t"
    val flat = "flat"
    withTable(t, flat, items) {
      createTable(t, Array(Column.create("id", LongType)), Array(identity("id")))
      sql(s"INSERT INTO testcat.ns.$t VALUES (1), (2)")
      createTable(flat, Array(Column.create("id", LongType)), Array.empty)
      sql(s"INSERT INTO testcat.ns.$flat VALUES (1), (2)")
      createTable(items, itemsColumns, Array(identity("id"), identity("name")))
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        (1 to 8).map(n => s"(1, 'n$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
          .mkString("", ", ", ", ") +
        s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
          // Each side uses its own median; the absolute threshold keeps small groups coalesced.
          SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
          SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
        val df = sql(
          s"""${selectWithMergeJoinHint("i", "j")} i.id, i.name, j.id
             |FROM testcat.ns.$items i
             |JOIN (SELECT /*+ MERGE(f, t) */ f.id AS id
             |      FROM testcat.ns.$flat f LEFT OUTER JOIN testcat.ns.$t t ON f.id = t.id) j
             |ON i.id = j.id
             |""".stripMargin)
        val expected = (1 to 8).map(n => Row(1L, s"n$n", 1L)) ++ Seq(Row(2L, "cc", 2L))
        checkAnswer(df, expected)

        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        val shuffles = collectAllShuffles(plan)
        assert(shuffles.size == 1,
          s"only the shuffle that lays the unpartitioned side onto the keyed one:\n$plan")
        val marked = keyedPartitioningsOf(shuffles)
        assert(marked.nonEmpty && marked.forall(_.mayContainUnknownPartitionKeys),
          s"test setup: the shuffled side must carry the marker, got $marked")
        // The stand-down is what keeps the marked side's grouping an identity one, and with it
        // the keyed partitioning the second join takes as satisfied. A split would reserve
        // several partitions for the one skewed key and the node would report an
        // `UnknownPartitioning` instead.
        val outer = collect(plan) { case j: ShuffledJoin => j }.head
        assert(keyedPartitioningsOf(Seq(outer.right)).exists(_.mayContainUnknownPartitionKeys),
          s"the marked side must keep its claim:\n$plan")
        assert(outer.right.outputPartitioning.numPartitions == 2,
          s"one partition per key, nothing reserved for a split:\n$plan")
        assert(collectAllGroupPartitions(plan).forall(
            _.expectedKeySpecs.get.forall(!_.skewed)),
          s"no key is marked skewed:\n$plan")
      }
    }
  }

  test("SPARK-59436: skewed group split keeps right outer join rows and null padding") {
    // A right outer join may replicate the left side but not the right, so the side holding
    // more of the skewed key's splits is the right one, which spreads them while the left
    // replicates its group. The right-only key 9 pads the left with a null row.
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id"), identity("price")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      (1 to 8).map(n => s"(1, ${50.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(9, 99.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "false",
        // Each side uses its own median; the absolute threshold keeps small groups coalesced.
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i RIGHT OUTER JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val expected = (1 to 8).map(n => Row(1, "aa", 50.0 + n)) ++ Seq(Row(null, null, 99.0))
      checkAnswer(df, expected)

      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val joins = collect(plan) { case j: ShuffledJoin if j.joinType == RightOuter => j }
      assert(joins.size == 1, s"expected one right outer join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      assert(collectAllGroupPartitions(plan).size == 2,
        s"one aligned grouping per join side:\n$plan")
      val sides = Seq(joins.head.left, joins.head.right).map(collectAllGroupPartitions)
      val splitsAndSpecs = sides.map(_.flatMap { g =>
        g.expectedKeySpecs.get.map(e => (e.numSplits, e.distribute, e.skewed))
      })
      assert(splitsAndSpecs ==
          Seq(Seq((8, false, true), (1, true, false), (1, true, false)),
            Seq((8, true, true), (1, false, false), (1, false, false))),
        s"left/right splits, specs and skew marks: $splitsAndSpecs")
      assert(sides(0).head.metrics("numReplicatedPartitionReads").value === 7)
    }
  }

  test("SPARK-59436: a subset-key window retains its grouping after skew rollback") {
    val columns = Array(Column.create("a", LongType), Column.create("b", LongType))
    createTable("review_l", columns, Array(identity("a"), identity("b")))
    createTable("review_r", columns, Array(identity("a"), identity("b")))
    sql("INSERT INTO testcat.ns.review_l VALUES " +
      (Seq.fill(8)("(1, 1)") :+ "(1, 2)").mkString(", "))
    sql("INSERT INTO testcat.ns.review_r VALUES (1, 1), (1, 2)")
    for {
      aqe <- Seq(false, true)
      enabled <- Seq(false, true)
    } {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> enabled.toString,
          SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
          SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "0.5") {
        val df = sql("SELECT /*+ MERGE(l), MERGE(r) */ l.a, l.b, " +
          "count(*) OVER (PARTITION BY l.a) AS n " +
          "FROM testcat.ns.review_l l JOIN testcat.ns.review_r r " +
          "ON l.a = r.a AND l.b = r.b")
        val rows = df.collect().toSeq
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(ValidateRequirements.validate(plan), plan.treeString)
        assert(collectAllShuffles(plan).isEmpty, plan.treeString)
        if (enabled) {
          val joins = collect(plan) { case j: ShuffledJoin => j }
          assert(joins.size == 1)
          assert(collectAllGroupPartitions(plan).exists(g =>
            g.joinKeyPositions.isDefined && g.exists(_.isInstanceOf[ShuffledJoin])),
            "the consumer must retain its subset-key grouping")
          assert(joins.head.children.flatMap(collectAllGroupPartitions)
            .forall(_.expectedKeySpecs.forall(_.forall(!_.skewed))))
        }
        assert(rows.forall(_.getLong(2) == 9L),
          s"all rows sharing a must belong to one window: $rows")
      }
    }
  }

  test("SPARK-59436: full outer join keeps both sides' unmatched rows coalesced") {
    // A full outer join may replicate neither side, so the skewed key stands down and both keys
    // coalesce; each side's unmatched key pads the other with nulls.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      (1 to 8).map(n => s"(1, 'n$n', ${40.0 + n}, cast('2020-01-01' as timestamp))")
        .mkString("", ", ", ", ") +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(3, 'dd', 12.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(9, 99.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_PARTITIONS_PER_GROUP_THRESHOLD.key -> "4",
        SQLConf.V2_BUCKETING_SKEW_JOIN_SKEWED_GROUP_FACTOR.key -> "1.0") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i FULL OUTER JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val expected = (1 to 8).map(n => Row(1, s"n$n", 42.0)) ++
        Seq(Row(2, "cc", null), Row(3, "dd", null), Row(null, null, 99.0))
      checkAnswer(df, expected)

      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val joins = collect(plan) { case j: ShuffledJoin if j.joinType == FullOuter => j }
      assert(joins.size == 1, s"expected one full outer join in\n$plan")
      assert(collectAllShuffles(plan).size == 0, s"the whole plan must not shuffle:\n$plan")
      val groupings = collectAllGroupPartitions(plan)
      assert(groupings.size == 2, s"one aligned grouping per join side:\n$plan")
      groupings.foreach { g =>
        assert(g.expectedKeySpecs.get.forall(e => e.numSplits == 1 && !e.skewed),
          s"neither side can replicate, so nothing splits:\n$plan")
        assert(!g.metrics.contains("numSkewedKeyGroups"),
          s"no key may be marked skewed:\n$plan")
      }
    }
  }
}
