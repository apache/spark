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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{
  Concat, CurrentRow, DenseRank, In, Literal, Rand, RowFrame, RowNumber, SpecifiedWindowFrame,
  UnboundedFollowing, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Window, WindowGroupLimit}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class TransposeWindowSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    // CollapseWindow must run in its own batch: a combined Once batch is not idempotent,
    // because TransposeWindow can make same-spec windows adjacent and the idempotence
    // re-check then lets CollapseWindow merge them, which is intended composition, not
    // part of what these tests assert.
    val batches =
      Batch("CollapseProject", FixedPoint(100), CollapseProject, RemoveNoopOperators) ::
      Batch("CollapseWindow", Once, CollapseWindow) ::
      Batch("TransposeWindow", Once, TransposeWindow) :: Nil
  }

  // Same batches as Optimize but with TransposeWindow run to a fixed point, as in the
  // real operator-optimization batch: a cut-boundary swap hands a window to the chain
  // below the cut, which a single application does not re-collect.
  object OptimizeToFixedPoint extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseProject", FixedPoint(100), CollapseProject, RemoveNoopOperators) ::
      Batch("CollapseWindow", Once, CollapseWindow) ::
      Batch("TransposeWindow", FixedPoint(100), TransposeWindow) :: Nil
  }

  val testRelation = LocalRelation($"a".string, $"b".string, $"c".int, $"d".string)

  val a = testRelation.output(0)
  val b = testRelation.output(1)
  val c = testRelation.output(2)
  val d = testRelation.output(3)

  val partitionSpec1 = Seq(a)
  val partitionSpec2 = Seq(a, b)
  val partitionSpec3 = Seq(d)
  val partitionSpec4 = Seq(b, a, d)

  val orderSpec1 = Seq(d.asc)
  val orderSpec2 = Seq(d.desc)

  test("transpose two adjacent windows with compatible partitions") {
    val query = testRelation
      .window(Seq(sum(c).as("sum_a_2")), partitionSpec2, orderSpec2)
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec1, orderSpec1)
      .window(Seq(sum(c).as("sum_a_2")), partitionSpec2, orderSpec2)
      .select($"a", $"b", $"c", $"d", $"sum_a_2", $"sum_a_1")

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("transpose two adjacent windows with differently ordered compatible partitions") {
    val query = testRelation
      .window(Seq(sum(c).as("sum_a_2")), partitionSpec4, Seq.empty)
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec2, Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec2, Seq.empty)
      .window(Seq(sum(c).as("sum_a_2")), partitionSpec4, Seq.empty)
      .select($"a", $"b", $"c", $"d", $"sum_a_2", $"sum_a_1")

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("don't transpose two adjacent windows with incompatible partitions") {
    val query = testRelation
      .window(Seq(sum(c).as("sum_a_2")), partitionSpec3, Seq.empty)
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec1, Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    comparePlans(optimized, analyzed)
  }

  test("don't transpose two adjacent windows with intersection of partition and output set") {
    val query = testRelation
      .window(Seq(Concat(Seq($"a", $"b")).as("e"),
        sum(c).as("sum_a_2")), partitionSpec3, Seq.empty)
      .window(Seq(sum(c).as("sum_a_1")), Seq(a, $"e"), Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    comparePlans(optimized, analyzed)
  }

  test("don't transpose two adjacent windows with non-deterministic expressions") {
    val query = testRelation
      .window(Seq(Rand(0).as("e"), sum(c).as("sum_a_2")), partitionSpec3, Seq.empty)
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec1, Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    comparePlans(optimized, analyzed)
  }

  test("don't transpose windows where a function argument references another window's output") {
    // The top window's function argument `sum(s1)` references the bottom window's output
    // `s1`. The top partition spec (k1) is a strict subset of the bottom's (k1, k2), so
    // without the reference stack reorder would move the [k1] window below [k1, k2].
    // `reorderable` computes `references` across the function-expressions clause too, so
    // this shape blocks stack reorder; the pairwise fallback also declines the pair
    // because of the same reference.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1, k2), order)
      .window(Seq(sum($"s1").as("s2")), Seq(k1), order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("flag-on falls back to pairwise transpose when a dependent window blocks the stack") {
    // Bottom-to-top: r PARTITION BY (k1, k2), s PARTITION BY (k1), t = sum(r) PARTITION BY
    // (k3). t reads r, so reorderable rejects the chain. s is still a strict-subset pair
    // with r; flag-off swaps s under r (2 exchanges). Flag-on must apply the same
    // pairwise fallback instead of leaving the 3-exchange input order.
    val query = wideRelation
      .window(Seq(rankOnK1K2OrderV), Seq(k1, k2), Seq(v.asc))
      .window(Seq(sum(v).as("s")), Seq(k1), order)
      .window(Seq(sum($"rn").as("t")), Seq(k3), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      Optimize.execute(analyzed)
    }
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = Optimize.execute(analyzed)
      comparePlans(once, flagOff)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("flag-on pairwise fallback still runs beside a nondeterministic window") {
    // Rand blocks stack reorder; the (k1) / (k1, k2) pair below it must still transpose.
    val query = wideRelation
      .window(Seq(sum(v).as("r")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("s")), Seq(k1), order)
      .window(Seq(Rand(0).as("nd"), sum(v).as("t")), Seq(k3), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      Optimize.execute(analyzed)
    }
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), flagOff)
    }
  }

  test("SPARK-34807: transpose two windows with compatible partitions " +
    "and a Project between them") {
    val query = testRelation
      .window(Seq(sum(c).as("_we0")), partitionSpec2, orderSpec2)
      .select(a, b, c, d, $"_we0" as "sum_a_2")
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec1, orderSpec1)
      .window(Seq(sum(c).as("_we0")), partitionSpec2, orderSpec2)
      .select($"a", $"b", $"c", $"d", $"_we0" as "sum_a_2", $"sum_a_1")

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("SPARK-34807: don't transpose two windows if project between them " +
    "generates an input column") {
    val query = testRelation
      .window(Seq(sum(c).as("sum_a_2")), partitionSpec2, orderSpec2)
      .select(a, b, c, d, $"sum_a_2", c + d as "e")
      .window(Seq(sum($"e").as("sum_a_1")), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    comparePlans(optimized, analyzed)
  }

  test("SPARK-38034: transpose two adjacent windows with compatible partitions " +
    "which is not a prefix") {
    val query = testRelation
      .window(Seq(sum(c).as("sum_a_2")), partitionSpec4, orderSpec2)
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec3, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as("sum_a_1")), partitionSpec3, orderSpec1)
      .window(Seq(sum(c).as("sum_a_2")), partitionSpec4, orderSpec2)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"),
        Symbol("sum_a_2"), Symbol("sum_a_1"))

    comparePlans(optimized, correctAnswer.analyze)
  }

  // Data-backed so that maxRows (Some(3)) does not suppress the WindowGroupLimit that
  // InferWindowGroupLimit produces in the rank-filter tests below.
  private val wideRelation = LocalRelation.fromExternalRows(
    Seq($"k1".string, $"k2".string, $"k3".string, $"k4".string, $"v".int, $"u".int),
    1.to(3).map(i => Row(s"a$i", s"b$i", s"c$i", s"d$i", i, i)))
  private val k1 = wideRelation.output(0)
  private val k2 = wideRelation.output(1)
  private val k3 = wideRelation.output(2)
  private val k4 = wideRelation.output(3)
  private val v = wideRelation.output(4)
  private val u = wideRelation.output(5)

  private val order = Seq(k1.asc)

  test("don't reorder window stacks with non-deterministic expressions") {
    // Interleaved partition specs that are regrouped when stack reordering is enabled (see
    // the test below). A nondeterministic window makes `reorderable` reject the chain, and
    // here the legacy pairwise path also finds no compatible adjacent pair, so the plan
    // stays unchanged.
    val query = wideRelation
      .window(Seq(sum(v).as("f_s")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("p_s")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("s_s")), Seq(k1, k4), order)
      .window(Seq(Rand(0).as("r"), sum(v).as("f_a")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("p_a")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("s_a")), Seq(k1, k4), order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("reorder a window stack by grouping minimal partition specs") {
    val query = wideRelation
      .window(Seq(sum(v).as("f_s")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("p_s")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("s_s")), Seq(k1, k4), order)
      .window(Seq(sum(v).as("f_a")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("p_a")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("s_a")), Seq(k1, k4), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("f_s")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("f_a")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("p_s")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("p_a")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("s_s")), Seq(k1, k4), order)
      .window(Seq(sum(v).as("s_a")), Seq(k1, k4), order)
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u",
        $"f_s", $"p_s", $"s_s", $"f_a", $"p_a", $"s_a")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("already optimally ordered window stack is unchanged") {
    // Distinct order specs so that CollapseWindow cannot merge the same-spec windows, and
    // ordered so that no window can move next to an equal-order window to share a sort: the
    // stack is optimal both in exchanges and in sorts, so the rule must leave it unchanged.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s2a")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("s2b")), Seq(k1, k2), Seq(k2.asc))
      .window(Seq(sum(v).as("s3")), Seq(k1, k2, k3), Seq(k3.asc))

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("windows with equal required child orderings become adjacent within a group") {
    // All three windows share ORDER BY k3, but WindowExec prepends partition keys to the
    // required sort. The middle window's extra partition key (k4) must not sit between the
    // two [k1, k2] windows, which share the complete ordering k1, k2, k3.
    val orderK3 = Seq(k3.asc)
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1, k2), orderK3)
      .window(Seq(sum(v).as("s2")), Seq(k1, k4, k2), orderK3)
      .window(Seq(sum(v).as("s3")), Seq(k1, k2), orderK3)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1, k2), orderK3)
      .window(Seq(sum(v).as("s3")), Seq(k1, k2), orderK3)
      .window(Seq(sum(v).as("s2")), Seq(k1, k4, k2), orderK3)
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u", $"s1", $"s2", $"s3")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("reordering is idempotent when a shared ordering crosses a group boundary") {
    // C [k1] and X1 [k1, k3] share the complete required ordering [k1, k3, u]. X1 can
    // ride both minimal specs, [k3] and [k1]; it joins the [k1] group because C anchors
    // that group and shares X1's ordering, so X1 keeps riding C's sort instead of adding
    // its own sort over hash(k3) while saving no exchange. The input order is already
    // optimal in exchanges and sorts, so the rule leaves the stack unchanged and one
    // application is a fixed point.
    val query = wideRelation
      .window(Seq(sum(v).as("L0")), Seq(k3), Seq(v.asc))
      .window(Seq(sum(v).as("A")), Seq(k1), Seq(k2.asc))
      .window(Seq(sum(v).as("B")), Seq(k1), Seq(k4.asc))
      .window(Seq(sum(v).as("C")), Seq(k1), Seq(k3.asc, u.asc))
      .window(Seq(sum(v).as("X1")), Seq(k1, k3), Seq(u.asc))

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, analyzed)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("a multi-ride window joins the group whose member shares its required ordering") {
    // X1 can ride both minimal specs, [k3] and [k1]. Its complete required child
    // ordering [k1, k3, u] equals C's, so it must join C's group and share C's sort:
    // joining the first-appearing [k3] group instead would add a sort over hash(k3)
    // while saving no exchange. The input order is already optimal in exchanges and
    // sorts, so the rule must leave the stack unchanged.
    val query = wideRelation
      .window(Seq(sum(v).as("l0")), Seq(k3), Seq(v.asc))
      .window(Seq(sum(v).as("c")), Seq(k1), Seq(k3.asc, u.asc))
      .window(Seq(sum(v).as("x1")), Seq(k1, k3), Seq(u.asc))

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, analyzed)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("a multi-ride window shares a sort its required ordering is a prefix of") {
    // X's required ordering [k1, k3, v] is a proper prefix of L's [k1, k3, v, u], and
    // EnsureRequirements reuses a sort on prefix satisfaction (SortOrder.orderingSatisfies),
    // so X can ride L's sort. X can ride both minimal specs, [k3] and [k1]; it must join
    // L's group. Joining the first-appearing [k3] group instead would emit M, X, L: X adds
    // Sort[k1, k3, v] over hash(k3) and L still adds hash(k1) and Sort[k1, k3, v, u] -- 2
    // exchanges and 3 sorts, against 2 and 2 here. The input order is already optimal, so
    // the rule leaves the stack unchanged.
    val query = wideRelation
      .window(Seq(sum(v).as("m")), Seq(k3), Seq(v.asc))
      .window(Seq(sum(v).as("l")), Seq(k1), Seq(k3.asc, v.asc, u.asc))
      .window(Seq(sum(v).as("x")), Seq(k1, k3), Seq(v.asc))

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, analyzed)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("member whose ordering the leader's satisfies stays beside the leader") {
    // Same stack as above with an extra member b between L and X. X's required ordering
    // [k1, k3, v] is a proper prefix of its group leader L's [k1, k3, v, u], so X rides
    // L's sort when emitted directly above it. Emitting X above b instead would bury
    // L's ordering under b's sort [k1, k2, k4] and force X to add its own sort.
    val query = wideRelation
      .window(Seq(sum(v).as("m")), Seq(k3), Seq(v.asc))
      .window(Seq(sum(v).as("l")), Seq(k1), Seq(k3.asc, v.asc, u.asc))
      .window(Seq(sum(v).as("b")), Seq(k1, k2), Seq(k4.asc))
      .window(Seq(sum(v).as("x")), Seq(k1, k3), Seq(v.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("m")), Seq(k3), Seq(v.asc))
      .window(Seq(sum(v).as("l")), Seq(k1), Seq(k3.asc, v.asc, u.asc))
      .window(Seq(sum(v).as("x")), Seq(k1, k3), Seq(v.asc))
      .window(Seq(sum(v).as("b")), Seq(k1, k2), Seq(k4.asc))
      .select(k1, k2, k3, k4, v, u, $"m", $"l", $"b", $"x")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("riders of a chain are emitted longest first") {
    // Bottom-to-top: a [k1] with ordering [k1, k2, v, u], c [k1] with [k1, k2], b [k1, k2]
    // with [k1, k2, v]. b's and c's orderings are nested prefixes of the leader a's
    // [k1, k2, v, u], so both ride a's sort. The rider order does not change the physical
    // cost -- a window preserves its child's ordering, so the riders ride the one sort in
    // any order -- it makes the emission deterministic and stable under re-application.
    val query = wideRelation
      .window(Seq(sum(v).as("a")), Seq(k1), Seq(k2.asc, v.asc, u.asc))
      .window(Seq(sum(v).as("c")), Seq(k1), Seq(k2.asc))
      .window(Seq(sum(v).as("b")), Seq(k1, k2), Seq(v.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("a")), Seq(k1), Seq(k2.asc, v.asc, u.asc))
      .window(Seq(sum(v).as("b")), Seq(k1, k2), Seq(v.asc))
      .window(Seq(sum(v).as("c")), Seq(k1), Seq(k2.asc))
      .select(k1, k2, k3, k4, v, u, $"a", $"c", $"b")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("a maximal candidate leads instead of an earlier non-maximal one") {
    // Bottom-to-top: l2 [k1] with ordering [k1, k2, v, u], l1 [k1] with [k1, k3],
    // w [k1, k2] with [k1, k2, v, u, k3]. l2 is the first [k1] candidate but not
    // maximal: w extends its ordering, so a sort at [k1, k2, v, u] would be subsumed
    // by w's chain. The maximal candidate l1 leads and w starts the second chain with
    // l2 riding it: 2 sorts, against 3 with l2 leading.
    val query = wideRelation
      .window(Seq(sum(v).as("l2")), Seq(k1), Seq(k2.asc, v.asc, u.asc))
      .window(Seq(sum(v).as("l1")), Seq(k1), Seq(k3.asc))
      .window(Seq(sum(v).as("w")), Seq(k1, k2), Seq(v.asc, u.asc, k3.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("l1")), Seq(k1), Seq(k3.asc))
      .window(Seq(sum(v).as("w")), Seq(k1, k2), Seq(v.asc, u.asc, k3.asc))
      .window(Seq(sum(v).as("l2")), Seq(k1), Seq(k2.asc, v.asc, u.asc))
      .select(k1, k2, k3, k4, v, u, $"l2", $"l1", $"w")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("a narrow maximal candidate leads over an earlier wide one") {
    // Bottom-to-top: w1 [k1] with ordering [k1, k3] (collect_list), n1 [k1] with
    // [k1, k2], z [k1, k4] with [k1, k4, v]. The three orderings are pairwise
    // incomparable, so all three windows pay their own sort whoever leads; both
    // candidates are maximal, and the free choice is spent on width -- the leader
    // sits at the bottom of the group, so a wide leader's unbounded payload would
    // cross every sort in it. n1 leads, and the chain starts above it put z before
    // w1, keeping w1 nearest the group top.
    val query = wideRelation
      .window(Seq(CollectList(v).toAggregateExpression().as("w1")), Seq(k1), Seq(k3.asc))
      .window(Seq(sum(v).as("n1")), Seq(k1), Seq(k2.asc))
      .window(Seq(sum(v).as("z")), Seq(k1, k4), Seq(v.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("n1")), Seq(k1), Seq(k2.asc))
      .window(Seq(sum(v).as("z")), Seq(k1, k4), Seq(v.asc))
      .window(Seq(CollectList(v).toAggregateExpression().as("w1")), Seq(k1), Seq(k3.asc))
      .select(k1, k2, k3, k4, v, u, $"w1", $"n1", $"z")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("member with the leader's complete ordering stays beside the leader") {
    // b and the leader share no ordering; c's required ordering equals the leader's
    // [k1, k1], so c is a rider and is emitted directly after the leader, riding its
    // sort. Emitting b between them would bury the leader's ordering under b's sort
    // [k1, k2, k2] and force c to add its own sort.
    val query = wideRelation
      .window(Seq(sum(v).as("b")), Seq(k1, k2), Seq(k2.asc))
      .window(Seq(sum(v).as("a")), Seq(k1), Seq(k1.asc))
      .window(Seq(sum(v).as("c")), Seq(k1), Seq(k1.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("a")), Seq(k1), Seq(k1.asc))
      .window(Seq(sum(v).as("c")), Seq(k1), Seq(k1.asc))
      .window(Seq(sum(v).as("b")), Seq(k1, k2), Seq(k2.asc))
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u", $"b", $"a", $"c")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("wide-output groups rank last among exchange-optimal orders") {
    // s1 [k1] and s4 [k4] key the two exchanges; evs [k1, k2] rides s1's exchange. Both
    // group orders cost two exchanges, but first appearance would put evs below the [k4]
    // exchange and ship each partition's full collect_list array through it and its sort.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s4")), Seq(k4), order)
      .window(Seq(CollectList(v).toAggregateExpression().as("evs")), Seq(k1, k2), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s4")), Seq(k4), order)
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(CollectList(v).toAggregateExpression().as("evs")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"s1", $"s4", $"evs")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      // The wide flags do not change with the stack order, so one application is a
      // fixed point.
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("a wide-output group leader also ranks last") {
    // evs keys its own exchange: its spec [k4] is minimal, so it leads its group. The
    // wide tier is about the group, not only non-leader members, so the [k1] group goes
    // first even though the wide window sits at the bottom of the stack.
    val query = wideRelation
      .window(Seq(CollectList(v).toAggregateExpression().as("evs")), Seq(k4), order)
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s2")), Seq(k1, k2), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s2")), Seq(k1, k2), order)
      .window(Seq(CollectList(v).toAggregateExpression().as("evs")), Seq(k4), order)
      .select(k1, k2, k3, k4, v, u, $"evs", $"s1", $"s2")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("a narrow maximal member starts a chain before a wide one") {
    // Bottom-to-top: leader l [k1] with ordering [k1, v], then the wide collect_list
    // window w [k1, k2] with [k1, k2, k4] and the narrow n [k1, k3] with [k1, k3, u].
    // The three orderings are pairwise incomparable, so each member pays its own sort
    // in any order -- the minimum for this membership -- and the choice among chain
    // starts is free. It is spent on width: n starts the second chain before w, so w
    // lands nearer the group top and its unbounded per-row payload crosses no sort
    // within the group, mirroring the wide group tier across groups. Stack order would
    // start w's chain first (w sits below n in the input).
    val query = wideRelation
      .window(Seq(sum(v).as("l")), Seq(k1), Seq(v.asc))
      .window(Seq(CollectList(v).toAggregateExpression().as("w")), Seq(k1, k2), Seq(k4.asc))
      .window(Seq(sum(v).as("n")), Seq(k1, k3), Seq(u.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("l")), Seq(k1), Seq(v.asc))
      .window(Seq(sum(v).as("n")), Seq(k1, k3), Seq(u.asc))
      .window(Seq(CollectList(v).toAggregateExpression().as("w")), Seq(k1, k2), Seq(k4.asc))
      .select(k1, k2, k3, k4, v, u, $"l", $"w", $"n")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("a multi-ride window joins the earliest emitted group when no ordering is shared") {
    // s2 [k1, k4] can ride both minimal specs: the narrow [k1] group and the wide [k4]
    // group, which is emitted last. No anchored member's required ordering satisfies
    // s2's (equal or a prefix), so it joins the earliest group of the emitted order:
    // [k1]. Joining the first-appearing [k4] group instead would move s2 on
    // re-application, so the first application would not be a fixed point.
    val query = wideRelation
      .window(Seq(CollectList(v).toAggregateExpression().as("cl")), Seq(k4), order)
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s2")), Seq(k1, k4), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s2")), Seq(k1, k4), order)
      .window(Seq(CollectList(v).toAggregateExpression().as("cl")), Seq(k4), order)
      .select(k1, k2, k3, k4, v, u, $"cl", $"s1", $"s2")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      comparePlans(once, correctAnswer.analyze)
      comparePlans(once, TransposeWindow.apply(once))
    }
  }

  test("windows with identical partition specs become adjacent") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(count(v).as("count_f")), Seq(k1, k2), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(count(v).as("count_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u", $"sum_f", $"sum_p", $"count_f")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("windows with permuted partition key specs collapse into one group") {
    // Under default subset semantics (k1, k2) and (k2, k1) are one minimal spec, so the
    // pair and the superset window form a single group and one exchange serves all
    // three: the leader's chain (a pair member) is hoisted to the bottom, replacing
    // the original two-exchange stack. The permuted member cannot share the leader's
    // sort -- its required ordering starts with the (k2, k1) partition sort -- so it
    // starts its own chain above the superset window. The strict-conf key-order test
    // below cannot pin this dedup: there the two specs are distinct and the dedup is
    // a no-op. An order-sensitive dedup would instead emit two groups, the second
    // staying empty (no window is anchored: each rides both exchanges), and the
    // leader search crashes with empty.minBy.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(count(v).as("count_f")), Seq(k2, k1), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(count(v).as("count_f")), Seq(k2, k1), order)
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u", $"sum_p", $"sum_f", $"count_f")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
    }
  }

  test("requireAllClusterKeysForDistribution groups identical specs, not subsets") {
    // (k1, k2) and (k1, k4) both contain k1 but are incomparable; the two
    // (k1, k2) windows bookend the (k1, k4) window. Under the subset model all windows group
    // under the minimal spec (k1) and the stack is already optimally ordered. Under
    // `requireAllClusterKeysForDistribution` a window can only ride an exchange with the
    // exact same keys in the same order, so the two (k1, k2) windows must become adjacent
    // to share one exchange; the distinct order specs of the two (k1, k2) windows keep
    // `CollapseWindow` from merging them.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), Seq(k1.asc))
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .window(Seq(sum(v).as("sum_f2")), Seq(k1, k2), Seq(k2.asc))
    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true") {
        val optimized = Optimize.execute(analyzed)

        val correctAnswer = wideRelation
          .window(Seq(sum(v).as("s1")), Seq(k1), order)
          .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), Seq(k1.asc))
          .window(Seq(sum(v).as("sum_f2")), Seq(k1, k2), Seq(k2.asc))
          .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
          .select($"k1", $"k2", $"k3", $"k4", $"v", $"u",
            $"s1", $"sum_f", $"sum_s", $"sum_f2")

        comparePlans(optimized, correctAnswer.analyze)
      }

      // With the default subset semantics the stack is already optimally ordered, so the rule
      // must leave it unchanged.
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("requireAllClusterKeysForDistribution distinguishes partition key order") {
    // Under strict clustering, (k1, k2) and (k2, k1) are distinct specs. The two (k1, k2)
    // windows must be regrouped together without treating (k2, k1) as the same class.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_fk1")), Seq(k1, k2), Seq(k1.asc))
      .window(Seq(sum(v).as("sum_kf")), Seq(k2, k1), Seq(k2.asc))
      .window(Seq(sum(v).as("sum_fk2")), Seq(k1, k2), Seq(k2.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_fk1")), Seq(k1, k2), Seq(k1.asc))
      .window(Seq(sum(v).as("sum_fk2")), Seq(k1, k2), Seq(k2.asc))
      .window(Seq(sum(v).as("sum_kf")), Seq(k2, k1), Seq(k2.asc))
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u", $"sum_fk1", $"sum_kf", $"sum_fk2")

    withSQLConf(
      SQLConf.WINDOW_REORDER_ENABLED.key -> "true",
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
    }
  }

  test("windowReorder disabled still transposes compatible adjacent windows") {
    // With the stack reordering off, the original adjacent-pair transposition applies: the
    // upper window's partition spec (k1, k2) is a proper subset of the lower one's (k1, k2,
    // k3), so the pair is swapped and the output order restored with a Project.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      val optimized = Optimize.execute(analyzed)

      val correctAnswer = wideRelation
        .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
        .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
        .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f")

      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("windowReorder disabled disables stack reordering") {
    // The stack (k1), (k1, k2), (k1, k4), (k1, k2) is regrouped by the stack reordering
    // under requireAllClusterKeysForDistribution (the two (k1, k2) windows become
    // adjacent, see the test above). With the config disabled no adjacent pair is
    // compatible, so the rule must leave the plan untouched.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), Seq(k1.asc))
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .window(Seq(sum(v).as("sum_f2")), Seq(k1, k2), Seq(k2.asc))
    val analyzed = query.analyze

    withSQLConf(
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true",
      SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("attribute-only projects between windows are transparent to reordering") {
    // The project between the windows drops the unused attribute `u`.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, $"sum_p")
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, $"sum_p", $"sum_f")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("derived aliases between windows cut the chain") {
    // A derived alias is not a transparent link: the project stays between the windows so
    // hoisting cannot pull its inputs through every reordered exchange. The two windows
    // are separate one-window chains, so there is nothing to stack-reorder, but the legacy
    // pairwise Window-Project-Window swap still applies at the cut ([k1] is a proper
    // subset of [k1, k2]): flag-on must produce the flag-off plan.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a",
        (v + u).as("s"))
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      Optimize.execute(analyzed)
    }
    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", (v + u).as("s"), $"sum_b")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
      comparePlans(optimized, flagOff)
    }
  }

  test("reorderable chain above a derived-alias cut swaps its bottom window across") {
    // W_c[k1, k2, k3] - Project(derived) - W_b[k1, k2] - W_a[k1]: the derived project cuts W_c
    // off from the chain above it. Flag-on first swaps the chain's bottom window W_b
    // across the cut, then the shortened one-window chain swaps W_a across too -- the
    // same two swaps flag-off performs. The merged chain below the cut is re-collected on
    // the next fixed-point iteration and regrouped with [k1] at the bottom, so both
    // flags converge to the same window stack (one exchange keyed [k1], down from three).
    val query = wideRelation
      .window(Seq(sum(v).as("sum_c")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_c", (v + u).as("s"))
      .window(Seq(sum(v).as("sum_b")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      OptimizeToFixedPoint.execute(analyzed)
    }
    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_b")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_c")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_b", $"sum_a", $"sum_c")
      .select(k1, k2, k3, k4, v, u, $"sum_c", (v + u).as("s"), $"sum_b", $"sum_a")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = OptimizeToFixedPoint.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
      // The legacy pairwise path converges to the same window stack; only the placement
      // of the order-restoring projects differs.
      val windowSpecs = (p: LogicalPlan) => p.collect {
        case w: Window => w.partitionSpec.map(_.sql)
      }
      assert(windowSpecs(optimized) === windowSpecs(flagOff))
    }
  }

  test("cut-boundary swap re-applies the links above the swapped window") {
    // W_c[k1, k2, k3] - Project(derived) - W_b[k1, k2] - Project(k1, k2, k3, v, sum_b,
    // u AS u2; drops k4) - W_a[k1]: the derived project cuts W_c off, so flag-on swaps
    // the chain's bottom window W_b across the cut. Rebuilding the chain must re-apply
    // the rename link between W_b and W_a: without it the link disappears, the dropped
    // k4 reappears in the stack, and the upper window sinks to the bottom. W_a swaps
    // across the cut too, and the regrouped stack converges to the flag-off window
    // order.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_c")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_c", (v + u).as("s"))
      .window(Seq(sum(v).as("sum_b")), Seq(k1, k2), order)
      .select(k1, k2, k3, v, $"sum_b", $"u".as("u2"))
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      OptimizeToFixedPoint.execute(analyzed)
    }

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_c")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_c", (v + u).as("s"), $"sum_b")
      .select(k1, k2, k3, v, $"sum_b", $"u".as("u2"))
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = OptimizeToFixedPoint.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
      // The legacy pairwise path converges to the same window stack; only the placement
      // of the order-restoring projects differs.
      val windowSpecs = (p: LogicalPlan) => p.collect {
        case w: Window => w.partitionSpec.map(_.sql)
      }
      assert(windowSpecs(optimized) === windowSpecs(flagOff))
    }
  }

  test("rank filter pins a one-window chain over a derived-alias cut") {
    // The rank window sits on a derived project over another window. The pairwise swap
    // at the cut would bury it under the lower window and lose WindowGroupLimit, so the
    // pin exempts it. (With the flag off there is no pin concept and the swap fires.)
    val query = wideRelation
      .window(Seq(sum(v).as("sum_c")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_c", (v + u).as("s"))
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("rank filter pins the top window of a rejected chain") {
    // The running-sum window's empty ORDER BY with a relative frame makes `reorderable`
    // reject the chain, so the legacy pairwise fallback runs. Without the pin the
    // fallback swaps rn below the [k1, k2, k3] window ([k1, k2] is a proper subset of
    // [k1, k2, k3]), burying it away from the Filter and losing the WindowGroupLimit
    // rewrite; the pin bounds the fallback to the windows below rn. The running sum
    // partitions by k3 so rn cannot ride a lower exchange -- if it could, the pin would
    // be skipped by design (see `topWindowRidesLowerExchangeUnpinned`).
    val runningSumOnK3 = WindowExpression(
      sum(v),
      WindowSpecDefinition(Seq(k3), Seq.empty,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("run_sum")
    val query = wideRelation
      .window(Seq(runningSumOnK3), Seq(k3), Seq.empty)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned rejected chain still swaps the windows below the pin") {
    // The empty ORDER BY with a relative frame rejects the stack reorder, so the pin
    // runs the legacy pairwise fallback bounded below the rank window. The [k1] sum
    // over the [k1, k2, k3] sum is a proper-subset pair, so it still swaps while rn
    // stays directly under the filter and the WindowGroupLimit rewrite survives. The
    // running sum partitions by [k4], a subset of no lower spec, so no second pair
    // forms and the swapping stops there.
    val runningSumOnK4 = WindowExpression(
      sum(v),
      WindowSpecDefinition(Seq(k4), Seq.empty,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("run_sum")
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(runningSumOnK4), Seq(k4), Seq.empty)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", $"sum_b")
      .window(Seq(runningSumOnK4), Seq(k4), Seq.empty)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
      comparePlans(Optimize.execute(analyzed),
        TransposeWindow.apply(Optimize.execute(analyzed)))
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned rejected chain lets lower swaps cascade below the pin") {
    // Same stack with the running sum on [k3], a proper subset of [k1, k2, k3]: once
    // the [k1] sum has sunk, the running sum sits above the swap's project and sinks
    // below the [k1, k2, k3] sum as well. The bounded pass fires both in one walk,
    // because it rewrites each node's children before the node itself; rn stays
    // directly under the filter throughout.
    val runningSumOnK3 = WindowExpression(
      sum(v),
      WindowSpecDefinition(Seq(k3), Seq.empty,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("run_sum")
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(runningSumOnK3), Seq(k3), Seq.empty)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(runningSumOnK3), Seq(k3), Seq.empty)
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", $"sum_b", $"run_sum")
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
      comparePlans(Optimize.execute(analyzed),
        TransposeWindow.apply(Optimize.execute(analyzed)))
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("derived project splits the stack into separately reordered chains") {
    // W-W-Project(derived)-W-W: the derived project cuts the stack. Each side still has
    // two windows, so each chain is stack-reordered alone ([k1, k2] before [k1, k2, k3]) while
    // the project stays in the middle instead of being hoisted over all four windows.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p1")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f1")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_p1", $"sum_f1", (v + u).as("s"))
      .window(Seq(sum(v).as("sum_p2")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f2")), Seq(k1, k2), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f1")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p1")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_p1", $"sum_f1")
      .select(k1, k2, k3, k4, v, u, $"sum_p1", $"sum_f1", (v + u).as("s"))
      .window(Seq(sum(v).as("sum_f2")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p2")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_p1", $"sum_f1", $"s", $"sum_p2", $"sum_f2")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("pairwise fallback does not rewrite a lower reordered chain") {
    // Bottom-to-top, the lower stack becomes (k3), (k1, k3), (k1). Its top pair is
    // legacy-swappable; Rand rejects the upper chain, but its pairwise fallback must stop
    // at the derived Project rather than descending and swapping that lower pair.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_k3")), Seq(k3), order)
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_k13")), Seq(k1, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_k3", $"sum_k1", $"sum_k13",
        (v + u).as("s"))
      .window(Seq(sum(v).as("sum_u1")), Seq(k2), order)
      .window(Seq(Rand(0).as("nd"), sum(v).as("sum_u2")), Seq(k4), order)

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_k3")), Seq(k3), order)
      .window(Seq(sum(v).as("sum_k13")), Seq(k1, k3), order)
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), order)
      .select(k1, k2, k3, k4, v, u, $"sum_k3", $"sum_k1", $"sum_k13")
      .select(k1, k2, k3, k4, v, u, $"sum_k3", $"sum_k1", $"sum_k13",
        (v + u).as("s"))
      .window(Seq(sum(v).as("sum_u1")), Seq(k2), order)
      .window(Seq(Rand(0).as("nd"), sum(v).as("sum_u2")), Seq(k4), order)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = TransposeWindow.apply(query.analyze)
      comparePlans(optimized, correctAnswer.analyze)
      comparePlans(optimized, TransposeWindow.apply(optimized))
    }
  }

  test("nondeterministic project between windows blocks stack reordering") {
    // Same window pair as the derived-alias cut test, but the between-window project adds
    // a nondeterministic alias. `isTransparentLink` rejects it, so the project is never
    // hoisted as a link and the chain is cut; the legacy pairwise swap at the cut still
    // applies (it keeps the project above both windows), as with the flag off.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", Rand(0).as("r"))
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      Optimize.execute(analyzed)
    }
    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", Rand(0).as("r"), $"sum_b")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
      comparePlans(optimized, flagOff)
    }
  }

  test("stack reordering preserves attributes across two transparent links") {
    // Two rename links that survive RemoveNoopOperators: the lower renames `sum_f` to `sf`;
    // the upper renames `sum_k1` to `sk` (so `sum_k1` is absent from the chain output)
    // while still referencing it. After hoist, the lower link must pass `sum_k1` through
    // for that rename (`refsAbove`); `boundaryIds` alone is not enough.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_f".as("sf"))
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), order)
      .select(k1, k2, k3, k4, v, u, $"sf", $"sum_k1".as("sk"))
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_f".as("sf"), $"sum_k1", $"sum_p")
      .select(k1, k2, k3, k4, v, u, $"sf", $"sum_k1".as("sk"), $"sum_p")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("window reading a between-link rename blocks stack reordering") {
    // `sf` renames the lower window's `sum_f`, and the upper window consumes it. The
    // window-output reference check sees different attributes; the alias guard must reject
    // the chain so reordering cannot move the consumer below the rename.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_f".as("sf"))
      .window(Seq(sum($"sf").as("sum_sf")), Seq(k1), order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      // comparePlans also runs CheckAnalysis on the transformed plan.
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("derived alias feeding an upper window cuts the chain") {
    // The derived alias `s = v + u` feeds the upper window. It is not a transparent link,
    // so the chain is cut and the plan is left untouched.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a",
        (v + u).as("s"))
      .window(Seq(sum($"s").as("sum_s")), Seq(k1), order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("window with an empty partition spec blocks stack reordering") {
    // Empty partition specs reject the whole-chain reorder; flag-on must still match
    // flag-off's pairwise path (which may still swap the empty-spec window under a
    // compatible non-empty neighbor).
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_all")), Seq.empty, order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      Optimize.execute(analyzed)
    }
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), flagOff)
    }
  }

  // A window with no ORDER BY and an explicit relative frame. Post-analysis it keeps the
  // `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame it was given.
  private def runningSumAlias = WindowExpression(
    sum(v),
    WindowSpecDefinition(Seq(k1), Seq.empty,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("run_sum")

  test("window with an empty order spec and a relative frame blocks stack reordering") {
    // The running-sum window has no ORDER BY, so its relative frame covers whatever row
    // prefix the operator receives, in the delivery order established by the nearest window
    // below it. A full stack permute would change results, so reorderable rejects the
    // chain; flag-on must still match flag-off's pairwise transposition.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(runningSumAlias), Seq(k1), Seq.empty)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      Optimize.execute(analyzed)
    }
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), flagOff)
    }
  }

  test("window with an empty order spec and the whole-partition frame is still reorderable") {
    // Without an explicit frame, analysis assigns the whole-partition frame
    // (`UNBOUNDED PRECEDING` to `UNBOUNDED FOLLOWING`) to an empty-order window, which
    // covers the same rows under any row order, so it does not pin the chain's order.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), Seq.empty)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), Seq.empty)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u", $"sum_f", $"sum_k1")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("a single window without a cut below is left unchanged") {
    // A lone window has nothing to reorder, and with a relation child there is no
    // Window-Project-Window cut-boundary swap either, so the plan is a no-op.
    val query = wideRelation.window(Seq(sum(v).as("s1")), Seq(k1), order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  private def rankOnK1K2K3 = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(Seq(k1, k2, k3), order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  private def rankOnK1K2 = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(Seq(k1, k2), order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  // Same spec as `rankOnK1K2` under a different alias, for stacks with two row_numbers.
  private def rowNumberOnK1K2 = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(Seq(k1, k2), order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("r")

  // A whole-partition sum on [k1, k2] with no ORDER BY, as SQL analysis shapes it: covers
  // the same rows under any row order, so it is reorderable and can merge into an ordered
  // same-spec window, taking that window's order spec.
  private def wholePartitionSumOnK1K2 = WindowExpression(
    sum(v),
    WindowSpecDefinition(Seq(k1, k2), Seq.empty,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("r")

  // A sum sharing the rank's [k1, k2] spec with an explicit relative frame, as SQL
  // analysis shapes `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW` in the same window
  // operator as a rank: mixed expanding and non-expanding expressions.
  private def relativeSumOnK1K2 = WindowExpression(
    sum(v),
    WindowSpecDefinition(Seq(k1, k2), order,
      SpecifiedWindowFrame(RowFrame, Literal(-3), CurrentRow))).as("rs")

  private def rankOnK1OrderK2 = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(Seq(k1), Seq(k2.asc),
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  private def rankOnK1K2OrderV = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(Seq(k1, k2), Seq(v.asc),
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  private def denseRankOnK1K2K3 = WindowExpression(
    DenseRank(Seq.empty),
    WindowSpecDefinition(Seq(k1, k2, k3), order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("dr")

  /**
   * CollapseWindow + TransposeWindow to a fixed point (as in the operator-optimization
   * batch), normalize the rank bound (including the IN/NOT shapes OptimizeIn and
   * BooleanSimplification rewrite), optionally push filters through top projects, then
   * InferWindowGroupLimit.
   */
  private object OptimizeWithGroupLimit extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator", FixedPoint(10),
        CollapseWindow, TransposeWindow, PushDownPredicates) ::
      Batch("NormalizeFilter", Once, ConstantFolding, UnwrapCastInBinaryComparison,
        OptimizeIn, BooleanSimplification) ::
      Batch("InferWindowGroupLimit", Once, InferWindowGroupLimit) :: Nil
  }

  test("rank filter on the top window does not pin when it rides lower exchange") {
    // The unpinned `windowGroups` order keeps rn on top while moving [k1, k2] below
    // [k1, k2, k3], so the top window still rides the lower [k1, k2] exchange. Prefer
    // that lower-exchange plan and accept losing WindowGroupLimit instead of pinning
    // just to protect it.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f", $"rn")
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.isEmpty)
    }
  }

  test("rank filter pins when unpinned order would displace the rank window") {
    // The unpinned `windowGroups` order is [0, 2, 1]: rn lands under the incomparable
    // [k1, k4]
    // window and InferWindowGroupLimit cannot match. The pin must keep rn on top. Putting
    // same-spec sum_f last under the pin would let rn ride that exchange, but CollapseWindow
    // would then merge the non-expanding sum into rn and defeat WGL, so the merge-safe order
    // leaves the under-pin windows unchanged.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val withoutFilter = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)

    val buriedRank = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_s", $"rn")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(withoutFilter.analyze), buriedRank.analyze)
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("rank filter pin overrides the wide-output group ranking") {
    // evs [k4] is the wide group and would rank last, but the pinned rn rides only the
    // [k1, k2] group's exchange, which must therefore stay last; the original order is
    // already that order, so the plan is unchanged and the group limit still fires.
    val query = wideRelation
      .window(Seq(CollectList(v).toAggregateExpression().as("evs")), Seq(k4), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("single-element IN rank filter pins the top window") {
    // OptimizeIn rewrites `rn IN (1)` to `rn = 1` later in the same fixed-point batch, so
    // TransposeWindow can see the IN shape first; the pin check simulates that
    // normalization (see `normalizeRankFilterCondition`) so the pin still fires. Same
    // displacing stack as the rank-filter pin test above.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where(In($"rn", Seq(Literal(1))))

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("negated comparison rank filter pins the top window") {
    // BooleanSimplification rewrites `NOT (rn > 1)` to `rn <= 1` later in the same
    // fixed-point batch, so TransposeWindow can see the NOT shape first; the pin check
    // simulates that normalization (see `normalizeRankFilterCondition`) so the pin still
    // fires. Same displacing stack as the rank-filter pin test.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where(!($"rn" > 1))

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("rank filters that cannot yield a group limit do not pin the top window") {
    // extractLimits recognizes only equality and upper bounds against an integer
    // literal, and the later normalizing rules turn none of these into one (the
    // rejected selectRankCandidate cases yield no group limit either): no
    // WindowGroupLimit follows, so pinning would only cost the exchange the reorder
    // saves. The flag-on reorder must bury the rank window under the superset-spec
    // window exactly as the flag-off pairwise swap does.
    val noPinConditions = Seq(
      $"rn" > 1, // lower bound
      $"rn" =!= 1, // inequality
      In($"rn", Seq(Literal(1), Literal(2))), // multi-value IN
      $"rn" <= 1 || $"u" > 0, // disjunction
      $"rn" <= $"v", // non-literal bound
      $"rn" <= 1001, // over windowGroupLimitThreshold (default 1000)
      $"rn" <= 3) // known child cardinality (3 rows) makes the rewrite unnecessary
    val windowSpecs = (p: LogicalPlan) => p.collect {
      case w: Window => w.partitionSpec.map(_.sql)
    }
    noPinConditions.foreach { cond =>
      val query = wideRelation
        .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
        .window(Seq(rankOnK1K2), Seq(k1, k2), order)
        .where(cond)

      val analyzed = query.analyze
      val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
        OptimizeToFixedPoint.execute(analyzed)
      }
      withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
        val optimized = OptimizeToFixedPoint.execute(analyzed)
        assert(windowSpecs(optimized) === windowSpecs(flagOff))
        assert(OptimizeWithGroupLimit.execute(analyzed)
          .collect { case _: WindowGroupLimit => () }.isEmpty)
      }
    }
  }

  test("rank filter does not pin a mixed expanding and relative-frame top window") {
    // `selectRankCandidate` inspects only the rank-like expressions, so it still finds
    // `rn` in the mixed window and would have the pin fire; `InferWindowGroupLimit`
    // however rewrites only windows whose every expression is expanding, so no
    // WindowGroupLimit can ever follow the mixed top window. Pinning it would only cost
    // the exchange the reorder saves. The flag-on reorder must bury the mixed window
    // under the superset-spec window exactly as the flag-off pairwise swap does.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2, relativeSumOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze
    val windowSpecs = (p: LogicalPlan) => p.collect {
      case w: Window => w.partitionSpec.map(_.sql)
    }
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      OptimizeToFixedPoint.execute(analyzed)
    }
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = OptimizeToFixedPoint.execute(analyzed)
      assert(windowSpecs(optimized) === windowSpecs(flagOff))
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.isEmpty)
    }
  }

  test("rank filters normalized to a bound by chained rules still pin the top window") {
    // `rn IN (1, 1)` dedups to a single value that OptimizeIn turns into `rn = 1`, and
    // `NOT (rn <> 1)` parses as `NOT (NOT (rn = 1))`, which BooleanSimplification's
    // double-negation rewrite turns back into `rn = 1`. Both reduce to a bound
    // extractLimits classifies, so they pin like `rn = 1` itself. Same displacing stack
    // as the rank-filter pin test.
    val pinConditions = Seq(
      In($"rn", Seq(Literal(1), Literal(1))),
      !($"rn" =!= 1))
    pinConditions.foreach { cond =>
      val query = wideRelation
        .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
        .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
        .window(Seq(rankOnK1K2), Seq(k1, k2), order)
        .where(cond)

      val analyzed = query.analyze

      withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
        comparePlans(Optimize.execute(analyzed), analyzed)
        assert(OptimizeWithGroupLimit.execute(analyzed)
          .collect { case _: WindowGroupLimit => () }.nonEmpty)
      }
    }
  }

  test("pinned rank window schedules a ridable lower group last") {
    // Pin on [k1, k2]; lower windows [k1] (pin can ride, different spec so no merge) then
    // [k3] (incomparable). Ignoring the pin keeps that order (3 exchanges). Putting [k3]
    // then [k1] under the pin lets rn ride the [k1] exchange (2 exchanges).
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_b")), Seq(k3), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k3), order)
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", $"sum_b")
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
      comparePlans(Optimize.execute(analyzed),
        TransposeWindow.apply(Optimize.execute(analyzed)))
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned reorder moves the last ridable group last") {
    // Pin on [k1, k2, k3]: the pin rides the exchange of a group whose leader spec is a
    // subset of the pin's, so it can ride s1 [k1] or s2 [k3] but not s3 [k4]. The last
    // rideable group in emission order is s2's, so it moves above s3; only the last
    // group decides the outcome.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s2")), Seq(k3), order)
      .window(Seq(sum(v).as("s3")), Seq(k4), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s3")), Seq(k4), order)
      .window(Seq(sum(v).as("s2")), Seq(k3), order)
      .select(k1, k2, k3, k4, v, u, $"s1", $"s2", $"s3")
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned reorder decides merge safety without enumerating window orders") {
    // Ten sums sharing the pin's partition spec but with empty ORDER BY over a
    // whole-partition frame: the rank window's longer required ordering leads the
    // unpinned group, so the unpinned order displaces it off the top and the pin fires
    // (see `topWindowRidesLowerExchangeUnpinned`). Emitted as any candidate's tail, a sum
    // inherits the pin's order spec and merges into it
    // (collapseWindowWithEmptyOrderSpecInChild) with a non-expanding expression, so no
    // under-pin tail is safe and the search must fall back to the identity order. The
    // probe checks one order per (last group, tail) pair, and the count assertion pins
    // that bound: enumerating 10! group-member permutations (or the group orders of a
    // many-group stack) would blow the count past it. The sums are explicit
    // `WindowExpression`s so their whole-partition frames are visible to the merge check
    // (the DSL leaves bare aggregates).
    // The rule is applied directly, as in the other pin tests, so the chain is not first
    // folded by `CollapseWindow`.
    val emptyOrderSum = (i: Int) => WindowExpression(
      sum(v),
      WindowSpecDefinition(Seq(k1, k2, k3), Seq.empty,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as(s"s$i")
    val chained = (1 to 10).foldLeft(wideRelation: LogicalPlan) { (plan, i) =>
      plan.window(Seq(emptyOrderSum(i)), Seq(k1, k2, k3), Seq.empty)
    }
    val query = chained
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    withSQLConf(
      SQLConf.WINDOW_REORDER_ENABLED.key -> "true",
      SQLConf.COLLAPSE_WINDOW_WITH_EMPTY_ORDER_SPEC_IN_CHILD.key -> "true") {
      TransposeWindow.groupLimitProbeCount.set(0)
      comparePlans(TransposeWindow.apply(analyzed), analyzed)
      // One group of ten defeating members: at most one probe per member tail.
      assert(TransposeWindow.groupLimitProbeCount.get() <= 10)
    }
  }

  test("rank filter does not pin when the top window rides the leader's exchange") {
    // One group under the top rank window: without pinning, the `windowGroups` order
    // still leaves
    // rn on top and lets it ride the leader's [k1, k2] exchange, so prefer that order even
    // though the pin would have picked a different safe tail to preserve WindowGroupLimit.
    val query = wideRelation
      .window(Seq(sum(v).as("s0")), Seq(k1, k4, k2), order)
      .window(Seq(sum(v).as("s1")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("s2")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s2")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("s0")), Seq(k1, k4, k2), order)
      .window(Seq(sum(v).as("s1")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"s0", $"s1", $"s2", $"rn")
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.isEmpty)
    }
  }

  test("pinned reorder prefers a later rideable group over an earlier leader-only-safe group") {
    // Two rideable groups under the pin. The earlier group's leader [k1, k2] has only
    // same-spec non-leaders, all of which would merge into the pin, so placing it
    // last would hoist the leader above them and split its exchange. The later group
    // [k1, k3] has a safe non-leader, so it keeps leader-first and stays a single exchange.
    // The greedy last-qualifying selection must keep the later group last, which here is the
    // original (already optimal) order.
    val query = wideRelation
      .window(Seq(sum(v).as("a")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("b")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("c")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("d")), Seq(k1, k3), order)
      .window(Seq(sum(v).as("e")), Seq(k1, k3, k4), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned reorder under requireAllClusterKeysForDistribution rides only exact specs") {
    // The pin is on (k1, k2, k3) and the window with that same spec uses a different order
    // spec, so it never merges into the pin. Under the strict clustering model the pin can
    // only ride a group keyed on exactly (k1, k2, k3), so that group goes last; under the
    // default subset model it also rides the (k1, k2) group, which is preferred instead.
    val orderK2 = Seq(k2.asc)
    val query = wideRelation
      .window(Seq(sum(v).as("a")), Seq(k1, k2, k3), orderK2)
      .window(Seq(sum(v).as("b")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("c")), Seq(k1, k4), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val strictAnswer = wideRelation
      .window(Seq(sum(v).as("b")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("c")), Seq(k1, k4), order)
      .window(Seq(sum(v).as("a")), Seq(k1, k2, k3), orderK2)
      .select(k1, k2, k3, k4, v, u, $"a", $"b", $"c")
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    val subsetAnswer = wideRelation
      .window(Seq(sum(v).as("c")), Seq(k1, k4), order)
      .window(Seq(sum(v).as("b")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("a")), Seq(k1, k2, k3), orderK2)
      .select(k1, k2, k3, k4, v, u, $"a", $"b", $"c")
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 1)

    withSQLConf(
      SQLConf.WINDOW_REORDER_ENABLED.key -> "true",
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), strictAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), subsetAnswer.analyze)
    }
  }

  test("rank filter with a widening cast bound still pins the top window") {
    // extractLimits only matches IntegerLiteral; without normalizing CAST(rn AS BIGINT) <= 1L
    // the pin misses, the unpinned order yields a, rn, s, and WindowGroupLimit is lost.
    // Differing order specs keep CollapseWindow from merging same-partition sum_a into rn,
    // so the pin can put the ridable [k1] group last.
    val orderV = Seq(v.asc)
    val orderVDesc = Seq(v.desc)
    val rnAlias = WindowExpression(
      RowNumber(),
      WindowSpecDefinition(Seq(k1), orderVDesc,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1), orderV)
      .window(Seq(sum(v).as("sum_s")), Seq(k2), orderV)
      .window(Seq(rnAlias), Seq(k1), orderVDesc)
      .where($"rn" <= Literal(1L))

    val analyzed = query.analyze

    val withoutFilter = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1), orderV)
      .window(Seq(sum(v).as("sum_s")), Seq(k2), orderV)
      .window(Seq(rnAlias), Seq(k1), orderVDesc)

    val buriedRank = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1), orderV)
      .window(Seq(rnAlias), Seq(k1), orderVDesc)
      .window(Seq(sum(v).as("sum_s")), Seq(k2), orderV)
      .select(k1, k2, k3, k4, v, u, $"sum_a", $"sum_s", $"rn")

    val pinnedAnswer = wideRelation
      .window(Seq(sum(v).as("sum_s")), Seq(k2), orderV)
      .window(Seq(sum(v).as("sum_a")), Seq(k1), orderV)
      .select(k1, k2, k3, k4, v, u, $"sum_a", $"sum_s")
      .window(Seq(rnAlias), Seq(k1), orderVDesc)
      .where($"rn" <= Literal(1L))

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(withoutFilter.analyze), buriedRank.analyze)
      comparePlans(Optimize.execute(analyzed), pinnedAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("rank filter does not pin when the top window rides lower [k1] exchange") {
    // Pinning could avoid the WGL-defeating merge by choosing (a, b, c), but the unpinned
    // exchange-optimal order b, c, a still leaves rn on top and lets it reuse the lower [k1]
    // exchange. Prefer that lower-exchange plan and give up WindowGroupLimit.
    val orderK1 = Seq(k1.asc)
    val orderK2 = Seq(k2.asc)
    val query = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k3), orderK1)
      .window(Seq(sum(v).as("sum_a")), Seq(k1), orderK2)
      .window(Seq(sum(v).as("sum_c")), Seq(k3), orderK2)
      .window(Seq(rankOnK1OrderK2), Seq(k1), orderK2)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k3), orderK1)
      .window(Seq(sum(v).as("sum_c")), Seq(k3), orderK2)
      .window(Seq(sum(v).as("sum_a")), Seq(k1), orderK2)
      .window(Seq(rankOnK1OrderK2), Seq(k1), orderK2)
      .select(k1, k2, k3, k4, v, u, $"sum_b", $"sum_a", $"sum_c", $"rn")
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.isEmpty)
    }
  }

  test("stacked transparent projects do not force pinning when top window rides lower exchange") {
    // Transparent renames do not cut the chain. The unpinned order keeps rn on top of the
    // [k1] group, so it rides that exchange, the pin is skipped, and WindowGroupLimit is
    // lost. Hoisted renames are not a bare window stack (see `rebuildReorderedStack`).
    // CollapseWindow then merges the two same-spec [k1] windows, so the specs bottom to
    // top are sum_b, sum_c, and the merged [k1] window (sum_a and rn).
    val orderK1 = Seq(k1.asc)
    val orderK2 = Seq(k2.asc)
    val query = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k3), orderK1)
      .select(k1, k2, k3, k4, v, u, $"sum_b".as("b"))
      .window(Seq(sum(v).as("sum_a")), Seq(k1), orderK2)
      .select(k1, k2, k3, k4, v, u, $"b", $"sum_a".as("a"))
      .window(Seq(sum(v).as("sum_c")), Seq(k3), orderK2)
      .select(k1, k2, k3, k4, v, u, $"b", $"a", $"sum_c".as("c"))
      .window(Seq(rankOnK1OrderK2), Seq(k1), orderK2)
      .where($"rn" <= 1)

    val analyzed = query.analyze
    // `collect` walks top-down; reverse is the emitted bottom-to-top order.
    val windowsBottomToTop = (p: LogicalPlan) => p.collect { case w: Window => w }.reverse
    val windowSpecs = (ws: Seq[Window]) => ws.map { w =>
      (w.partitionSpec.map(_.sql), w.orderSpec.map(_.sql))
    }
    val lowerExchangeOrder = Seq(
      (Seq(k3), orderK1),
      (Seq(k3), orderK2),
      (Seq(k1), orderK2)).map {
      case (parts, orders) => (parts.map(_.sql), orders.map(_.sql))
    }
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = OptimizeWithGroupLimit.execute(analyzed)
      val windows = windowsBottomToTop(optimized)
      assert(windowSpecs(windows) === lowerExchangeOrder)
      assert(windows.last.windowExpressions.length == 2)
      assert(windows.last.windowExpressions.exists(_.exists(_.isInstanceOf[RowNumber])))
      assert(optimized.collect { case _: WindowGroupLimit => () }.isEmpty)
    }
  }

  test("pinned reorder checks merge safety against the candidate's real order") {
    // Bottom-to-top: RANGE sum s [k1, k2], x [k3], row_number r [k1, k2], y [k4], all
    // ORDER BY k1, under rn [k1, k2] ORDER BY k1 with rn <= 1. r alone is a merge-safe
    // tail, but the order it emits packs its group below it: x, y, s, r. CollapseWindow
    // then merges bottom-up -- s into r, then the merged RANGE sum into rn -- defeating
    // InferWindowGroupLimit. Rejecting that candidate gives up the ridable [k1, k2] group
    // (its other member s merges into rn directly) and puts y last instead.
    val query = wideRelation
      .window(Seq(sum(v).as("s")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("x")), Seq(k3), order)
      .window(Seq(rowNumberOnK1K2), Seq(k1, k2), order)
      .window(Seq(sum(v).as("y")), Seq(k4), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s")), Seq(k1, k2), order)
      .window(Seq(rowNumberOnK1K2), Seq(k1, k2), order)
      .window(Seq(sum(v).as("x")), Seq(k3), order)
      .window(Seq(sum(v).as("y")), Seq(k4), order)
      .select(k1, k2, k3, k4, v, u, $"s", $"x", $"r", $"y")
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned merge safety sees the order an empty-order window inherits on merge") {
    // Same stack, but r is a whole-partition sum with no ORDER BY. On its own r cannot
    // merge into rn: an empty-order child merging into an ordered parent is gated by
    // spark.sql.optimizer.collapseWindowWithEmptyOrderSpecInChild (off by default). But
    // the emitted order packs r's group below it (x, y, s, r), and r takes s's ORDER BY
    // k1 when CollapseWindow merges them (that direction is not gated); the merged window
    // then merges into rn. Probing the candidate's real order rejects r as a tail.
    val query = wideRelation
      .window(Seq(sum(v).as("s")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("x")), Seq(k3), order)
      .window(Seq(wholePartitionSumOnK1K2), Seq(k1, k2), Seq.empty)
      .window(Seq(sum(v).as("y")), Seq(k4), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s")), Seq(k1, k2), order)
      .window(Seq(wholePartitionSumOnK1K2), Seq(k1, k2), Seq.empty)
      .window(Seq(sum(v).as("x")), Seq(k3), order)
      .window(Seq(sum(v).as("y")), Seq(k4), order)
      .select(k1, k2, k3, k4, v, u, $"s", $"x", $"r", $"y")
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("attribute-only top project does not force pinning when top window rides lower exchange") {
    // The transparent top project still lets the filter target rn, but unpinned reorder keeps
    // rn on top and lets it ride the lower [k1, k2] exchange, so prefer that lower-exchange
    // plan over preserving WindowGroupLimit.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_p", $"rn")
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_p", $"rn")
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.isEmpty)
    }
  }

  test("non-rank filter above the chain allows full reordering") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"sum_f" > 5)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f", $"rn")
      .where($"sum_f" > 5)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("over-threshold rank filter above the chain allows full reordering") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 2000)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f", $"rn")
      .where($"rn" <= 2000)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("competing rank predicates pin only the candidate InferWindowGroupLimit selects") {
    // The window carries both a row_number and a dense_rank expression. The group-limit
    // rule prefers RowNumber and checks only the selected function's limit: `rn <= 2000`
    // is over the threshold, so the rewrite never happens and the chain must stay fully
    // reorderable, even though the dense_rank limit `dr <= 1` alone would have qualified.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3, denseRankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 2000 && $"dr" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3, denseRankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f", $"rn", $"dr")
      .where($"rn" <= 2000 && $"dr" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("a rank filter within a small known child does not pin the top window") {
    // `wideRelation` has 3 rows, so a limit of 3 satisfies the threshold but the
    // group-limit rewrite is unnecessary (child.maxRows is not greater than the limit);
    // pinning would only retain an avoidable exchange.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .where($"rn" <= 3)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f", $"rn")
      .where($"rn" <= 3)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("renaming top project does not force pinning when top window rides lower exchange") {
    // The top project still renames the rank attribute through a transparent link, but the
    // unpinned order keeps rn on top and lets it reuse the lower [k1, k2] exchange.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"rn".as("rank"))
      .where($"rank" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"rn".as("rank"))
      .where($"rank" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.isEmpty)
    }
  }

  test("two renaming top projects do not force pinning when top window rides lower exchange") {
    // Two stacked rename-only top projects: the filter still resolves through both aliases,
    // but the selective policy prefers the lower-exchange unpinned order once rn remains on
    // top and can reuse the lower [k1, k2] exchange. Apply `TransposeWindow` alone so the
    // alias peel is tested before `CollapseProject` can fold the renames.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"rn".as("rank"))
      .select(k1, k2, k3, k4, v, u, $"rank".as("rank2"), $"rank".as("rank3"))
      .where($"rank2" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"rn".as("rank"))
      .select(k1, k2, k3, k4, v, u, $"rank".as("rank2"), $"rank".as("rank3"))
      .where($"rank2" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.isEmpty)
    }
  }

  test("renamed rank filter pins the top window through the alias peel") {
    // Same displacing stack as the rank-filter pin test, but a transparent top link
    // renames rn to rank and the filter references the rename. TransposeWindow runs
    // before PushDownPredicates in the batch, so it sees the Filter above the link, and
    // only the alias peel in shouldPinTopWindow maps rank back to rn; without it the
    // condition matches no rank output, nothing pins, and the reorder buries the rank
    // window under the incomparable [k1, k4] window, losing the WindowGroupLimit.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_s", $"rn".as("rank"))
      .where($"rank" <= 1)

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("single-element IN rank filter does not pin when the group-limit rewrite is off") {
    // With windowGroupLimitThreshold = -1 nothing consumes the pin, so even a
    // normalizable rank filter must not pin: the displacing stack reorders exactly as
    // unpinned, burying the rank window under the incomparable [k1, k4] window.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .where(In($"rn", Seq(Literal(1))))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_s")), Seq(k1, k4), order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_s", $"rn")
      .where(In($"rn", Seq(Literal(1))))

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true",
      SQLConf.WINDOW_GROUP_LIMIT_THRESHOLD.key -> "-1") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
    }
  }

  test("non-rank condition on a computed top project does not pin the top window") {
    // The top project computes derived `t = v + u`, so it is not a transparent link. The
    // filter's child is therefore not collected as a chain from the Filter (no pin). The
    // project still bounds the window stack below it: stack reorder runs with that project
    // as parent (unpinned) and leaves the derived project in place above the windows.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, v, u, $"rn", (v + u).as("t"))
      .where($"t" <= 2)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("sum_p")), Seq(k1, k2, k3), order)
      .window(Seq(rankOnK1K2K3), Seq(k1, k2, k3), order)
      .select(k1, k2, k3, k4, $"rn", $"sum_f", $"sum_p", u, v)
      .select(k1, k2, k3, k4, v, u, $"rn", (v + u).as("t"))
      .where($"t" <= 2)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

}
