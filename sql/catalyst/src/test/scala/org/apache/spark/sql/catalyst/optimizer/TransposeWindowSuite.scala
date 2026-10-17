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

import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{
  Concat, CurrentRow, DenseRank, Literal, Rand, RowFrame, RowNumber, SpecifiedWindowFrame,
  UnboundedFollowing, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
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

  // specF is a strict subset of specP; specS is incomparable to both.
  private val specF = Seq(k1, k2)
  private val specKF = Seq(k2, k1)
  private val specP = Seq(k1, k2, k3)
  private val specS = Seq(k1, k4)
  // Partition spec with an extra key between two shared keys (subset-compatible with specF).
  private val specFExtra = Seq(k1, k4, k2)
  private val order = Seq(k1.asc)

  test("don't reorder window stacks with non-deterministic expressions") {
    // Interleaved partition specs that are regrouped when stack reordering is enabled (see
    // the test below). A nondeterministic window makes `reorderable` reject the chain, and
    // here the legacy pairwise path also finds no compatible adjacent pair, so the plan
    // stays unchanged.
    val query = wideRelation
      .window(Seq(sum(v).as("f_s")), specF, order)
      .window(Seq(sum(v).as("p_s")), specP, order)
      .window(Seq(sum(v).as("s_s")), specS, order)
      .window(Seq(Rand(0).as("r"), sum(v).as("f_a")), specF, order)
      .window(Seq(sum(v).as("p_a")), specP, order)
      .window(Seq(sum(v).as("s_a")), specS, order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("reorder a window stack by grouping minimal partition specs") {
    val query = wideRelation
      .window(Seq(sum(v).as("f_s")), specF, order)
      .window(Seq(sum(v).as("p_s")), specP, order)
      .window(Seq(sum(v).as("s_s")), specS, order)
      .window(Seq(sum(v).as("f_a")), specF, order)
      .window(Seq(sum(v).as("p_a")), specP, order)
      .window(Seq(sum(v).as("s_a")), specS, order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("f_s")), specF, order)
      .window(Seq(sum(v).as("f_a")), specF, order)
      .window(Seq(sum(v).as("p_s")), specP, order)
      .window(Seq(sum(v).as("p_a")), specP, order)
      .window(Seq(sum(v).as("s_s")), specS, order)
      .window(Seq(sum(v).as("s_a")), specS, order)
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
      .window(Seq(sum(v).as("s2a")), specF, order)
      .window(Seq(sum(v).as("s2b")), specF, Seq(k2.asc))
      .window(Seq(sum(v).as("s3")), specP, Seq(k3.asc))

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("windows with equal required child orderings become adjacent within a group") {
    // All three windows share ORDER BY k3, but WindowExec prepends partition keys to the
    // required sort. The middle window's extra partition key (k4) must not sit between the
    // two specF windows, which share the complete ordering k1, k2, k3.
    val orderK3 = Seq(k3.asc)
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), specF, orderK3)
      .window(Seq(sum(v).as("s2")), specFExtra, orderK3)
      .window(Seq(sum(v).as("s3")), specF, orderK3)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s1")), specF, orderK3)
      .window(Seq(sum(v).as("s3")), specF, orderK3)
      .window(Seq(sum(v).as("s2")), specFExtra, orderK3)
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u", $"s1", $"s2", $"s3")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("reordering is idempotent when an ordering class spans groups") {
    // C [k1] and X1 [k1, k3] share the complete required ordering [k1, k3, u], but X1
    // joins group (k3). With a global first-appearance orderClass table, the first
    // application yields L0, X1, A, B, C and the second yields L0, X1, A, C, B. Ranking
    // order classes per group makes the first application a fixed point.
    val query = wideRelation
      .window(Seq(sum(v).as("L0")), Seq(k3), Seq(v.asc))
      .window(Seq(sum(v).as("A")), Seq(k1), Seq(k2.asc))
      .window(Seq(sum(v).as("B")), Seq(k1), Seq(k4.asc))
      .window(Seq(sum(v).as("C")), Seq(k1), Seq(k3.asc, u.asc))
      .window(Seq(sum(v).as("X1")), Seq(k1, k3), Seq(u.asc))

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val once = TransposeWindow.apply(analyzed)
      val twice = TransposeWindow.apply(once)
      comparePlans(once, twice)

      val correctAnswer = wideRelation
        .window(Seq(sum(v).as("L0")), Seq(k3), Seq(v.asc))
        .window(Seq(sum(v).as("X1")), Seq(k1, k3), Seq(u.asc))
        .window(Seq(sum(v).as("A")), Seq(k1), Seq(k2.asc))
        .window(Seq(sum(v).as("B")), Seq(k1), Seq(k4.asc))
        .window(Seq(sum(v).as("C")), Seq(k1), Seq(k3.asc, u.asc))
        .select($"k1", $"k2", $"k3", $"k4", $"v", $"u",
          $"L0", $"A", $"B", $"C", $"X1")
      comparePlans(once, correctAnswer.analyze)
    }
  }

  test("member with the leader's complete ordering stays beside the leader") {
    // b and the leader share no complete ordering; c matches the leader's [k1, k1] and
    // must keep the special rank -1 so it stays immediately after the leader. Without
    // that rank, pulling the leader forward would change c's class's first appearance.
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

  test("windows with identical partition specs become adjacent") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(count(v).as("count_f")), specF, order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(count(v).as("count_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .select($"k1", $"k2", $"k3", $"k4", $"v", $"u", $"sum_f", $"sum_p", $"count_f")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("requireAllClusterKeysForDistribution groups identical specs, not subsets") {
    // specF = (k1, k2) and specS = (k1, k4) both contain k1 but are incomparable; the two
    // (k1, k2) windows bookend the (k1, k4) window. Under the subset model all windows group
    // under the minimal spec (k1) and the stack is already optimally ordered. Under
    // `requireAllClusterKeysForDistribution` a window can only ride an exchange with the
    // exact same keys in the same order, so the two (k1, k2) windows must become adjacent
    // to share one exchange; the distinct order specs of the two (k1, k2) windows keep
    // `CollapseWindow` from merging them.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_f")), specF, Seq(k1.asc))
      .window(Seq(sum(v).as("sum_s")), specS, order)
      .window(Seq(sum(v).as("sum_f2")), specF, Seq(k2.asc))
    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true") {
        val optimized = Optimize.execute(analyzed)

        val correctAnswer = wideRelation
          .window(Seq(sum(v).as("s1")), Seq(k1), order)
          .window(Seq(sum(v).as("sum_f")), specF, Seq(k1.asc))
          .window(Seq(sum(v).as("sum_f2")), specF, Seq(k2.asc))
          .window(Seq(sum(v).as("sum_s")), specS, order)
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
      .window(Seq(sum(v).as("sum_fk1")), specF, Seq(k1.asc))
      .window(Seq(sum(v).as("sum_kf")), specKF, Seq(k2.asc))
      .window(Seq(sum(v).as("sum_fk2")), specF, Seq(k2.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_fk1")), specF, Seq(k1.asc))
      .window(Seq(sum(v).as("sum_fk2")), specF, Seq(k2.asc))
      .window(Seq(sum(v).as("sum_kf")), specKF, Seq(k2.asc))
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
      .window(Seq(sum(v).as("sum_p")), specP, order)  // bottom (k1, k2, k3)
      .window(Seq(sum(v).as("sum_f")), specF, order)  // top (k1, k2)
    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      val optimized = Optimize.execute(analyzed)

      val correctAnswer = wideRelation
        .window(Seq(sum(v).as("sum_f")), specF, order)
        .window(Seq(sum(v).as("sum_p")), specP, order)
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
      .window(Seq(sum(v).as("sum_f")), specF, Seq(k1.asc))
      .window(Seq(sum(v).as("sum_s")), specS, order)
      .window(Seq(sum(v).as("sum_f2")), specF, Seq(k2.asc))
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
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .select(k1, k2, k3, k4, v, $"sum_p")
      .window(Seq(sum(v).as("sum_f")), specF, order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
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
      .window(Seq(sum(v).as("sum_a")), specF, order)
      .select(k1, k2, k3, k4, v, u, $"sum_a",
        (v + u).as("s"))
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      Optimize.execute(analyzed)
    }
    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_a")), specF, order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", (v + u).as("s"), $"sum_b")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
      comparePlans(optimized, flagOff)
    }
  }

  test("reorderable chain above a derived-alias cut swaps its bottom window across") {
    // W_c[specP] - Project(derived) - W_b[specF] - W_a[k1]: the derived project cuts W_c
    // off from the chain above it. Flag-on first swaps the chain's bottom window W_b
    // across the cut, then the shortened one-window chain swaps W_a across too -- the
    // same two swaps flag-off performs. The merged chain below the cut is re-collected on
    // the next fixed-point iteration and regrouped with [k1] at the bottom, so both
    // flags converge to the same window stack (one exchange keyed [k1], down from three).
    val query = wideRelation
      .window(Seq(sum(v).as("sum_c")), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_c", (v + u).as("s"))
      .window(Seq(sum(v).as("sum_b")), specF, order)
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      OptimizeToFixedPoint.execute(analyzed)
    }
    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_b")), specF, order)
      .window(Seq(sum(v).as("sum_c")), specP, order)
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

  test("rank filter pins a one-window chain over a derived-alias cut") {
    // The rank window sits on a derived project over another window. The pairwise swap
    // at the cut would bury it under the lower window and lose WindowGroupLimit, so the
    // pin exempts it. (With the flag off there is no pin concept and the swap fires.)
    val query = wideRelation
      .window(Seq(sum(v).as("sum_c")), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_c", (v + u).as("s"))
      .window(Seq(rankOnSpecF), specF, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("derived project splits the stack into separately reordered chains") {
    // W-W-Project(derived)-W-W: the derived project cuts the stack. Each side still has
    // two windows, so each chain is stack-reordered on its own (specF before specP) while
    // the project stays in the middle instead of being hoisted over all four windows.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p1")), specP, order)
      .window(Seq(sum(v).as("sum_f1")), specF, order)
      .select(k1, k2, k3, k4, v, u, $"sum_p1", $"sum_f1", (v + u).as("s"))
      .window(Seq(sum(v).as("sum_p2")), specP, order)
      .window(Seq(sum(v).as("sum_f2")), specF, order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f1")), specF, order)
      .window(Seq(sum(v).as("sum_p1")), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_p1", $"sum_f1")
      .select(k1, k2, k3, k4, v, u, $"sum_p1", $"sum_f1", (v + u).as("s"))
      .window(Seq(sum(v).as("sum_f2")), specF, order)
      .window(Seq(sum(v).as("sum_p2")), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_p1", $"sum_f1", $"s", $"sum_p2", $"sum_f2")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("nondeterministic project between windows blocks stack reordering") {
    // Same window pair as the derived-alias cut test, but the between-window project adds
    // a nondeterministic alias. `isTransparentLink` rejects it, so the project is never
    // hoisted as a link and the chain is cut; the legacy pairwise swap at the cut still
    // applies (it keeps the project above both windows), as with the flag off.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), specF, order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", Rand(0).as("r"))
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)

    val analyzed = query.analyze
    val flagOff = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
      Optimize.execute(analyzed)
    }
    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_a")), specF, order)
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
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .select(k1, k2, k3, k4, v, u, $"sum_f".as("sf"))
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), order)
      .select(k1, k2, k3, k4, v, u, $"sf", $"sum_k1".as("sk"))
      .window(Seq(sum(v).as("sum_p")), specP, order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_f".as("sf"), $"sum_k1", $"sum_p")
      .select(k1, k2, k3, k4, v, u, $"sf", $"sum_k1".as("sk"), $"sum_p")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("derived alias feeding an upper window cuts the chain") {
    // The derived alias `s = v + u` feeds the upper window. It is not a transparent link,
    // so the chain is cut and the plan is left untouched.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), specF, order)
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
      .window(Seq(sum(v).as("sum_f")), specF, order)
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
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(runningSumAlias), Seq(k1), Seq.empty)
      .window(Seq(sum(v).as("sum_s")), specS, order)

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
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), Seq.empty)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_k1")), Seq(k1), Seq.empty)
      .window(Seq(sum(v).as("sum_f")), specF, order)
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

  private def rankAlias = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(specP, order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  private def rankOnSpecF = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(specF, order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  // Same spec as `rankOnSpecF` under a different alias, for stacks with two row_numbers.
  private def rowNumberOnSpecF = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(specF, order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("r")

  // A whole-partition sum on specF with no ORDER BY, as SQL analysis shapes it: covers
  // the same rows under any row order, so it is reorderable and can merge into an ordered
  // same-spec window, taking that window's order spec.
  private def wholePartitionSumOnSpecF = WindowExpression(
    sum(v),
    WindowSpecDefinition(specF, Seq.empty,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("r")

  private def rankOnK1OrderK2 = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(Seq(k1), Seq(k2.asc),
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  private def rankOnK1K2OrderV = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(Seq(k1, k2), Seq(v.asc),
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  private def denseRankAlias = WindowExpression(
    DenseRank(Seq.empty),
    WindowSpecDefinition(specP, order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("dr")

  /**
   * CollapseWindow + TransposeWindow to a fixed point (as in the operator-optimization
   * batch), normalize the rank bound, optionally push filters through top projects, then
   * InferWindowGroupLimit.
   */
  private object OptimizeWithGroupLimit extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator", FixedPoint(10),
        CollapseWindow, TransposeWindow, PushDownPredicates) ::
      Batch("NormalizeFilter", Once, ConstantFolding, UnwrapCastInBinaryComparison) ::
      Batch("InferWindowGroupLimit", Once, InferWindowGroupLimit) :: Nil
  }

  test("rank filter on the top window pins it without a WGL-defeating reorder") {
    // The exchange-optimal order under the pin would put same-spec sum_p directly under rn;
    // CollapseWindow would then merge the RANGE sum into rn and defeat InferWindowGroupLimit.
    // The merge-safe pin keeps the original order (sum_f under rn) so the group limit fires.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("rank filter pins when unpinned order would displace the rank window") {
    // Unpinned exchangeOptimalOrder is [0, 2, 1]: rn lands under the incomparable [k1, k4]
    // window and InferWindowGroupLimit cannot match. The pin must keep rn on top. Putting
    // same-spec sum_f last under the pin would let rn ride that exchange, but CollapseWindow
    // would then merge the non-expanding sum into rn and defeat WGL, so the merge-safe order
    // leaves the under-pin windows unchanged.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_s")), specS, order)
      .window(Seq(rankOnSpecF), specF, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val withoutFilter = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_s")), specS, order)
      .window(Seq(rankOnSpecF), specF, order)

    val buriedRank = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankOnSpecF), specF, order)
      .window(Seq(sum(v).as("sum_s")), specS, order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_s", $"rn")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(withoutFilter.analyze), buriedRank.analyze)
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned rank window schedules a ridable lower group last") {
    // Pin on [k1, k2]; lower windows [k1] (pin can ride, different spec so no merge) then
    // [k3] (incomparable). Ignoring the pin keeps that order (3 exchanges). Putting [k3]
    // then [k1] under the pin lets rn ride the [k1] exchange (2 exchanges).
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_b")), Seq(k3), order)
      .window(Seq(rankOnSpecF), specF, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k3), order)
      .window(Seq(sum(v).as("sum_a")), Seq(k1), order)
      .select(k1, k2, k3, k4, v, u, $"sum_a", $"sum_b")
      .window(Seq(rankOnSpecF), specF, order)
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
    // Pin on [k1, k2, k3]; lower windows s1 [k1] and s2 [k3] can ride the pin's exchange,
    // while s3 [k4] cannot. The first group order whose last group the pin can ride is
    // (s1, s3, s2), so s2 must move above s3; only the last group decides the outcome.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s2")), Seq(k3), order)
      .window(Seq(sum(v).as("s3")), Seq(k4), order)
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s3")), Seq(k4), order)
      .window(Seq(sum(v).as("s2")), Seq(k3), order)
      .select(k1, k2, k3, k4, v, u, $"s1", $"s2", $"s3")
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned reorder decides merge safety without enumerating window orders") {
    // Ten same-spec sums under the pin: each candidate tail's emitted order ends with a
    // window that merges into the pin (same partition and order spec) with a non-expanding
    // expression, so no under-pin tail is safe and the search must fall back to the
    // identity order. The probe checks one order per (last group, tail) pair, so deciding
    // this must not enumerate 10! group-member permutations (or the group orders of a
    // many-group stack), each rebuilding and collapsing the stack.
    // The rule is applied directly, as in the other pin tests, so the chain is not first
    // folded by `CollapseWindow`.
    val chained = (1 to 10).foldLeft(wideRelation: LogicalPlan) { (plan, i) =>
      plan.window(Seq(sum(v).as(s"s$i")), specP, order)
    }
    val query = chained
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      failAfter(Span(5, Seconds)) {
        comparePlans(TransposeWindow.apply(analyzed), analyzed)
      }
    }
  }

  test("pinned reorder picks a safe member that is not the group's last") {
    // One group under the pin, in group order: leader [k1, k2] (the pin rides its exchange),
    // then [k1, k4, k2] and [k1, k2, k3]. The group's last member [k1, k2, k3] would merge
    // into the pin, so the only safe tail is [k1, k4, k2], which is not the last member:
    // taking it permutes the group, and the result differs from the identity fallback.
    val query = wideRelation
      .window(Seq(sum(v).as("s0")), specFExtra, order)
      .window(Seq(sum(v).as("s1")), specP, order)
      .window(Seq(sum(v).as("s2")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s2")), specF, order)
      .window(Seq(sum(v).as("s1")), specP, order)
      .window(Seq(sum(v).as("s0")), specFExtra, order)
      .select(k1, k2, k3, k4, v, u, $"s0", $"s1", $"s2")
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned reorder prefers a later rideable group over an earlier leader-only-safe group") {
    // Two rideable groups under the pin. The earlier group's leader [k1, k2] has only
    // same-spec (specP) non-leaders, all of which would merge into the pin, so placing it
    // last would hoist the leader above them and split its exchange. The later group
    // [k1, k3] has a safe non-leader, so it keeps leader-first and stays a single exchange.
    // The greedy last-qualifying selection must keep the later group last, which here is the
    // original (already optimal) order.
    val specT = Seq(k1, k3)
    val specTK = Seq(k1, k3, k4)
    val query = wideRelation
      .window(Seq(sum(v).as("a")), specF, order)
      .window(Seq(sum(v).as("b")), specP, order)
      .window(Seq(sum(v).as("c")), specP, order)
      .window(Seq(sum(v).as("d")), specT, order)
      .window(Seq(sum(v).as("e")), specTK, order)
      .window(Seq(rankAlias), specP, order)
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
      .window(Seq(sum(v).as("a")), specP, orderK2)
      .window(Seq(sum(v).as("b")), specF, order)
      .window(Seq(sum(v).as("c")), specS, order)
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val strictAnswer = wideRelation
      .window(Seq(sum(v).as("b")), specF, order)
      .window(Seq(sum(v).as("c")), specS, order)
      .window(Seq(sum(v).as("a")), specP, orderK2)
      .select(k1, k2, k3, k4, v, u, $"a", $"b", $"c")
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    val subsetAnswer = wideRelation
      .window(Seq(sum(v).as("c")), specS, order)
      .window(Seq(sum(v).as("b")), specF, order)
      .window(Seq(sum(v).as("a")), specP, orderK2)
      .select(k1, k2, k3, k4, v, u, $"a", $"b", $"c")
      .window(Seq(rankAlias), specP, order)
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
    // the pin misses, exchangeOptimalOrder yields a, rn, s, and WindowGroupLimit is lost.
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

  test("pinned order avoids CollapseWindow merge that defeats WindowGroupLimit") {
    // Pinned exchangeOptimalOrder is b, c, a: CollapseWindow then merges same-spec RANGE sum
    // a into rn and isExpandingWindow fails. groupLimitSafeOrder (a, b, c) keeps WGL.
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
      .window(Seq(sum(v).as("sum_a")), Seq(k1), orderK2)
      .window(Seq(sum(v).as("sum_b")), Seq(k3), orderK1)
      .window(Seq(sum(v).as("sum_c")), Seq(k3), orderK2)
      .select(k1, k2, k3, k4, v, u, $"sum_b", $"sum_a", $"sum_c")
      .window(Seq(rankOnK1OrderK2), Seq(k1), orderK2)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("pinned merge check collapses stacked transparent projects under the pin") {
    // Rename links between windows are hoisted as a Project spine under the pin. A merge
    // check that looked only at the raw spine would miss Window(a) under Project+ when
    // pin-ride puts same-spec sum_a under rn; the probe collapses the candidate's
    // under-pin plan first, so the merge into rn is still seen and WGL is kept (the
    // withColumn shape covered end-to-end in TransposeWindowQuerySuite).
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
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
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
      .window(Seq(sum(v).as("s")), specF, order)
      .window(Seq(sum(v).as("x")), Seq(k3), order)
      .window(Seq(rowNumberOnSpecF), specF, order)
      .window(Seq(sum(v).as("y")), Seq(k4), order)
      .window(Seq(rankOnSpecF), specF, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s")), specF, order)
      .window(Seq(rowNumberOnSpecF), specF, order)
      .window(Seq(sum(v).as("x")), Seq(k3), order)
      .window(Seq(sum(v).as("y")), Seq(k4), order)
      .select(k1, k2, k3, k4, v, u, $"s", $"x", $"r", $"y")
      .window(Seq(rankOnSpecF), specF, order)
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
      .window(Seq(sum(v).as("s")), specF, order)
      .window(Seq(sum(v).as("x")), Seq(k3), order)
      .window(Seq(wholePartitionSumOnSpecF), specF, Seq.empty)
      .window(Seq(sum(v).as("y")), Seq(k4), order)
      .window(Seq(rankOnSpecF), specF, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s")), specF, order)
      .window(Seq(wholePartitionSumOnSpecF), specF, Seq.empty)
      .window(Seq(sum(v).as("x")), Seq(k3), order)
      .window(Seq(sum(v).as("y")), Seq(k4), order)
      .select(k1, k2, k3, k4, v, u, $"s", $"x", $"r", $"y")
      .window(Seq(rankOnSpecF), specF, order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), correctAnswer.analyze)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("rank filter above an attribute-only top project still pins the top window") {
    // Same merge-safe pin as the Filter-over-Window case: refuse the order that would put
    // same-spec sum_p under rn, so the plan stays unchanged and WindowGroupLimit still fires.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_p", $"rn")
      .where($"rn" <= 1)

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("non-rank filter above the chain allows full reordering") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .where($"sum_f" > 5)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f", $"rn")
      .where($"sum_f" > 5)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("over-threshold rank filter above the chain allows full reordering") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 2000)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(rankAlias), specP, order)
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
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias, denseRankAlias), specP, order)
      .where($"rn" <= 2000 && $"dr" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(rankAlias, denseRankAlias), specP, order)
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
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 3)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f", $"rn")
      .where($"rn" <= 3)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("rank filter above a renaming top project pins the top window") {
    // The top project renames the rank attribute, so `isRankFilterOnTopWindow` must peel
    // the alias off the filter condition to see the rank reference underneath. Merge-safe
    // pin keeps the original window order (see the Filter-over-Window pin test).
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, v, u, $"rn".as("rank"))
      .where($"rank" <= 1)

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("rank filter across two renaming top projects pins the top window") {
    // Two stacked rename-only top projects: the filter references the outer rename
    // `rank2`, whose definition goes through the inner rename `rank`.
    // `isRankFilterOnTopWindow` must peel both aliases. Apply `TransposeWindow` alone so
    // `CollapseProject` does not fold the renames before the peel runs.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, v, u, $"rn".as("rank"))
      .select(k1, k2, k3, k4, v, u, $"rank".as("rank2"), $"rank".as("rank3"))
      .where($"rank2" <= 1)

    val analyzed = query.analyze

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(TransposeWindow.apply(analyzed), analyzed)
      assert(OptimizeWithGroupLimit.execute(analyzed)
        .collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("non-rank condition on a computed top project does not pin the top window") {
    // The top project computes derived `t = v + u`, so it is not a transparent link. The
    // filter's child is therefore not collected as a chain from the Filter (no pin). The
    // project still bounds the window stack below it: stack reorder runs with that project
    // as parent (unpinned) and leaves the derived project in place above the windows.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, v, u, $"rn", (v + u).as("t"))
      .where($"t" <= 2)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, $"rn", $"sum_f", $"sum_p", u, v)
      .select(k1, k2, k3, k4, v, u, $"rn", (v + u).as("t"))
      .where($"t" <= 2)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

}
