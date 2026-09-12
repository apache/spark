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
import org.apache.spark.sql.catalyst.expressions.{Concat, CurrentRow, Rand, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, WindowGroupLimit}
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
    // without the reference the windows above [k1] would be reordered below the [k1, k2]
    // window to minimize exchanges. `reorderable` computes `references` across the
    // function-expressions clause too, so this shape must block the reorder just like a
    // partition-spec reference does.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1, k2), order)
      .window(Seq(sum($"s1").as("s2")), Seq(k1), order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
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
  private val specP = Seq(k1, k2, k3)
  private val specS = Seq(k1, k4)
  private val order = Seq(k1.asc)

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
    // Distinct order specs so that CollapseWindow cannot merge the same-spec windows.
    // All windows contain (k1), so they share one exchange; within the group the leader
    // (k1) pays it and the members keep smaller specs first, which is the original order.
    val query = wideRelation
      .window(Seq(sum(v).as("s1")), Seq(k1), order)
      .window(Seq(sum(v).as("s2a")), specF, order)
      .window(Seq(sum(v).as("s2b")), specF, Seq(k2.asc))
      .window(Seq(sum(v).as("s3")), specP, order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
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

  test("derived aliases in transparent links do not block reordering") {
    // The project between the windows adds the derived alias `s = v + u`. Since neither
    // window above it references `s`, the link is safe to hoist above the reordered
    // windows: windows only append columns, so `s` computes the same values wherever it
    // is re-applied.
    val query = wideRelation
      .window(Seq(sum(v).as("sum_a")), specF, order)
      .select(k1, k2, k3, k4, v, u, $"sum_a",
        (v + u).as("s"))
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_b")), Seq(k1), order)
      .window(Seq(sum(v).as("sum_a")), specF, order)
      .select(k1, k2, k3, k4, v, u, $"sum_a",
        (v + u).as("s"), $"sum_b")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("transparent link alias referenced by an upper window blocks reordering") {
    // The derived alias `s = v + u` feeds the upper window, so hoisting the link above it
    // would hide `s` from that window; the chain must be left untouched.
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

  test("window with an empty partition spec blocks reordering") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_all")), Seq.empty, order)

    val analyzed = query.analyze
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("a single window is not a chain and is left unchanged") {
    // `collectChain` requires at least two adjacent windows, so a lone window is a no-op.
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

  private def rankAliasOnK1 = WindowExpression(
    RowNumber(),
    WindowSpecDefinition(Seq(k1), order,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

  test("duplicate partition keys do not break the exchange-minimal order") {
    // HashPartitioning(k1, k1) satisfies the relaxed ClusteredDistribution(k1, k2), so
    // once the (k1, k1) window is placed below the (k1, k2) window, its exchange serves
    // both windows. The two specs have the same length, so spec length alone cannot
    // order them: (k1, k1) must normalize to (k1) to sort first.
    val query = wideRelation
      .window(Seq(sum(v).as("s_ab")), Seq(k1, k2), order)
      .window(Seq(sum(v).as("s_aa")), Seq(k1, k1), Seq(k2.asc))

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s_aa")), Seq(k1, k1), Seq(k2.asc))
      .window(Seq(sum(v).as("s_ab")), Seq(k1, k2), order)
      .select(k1, k2, k3, k4, v, u, $"s_ab", $"s_aa")

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
    }

    // Under `requireAllClusterKeysForDistribution` a partitioning matches only the exact
    // same keys in the same order, so the two specs are incomparable: neither window can
    // ride the other's exchange and the stack is left unchanged.
    withSQLConf(
      SQLConf.WINDOW_REORDER_ENABLED.key -> "true",
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true") {
      comparePlans(Optimize.execute(analyzed), analyzed)
    }
  }

  test("a rank-pinned top window constrains the order of the windows below") {
    // The pinned window rides the exchange of the group scheduled last below it, so the
    // (k2) window must go below the (k1) window: (k2) -> (k1) -> pinned (k1) needs two
    // exchanges, while the original order needs three because the pinned window cannot
    // ride the (k2)-keyed exchange.
    val query = wideRelation
      .window(Seq(sum(v).as("s_k1")), Seq(k1), order)
      .window(Seq(sum(v).as("s_k2")), Seq(k2), order)
      .window(Seq(rankAliasOnK1), Seq(k1), order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("s_k2")), Seq(k2), order)
      .window(Seq(sum(v).as("s_k1")), Seq(k1), order)
      .select(k1, k2, k3, k4, v, u, $"s_k1", $"s_k2")
      .window(Seq(rankAliasOnK1), Seq(k1), order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(analyzed), correctAnswer.analyze)
    }
  }

  test("rank filter on the top window pins it and reorders the windows below") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f")
      .window(Seq(rankAlias), specP, order)
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
    }

    // The pinned reorder keeps the Filter directly over the rank window, so the strict
    // Filter-over-Window shape survives and InferWindowGroupLimit still fires.
    object OptimizeMore extends RuleExecutor[LogicalPlan] {
      val batches =
        Batch("TransposeWindow", Once, TransposeWindow) ::
        Batch("InferWindowGroupLimit", Once, InferWindowGroupLimit) :: Nil
    }
    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val withGroupLimit = OptimizeMore.execute(analyzed)
      assert(withGroupLimit.collect { case _: WindowGroupLimit => () }.nonEmpty)
    }
  }

  test("rank filter above an attribute-only top project still pins the top window") {
    val query = wideRelation
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_p", $"rn")
      .where($"rn" <= 1)

    val analyzed = query.analyze

    val correctAnswer = wideRelation
      .window(Seq(sum(v).as("sum_f")), specF, order)
      .window(Seq(sum(v).as("sum_p")), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_p", $"sum_f")
      .window(Seq(rankAlias), specP, order)
      .select(k1, k2, k3, k4, v, u, $"sum_f", $"sum_p", $"rn")
      .where($"rn" <= 1)

    withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
      val optimized = Optimize.execute(analyzed)
      comparePlans(optimized, correctAnswer.analyze)
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

}
