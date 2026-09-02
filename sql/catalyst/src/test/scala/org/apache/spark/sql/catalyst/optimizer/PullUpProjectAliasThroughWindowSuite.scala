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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.MetadataBuilder

class PullUpProjectAliasThroughWindowSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Pull up project alias through window", FixedPoint(20),
        PullUpProjectAliasThroughWindow) :: Nil
  }

  private val testRelation = LocalRelation($"key".int, $"key2".int, $"value".string)
  private val key = testRelation.output(0)
  private val key2 = testRelation.output(1)
  private val value = testRelation.output(2)
  private val windowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

  // Builds `Window [row_number() ... AS <name>], <partitionSpec>, <orderSpec>` over `child`.
  private def windowOver(
      child: LogicalPlan,
      name: String,
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder] = Nil): Window = {
    val spec = windowSpec(partitionSpec, orderSpec, windowFrame)
    val winExpr = windowExpr(RowNumber(), spec).as(name)
    Window(Seq(winExpr), partitionSpec, orderSpec, child)
  }

  test("pull up a rename of the window partition key") {
    // Project [userid, w]            <- both bare attributes
    // +- Window [... AS w], [key]
    //    +- Project [key AS userid, value, key]
    val userid = Alias(key, "userid")()
    val bottom = Project(Seq(userid, value, key), testRelation)
    val window = windowOver(bottom, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(userid.toAttribute, w), window)

    // `userid` is pulled up into the parent project; the lower project keeps the rest.
    val prunedBottom = Project(Seq(value, key), testRelation)
    val expected = Project(
      Seq(Alias(key, "userid")(exprId = userid.exprId), w),
      window.copy(child = prunedBottom))
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("pull up every applicable key in a multi-key window") {
    val u1 = Alias(key, "u1")()
    val u2 = Alias(key2, "u2")()
    val bottom = Project(Seq(u1, u2, value, key, key2), testRelation)
    val window = windowOver(bottom, "w", key :: key2 :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(u1.toAttribute, u2.toAttribute, w), window)

    val prunedBottom = Project(Seq(value, key, key2), testRelation)
    val expected = Project(
      Seq(
        Alias(key, "u1")(exprId = u1.exprId),
        Alias(key2, "u2")(exprId = u2.exprId),
        w),
      window.copy(child = prunedBottom))
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("pull up a rename of the window order key") {
    // Window ordered by `value`; the top Project passes `tstamp` (a rename of `value`) through as
    // a bare attribute. It must be pulled up so the window's output ordering projects through it.
    val tstamp = Alias(value, "tstamp")()
    val bottom = Project(Seq(key, tstamp, value), testRelation)
    val window = windowOver(bottom, "w", key :: Nil, SortOrder(value, Ascending) :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(key, tstamp.toAttribute, w), window)

    val prunedBottom = Project(Seq(key, value), testRelation)
    val expected = Project(
      Seq(key, Alias(value, "tstamp")(exprId = tstamp.exprId), w),
      window.copy(child = prunedBottom))
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("pull up renames of both partition and order keys") {
    val userid = Alias(key, "userid")()
    val tstamp = Alias(value, "tstamp")()
    val bottom = Project(Seq(userid, tstamp, key, value), testRelation)
    val window = windowOver(bottom, "w", key :: Nil, SortOrder(value, Ascending) :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(userid.toAttribute, tstamp.toAttribute, w), window)

    val prunedBottom = Project(Seq(key, value), testRelation)
    val expected = Project(
      Seq(
        Alias(key, "userid")(exprId = userid.exprId),
        Alias(value, "tstamp")(exprId = tstamp.exprId),
        w),
      window.copy(child = prunedBottom))
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("keep a bare pass-through column the window does not reference below the window") {
    // `key2` is a bare pass-through that the window neither partitions nor orders by. It must stay
    // below the window so the window keeps producing it for the parent project to reference; only
    // the rename `key AS userid` is pulled up.
    val userid = Alias(key, "userid")()
    val bottom = Project(Seq(userid, key2, value, key), testRelation)
    val window = windowOver(bottom, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(userid.toAttribute, key2, w), window)

    val prunedBottom = Project(Seq(key2, value, key), testRelation)
    val expected = Project(
      Seq(Alias(key, "userid")(exprId = userid.exprId), key2, w),
      window.copy(child = prunedBottom))
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("no rewrite when the renamed column is not a window key") {
    // `userid` renames `key`, but the window is partitioned by `value`.
    val userid = Alias(key, "userid")()
    val bottom = Project(Seq(userid, value, key), testRelation)
    val window = windowOver(bottom, "w", value :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(userid.toAttribute, w), window)

    comparePlans(Optimize.execute(originalQuery), originalQuery)
  }

  test("no rewrite for a computed alias whose input is not a window key") {
    // `(key2 + 1) AS z` depends on `key2`, which the window does not produce once pruned, so it
    // cannot be pulled above the window and must stay below.
    val z = Alias(key2 + 1, "z")()
    val bottom = Project(Seq(z, value, key), testRelation)
    val window = windowOver(bottom, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(z.toAttribute, w), window)

    comparePlans(Optimize.execute(originalQuery), originalQuery)
  }

  test("pull up a computed alias whose inputs are all window keys") {
    // `(key + 1) AS z` is computed rather than a pure rename, but its only input `key` is the
    // window partition key and is retained below. It is pulled up so the expression is evaluated
    // in the top project, above the window's shuffle rather than being carried across it. This
    // narrows the shuffled data even though it yields no partitioning benefit (`HashPartitioning`
    // on `key` does not project through `key + 1`).
    val z = Alias(key + 1, "z")()
    val bottom = Project(Seq(z, value, key), testRelation)
    val window = windowOver(bottom, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(z.toAttribute, w), window)

    val prunedBottom = Project(Seq(value, key), testRelation)
    val expected = Project(
      Seq(Alias(key + 1, "z")(exprId = z.exprId), w),
      window.copy(child = prunedBottom))
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("pull up a computed alias combining a partition key and an order key") {
    // `(key + key2) AS z` combines the partition key `key` and the order key `key2`; both are
    // retained below, so the whole expression lifts above the window.
    val z = Alias(key + key2, "z")()
    val bottom = Project(Seq(z, key, key2), testRelation)
    val window = windowOver(bottom, "w", key :: Nil, SortOrder(key2, Ascending) :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(z.toAttribute, w), window)

    val prunedBottom = Project(Seq(key, key2), testRelation)
    val expected = Project(
      Seq(Alias(key + key2, "z")(exprId = z.exprId), w),
      window.copy(child = prunedBottom))
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("no rewrite when the window child is not a Project") {
    val window = windowOver(testRelation, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(key, w), window)

    comparePlans(Optimize.execute(originalQuery), originalQuery)
  }

  test("pull up an alias across a chain of windows") {
    // Project [userid, w1, w2]
    // +- Window [... AS w2], [key], [value DESC]
    //    +- Window [... AS w1], [key], [value]     <- adjacent, no Project between
    //       +- Project [key AS userid, value, key]
    // `userid` renames `key` and is referenced by neither window, so it is pulled all the way up
    // to the top project, through both windows.
    val userid = Alias(key, "userid")()
    val bottom = Project(Seq(userid, value, key), testRelation)
    val w1 = windowOver(bottom, "w1", key :: Nil, SortOrder(value, Ascending) :: Nil)
    val w1a = w1.windowExpressions.head.toAttribute
    val w2 = windowOver(w1, "w2", key :: Nil, SortOrder(value, Descending) :: Nil)
    val w2a = w2.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(userid.toAttribute, w1a, w2a), w2)

    val prunedBottom = Project(Seq(value, key), testRelation)
    val newW1 = w1.copy(child = prunedBottom)
    val newW2 = w2.copy(child = newW1)
    val expected = Project(
      Seq(Alias(key, "userid")(exprId = userid.exprId), w1a, w2a), newW2)
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("keep an alias referenced by an inner window below the chain") {
    // The chain's inner window orders by `tstamp` (a rename of `value`), so `tstamp` is referenced
    // by a window and must stay below; only `userid` (referenced by no window) is pulled up.
    val userid = Alias(key, "userid")()
    val tstamp = Alias(value, "tstamp")()
    val bottom = Project(Seq(userid, tstamp, key, value), testRelation)
    val w1 = windowOver(bottom, "w1", key :: Nil, SortOrder(tstamp.toAttribute, Ascending) :: Nil)
    val w1a = w1.windowExpressions.head.toAttribute
    val w2 = windowOver(w1, "w2", key :: Nil)
    val w2a = w2.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(userid.toAttribute, tstamp.toAttribute, w1a, w2a), w2)

    // `tstamp` stays below (referenced by w1's order spec); `userid` is pulled up.
    val prunedBottom = Project(Seq(tstamp, key, value), testRelation)
    val newW1 = w1.copy(child = prunedBottom)
    val newW2 = w2.copy(child = newW1)
    val expected = Project(
      Seq(Alias(key, "userid")(exprId = userid.exprId), tstamp.toAttribute, w1a, w2a), newW2)
    comparePlans(Optimize.execute(originalQuery), expected)
  }

  test("no rewrite for a nondeterministic alias") {
    // `spark_partition_id() AS pid` is a leaf with no references, so it would pass the input-
    // survival check vacuously, but moving it above the window's exchange/sort would change its
    // per-partition value. It must stay below.
    val pid = Alias(SparkPartitionID(), "pid")()
    val bottom = Project(Seq(pid, value, key), testRelation)
    val window = windowOver(bottom, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(pid.toAttribute, w), window)

    comparePlans(Optimize.execute(originalQuery), originalQuery)
  }

  test("rewrite preserves the top attribute's name, qualifier, and metadata") {
    // The lookup is keyed by expr id only, so a resolved top attribute can carry a different name,
    // qualifier, and metadata than the lower alias (same expr id). The rebuilt alias must adopt the
    // top attribute's full identity so the output schema is byte-for-byte unchanged.
    val userid = Alias(key, "userid")()
    val bottom = Project(Seq(userid, value, key), testRelation)
    val window = windowOver(bottom, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    // The top project references `userid` under a different name, qualifier, and metadata.
    val metadata = new MetadataBuilder().putString("comment", "external").build()
    val topAttr = AttributeReference("external", key.dataType, key.nullable, metadata)(
      exprId = userid.exprId, qualifier = Seq("sub"))
    val originalQuery = Project(Seq(topAttr, w), window)

    val prunedBottom = Project(Seq(value, key), testRelation)
    val expected = Project(
      Seq(
        Alias(key, "external")(
          exprId = userid.exprId, qualifier = Seq("sub"), explicitMetadata = Some(metadata)),
        w),
      window.copy(child = prunedBottom))
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, expected)
    // The output attribute must match the top attribute exactly, not the lower alias's identity.
    val out = optimized.output.head
    assert(out.name == "external")
    assert(out.qualifier == Seq("sub"))
    assert(out.metadata == metadata)
  }

  test("rewrite is idempotent") {
    val userid = Alias(key, "userid")()
    val bottom = Project(Seq(userid, value, key), testRelation)
    val window = windowOver(bottom, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(userid.toAttribute, w), window)

    val once = Optimize.execute(originalQuery)
    val twice = Optimize.execute(once)
    comparePlans(once, twice)
  }

  test("rewrite preserves the output schema (exprId, name, type, nullability)") {
    val userid = Alias(key, "userid")()
    val bottom = Project(Seq(userid, value, key), testRelation)
    val window = windowOver(bottom, "w", key :: Nil)
    val w = window.windowExpressions.head.toAttribute
    val originalQuery = Project(Seq(userid.toAttribute, w), window)

    val optimized = Optimize.execute(originalQuery)
    // The output must be byte-for-byte identical so that downstream references still resolve.
    assert(optimized.output === originalQuery.output)
  }
}
