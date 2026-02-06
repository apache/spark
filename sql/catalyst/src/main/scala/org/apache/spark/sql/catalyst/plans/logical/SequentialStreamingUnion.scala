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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * Logical plan for unioning multiple streaming plans sequentially, processing each child to
 * completion before moving to the next. This is used for backfill-to-live streaming scenarios
 * where historical data should be processed completely before switching to live data.
 *
 * This is a logical placeholder node that acts as a marker in the query plan. During physical
 * planning, the streaming execution engine will match against this node and generate specific
 * execution steps for sequential processing, rather than creating a corresponding physical
 * SequentialStreamingUnion operator.
 *
 * Unlike [[Union]] which processes all children concurrently in streaming queries,
 * SequentialStreamingUnion processes each child source sequentially:
 * 1. First child processes until complete (bounded sources reach their end)
 * 2. Second child begins processing
 * 3. And so on...
 *
 * Requirements:
 * - Minimum 2 children required
 * - All children must be streaming sources
 * - All non-final children (i.e., all children except the last) must support bounded execution
 *   (SupportsTriggerAvailableNow). The last child typically remains unbounded for live streaming.
 * - All children must have explicit names when used in streaming queries
 * - Children cannot contain stateful operations (aggregations, joins, etc.)
 * - Schema compatibility is enforced via UnionBase
 *
 * State preservation: Stateful operators applied AFTER SequentialStreamingUnion (aggregations,
 * watermarks, deduplication, joins) preserve their state across source transitions,
 * enabling seamless backfill-to-live scenarios.
 *
 * Example:
 * {{{
 *   val historical = spark.readStream.format("delta").name("historical").load("/data")
 *   val live = spark.readStream.format("kafka").name("live").load()
 *   // Correct: stateful operations after SequentialStreamingUnion
 *   historical.followedBy(live).groupBy("key").count()
 *
 *   // Incorrect: stateful operations before SequentialStreamingUnion
 *   // historical.groupBy("key").count().followedBy(live.groupBy("key").count()) // Not allowed
 * }}}
 *
 * @param children        The logical plans to union sequentially (must be streaming sources)
 * @param byName          Whether to resolve columns by name (vs. by position)
 * @param allowMissingCol When true (requires byName = true), allows children to have different
 *                        columns. Missing columns in any child are filled with nulls. When false,
 *                        all children must have the same set of columns.
 */
case class SequentialStreamingUnion(
    children: Seq[LogicalPlan],
    byName: Boolean,
    allowMissingCol: Boolean) extends UnionBase {
  assert(!allowMissingCol || byName,
    "`allowMissingCol` can be true only if `byName` is true.")

  final override val nodePatterns: Seq[TreePattern] = Seq(SEQUENTIAL_STREAMING_UNION)

  /**
   * This node is considered resolved when:
   * 1. children.length >= 2: Has at least 2 children (cannot create sequential union
   *    with < 2 sources)
   * 2. !(byName || allowMissingCol): Column resolution is by position (default). When
   *    byName or allowMissingCol is true, the ResolveUnion rule must first transform
   *    this into a resolved form with schema alignment projections.
   * 3. childrenResolved: All child nodes have been resolved by the analyzer
   * 4. allChildrenCompatible: All children have compatible schemas (enforced by
   *    UnionBase)
   *
   * This matches Union's resolution logic but without duplicate column checking, since
   * SequentialStreamingUnion is validated separately for streaming-specific
   * constraints.
   */
  override lazy val resolved: Boolean = {
    children.length >= 2 &&
    !(byName || allowMissingCol) &&
    childrenResolved &&
    allChildrenCompatible
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): SequentialStreamingUnion = {
    copy(children = newChildren)
  }
}

object SequentialStreamingUnion {
  def apply(left: LogicalPlan, right: LogicalPlan): SequentialStreamingUnion = {
    SequentialStreamingUnion(left :: right :: Nil, byName = false, allowMissingCol = false)
  }

  /**
   * Recursively flattens direct SequentialStreamingUnion children.
   * This enables chaining: df1.followedBy(df2).followedBy(df3).followedBy(df4)
   *
   * Example:
   *   SequentialStreamingUnion(SequentialStreamingUnion(df1, df2), df3)
   * Flattens to:
   *   SequentialStreamingUnion(df1, df2, df3)
   *
   * Note: This only handles direct children. SequentialStreamingUnions nested
   * through other operators (e.g., Project(SequentialStreamingUnion(...))) are
   * not flattened and will be caught by validation as invalid.
   *
   * @param plans The plans to flatten
   * @return Flattened sequence of plans
   */
  def flatten(plans: Seq[LogicalPlan]): Seq[LogicalPlan] = {
    plans.flatMap {
      case SequentialStreamingUnion(children, _, _) =>
        // Recursively flatten direct SequentialStreamingUnion children
        flatten(children)
      case other =>
        Seq(other)
    }
  }
}
