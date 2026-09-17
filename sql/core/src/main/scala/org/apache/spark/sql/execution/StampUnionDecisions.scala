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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Fixes each [[UnionExec]]'s partitioning decision and the confs its codegen gate reads, at one
 * defined point.
 *
 * `UnionExec` derives both from state that moves: its children's `outputPartitioning` sharpens as
 * AQE finalises the plans behind them, and `conf` is the live session conf. Every reader used to
 * derive its own answer, so the answer depended on when it was read: the codegen gate could fuse a
 * union whose copy in the shell then answered the other way, so `metrics` came back empty and
 * `doProduce` failed asking `metricTerm` for `numOutputRows`. This rule asks right after
 * `EnsureRequirements`, so the decision the exchanges around a union were planned against is the
 * one `unionRDDs` and the codegen gate use.
 *
 * It is listed again after the injected columnar and query-stage rules, the hooks that can add a
 * `UnionExec` of their own. One created there has no decision yet, and would otherwise take one
 * wherever it is first asked, where the copy in the codegen shell can disagree with the gate. The
 * cached-scan branch of stage creation needs no barrier: it rejects a result that is no longer an
 * `InMemoryTableScanLike`, which is a leaf.
 *
 * A tag rather than a constructor field, because a field would land in `argString` and so in
 * every `explain` and `PlanStability` golden holding a `Union`, and in `canonicalized`, which
 * exchange and cached-plan reuse key on. Writing it in place is safe here, unlike in
 * `MarkSingleTaskExecution`, because preparation runs on `sparkPlan.clone()` and every
 * shared-subtree boundary is a leaf, so `foreach` cannot reach a node another query owns.
 *
 * It only writes what is not there yet, so a later pass cannot move a decision already stamped on
 * a node, a second pass over the same nodes keeps the first answer, and a node rebuilt from a
 * stamped one keeps the tag `copyTagsFrom` gave it. AQE re-plans between rounds, so a union above
 * the stages already created is stamped again, from what that round sees and against that round's
 * exchanges; a round whose plan loses on cost is discarded whole, stamps included. A union inside a
 * stage is not revisited, since `foreach` stops at `QueryStageExec`.
 */
class StampUnionDecisions(snapshot: UnionConfSnapshot) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach {
      case u: UnionExec => u.stampDecisions(snapshot)
      case _ =>
    }
    plan
  }
}

/**
 * Records on each [[UnionExec]] the `UNION_OUTPUT_PARTITIONING` value it answers from, before
 * `EnsureRequirements` asks it what it reports.
 *
 * Without this, the two phases sample the conf separately: `EnsureRequirements` reads what the
 * union reports under the value then, and [[StampUnionDecisions]] freezes the decision under the
 * value one rule later. `conf` is the live session conf, so another thread turning it off in that
 * window would let a parent drop an exchange over a concrete partitioning and then have the union
 * concatenate, which puts one group in two partitions.
 *
 * Only the conf is recorded, never a partitioning. `EnsureRequirements` has not inserted the
 * exchanges it adds yet, so a decision taken now would freeze plain on a union whose children only
 * become co-partitioned there, which is why the decision itself waits for the barrier behind it.
 *
 * The value comes from the preparation's [[UnionConfSnapshot]], so every union in it answers from
 * the same read. Writing the tag in place is safe for the reason given on [[StampUnionDecisions]].
 */
class SnapshotUnionOutputPartitioningConf(snapshot: UnionConfSnapshot) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach {
      case u: UnionExec => u.snapshotOutputPartitioningConf(snapshot.outputPartitioning)
      case _ =>
    }
    plan
  }
}

/**
 * The union confs one preparation answers from, read once and shared by every barrier in it.
 *
 * A barrier that read the live conf instead would let two of them disagree: an injected rule can
 * return an equivalent `UnionExec` carrying tags it set itself, and `copyTagsFrom` adds nothing to
 * a node that already has one, so such a replacement reaches the late barrier with no record of its
 * own and would be stamped from whatever the conf says by then, rather than from what the exchanges
 * above it were planned against.
 */
case class UnionConfSnapshot(
    outputPartitioning: Boolean,
    codegenEnabled: Boolean,
    maxChildren: Int)

object UnionConfSnapshot {
  def apply(conf: SQLConf): UnionConfSnapshot = UnionConfSnapshot(
    outputPartitioning = conf.getConf(SQLConf.UNION_OUTPUT_PARTITIONING),
    codegenEnabled = conf.getConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED),
    maxChildren = conf.getConf(SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN))
}
