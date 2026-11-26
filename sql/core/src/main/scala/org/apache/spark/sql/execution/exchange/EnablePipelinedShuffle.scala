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

import scala.annotation.tailrec

import org.apache.spark.SparkEnv
import org.apache.spark.internal.LogKeys.{CONFIG, CONFIG2}
import org.apache.spark.internal.config
import org.apache.spark.shuffle.streaming.StreamingShuffleManager
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Opt-in (SPARK-57399). Rewrites EVERY [[ShuffleExchangeExec]] in a
 * batch physical plan to `pipelined = true`, so each shuffle is served by the in-process
 * pipelined channel manager and the concurrent-stage scheduler runs the map and reduce stages
 * together.
 *
 * The rewrite considers the whole group: the scheduler requires a job to
 * be all-pipelined (or a materialized prefix below a pipelined suffix), so choosing per exchange
 * would produce exactly the mixed shapes it rejects. What the rule does decide is ELIGIBILITY --
 * the environment gates in [[PipelinedShuffleEligibility]] and the plan shapes below that force the
 * whole plan back to regular.
 *
 * Enabled only when `spark.sql.shuffle.localPipelined.enabled=true`. It runs in the non-AQE
 * `preparations` list, so it also requires AQE to be off (under AQE the plan is hidden behind
 * an opaque `AdaptiveSparkPlanExec` leaf and this rule sees no exchanges).
 *
 * Rewriting ALL shuffles (not just hash-partitioning ones) keeps the job all-pipelined, which
 * avoids a pipelined producer below a regular boundary, a shape the scheduler rejects.
 * SinglePartition and RangePartitioning exchanges pipeline fine -- the channel transport only
 * routes by `partitioner.getPartition(key)` and does not care which partitioning produced the
 * id (SinglePartition is the numPartitions == 1 degenerate case).
 *
 * These shapes make the rule leave the whole plan regular:
 *   - the estimated producer and consumer widths cannot fit local task capacity.
 *   - Dataset.rdd deserialization and cached inputs (the shared eligibility gate): their actual
 *     shuffle readers depend on consumers or cache availability outside this plan.
 *   - reuse: a pipelined producer with more than one consumer (fan-out) is rejected, so if any
 *     exchange in the plan is reused the rule bails out.
 *   - an UNSUPPORTED CONSUMER reading a shuffle (see [[readsShuffleThroughUnsupportedConsumer]]):
 *     an operator that would drain a shuffle in a way the channel transport cannot serve, or that
 *     builds its own hidden regular shuffle. If such an operator sits above any shuffle the rule
 *     leaves the WHOLE plan regular (leaving only that one exchange regular would put a pipelined
 *     exchange below a regular boundary, which the scheduler rejects -- so it is all-or-nothing).
 *     The query runs correctly, just not pipelined. The unsupported consumers are:
 *       - `CoalesceExec` (user `.coalesce(n)`): its `CoalescedRDD` makes ONE reduce task drain
 *         SEVERAL reduce partitions sequentially. The single-threaded writer parks on a full
 *         bounded queue filling a later partition before emitting an earlier one's markers, so a
 *         reader draining partitions in order deadlocks the writer with no timeout escape;
 *         `coalesce`'s narrow-merge contract also cannot be honored by re-hashing to `n`.
 *       - `CartesianProductExec`: its `UnsafeCartesianRDD` reads each left (child) partition once
 *         per right partition, so N reduce tasks mint N readers on the SAME rendezvous queue for
 *         one `(shuffleId, epoch, pid)` -- rows and end-of-stream markers split
 *         nondeterministically (wrong results), a reader short of `numMaps` markers hangs, and
 *         the first to finish abandons the queue and discards the others' data. The fan-out check
 *         does not catch it (one consumer RDD, computed many times), nor the width-1 require.
 *       - `CollectLimitExec` / `CollectTailExec` / `TakeOrderedAndProjectExec`: each builds a
 *         hidden regular (`pipelined = false`) shuffle inside `doExecute` via
 *         `prepareShuffleDependency`, invisible to this plan walk. A flipped exchange below one of
 *         them would sit under that unmaterialized regular boundary and the job would hard-fail at
 *         submission (`classifyJobShuffleShape`'s pipelined-below-regular rejection). (`.collect()`
 *         on a limit takes `executeTake` and never hits `doExecute`; `.write` / `.toLocalIterator`
 *         / a non-root position do.)
 */
object EnablePipelinedShuffle extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    // Shared environment gate (opt-in flag, single-executor local mode, channel manager active),
    // identical to the AQE rule's -- see PipelinedShuffleEligibility for why it is a correctness
    // gate. It also requires AQE off implicitly: under AQE this rule sees no exchanges.
    if (!PipelinedShuffleEligibility.enabled(plan, conf)) return plan

    val shuffles = plan.collect { case s: ShuffleExchangeExec => s }
    if (shuffles.isEmpty) return plan

    // A reused exchange has more than one consumer; a pipelined producer cannot fan out, so
    // leave the whole plan regular rather than produce a rejected job. Check subquery plans
    // too (plan.exists walks the operator tree only): today no SQL shape can place a reused
    // PIPELINED exchange there -- same-tree reuse is caught here, main-vs-subquery reuse
    // never fires because the subquery's own preparation pass (PlanSubqueries ->
    // prepareExecutedPlan, which includes this rule) flips its exchanges pipelined BEFORE
    // the outer ReuseExchangeAndSubquery compares canonical forms, and subquery-vs-subquery
    // duplication is collapsed by MergeScalarSubqueries / subquery reuse first -- but the
    // second mechanism is an accident of rule ordering and the third is optimizer behavior,
    // so this gate does not rely on either.
    if (plan.collectWithSubqueries { case r: ReusedExchangeExec => r }.nonEmpty) {
      // Not a warning: this is a normal, expected fallback (reuse is routine optimizer output,
      // e.g. self-joins), the query still runs correctly as a regular shuffle, and the user has
      // nothing to act on. Log at DEBUG as diagnostic ("why this query did not go pipelined")
      // rather than WARN, which would fire on every reuse-bearing query and read as a fault.
      logDebug("EnablePipelinedShuffle: plan has a reused exchange; leaving it regular to " +
        "avoid a fan-out pipelined job.")
      return plan
    }

    // An operator that would read a shuffle in a way the channel transport cannot serve, or that
    // builds its own hidden regular shuffle, forces the whole plan regular (see class doc). Like
    // the reuse fallback this is a normal, expected outcome, so log at DEBUG rather than WARN.
    if (readsShuffleThroughUnsupportedConsumer(plan)) {
      logDebug("EnablePipelinedShuffle: a shuffle is read through an operator the channel " +
        "transport cannot serve (coalesce / cartesian product / a limit operator that builds a " +
        "hidden shuffle); leaving the plan regular.")
      return plan
    }

    if (!PipelinedShuffleEligibility.fitsLocalCapacity(plan, shuffles.map(_.id).toSet)) {
      return plan
    }

    PipelinedShuffleEligibility.markExchanges(plan)(_ => true)
  }

  /**
   * True if any [[ShuffleExchangeExec]] in `plan` is read by an operator the channel transport
   * cannot serve. The unsupported operators (see class doc for why each is fatal) are
   * `CoalesceExec`, `CartesianProductExec`, and the limit operators `CollectLimitExec` /
   * `CollectTailExec` / `TakeOrderedAndProjectExec`. For each such operator anywhere in the plan,
   * check whether a shuffle is reachable below it.
   *
   * The reachability walk descends through EVERY child of a non-exchange node -- not only unary
   * children -- so a shuffle behind a `UnionExec`/join (a `BinaryExecNode`) beneath the operator
   * is still found. It stops at the FIRST [[ShuffleExchangeExec]] on each path: a shuffle deeper
   * than that first one is not read by this operator (the intervening exchange's own reader reads
   * one reduce partition per task), so it is not this operator's concern.
   */
  private def readsShuffleThroughUnsupportedConsumer(plan: SparkPlan): Boolean = {
    def reachesShuffle(p: SparkPlan): Boolean = p match {
      case _: ShuffleExchangeExec => true
      case other => other.children.exists(reachesShuffle)
    }
    plan.exists {
      case p if PipelinedShuffleEligibility.isUnsupportedConsumer(p) =>
        p.children.exists(reachesShuffle)
      case _ => false
    }
  }
}

/** Plan a single-source, full-input pipelined group after exchange reuse. */
private[sql] object DistributedPipelinedShuffle extends Rule[SparkPlan] {
  val candidate = TreeNodeTag[Int]("batchPipelinedShuffleCandidate")

  val ownsShuffles = TreeNodeTag[Unit]("ownsDistributedPipelinedShuffles")

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.DISTRIBUTED_PIPELINED_SHUFFLE_ENABLED)) return plan
    val input = plan match {
      // A V1 file write drains its child when it writes. Inspect the child for exchanges and
      // keep the command as the execution root. Other DataWritingCommands decide how much of
      // their input to consume, so leave them behind the command guard below.
      case DataWritingCommandExec(_: InsertIntoHadoopFsRelationCommand, child) => child
      case other => other
    }
    if (input.exists(_.logicalLink.exists(p => p.isStreaming || p.isInstanceOf[Command]))) {
      return plan
    }

    if (conf.adaptiveExecutionEnabled) {
      logWarning(log"Skipping distributed pipelined shuffle for this batch query. " +
        log"${MDC(CONFIG, SQLConf.DISTRIBUTED_PIPELINED_SHUFFLE_ENABLED.key)} is enabled, " +
        log"but it requires ${MDC(CONFIG2, SQLConf.ADAPTIVE_EXECUTION_ENABLED.key)}=false.")
      return plan
    }
    if (conf.localPipelinedShuffleEnabled && plan.session.sparkContext.isLocal) return plan

    // Subqueries and reused exchanges can introduce another reader outside the unary chain.
    if (input.exists(p => p.subqueries.nonEmpty || p.isInstanceOf[ReusedExchangeExec])) return plan

    val isResult = input eq plan
    val consumer = consumerInput(input, collectTopK = isResult)
    val group = singleSourceGroup(consumer)
    if (group.isEmpty) return plan
    group.foreach(s => s.setTagValue(candidate, s.id))
    val sc = plan.session.sparkContext
    if (sc.isLocal || sc.cleaner.isEmpty || sc.conf.get(config.SPECULATION_ENABLED) ||
        sc.conf.get(config.DYN_ALLOCATION_ENABLED) ||
        !sc.conf.get(config.PIPELINED_GROUP_SLOT_CHECK_ENABLED) ||
        !SparkEnv.get.pipelinedShuffleManager.isInstanceOf[StreamingShuffleManager]) return plan
    val readers = group.map(_.outputPartitioning.numPartitions)
    val fits = stageWidth(group.head.child).exists { source =>
      PipelinedShuffleEligibility.fitsCapacity(plan, source +: readers)
    }
    if (!fits) return plan
    val ids = group.iterator.map(_.id).toSet
    if (isResult) {
      PipelinedShuffleResultExec(input, ids)
    } else {
      val write = plan.withNewChildren(Seq(pipeline(input, ids).clone()))
      write.setTagValue(ownsShuffles, ())
      write
    }
  }

  // These unary operators preserve the task-to-partition mapping and consuming all output drains
  // their inputs. In particular a local sort reads its entire input before yielding a row.
  private def boundary(plan: SparkPlan): SparkPlan = plan match {
    case _: ProjectExec | _: FilterExec | _: WholeStageCodegenExec | _: HashAggregateExec |
        _: WindowExec | _: SortExec | _: ExpandExec | _: InputAdapter | _: ColumnarToRowExec =>
      boundary(plan.children.head)
    case other => other
  }

  private def stageWidth(plan: SparkPlan): Option[Int] = boundary(plan) match {
    case r: RangeExec => Some(r.outputPartitioning.numPartitions)
    case f: FileSourceScanExec => Some(f.inputRDD.getNumPartitions)
    case _ => None
  }

  // Walk from the final reader to the scan. One unsupported edge disqualifies the whole chain.
  @tailrec
  private def singleSourceGroup(
      plan: SparkPlan,
      downstream: List[ShuffleExchangeExec] = Nil): List[ShuffleExchangeExec] = {
    boundary(plan) match {
      case s: ShuffleExchangeExec if !s.pipelined &&
          s.outputPartitioning.isInstanceOf[HashPartitioning] =>
        singleSourceGroup(s.child, s :: downstream)
      case _: RangeExec | _: FileSourceScanExec => downstream
      case _ => Nil
    }
  }

  private def consumerInput(plan: SparkPlan, collectTopK: Boolean): SparkPlan = plan match {
    // Only the root uses TakeOrderedAndProjectExec.executeCollect. With ordered input it
    // takes at most K rows per partition, and nested Top-K can introduce another shuffle.
    case k: TakeOrderedAndProjectExec if collectTopK && k.limit > 0 &&
        !SortOrder.orderingSatisfies(k.child.outputOrdering, k.sortOrder) => k.child
    case w: WriteFilesExec => w.child
    case other => other
  }

  private[exchange] def pipeline(plan: SparkPlan, ids: Set[Int]): SparkPlan =
    PipelinedShuffleEligibility.markExchanges(plan)(_.getTagValue(candidate).exists(ids.contains))

  def cleanup(plan: SparkPlan): Unit = {
    plan.session.sparkContext.cleaner.foreach { cleaner =>
      plan.foreach {
        case s: ShuffleExchangeExec =>
          s.initializedShuffleId.foreach(cleaner.doCleanupShuffle(_, blocking = true))
        case _ =>
      }
    }
  }
}
