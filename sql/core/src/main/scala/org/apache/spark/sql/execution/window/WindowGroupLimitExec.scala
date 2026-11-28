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

package org.apache.spark.sql.execution.window

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, BaseOrdering, BoundReference, DenseRank, Expression, Rank, RowNumber, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, GenerateOrdering, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf

sealed trait WindowGroupLimitMode

case object Partial extends WindowGroupLimitMode

case object Final extends WindowGroupLimitMode

/**
 * This operator is designed to filter out unnecessary rows before WindowExec
 * for top-k computation.
 * @param partitionSpec Should be the same as [[WindowExec#partitionSpec]].
 * @param orderSpec Should be the same as [[WindowExec#orderSpec]].
 * @param rankLikeFunction The function to compute row rank, should be RowNumber/Rank/DenseRank.
 * @param limit The limit for rank value.
 * @param mode The mode describes [[WindowGroupLimitExec]] before or after shuffle.
 * @param child The child spark plan.
 */
case class WindowGroupLimitExec(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def requiredChildDistribution: Seq[Distribution] = mode match {
    case Partial => super.requiredChildDistribution
    case Final =>
      if (partitionSpec.isEmpty) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(partitionSpec) :: Nil
      }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val evaluatorFactory =
      new WindowGroupLimitEvaluatorFactory(
        partitionSpec,
        orderSpec,
        rankLikeFunction,
        limit,
        child.output,
        longMetric("numOutputRows"))

    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.execute().mapPartitionsWithIndexInternal { (index, rowIterator) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(index, rowIterator)
      }
    }
  }

  // RowNumber, Rank and DenseRank are wired up for codegen (see `doConsume`) only when there is a
  // `PARTITION BY`. Without one, the whole partition is a single group, and this operator has no
  // way to tell its child to stop producing rows once the limit is reached the way the
  // interpreted `BaseLimitIterator.hasNext` (`rank < limit && input.hasNext`) does: every
  // remaining row would still have to be pulled from the child and rejected one at a time, so the
  // post-limit cost would grow with the partition size instead of staying bounded. Any other
  // rank-like function also falls through to `false`, defaulting to the interpreted path above
  // (`WindowGroupLimitEvaluatorFactory`). `ENABLE_WINDOW_GROUP_LIMIT_CODEGEN` is an escape hatch
  // back to that same interpreted path, mirroring `SortAggregateExec`'s `supportCodegen`.
  override def supportCodegen: Boolean = super.supportCodegen &&
    conf.getConf(SQLConf.ENABLE_WINDOW_GROUP_LIMIT_CODEGEN) &&
    partitionSpec.nonEmpty && (rankLikeFunction match {
      case _: RowNumber | _: Rank | _: DenseRank => true
      case _ => false
    })

  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  // This operator neither materializes nor reorders columns itself, so let every column stay
  // lazy until a downstream operator actually needs it, the same way `FilterExec` does. Only the
  // `partitionSpec` columns end up evaluated by `genKeyChangeCode` (shared by all three
  // functions' grouped branch); the `orderSpec` columns (`Rank`/`DenseRank` only) are evaluated by
  // the `genKeyProjection` calls inside `doConsumeRank`/`doConsumeDenseRank`.
  override def usedInputs: AttributeSet = AttributeSet.empty

  // Stable field names, computed once per operator instance rather than inside `doConsume*`.
  // `doProduce` runs exactly once per operator, but `doConsume` can run more than once for the
  // same operator if its immediate child calls `consume()` from more than one code site -- e.g. a
  // fused sort-merge join's matched-row and streamed-only-guard branches -- so declaring these in
  // `doProduce` and only referencing the names in `doConsume*` guarantees every call site shares
  // one counter instead of each independently declaring its own. Mirrors
  // `BaseLimitExec.countTerm`/`newLimitCountTerm`.
  private lazy val rankTerm = WindowGroupLimitExec.newStateTerm("rank")
  private lazy val groupKeyTerm = WindowGroupLimitExec.newStateTerm("groupKey")
  private lazy val countTerm = WindowGroupLimitExec.newStateTerm("count")
  private lazy val orderKeyTerm = WindowGroupLimitExec.newStateTerm("orderKey")
  private lazy val orderingTerm = WindowGroupLimitExec.newStateTerm("ordering")

  // Builds `orderKeyOrdering()` without pulling this operator (and, through `addReferenceObj`'s
  // object-graph serialization, its whole child subtree) into every task binary just to call back
  // into it -- see `genOrderKeyOrderingTerm` and `SortExec.sorterHolder` for the same pattern.
  @transient private lazy val orderKeyOrderingHolder =
    new WindowGroupLimitOrderKeyOrderingHolder(orderSpec)

  protected override def doProduce(ctx: CodegenContext): String = {
    ctx.addMutableState(CodeGenerator.JAVA_INT, rankTerm, forceInline = true, useFreshName = false)
    ctx.addMutableState("UnsafeRow", groupKeyTerm, forceInline = true, useFreshName = false)
    rankLikeFunction match {
      case _: RowNumber => // No further per-function state.
      case _: Rank =>
        ctx.addMutableState(CodeGenerator.JAVA_INT, countTerm, forceInline = true,
          useFreshName = false)
        ctx.addMutableState("UnsafeRow", orderKeyTerm, forceInline = true, useFreshName = false)
        genOrderKeyOrderingTerm(ctx)
      case _: DenseRank =>
        ctx.addMutableState("UnsafeRow", orderKeyTerm, forceInline = true, useFreshName = false)
        genOrderKeyOrderingTerm(ctx)
      case other =>
        // Unreachable: `supportCodegen` keeps any other rank-like function off the codegen path.
        throw SparkException.internalError(
          s"Codegen for rank-like function $other is not supported in WindowGroupLimitExec.")
    }
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    rankLikeFunction match {
      case _: RowNumber => doConsumeRowNumber(ctx, input)
      case _: Rank => doConsumeRank(ctx, input)
      case _: DenseRank => doConsumeDenseRank(ctx, input)
      case other =>
        // Unreachable: `supportCodegen` keeps any other rank-like function off the codegen path.
        throw SparkException.internalError(
          s"Codegen for rank-like function $other is not supported in WindowGroupLimitExec.")
    }
  }

  /**
   * Projects `keyExprs` (bound against `child.output`) into a reused `UnsafeRow` -- the
   * `rowWriter` row `GenerateUnsafeProjection.createCode` resets and returns, which is why
   * `genKeyChangeBranch` below `.copy()`s it before holding onto it across rows -- returning the
   * code that computes it (to be emitted by the caller) together with the Java expression that
   * reads it. Evaluating a key this way -- rather than through `genKeyChangeCode` below, which
   * also does the comparison -- is for cases where the evaluation must happen before a
   * `consume()` call but the comparison/branch may happen after it; see the order-key handling in
   * `doConsumeRank`/`doConsumeDenseRank`.
   *
   * The returned code MUST be emitted, in the generated method, before any `consume()` call for
   * the same row. `usedInputs` is empty (columns stay lazy), so these columns' evaluation code is
   * still attached to `input` here; `evaluateRequiredVariables` evaluates and clears it directly
   * on `input`. Emitting after `consume(ctx, input)` instead would be wrong for the opposite
   * reason: `consume()` calls `prepareRowVar` on `input` itself, whose `evaluateVariables` clears
   * `input`'s code there, and only afterward points `ctx.currentVars` at a separate set of copies
   * for whatever `consume()`'s caller builds next (see its `outputVars.map(_.copy())`). A
   * projection built after `consume(ctx, input)` would read `input`'s now-cleared `ExprCode`s
   * directly, not those copies, and risk reading a Java variable that was never actually assigned
   * -- the same hazard `checkLimitAndConsume` (in `doConsumeRowNumber`) calls out for the opposite
   * ordering.
   */
  private def genKeyProjection(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      keyExprs: Seq[Expression]): (String, String) = {
    val requiredAttrs = AttributeSet(keyExprs.flatMap(_.references))
    val evaluateCols = evaluateRequiredVariables(child.output, input, requiredAttrs)
    val keyCode = GenerateUnsafeProjection.createCode(ctx, bindReferences(keyExprs, child.output))
    (s"$evaluateCols\n${keyCode.code}", keyCode.value.toString)
  }

  /**
   * Generates the code that compares an already-projected key (`keyValue`, from
   * `genKeyProjection`) against the mutable `UnsafeRow` snapshot of the previous row's key
   * (`keyTerm`), and runs `onChange` whenever they differ, per `isDifferent` (e.g. `!a.equals(b)`,
   * or an `Ordering.compare(a, b) != 0`). `onChange` does not run for the very first row seen
   * (there is nothing to compare against yet, matching `BaseLimitIterator`'s subclasses, which
   * only initialize their tracking state then).
   */
  private def genKeyChangeBranch(
      keyTerm: String,
      keyValue: String,
      isDifferent: (String, String) => String,
      onChange: String): String = {
    s"""
       |if ($keyTerm == null) {
       |  $keyTerm = ($keyValue).copy();
       |} else if (${isDifferent(keyTerm, keyValue)}) {
       |  $onChange
       |  $keyTerm = ($keyValue).copy();
       |}
     """.stripMargin
  }

  /**
   * `genKeyProjection` followed immediately by `genKeyChangeBranch` against its result -- for
   * callers that evaluate and react to a key change in one place, with nothing else that needs to
   * run before a `consume()` call in between. This is the partition-key boundary check shared by
   * `RowNumber`/`Rank`/`DenseRank` alike: see `doConsumeRowNumber` and the grouped branches of
   * `doConsumeRank`/`doConsumeDenseRank`. It is not used for the order-key check in the latter two,
   * which must evaluate before `consume()` but branch after -- see `genKeyProjection`.
   */
  private def genKeyChangeCode(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      keyExprs: Seq[Expression],
      keyTerm: String,
      isDifferent: (String, String) => String,
      onChange: String): String = {
    val (evalCode, keyValue) = genKeyProjection(ctx, input, keyExprs)
    s"""
       |$evalCode
       |${genKeyChangeBranch(keyTerm, keyValue, isDifferent, onChange)}
     """.stripMargin
  }

  private def notEqualsCheck(a: String, b: String): String = s"!$a.equals($b)"

  /**
   * Builds the per-task `BaseOrdering` over `orderSpec`, used to compare the previous row's
   * projected order key (via `genKeyProjection`) against the current one. The `Ordering` itself is
   * built by `orderKeyOrderingHolder.orderKeyOrdering()`, not by referencing this operator: that
   * method compiles a fresh Janino class via `GenerateOrdering.generate`, which exists only in the
   * generating JVM and so can't be shipped to a task pre-built. Referencing the holder (instead of
   * `ctx.addReferenceObj("plan", this)`) lets the generated `init()` rebuild it once per task
   * without pulling this operator or its child subtree into the task binary -- see
   * `SortExec.sorterHolder` for the same pattern, and `orderKeyOrderingHolder`'s declaration above
   * for why that matters here.
   */
  private def genOrderKeyOrderingTerm(ctx: CodegenContext): String = {
    val holderTerm = ctx.addReferenceObj("orderKeyOrderingHolder", orderKeyOrderingHolder)
    ctx.addMutableState(classOf[BaseOrdering].getName, orderingTerm,
      v => s"$v = $holderTerm.orderKeyOrdering();", forceInline = true, useFreshName = false)
  }

  private def doConsumeRowNumber(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    // A `def`, not a `val`: `consume(ctx, input)` has the side effect of emitting (and marking
    // consumed) the evaluation code for every column `input` still owes. It must run after the
    // grouped branch below has already claimed the partition columns' code via
    // `genKeyChangeCode`, not before -- otherwise both would emit it, independently.
    def checkLimitAndConsume(): String =
      s"""
         |if ($rankTerm < $limit) {
         |  $rankTerm++;
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |}
       """.stripMargin

    // Rows arrive already clustered by `partitionSpec` (see `requiredChildDistribution` and
    // `requiredChildOrdering`; `supportCodegen` keeps this operator off the codegen path when
    // `partitionSpec` is empty), so a change in the projected partition key marks the start of a
    // new group: reset the rank counter, mirroring the group-boundary detection
    // `SortAggregateExec.doConsumeWithKeys` uses for its sort-based grouping.
    genKeyChangeCode(ctx, input, partitionSpec, groupKeyTerm, notEqualsCheck, s"$rankTerm = 0;") +
      checkLimitAndConsume()
  }

  private def doConsumeRank(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    // A `def`, not a `val`, for the same reason given on `doConsumeRowNumber`'s
    // `checkLimitAndConsume`: it must run after the grouped branch's partition-key check has
    // already claimed its columns' code.
    //
    // The whole body -- order-key projection, admission, and the resulting rank advance -- is
    // gated on `rank < limit`, mirroring `BaseLimitIterator.hasNext`/
    // `GroupIterator.skipRemainingRows`: once a group's rank reaches `limit`, the rest of
    // that group is frozen (no further admission, no further order-key work) until the next
    // group's partition-key reset. Neither this operator nor the interpreted
    // `GroupIterator.skipRemainingRows` can skip straight to the next group's first row -- both
    // still pay for the partition-key check on every remaining row of a rejected group -- but
    // neither does the order-key work for those rows either: the `rank < limit` guard is checked,
    // and found false, *before* `genKeyProjection` ever projects or compares the order key for an
    // already-rejected row.
    //
    // The guard uses `rank` as it stood after the *previous* row, matching how
    // `BaseLimitIterator.next()` checks `hasNext` (using the current `rank`) before calling
    // `increaseRank()`. A row can thus be admitted even though it is the one that pushes the
    // group's rank to (or past) `limit`; that one extra row is harmless, since the exact-but-more-
    // expensive `Filter` that runs after the real `WindowExec` trims it if needed.
    def body(): String = {
      val (orderKeyEval, orderKeyValue) = genKeyProjection(ctx, input, orderSpec.map(_.child))
      // `rank` jumps to `count` on an order-key change -- ties share a rank, and `rank` can skip
      // values across them -- while `count` (the number of rows seen so far) always advances.
      // Mirrors `RankLimitIterator.increaseRank`.
      val advanceRank = genKeyChangeBranch(orderKeyTerm, orderKeyValue,
        (a, b) => s"$orderingTerm.compare($a, $b) != 0", s"$rankTerm = $countTerm;")
      s"""
         |if ($rankTerm < $limit) {
         |  $orderKeyEval
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |  $advanceRank
         |  $countTerm++;
         |}
       """.stripMargin
    }

    genKeyChangeCode(ctx, input, partitionSpec, groupKeyTerm, notEqualsCheck,
      s"""
         |$rankTerm = 0;
         |$countTerm = 0;
         |$orderKeyTerm = null;
       """.stripMargin) + body()
  }

  private def doConsumeDenseRank(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    // A `def`, not a `val`, for the same reason given on `doConsumeRowNumber`'s
    // `checkLimitAndConsume`: it must run after the grouped branch's partition-key check has
    // already claimed its columns' code.
    //
    // Gated on `rank < limit` exactly as `doConsumeRank`'s `body` is -- see the longer comment
    // there for why the whole body (not just admission) is inside the guard, and how the guard's
    // use of the *previous* row's `rank` still allows one harmless extra admission at the cutoff.
    def body(): String = {
      val (orderKeyEval, orderKeyValue) = genKeyProjection(ctx, input, orderSpec.map(_.child))
      // Unlike `Rank`, `DenseRank` never skips values across ties: `rank` simply advances by one
      // on every order-key change. Mirrors `DenseRankLimitIterator.increaseRank`.
      val advanceRank = genKeyChangeBranch(orderKeyTerm, orderKeyValue,
        (a, b) => s"$orderingTerm.compare($a, $b) != 0", s"$rankTerm++;")
      s"""
         |if ($rankTerm < $limit) {
         |  $orderKeyEval
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |  $advanceRank
         |}
       """.stripMargin
    }

    genKeyChangeCode(ctx, input, partitionSpec, groupKeyTerm, notEqualsCheck,
      s"""
         |$rankTerm = 0;
         |$orderKeyTerm = null;
       """.stripMargin) + body()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowGroupLimitExec =
    copy(child = newChild)
}

object WindowGroupLimitExec {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  private def newStateTerm(prefix: String): String = {
    val id = curId.getAndIncrement()
    s"_windowGroupLimit_${prefix}_$id"
  }
}

/**
 * Builds the `orderKeyOrdering()` used by `doConsumeRank`/`doConsumeDenseRank`'s generated code to
 * compare order keys, capturing only `orderSpec` -- not the full `WindowGroupLimitExec` node and
 * its child subtree. See `WindowGroupLimitExec.genOrderKeyOrderingTerm` and
 * `SortExec.sorterHolder` for why that matters: everything that runs in a task should capture this
 * holder rather than the node, so the node and its child subtree are not serialized into every
 * task binary just to build this ordering.
 */
private[sql] class WindowGroupLimitOrderKeyOrderingHolder(orderSpec: Seq[SortOrder])
  extends Serializable {

  /**
   * An `Ordering` over a row made up of just `orderSpec`'s columns (in order, at ordinal
   * positions 0..n-1), rather than the full `child.output`. Preserves each column's own
   * direction and null ordering, same as the interpreted `OrderSpecProvider.ordering`, but is
   * usable against the narrow projection `genKeyProjection` builds for the order-key check (see
   * `doConsumeRank`/`doConsumeDenseRank`).
   *
   * Not `private`: generated code calls it by reflection-free reference (see
   * `genOrderKeyOrderingTerm`). The qualifier on the enclosing class is enforced at Scala compile
   * time only -- the bytecode stays public -- so runtime-generated code links to it unchanged.
   */
  def orderKeyOrdering(): BaseOrdering = {
    val boundOrders = orderSpec.zipWithIndex.map { case (order, i) =>
      order.copy(child = BoundReference(i, order.dataType, order.nullable))
    }
    GenerateOrdering.generate(boundOrders)
  }
}

abstract class BaseLimitIterator extends Iterator[InternalRow] {
  def numOutputRows: SQLMetric

  def input: Iterator[InternalRow]

  def limit: Int

  var rank = 0

  var nextRow: UnsafeRow = null

  // Increase the rank value.
  def increaseRank(): Unit

  override def hasNext: Boolean = rank < limit && input.hasNext

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    nextRow = input.next().asInstanceOf[UnsafeRow]
    increaseRank()
    numOutputRows += 1
    nextRow
  }

  def reset(): Unit
}

case class SimpleLimitIterator(
    input: Iterator[InternalRow],
    limit: Int,
    numOutputRows: SQLMetric) extends BaseLimitIterator {

  override def increaseRank(): Unit = {
    rank += 1
  }

  override def reset(): Unit = {
    rank = 0
  }
}

trait OrderSpecProvider {
  def output: Seq[Attribute]
  def orderSpec: Seq[SortOrder]

  val ordering = GenerateOrdering.generate(orderSpec, output)
  var currentRankRow: UnsafeRow = null
}

case class RankLimitIterator(
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    orderSpec: Seq[SortOrder],
    limit: Int,
    numOutputRows: SQLMetric) extends BaseLimitIterator with OrderSpecProvider {

  var count = 0

  override def increaseRank(): Unit = {
    if (count == 0) {
      currentRankRow = nextRow.copy()
    } else {
      if (ordering.compare(currentRankRow, nextRow) != 0) {
        rank = count
        currentRankRow = nextRow.copy()
      }
    }
    count += 1
  }

  override def reset(): Unit = {
    rank = 0
    count = 0
    currentRankRow = null
  }
}

case class DenseRankLimitIterator(
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    orderSpec: Seq[SortOrder],
    limit: Int,
    numOutputRows: SQLMetric) extends BaseLimitIterator with OrderSpecProvider {

  override def increaseRank(): Unit = {
    if (currentRankRow == null) {
      currentRankRow = nextRow.copy()
    } else {
      if (ordering.compare(currentRankRow, nextRow) != 0) {
        rank += 1
        currentRankRow = nextRow.copy()
      }
    }
  }

  override def reset(): Unit = {
    rank = 0
    currentRankRow = null
  }
}

class GroupedLimitIterator(
    input: Iterator[InternalRow],
    output: Seq[Attribute],
    partitionSpec: Seq[Expression],
    createLimitIterator: Iterator[InternalRow] => BaseLimitIterator)
  extends Iterator[InternalRow] {

  val grouping = UnsafeProjection.create(partitionSpec, output)

  // Manage the stream and the grouping.
  var nextRow: UnsafeRow = null
  var nextGroup: UnsafeRow = null
  var nextRowAvailable: Boolean = false
  protected[this] def fetchNextRow(): Unit = {
    nextRowAvailable = input.hasNext
    if (nextRowAvailable) {
      nextRow = input.next().asInstanceOf[UnsafeRow]
      nextGroup = grouping(nextRow)
    }
  }
  fetchNextRow()

  var groupIterator: GroupIterator = _
  var limitIterator: BaseLimitIterator = _
  if (nextRowAvailable) {
    groupIterator = new GroupIterator()
    limitIterator = createLimitIterator(groupIterator)
  }

  override final def hasNext: Boolean = nextRowAvailable && {
    if (!limitIterator.hasNext) {
      // if `limitIterator.hasNext` is false, we should jump to the next group if present
      groupIterator.skipRemainingRows()
      limitIterator.reset()
    }
    limitIterator.hasNext
  }

  override final def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    limitIterator.next()
  }

  class GroupIterator() extends Iterator[InternalRow] {
    // Before we start to fetch new input rows, make a copy of nextGroup.
    var currentGroup = nextGroup.copy()

    def hasNext: Boolean = nextRowAvailable && nextGroup == currentGroup

    def next(): InternalRow = {
      if (!hasNext) throw new NoSuchElementException
      val currentRow = nextRow.copy()
      fetchNextRow()
      currentRow
    }

    def skipRemainingRows(): Unit = {
      // Skip all the remaining rows in this group
      while (hasNext) {
        fetchNextRow()
      }

      // Switch to next group
      if (nextRowAvailable) currentGroup = nextGroup.copy()
    }
  }
}
