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

package org.apache.spark.sql.execution.joins

import java.io.Closeable
import java.math.{BigDecimal => JavaBigDecimal}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode,
  FalseLiteral, GeneratePredicate}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{AnyTimestampNanoType, BinaryType, BooleanType, DataType,
  DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegralType,
  NullType, NumericType, StringType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Performs an AS-OF join using sort-merge. Both sides are co-partitioned
 * by the equi-join keys and sorted by (equi-join keys, as-of key).
 * For each left row, we scan the right-side group buffer (forward-only)
 * to find the nearest match satisfying the as-of condition.
 *
 * The right-side buffer uses [[ExternalAppendOnlyUnsafeRowArray]] which
 * spills to disk when the in-memory threshold is exceeded, avoiding OOM
 * for skewed equi-key groups. Joins that can use a cursor scan (see
 * [[SortMergeAsOfJoinScanner.supportsCursorScan]]) read the right side once
 * and buffer nothing.
 *
 * Backward and forward joins support whole-stage codegen. The generated code runs the cursor
 * scan or the buffered scan and checks the conditions. The right-side buffer stays a Scala
 * object, since it can spill to disk. Nearest joins keep the interpreted scan. `codegenEnabled`
 * is the codegen config as it was when the join was planned. A join built by hand, not by the
 * planner, gets `false`.
 *
 * Note: When there are no equi-keys, both sides are collected into a
 * single partition (AllTuples) and, without a cursor scan, the entire
 * right side is buffered.
 */
case class SortMergeAsOfJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftSortExprs: Seq[Expression],
    rightSortExprs: Seq[Expression],
    asOfCondition: Expression,
    direction: AsOfJoinDirection,
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    codegenEnabled: Boolean = false,
    isSkewJoin: Boolean = false) extends ShuffledJoin {

  // The sides may have different key counts: each side is sorted on its own, and only the
  // equi-keys are compared across sides.
  require(leftSortExprs.nonEmpty && rightSortExprs.nonEmpty,
    s"$nodeName requires non-empty sort expressions on both sides")

  require(Seq(Inner, LeftOuter).exists(joinType == _),
    s"$nodeName does not support join type: $joinType")

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "numAsOfCursorTasks" -> SQLMetrics.createMetric(sparkContext, "number of cursor scan tasks"))

  override def requiredChildDistribution: Seq[Distribution] = {
    if (leftKeys.isEmpty) {
      AllTuples :: AllTuples :: Nil
    } else {
      super.requiredChildDistribution
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val leftOrdering = leftKeys.map(SortOrder(_, Ascending)) ++
      leftSortExprs.map(SortOrder(_, Ascending))
    val rightOrdering = rightKeys.map(SortOrder(_, Ascending)) ++
      rightSortExprs.map(SortOrder(_, Ascending))
    leftOrdering :: rightOrdering :: Nil
  }

  override def outputOrdering: Seq[SortOrder] = left.outputOrdering

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val spillSize = longMetric("spillSize")
    val inMemoryThreshold = conf.sortMergeJoinExecBufferInMemoryThreshold
    val sizeInBytesSpillThreshold = conf.sortMergeJoinExecBufferSpillSizeThreshold
    val spillThreshold = conf.sortMergeJoinExecBufferSpillThreshold
    val numAsOfCursorTasks = longMetric("numAsOfCursorTasks")
    val useCursorScan = cursorScanEnabled

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val scanner = new SortMergeAsOfJoinScanner(
        leftIter, rightIter,
        left.output, right.output,
        leftKeys, rightKeys,
        leftSortExprs, rightSortExprs,
        asOfCondition,
        joinType, condition,
        numOutputRows, spillSize, direction,
        inMemoryThreshold, sizeInBytesSpillThreshold, spillThreshold,
        useCursorScan, numAsOfCursorTasks
      )
      TaskContext.get().addTaskCompletionListener[Unit](_ => scanner.close())
      scanner.iterator
    }
  }

  /** Whether the join reads the right side once with a cursor, instead of buffering it. */
  private def cursorScanEnabled: Boolean = conf.sortMergeAsOfJoinCursorScanEnabled &&
    SortMergeAsOfJoinScanner.supportsCursorScan(
      direction, asOfCondition, condition, leftSortExprs, rightSortExprs)

  // Only a direction with an [[AsOfScanStrategy]] has a generated scan. Nearest joins keep the
  // interpreted scan, as its distance compare is harder to generate. The codegen config is read
  // once, when the join is planned.
  override def supportCodegen: Boolean = AsOfScanStrategy.of(direction).isDefined && codegenEnabled

  // ShuffledJoin already hides `isSkewJoin`, the last field. This also hides `codegenEnabled`,
  // the field before it, in the plan string.
  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    left.execute() :: right.execute() :: Nil

  // A left row makes at most one output row, so the output row needs no copy.
  override def needCopyResult: Boolean = false

  // The join loop reads the stage's input, so it stops at a LIMIT fused above it.
  override protected def canCheckLimitNotReached: Boolean = true

  /**
   * Generates the join loop. For each left row, a generated function finds the best right row,
   * or null: the cursor scan, or a scan of the buffered right group. The output columns go
   * straight to the parent operator.
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    // Inline mutable state since not many join operations in a task
    val leftInput = ctx.addMutableState("scala.collection.Iterator", "leftInput",
      v => s"$v = inputs[0];", forceInline = true)
    val rightInput = ctx.addMutableState("scala.collection.Iterator", "rightInput",
      v => s"$v = inputs[1];", forceInline = true)
    // The next right row, or null once the right side is done. Reading the right row after it
    // can overwrite it, so anything that keeps it longer must copy it.
    val rightPeek = ctx.addMutableState("UnsafeRow", "rightPeek", forceInline = true)
    val nextRightName = ctx.freshName("nextRight")
    val nextRight = ctx.addNewFunction(nextRightName,
      s"""
         |private UnsafeRow $nextRightName() {
         |  return $rightInput.hasNext() ? (UnsafeRow) $rightInput.next() : null;
         |}
       """.stripMargin, inlineToOuterClass = true)

    val (findMatch, initScan) = if (cursorScanEnabled) {
      genCursorScan(ctx, rightPeek, nextRight)
    } else {
      genBufferedScan(ctx, rightInput, rightPeek, nextRight)
    }

    val leftRow = ctx.freshName("leftRow")
    val matched = ctx.freshName("matched")
    val leftVars = genOneSideJoinVars(ctx, leftRow, left, setDefaultValue = false)
    val rightVars =
      genOneSideJoinVars(ctx, matched, right, setDefaultValue = joinType == LeftOuter)
    // An inner join drops a left row with no match. A left outer join keeps it with nulls.
    val skipUnmatched = if (joinType == LeftOuter) "" else s"if ($matched == null) continue;"
    val numOutput = metricTerm(ctx, "numOutputRows")
    val initJoin = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initJoin")
    val thisPlan = ctx.addReferenceObj("plan", this)

    s"""
       |if (!$initJoin) {
       |  $initJoin = true;
       |  $rightPeek = $nextRight();
       |  $initScan
       |}
       |while ($limitNotReachedCond $leftInput.hasNext()) {
       |  InternalRow $leftRow = (InternalRow) $leftInput.next();
       |  UnsafeRow $matched = $findMatch($leftRow);
       |  $skipUnmatched
       |  $numOutput.add(1);
       |  ${consume(ctx, leftVars ++ rightVars)}
       |  if (shouldStop()) return;
       |}
       |$thisPlan.cleanupResources();
     """.stripMargin
  }

  /**
   * Generates the cursor scan, which reads the right side once and never steps back. The left
   * rows of a group come in ascending as-of key order, so backward keeps its best row for the
   * next left row, and forward drops each failing row. Returns the function that finds the best
   * right row for a left row, and the code that starts the scan.
   */
  private def genCursorScan(
      ctx: CodegenContext,
      rightPeek: String,
      nextRight: String): (String, String) = {
    // The best right row so far: null, or `bestBuffer`, which holds a copy of it.
    val cursorBest = ctx.addMutableState("UnsafeRow", "cursorBest", forceInline = true)
    val bestBuffer = ctx.addMutableState("UnsafeRow", "cursorBestBuffer", forceInline = true)
    val groupHasRight =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "cursorGroupHasRight", forceInline = true)
    val copyToBestName = ctx.freshName("copyToCursorBestBuffer")
    val copyToBest = ctx.addNewFunction(copyToBestName,
      s"""
         |private UnsafeRow $copyToBestName(UnsafeRow row) {
         |  if ($bestBuffer == null) {
         |    $bestBuffer = row.copy();
         |  } else {
         |    $bestBuffer.copyFrom(row);
         |  }
         |  return $bestBuffer;
         |}
       """.stripMargin, inlineToOuterClass = true)
    val leftRow = ctx.freshName("leftRow")
    val (readLeftKeys, compareRightKeys, onNewGroup) =
      genEquiKeys(ctx, leftRow, rightPeek)
    val leftAsOfKey = createJoinKey(ctx, leftRow, leftSortExprs.take(1), left.output).head
    val rightAsOfKey = createJoinKey(ctx, rightPeek, rightSortExprs.take(1), right.output).head
    val asOfHolds = genConditionFunction(ctx, asOfCondition, "asOfHolds")
    // A new group skips the right rows of earlier groups without evaluating either as-of key,
    // then records whether the group has a right row. Without equi-keys, the only group starts
    // with the scan.
    val (startGroup, startOnlyGroup) = if (leftKeys.isEmpty) {
      ("", s"$groupHasRight = $rightPeek != null;")
    } else {
      val start =
        s"""
           |$cursorBest = null;
           |while ($rightPeek != null) {
           |  $compareRightKeys
           |  if (comp <= 0) break;
           |  $rightPeek = $nextRight();
           |}
           |$groupHasRight = $rightPeek != null && comp == 0;
         """.stripMargin
      (start, "")
    }
    // The same decisions as the interpreted `SortMergeAsOfJoinScanner.findWithCursor`.
    val strategy = AsOfScanStrategy(direction)
    // A kept row that a later left row can fail is checked again first.
    val checkKeptRow = if (strategy.matchOutlivesLeftRow) {
      ""
    } else {
      s"""
         |if ($cursorBest != null) {
         |  if ($asOfHolds($leftRow, $cursorBest)) return $cursorBest;
         |  $cursorBest = null;
         |}
       """.stripMargin
    }
    val onPass = if (strategy.firstMatchWins) s"return $cursorBest;" else ""
    // A failing row either waits for a later left row, or can match none and is dropped.
    val onFail =
      if (strategy.matchOutlivesLeftRow) s"return $cursorBest;" else s"$rightPeek = $nextRight();"

    val funcName = ctx.freshName("findWithCursor")
    val func = ctx.addNewFunction(funcName,
      s"""
         |private UnsafeRow $funcName(InternalRow $leftRow) {
         |  int comp = 0;
         |  $readLeftKeys
         |  ${onNewGroup(startGroup)}
         |  // A group with no right rows never evaluates the left as-of key, which can fail at
         |  // runtime, for example on an invalid cast under ANSI.
         |  if (!$groupHasRight) return null;
         |  ${leftAsOfKey.code}
         |  // A NULL left as-of key matches nothing. It must not move the cursor.
         |  if (${leftAsOfKey.isNull}) return null;
         |  $checkKeptRow
         |  while ($rightPeek != null) {
         |    $compareRightKeys
         |    // The group's right rows end at the first row of a later group.
         |    if (comp != 0) return $cursorBest;
         |    ${rightAsOfKey.code}
         |    if (${rightAsOfKey.isNull}) {
         |      $rightPeek = $nextRight();
         |      continue;
         |    }
         |    if ($asOfHolds($leftRow, $rightPeek)) {
         |      // The next right row can overwrite `rightPeek`, so keep a copy.
         |      $cursorBest = $copyToBest($rightPeek);
         |      $rightPeek = $nextRight();
         |      $onPass
         |    } else {
         |      $onFail
         |    }
         |  }
         |  return $cursorBest;
         |}
       """.stripMargin, inlineToOuterClass = true)
    (func, s"""
       |${metricTerm(ctx, "numAsOfCursorTasks")}.add(1);
       |$startOnlyGroup
     """.stripMargin)
  }

  /**
   * Generates the buffered scan. Each right group stays in an [[ExternalAppendOnlyUnsafeRowArray]],
   * which can spill, and is scanned for every left row. Returns the function that finds the best
   * right row for a left row, and the code that starts the scan.
   */
  private def genBufferedScan(
      ctx: CodegenContext,
      rightInput: String,
      rightPeek: String,
      nextRight: String): (String, String) = {
    val clsName = classOf[ExternalAppendOnlyUnsafeRowArray].getName
    val inMemoryThreshold = conf.sortMergeJoinExecBufferInMemoryThreshold
    val sizeInBytesSpillThreshold = conf.sortMergeJoinExecBufferSpillSizeThreshold
    val spillThreshold = conf.sortMergeJoinExecBufferSpillThreshold
    // Inline mutable state since not many join operations in a task
    val group = ctx.addMutableState(clsName, "rightGroup",
      v => s"$v = new $clsName($inMemoryThreshold, ${sizeInBytesSpillThreshold}L, " +
        s"$spillThreshold, ${sizeInBytesSpillThreshold}L);",
      forceInline = true)
    val leftRow = ctx.freshName("leftRow")

    val fillGroup = if (leftKeys.isEmpty) {
      val allBuffered = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "allRightBuffered")
      s"""
         |if (!$allBuffered) {
         |  // No equi-keys, so the whole right side is one group.
         |  $allBuffered = true;
         |  if ($rightPeek != null) {
         |    $group.add($rightPeek);
         |    $rightPeek = null;
         |  }
         |  while ($rightInput.hasNext()) {
         |    $group.add((UnsafeRow) $rightInput.next());
         |  }
         |}
       """.stripMargin
    } else {
      val (readLeftKeys, compareRightKeys, onNewGroup) =
        genEquiKeys(ctx, leftRow, rightPeek)
      val bufferGroup =
        s"""
           |$group.clear();
           |while ($rightPeek != null) {
           |  $compareRightKeys
           |  if (comp > 0) {
           |    $rightPeek = $nextRight();
           |    continue;
           |  }
           |  if (comp < 0) break;
           |  $group.add($rightPeek);
           |  $rightPeek = $nextRight();
           |}
         """.stripMargin
      s"""
         |$readLeftKeys
         |${onNewGroup(bufferGroup)}
       """.stripMargin
    }

    val asOfHolds = genConditionFunction(ctx, asOfCondition, "asOfHolds")
    val rightRow = ctx.freshName("rightRow")
    val residualHolds = condition
      .map(cond => s"${genConditionFunction(ctx, cond, "residualHolds")}($leftRow, $rightRow)")
      .getOrElse("true")
    val needsCopy = ctx.freshName("needsCopy")
    val best = ctx.freshName("best")
    // A spilled group reuses one row on every next(), so a kept match must be copied.
    val retained = s"($needsCopy ? $rightRow.copy() : $rightRow)"
    // The same decision as the interpreted `SortMergeAsOfJoinScanner.findInOrder`. Once the
    // as-of condition has held, the first row that fails it ends the scan.
    val stopAtMatch = if (AsOfScanStrategy(direction).firstMatchWins) s"return $best;" else ""
    val iter = ctx.freshName("iter")
    val asOfSeen = ctx.freshName("asOfSeen")

    val funcName = ctx.freshName("findInGroup")
    // The scan often stops early, so it closes the iterator to close the spill files it reads.
    val func = ctx.addNewFunction(funcName,
      s"""
         |private UnsafeRow $funcName(InternalRow $leftRow) throws java.io.IOException {
         |  int comp = 0;
         |  $fillGroup
         |  scala.collection.Iterator<UnsafeRow> $iter = $group.generateIterator();
         |  try {
         |    boolean $needsCopy = $group.isSpillBacked();
         |    boolean $asOfSeen = false;
         |    UnsafeRow $best = null;
         |    while ($iter.hasNext()) {
         |      UnsafeRow $rightRow = (UnsafeRow) $iter.next();
         |      if ($asOfHolds($leftRow, $rightRow)) {
         |        $asOfSeen = true;
         |        if ($residualHolds) {
         |          $best = $retained;
         |          $stopAtMatch
         |        }
         |      } else if ($asOfSeen) {
         |        return $best;
         |      }
         |    }
         |    return $best;
         |  } finally {
         |    if ($iter instanceof java.io.Closeable) {
         |      ((java.io.Closeable) $iter).close();
         |    }
         |  }
         |}
       """.stripMargin, inlineToOuterClass = true)
    val spillHook = s"${classOf[JoinHelper].getName}.recordSpillSizeOnTaskCompletion(" +
      s"$group, ${metricTerm(ctx, "spillSize")});"
    (func, spillHook)
  }

  /**
   * Generates the equi-key code of a scan, as three parts:
   *  - read the left keys, and return null for a NULL key, which matches nothing;
   *  - set `comp` to the order of the left keys against the keys of `rightPeek`, where a NULL
   *    right key sorts before any value;
   *  - run the given code when the left keys start a new group.
   * With no equi-keys all three are empty, so `comp` stays 0 and there is one group.
   */
  private def genEquiKeys(
      ctx: CodegenContext,
      leftRow: String,
      rightPeek: String): (String, String, String => String) = {
    if (leftKeys.isEmpty) return ("", "", _ => "")
    val leftKeyVars = createJoinKey(ctx, leftRow, leftKeys, left.output)
    val rightKeyVars = createJoinKey(ctx, rightPeek, rightKeys, right.output)
    // The keys of the current left group, kept across calls.
    val groupKeyVars = copyKeys(ctx, leftKeyVars)
    val groupValid = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "groupValid")

    val readLeftKeys =
      s"""
         |${leftKeyVars.map(_.code).mkString("\n")}
         |${anyNull(leftKeyVars).map(isNull => s"if ($isNull) return null;").getOrElse("")}
       """.stripMargin
    // A NULL right key is compared, not skipped. It sorts before any value.
    val compareRightKeys =
      s"""
         |${rightKeyVars.map(_.code).mkString("\n")}
         |${genRightKeyComparison(ctx, leftKeyVars, rightKeyVars)}
       """.stripMargin
    val newGroup = ctx.freshName("newGroup")
    val onNewGroup = (code: String) =>
      s"""
         |boolean $newGroup = !$groupValid;
         |if ($groupValid) {
         |  ${genComparison(ctx, leftKeyVars, groupKeyVars)}
         |  $newGroup = comp != 0;
         |}
         |if ($newGroup) {
         |  ${groupKeyVars.map(_.code).mkString("\n")}
         |  $groupValid = true;
         |  $code
         |}
       """.stripMargin
    (readLeftKeys, compareRightKeys, onNewGroup)
  }

  /** The Java check that any of `keys` is NULL, or None when no key can be NULL. */
  private def anyNull(keys: Seq[ExprCode]): Option[String] = {
    val isNulls = keys.map(_.isNull).filter(_ != FalseLiteral)
    if (isNulls.isEmpty) None else Some(isNulls.mkString(" || "))
  }

  /**
   * Generates code that sets `comp` to the order of the left keys against the right keys, key
   * by key. The left keys are never NULL here. A NULL right key sorts before any value, so it
   * compares as smaller.
   */
  private def genRightKeyComparison(
      ctx: CodegenContext,
      leftVars: Seq[ExprCode],
      rightVars: Seq[ExprCode]): String = {
    leftVars.zip(rightVars).zipWithIndex.map { case ((l, r), i) =>
      val compare = ctx.genComp(leftKeys(i).dataType, l.value, r.value)
      val withNull = if (r.isNull == FalseLiteral) compare else s"(${r.isNull} ? 1 : ($compare))"
      s"if (comp == 0) comp = $withNull;"
    }.mkString("comp = 0;\n", "\n", "")
  }

  /** Generates a function that tells whether `cond` is TRUE for a left row and a right row. */
  private def genConditionFunction(
      ctx: CodegenContext,
      cond: Expression,
      name: String): String = {
    val leftRow = ctx.freshName("leftRow")
    val rightRow = ctx.freshName("rightRow")
    val leftVars = genOneSideJoinVars(ctx, leftRow, left, setDefaultValue = false)
    val (_, checkCondition, _) =
      getJoinCondition(ctx, cond, leftVars, left, right, Some(rightRow))
    val funcName = ctx.freshName(name)
    ctx.addNewFunction(funcName,
      s"""
         |private boolean $funcName(InternalRow $leftRow, InternalRow $rightRow) {
         |  $checkCondition {
         |    return true;
         |  }
         |  return false;
         |}
       """.stripMargin)
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SortMergeAsOfJoinExec = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * Performs the sort-merge AS-OF join scan using forward-only iteration.
 *
 * Both inputs are sorted by (equi-keys, as-of key) ascending. For each
 * left row within an equi-key group, we scan the right-side buffer
 * forward to find the best match:
 *  - Backward (left.t >= right.t): the last satisfying row is the closest.
 *  - Forward (left.t <= right.t): the first satisfying row is the closest.
 *  - Nearest: the closest eligible rows before and after the left key are compared.
 *
 * With `useCursorScan`, the scan does not buffer the group. It walks the right input once and
 * keeps only the best row so far (see [[findWithCursor]]).
 */
private[joins] class SortMergeAsOfJoinScanner(
    leftIter: Iterator[InternalRow],
    rightIter: Iterator[InternalRow],
    leftOutput: Seq[Attribute],
    rightOutput: Seq[Attribute],
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftSortExprs: Seq[Expression],
    rightSortExprs: Seq[Expression],
    asOfCondition: Expression,
    joinType: JoinType,
    residualCondition: Option[Expression],
    numOutputRows: SQLMetric,
    spillSize: SQLMetric,
    direction: AsOfJoinDirection,
    inMemoryThreshold: Int,
    sizeInBytesSpillThreshold: Long,
    spillThreshold: Int,
    useCursorScan: Boolean,
    numAsOfCursorTasks: SQLMetric) {

  private val joinedOutput = leftOutput ++ rightOutput
  private val joinedRow = new JoinedRow()
  // Use nullable bound references so outer-join null padding is safe even when
  // right attributes are NOT NULL in the catalog schema. Every output row reuses the
  // projection's row.
  private val resultProjection = {
    val nullableRefs = joinedOutput.zipWithIndex.map { case (attr, i) =>
      BoundReference(i, attr.dataType, nullable = true)
    }
    UnsafeProjection.create(nullableRefs, joinedOutput)
  }

  // A repeated part must not run before its guard: `y <> 0 AND x / y > 1 AND x / y < 10` would
  // divide by zero when y is 0. So the generated predicates skip subexpression elimination. The
  // interpreted fallback keeps it, but runs a shared part only when an operand reaches it, so
  // AND, OR and IF still short-circuit. Both are lazy, so a task that checks no right row
  // compiles neither condition.
  private lazy val boundAsOfCond = ShortCircuitPredicate.create(asOfCondition, joinedOutput)
  private lazy val boundResidualCond =
    residualCondition.map(ShortCircuitPredicate.create(_, joinedOutput))

  // A right key can be NULL even when the left key is NOT NULL, so every key is nullable here.
  private val equiKeyOrdering: Option[BaseOrdering] =
    if (leftKeys.nonEmpty) {
      Some(RowOrdering.createNaturalAscendingOrdering(leftKeys.map(_.dataType)))
    } else {
      None
    }

  private val leftKeyProj = UnsafeProjection.create(leftKeys, leftOutput)
  private val rightKeyProj = UnsafeProjection.create(rightKeys, rightOutput)

  // Lazy, since only Nearest reads the keys.
  private lazy val nearestKeys: (Expression, Expression, Ordering[Any]) =
    (leftSortExprs, rightSortExprs) match {
      case (Seq(leftKey), Seq(rightKey))
          if AsOfJoin.supportsNearestKeys(leftKey.dataType, rightKey.dataType) =>
        (
          bindReference(leftKey, leftOutput),
          bindReference(rightKey, rightOutput),
          TypeUtils.getInterpretedOrdering(leftKey.dataType))
      case _ =>
        throw SparkException.internalError(
          s"Unexpected nearest keys: $leftSortExprs and $rightSortExprs")
    }

  // None for a nearest join, which has no ordered scan.
  private val orderedScanStrategy: Option[AsOfScanStrategy] = AsOfScanStrategy.of(direction)

  // Lazy, since only the cursor scan reads the keys.
  private lazy val cursorKeys: (Expression, Expression) =
    (bindReference(leftSortExprs.head, leftOutput), bindReference(rightSortExprs.head, rightOutput))

  // Materialize an all-null right row as UnsafeRow. GenericInternalRow cannot be
  // passed through identity UnsafeProjection when right columns are NOT NULL.
  private val nullRightRow: InternalRow = {
    val nullableRefs = rightOutput.zipWithIndex.map { case (attr, i) =>
      BoundReference(i, attr.dataType, nullable = true)
    }
    val proj = UnsafeProjection.create(nullableRefs, rightOutput)
    proj(new GenericInternalRow(rightOutput.length))
  }

  // Spill-backed right-side buffer
  private val rightGroupBuffer = new ExternalAppendOnlyUnsafeRowArray(
    inMemoryThreshold, sizeInBytesSpillThreshold, spillThreshold, sizeInBytesSpillThreshold)

  private var rightGroupKey: UnsafeRow = _
  // The next right row. It is the output row of `rightToUnsafe`, so reading the right row after
  // it overwrites it. Anything that keeps it longer must copy it.
  private var rightPeek: UnsafeRow = _
  private var rightDone: Boolean = !rightIter.hasNext

  // Cursor scan state: the left equi-key of the current group, whether the group has any right
  // row, and its best right row so far. `cursorBest` is null or `cursorBestBuffer`, which holds
  // a copy of the best row and is reused for every new best row.
  private var cursorGroupKey: UnsafeRow = _
  private var cursorGroupHasRight: Boolean = false
  private var cursorBest: UnsafeRow = _
  private var cursorBestBuffer: UnsafeRow = _

  // Projection to convert right rows to UnsafeRow for the buffer
  private val rightToUnsafe = UnsafeProjection.create(rightOutput, rightOutput)

  if (!rightDone) {
    rightPeek = rightToUnsafe(rightIter.next())
  }

  if (useCursorScan) {
    // Only an ordered scan has a cursor. Checked here, so that it fails whatever the input is.
    if (orderedScanStrategy.isEmpty) {
      throw SparkException.internalError(s"The $direction AS-OF join has no cursor scan")
    }
    numAsOfCursorTasks += 1
  }

  def close(): Unit = {
    spillSize += rightGroupBuffer.spillSize
    rightGroupBuffer.clear()
  }

  def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
    private var nextRow: InternalRow = _
    private val leftIterBuffered = leftIter.buffered

    override def hasNext: Boolean = {
      if (nextRow != null) return true
      nextRow = findNext()
      nextRow != null
    }

    override def next(): InternalRow = {
      if (!hasNext) throw new NoSuchElementException
      val result = nextRow
      nextRow = null
      result
    }

    private def findNext(): InternalRow = {
      while (leftIterBuffered.hasNext) {
        val leftRow = leftIterBuffered.next()
        val leftKey = leftKeyProj(leftRow).copy()

        // Skip left rows with null equi-keys (EqualTo semantics:
        // NULL = NULL -> NULL, i.e. no match)
        if (leftKeys.nonEmpty && leftKey.anyNull) {
          if (joinType == LeftOuter) {
            numOutputRows += 1
            joinedRow.withLeft(leftRow).withRight(nullRightRow)
            return resultProjection(joinedRow)
          }
        } else {
          val bestMatch = if (useCursorScan) {
            findWithCursor(leftRow, leftKey)
          } else {
            advanceRightTo(leftKey)
            findBestInGroup(leftRow)
          }

          if (bestMatch != null) {
            numOutputRows += 1
            joinedRow.withLeft(leftRow).withRight(bestMatch)
            return resultProjection(joinedRow)
          } else if (joinType == LeftOuter) {
            numOutputRows += 1
            joinedRow.withLeft(leftRow).withRight(nullRightRow)
            return resultProjection(joinedRow)
          }
        }
      }
      null
    }
  }

  private def advanceRightTo(leftKey: UnsafeRow): Unit = {
    equiKeyOrdering match {
      case None =>
        if (rightGroupBuffer.isEmpty && !rightDone) {
          bufferAllRight()
        }
      case Some(ordering) =>
        if (rightGroupKey != null &&
            ordering.compare(leftKey, rightGroupKey) == 0) {
          return
        }

        while (!rightDone && rightPeek != null) {
          val rightKey = rightKeyProj(rightPeek)
          val cmp = ordering.compare(leftKey, rightKey)
          if (cmp > 0) {
            advanceRightPeek()
          } else if (cmp == 0) {
            bufferRightGroup(leftKey, ordering)
            return
          } else {
            rightGroupBuffer.clear()
            rightGroupKey = null
            return
          }
        }
        rightGroupBuffer.clear()
        rightGroupKey = null
    }
  }

  private def bufferRightGroup(
      leftKey: UnsafeRow, ordering: BaseOrdering): Unit = {
    rightGroupBuffer.clear()
    rightGroupKey = leftKey.copy()

    while (!rightDone && rightPeek != null) {
      val rightKey = rightKeyProj(rightPeek)
      if (ordering.compare(leftKey, rightKey) == 0) {
        rightGroupBuffer.add(rightPeek)
        advanceRightPeek()
      } else {
        return
      }
    }
  }

  /**
   * Moves `rightPeek` to the next right row, or to null at the end of the right input. The row
   * is not copied: the buffer copies the rows it keeps, and the cursor copies its best row.
   */
  private def advanceRightPeek(): Unit = {
    rightPeek = if (rightIter.hasNext) {
      rightToUnsafe(rightIter.next())
    } else {
      rightDone = true; null
    }
  }

  private def bufferAllRight(): Unit = {
    rightGroupBuffer.clear()
    if (rightPeek != null) {
      rightGroupBuffer.add(rightPeek)
      rightPeek = null
    }
    while (rightIter.hasNext) {
      rightGroupBuffer.add(rightToUnsafe(rightIter.next()))
    }
    rightDone = true
  }

  /** Finds the best right row in the current group for `leftRow`, or null if none passes. */
  private def findBestInGroup(leftRow: InternalRow): InternalRow = orderedScanStrategy match {
    case Some(strategy) => findInOrder(leftRow, strategy)
    case None => findNearest(leftRow)
  }

  /**
   * Retains `rightRow` as the best match found so far.
   *
   * While the right-side buffer is held in memory, its iterator yields the distinct rows stored
   * in the buffer and a match can be retained as is. Once the buffer is spill-backed the
   * iterator re-points a single [[UnsafeRow]] on every `next()`, so the match has to be copied
   * out before the scan advances. `needsCopy` is read once per scan by the callers.
   */
  private def retainMatch(rightRow: UnsafeRow, needsCopy: Boolean): UnsafeRow = {
    if (needsCopy) rightRow.copy() else rightRow
  }

  /**
   * Closes a right-group iterator, which the scans below often leave before its end. Once the
   * group has spilled, the iterator holds a reader open on every spill file until it ends or is
   * closed, so an unclosed one per left row would keep them all open until the task ends. A kept
   * match is already a copy then, so closing the iterator does not change it.
   */
  private def closeIterator(iter: Iterator[UnsafeRow]): Unit = iter match {
    case closeable: Closeable => closeable.close()
    case _ =>
  }

  /** Whether `cond` is TRUE on the current joined row. FALSE and NULL both fail. */
  private def holds(cond: BasePredicate): Boolean = cond.eval(joinedRow)

  /** Whether the residual ON condition, if any, holds on the current joined row. */
  private def residualHolds: Boolean = boundResidualCond match {
    // A match, not `forall`, so the per-row check makes no closure.
    case Some(cond) => cond.eval(joinedRow)
    case None => true
  }

  /**
   * Backward and forward joins scan the group in as-of key order for the row that passes the
   * as-of and residual conditions and is closest to the left key: the first passing row or the
   * last, as [[AsOfScanStrategy.firstMatchWins]] tells. Once the as-of condition has been true,
   * the scan stops at the first row that fails it, as later sorted rows cannot satisfy it. That
   * row is at or past the left key for a backward join, and at or past the tolerance bound for a
   * forward join.
   */
  private def findInOrder(leftRow: InternalRow, strategy: AsOfScanStrategy): InternalRow = {
    val firstMatchWins = strategy.firstMatchWins
    var bestMatch: InternalRow = null
    val iter = rightGroupBuffer.generateIterator()
    val needsCopy = rightGroupBuffer.isSpillBacked
    var asOfSeen = false

    joinedRow.withLeft(leftRow)
    try {
      while (iter.hasNext) {
        val rightRow = iter.next()
        joinedRow.withRight(rightRow)

        if (holds(boundAsOfCond)) {
          asOfSeen = true
          if (residualHolds) {
            bestMatch = retainMatch(rightRow, needsCopy)
            if (firstMatchWins) return bestMatch
          }
        } else if (asOfSeen) {
          return bestMatch
        }
      }
      bestMatch
    } finally {
      closeIterator(iter)
    }
  }

  /**
   * Finds the best right row for `leftRow` by moving a cursor over the right input, or null.
   *
   * Left rows of a group come in ascending as-of key order. So a right row that passes for one
   * left row also passes for the next (backward), and a row that fails stays failed (forward).
   * The cursor never steps back: backward stops at the first failing row and keeps it for the
   * next left row, and forward drops each failing row, since no later left row can match it.
   * The [[AsOfScanStrategy]] of the direction makes these choices, as in the generated scan.
   */
  private def findWithCursor(leftRow: InternalRow, leftKey: UnsafeRow): InternalRow = {
    // Without equi-keys, all rows form one group, so only the first left row starts it.
    if (cursorGroupKey == null || equiKeyOrdering.exists(_.compare(leftKey, cursorGroupKey) != 0)) {
      startCursorGroup(leftKey)
    }
    // Like the buffered scan, a group with no right rows never evaluates the left as-of key,
    // which can fail at runtime, for example on an invalid cast under ANSI.
    if (!cursorGroupHasRight) return null

    val (leftAsOfKey, rightAsOfKey) = cursorKeys
    // A NULL left as-of key matches nothing. It must not move the cursor, or forward would drop
    // right rows that later left rows need.
    if (leftAsOfKey.eval(leftRow) == null) return null

    // The scanner checked that the direction has a strategy when it was built.
    val strategy = orderedScanStrategy.get
    joinedRow.withLeft(leftRow)
    if (!strategy.matchOutlivesLeftRow && cursorBest != null) {
      // The cursor is already past the best row, and every row before it failed.
      joinedRow.withRight(cursorBest)
      if (holds(boundAsOfCond)) return cursorBest
      cursorBest = null
    }

    // The group's right rows end at the first row of a later group.
    while (rightPeek != null && compareToRightPeek(leftKey) == 0) {
      if (rightAsOfKey.eval(rightPeek) == null) {
        // A NULL right as-of key: no left row can match it.
        advanceRightPeek()
      } else {
        joinedRow.withRight(rightPeek)
        if (holds(boundAsOfCond)) {
          // The next right row overwrites `rightPeek`, so keep a copy.
          cursorBest = copyToCursorBestBuffer(rightPeek)
          advanceRightPeek()
          if (strategy.firstMatchWins) return cursorBest
        } else if (strategy.matchOutlivesLeftRow) {
          // The row waits for a later left row.
          return cursorBest
        } else {
          // No later left row can match the row.
          advanceRightPeek()
        }
      }
    }
    cursorBest
  }

  /**
   * Starts the cursor group of `leftKey`. It skips the right rows of earlier groups without
   * evaluating either as-of key, then records whether the group has a right row. A right row
   * with a NULL as-of key counts, since the buffered scan evaluates the as-of condition on it.
   */
  private def startCursorGroup(leftKey: UnsafeRow): Unit = {
    cursorGroupKey = leftKey
    cursorBest = null
    while (rightPeek != null && compareToRightPeek(leftKey) > 0) {
      advanceRightPeek()
    }
    cursorGroupHasRight = rightPeek != null && compareToRightPeek(leftKey) == 0
  }

  /**
   * Copies `row` into `cursorBestBuffer` and returns the buffer. One buffer is enough, since the
   * cursor never needs the old best row once it has a new one.
   */
  private def copyToCursorBestBuffer(row: UnsafeRow): UnsafeRow = {
    if (cursorBestBuffer == null) {
      cursorBestBuffer = row.copy()
    } else {
      cursorBestBuffer.copyFrom(row)
    }
    cursorBestBuffer
  }

  /** Compares `leftKey` with the equi-key of `rightPeek`. Without equi-keys, all rows tie. */
  private def compareToRightPeek(leftKey: UnsafeRow): Int = equiKeyOrdering match {
    case Some(ordering) => ordering.compare(leftKey, rightKeyProj(rightPeek))
    case None => 0
  }

  /** Finds the nearest row passing both conditions, preferring the predecessor on equal gaps. */
  private def findNearest(leftRow: InternalRow): InternalRow = {
    val (leftKeyExpr, rightKeyExpr, keyOrdering) = nearestKeys
    val leftKey = leftKeyExpr.eval(leftRow)
    if (leftKey == null) return null

    var bestBefore: UnsafeRow = null
    var bestBeforeKey: Any = null
    val iter = rightGroupBuffer.generateIterator()
    val needsCopy = rightGroupBuffer.isSpillBacked

    joinedRow.withLeft(leftRow)
    try {
      while (iter.hasNext) {
        val rightRow = iter.next()
        joinedRow.withRight(rightRow)

        if (holds(boundAsOfCond) && residualHolds) {
          val rightKey = rightKeyExpr.eval(rightRow)
          if (rightKey != null) {
            if (keyOrdering.lt(rightKey, leftKey)) {
              // Keep the first eligible row at each key so equal-distance ties stay stable.
              if (bestBefore == null || keyOrdering.lt(bestBeforeKey, rightKey)) {
                bestBefore = retainMatch(rightRow, needsCopy)
                bestBeforeKey = InternalRow.copyValue(rightKey)
              }
            } else if (keyOrdering.equiv(rightKey, leftKey)) {
              // An exact match is the nearest row, even for an infinite key (Inf - Inf is NaN).
              return retainMatch(rightRow, needsCopy)
            } else if (bestBefore == null ||
                isAfterCloser(bestBeforeKey, leftKey, rightKey, leftKeyExpr.dataType)) {
              return retainMatch(rightRow, needsCopy)
            } else {
              return bestBefore
            }
          }
        }
      }

      bestBefore
    } finally {
      closeIterator(iter)
    }
  }

  /** The exact value of a nearest key, or None for a NaN or infinite floating-point key. */
  private def exactNearestKeyValue(key: Any, dataType: DataType): Option[JavaBigDecimal] =
    dataType match {
      case _: IntegralType | DateType | _: YearMonthIntervalType | _: DayTimeIntervalType |
           TimestampType | TimestampNTZType =>
        Some(JavaBigDecimal.valueOf(key.asInstanceOf[java.lang.Number].longValue()))
      case _: AnyTimestampNanoType =>
        val timestamp = key.asInstanceOf[TimestampNanosVal]
        Some(JavaBigDecimal.valueOf(timestamp.epochMicros)
          .multiply(JavaBigDecimal.valueOf(1000))
          .add(JavaBigDecimal.valueOf(timestamp.nanosWithinMicro)))
      case _: DecimalType => Some(key.asInstanceOf[Decimal].toJavaBigDecimal)
      case FloatType | DoubleType =>
        val value = key.asInstanceOf[java.lang.Number].doubleValue()
        // The BigDecimal(double) constructor keeps the exact binary value; valueOf rounds it.
        if (value.isNaN || value.isInfinite) None else Some(new JavaBigDecimal(value))
      case _ => throw SparkException.internalError(s"Unsupported nearest key type: $dataType")
    }

  /** Whether the row after the left key is strictly closer than the row before it. */
  private def isAfterCloser(
      beforeKey: Any,
      leftKey: Any,
      afterKey: Any,
      dataType: DataType): Boolean = {
    (exactNearestKeyValue(beforeKey, dataType),
      exactNearestKeyValue(leftKey, dataType),
      exactNearestKeyValue(afterKey, dataType)) match {
      case (Some(before), Some(left), Some(after)) =>
        after.subtract(left).compareTo(left.subtract(before)) < 0
      case _ =>
        // A NaN or infinite key leaves at most one gap finite.
        nonFiniteGapRank(leftKey, afterKey) < nonFiniteGapRank(beforeKey, leftKey)
    }
  }

  /** 0 for a gap between finite keys, 1 for an infinite gap, and 2 for a gap from NaN. */
  private def nonFiniteGapRank(lowerKey: Any, upperKey: Any): Int = {
    val lower = lowerKey.asInstanceOf[java.lang.Number].doubleValue()
    val upper = upperKey.asInstanceOf[java.lang.Number].doubleValue()
    if (lower.isNaN || upper.isNaN) 2 else if (lower.isInfinite || upper.isInfinite) 1 else 0
  }
}

private[execution] object SortMergeAsOfJoinScanner {

  /**
   * Whether the scan can read the right side once with a cursor, instead of buffering each
   * right-side group and scanning it again for every left row.
   *
   * This needs a backward or forward join with no residual condition, whose as-of condition
   * only compares the two sort keys. Both sides are sorted on exactly those keys, so the right
   * rows that pass the condition move forward as the left key grows. Anything else, such as a
   * tolerance, a tuple, or an operand cast that the sort key does not have, keeps the buffer.
   *
   * The condition must name the left side first, since the cursor evaluates the left key first.
   * With the right side first, a NULL right key ends the condition before the left key, which
   * can fail under ANSI. Only a DataFrame join with reversed keys builds such a condition.
   */
  def supportsCursorScan(
      direction: AsOfJoinDirection,
      asOfCondition: Expression,
      residualCondition: Option[Expression],
      leftSortExprs: Seq[Expression],
      rightSortExprs: Seq[Expression]): Boolean = {
    // The operands as (left, right). A direction without an ordered scan has no cursor scan.
    val operands = AsOfScanStrategy.of(direction).flatMap(_.cursorOperands(asOfCondition))
    (leftSortExprs, rightSortExprs, operands) match {
      case (Seq(leftKey), Seq(rightKey), Some((leftOperand, rightOperand))) =>
        residualCondition.isEmpty &&
          leftOperand.semanticEquals(leftKey) &&
          rightOperand.semanticEquals(rightKey) &&
          leftKey.dataType == rightKey.dataType &&
          supportsCursorKey(leftKey.dataType)
      case _ => false
    }
  }

  /**
   * The key types the cursor scan supports. STRUCT, ARRAY, TIME, nanosecond timestamp and
   * user-defined type keys keep the buffered scan for now, since the cursor scan has no tests
   * for them yet.
   */
  private def supportsCursorKey(dataType: DataType): Boolean = dataType match {
    case _: NumericType | BooleanType | _: StringType | BinaryType | NullType => true
    case DateType | TimestampType | TimestampNTZType => true
    case _: YearMonthIntervalType | _: DayTimeIntervalType => true
    case _ => false
  }
}

/**
 * What a backward or forward scan of a right group does differently. The right rows of a group
 * and its left rows both come in ascending as-of key order. The interpreted scans of
 * [[SortMergeAsOfJoinScanner]] and the generated scans of [[SortMergeAsOfJoinExec]] both read
 * the choices below. The scan loops themselves are still written once for each.
 *
 * A nearest join has no strategy: it compares the gaps on both sides of the left key, and only
 * has an interpreted scan.
 */
private[joins] sealed abstract class AsOfScanStrategy {

  /**
   * Whether the first right row that passes the as-of and residual conditions is the closest to
   * the left key, so the scan can stop there. Otherwise the last such row is the closest, and
   * the scan reads on until a row fails the as-of condition.
   */
  def firstMatchWins: Boolean

  /**
   * The operands of an as-of condition that the cursor scan of this direction takes, as
   * (left, right), or None for any other condition. The cursor scan only takes a comparison
   * that the right rows at one end of a group pass: the start when the last match wins, and the
   * end when the first match wins.
   */
  def cursorOperands(asOfCondition: Expression): Option[(Expression, Expression)]

  /**
   * For the cursor scan: whether a right row that passes for one left row also passes for every
   * later left row of the group.
   *
   * If so, a failing row is at or ahead of the left key. The scan stops there, and keeps the row
   * for the next left row. If not, a failing row is at or behind the left key. No later left row
   * can match it, so the scan drops it, and it checks the kept best row again for every left
   * row.
   *
   * This is the opposite of [[firstMatchWins]], as the conditions of [[cursorOperands]] tell.
   * When the last match wins, the passing rows start the group, and a larger left key only adds
   * rows to them. When the first match wins, they end the group, and a larger left key only
   * takes rows from them.
   */
  final def matchOutlivesLeftRow: Boolean = !firstMatchWins
}

private[joins] object AsOfScanStrategy {

  /** Backward (left.t >= right.t): the passing rows come first, and the last one is closest. */
  case object LastMatch extends AsOfScanStrategy {
    override def firstMatchWins: Boolean = false
    override def cursorOperands(asOfCondition: Expression): Option[(Expression, Expression)] =
      asOfCondition match {
        case GreaterThanOrEqual(left, right) => Some((left, right))
        case GreaterThan(left, right) => Some((left, right))
        case _ => None
      }
  }

  /** Forward (left.t <= right.t): the passing rows come last, and the first one is closest. */
  case object FirstMatch extends AsOfScanStrategy {
    override def firstMatchWins: Boolean = true
    override def cursorOperands(asOfCondition: Expression): Option[(Expression, Expression)] =
      asOfCondition match {
        case LessThanOrEqual(left, right) => Some((left, right))
        case LessThan(left, right) => Some((left, right))
        case _ => None
      }
  }

  /** The strategy of `direction`, or None for a direction that has no ordered scan. */
  def of(direction: AsOfJoinDirection): Option[AsOfScanStrategy] = direction match {
    case Backward => Some(LastMatch)
    case Forward => Some(FirstMatch)
    case Nearest => None
  }

  /** The strategy of `direction`, which must have one. */
  def apply(direction: AsOfJoinDirection): AsOfScanStrategy = of(direction).getOrElse {
    throw SparkException.internalError(s"The $direction AS-OF join has no ordered scan")
  }
}

/**
 * Builds a predicate whose AND, OR and IF still skip the parts they do not need. The generated
 * predicate skips subexpression elimination, which would run a repeated part first. The
 * interpreted fallback, used when code generation is off or fails, keeps subexpression
 * elimination, but runs a shared part only when an operand reaches it.
 */
private[joins] object ShortCircuitPredicate
    extends CodeGeneratorWithInterpretedFallback[Expression, BasePredicate] {

  def create(e: Expression, inputSchema: Seq[Attribute]): BasePredicate =
    createObject(bindReference(e, inputSchema))

  override protected def createCodeGeneratedObject(in: Expression): BasePredicate =
    GeneratePredicate.generate(in, useSubexprElimination = false)

  override protected def createInterpretedObject(in: Expression): BasePredicate =
    InterpretedPredicate(in)
}
