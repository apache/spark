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

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.RangeJoin
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CodegenSupport, ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs a range join on two tables. Supported types are inner, left outer, right outer,
 * left semi, and left anti. The preserved side is streamed: unmatched build rows are not
 * emitted, because the broadcast is copied to every partition. See `ExtractRangeJoinKeys`
 * for the exact set of join-condition shapes that are recognized as a range join.
 *
 * The build side is an index. Point-in-range and interval overlap both build an
 * [[IntervalIndex]] and probe it with `overlapping`. A single inequality builds a
 * [[PointIndex]] and scans the side of the bound the build side holds. Keys that
 * map to one order-preserving long instead build [[LongIntervalIndex]] or
 * [[LongPointIndex]], so their probes compare raw longs without boxing. Either
 * probe returns a superset. A candidate is kept only when the original join
 * condition is true, so inclusivity stays on the predicate. Heavy overlap can
 * make one probe return almost every build row.
 */
case class BroadcastRangeJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    rangeJoin: RangeJoin)
  extends JoinCodegenSupport {

  if (condition.isEmpty) {
    throw SparkException.internalError("BroadcastRangeJoinExec requires a join condition.")
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override lazy val requiredChildDistribution: Seq[Distribution] = {
    val mode = RangeRelationBroadcastMode(
      BindReferences.bindReferences(buildKeys, buildOutput),
      RangeIndexKind(rangeJoin))
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case LeftSemi | LeftAnti =>
        left.output
      case t =>
        throw SparkException.internalError(s"BroadcastRangeJoin should not take $t as the JoinType")
    }
  }

  // The preserved side is streamed, so its partitioning and ordering pass through.
  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = streamedPlan.outputOrdering

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType $buildSide ($opId)".trim
  }

  private[this] lazy val buildPlan = if (buildSide == BuildLeft) left else right
  private[this] lazy val streamedPlan = if (buildSide == BuildLeft) right else left

  private[this] lazy val buildKeys = if (buildSide == BuildLeft) leftKeys else rightKeys
  private[this] lazy val streamedKeys = if (buildSide == BuildLeft) rightKeys else leftKeys

  @transient private[this] lazy val buildOutput = buildPlan.output
  @transient private[this] lazy val streamedOutput = streamedPlan.output

  // The point-index scan used to probe this join's build side. See `PointScan.forJoin`.
  private lazy val pointScan: PointScan = PointScan.forJoin(rangeJoin, buildSide)

  // Alias so `innerJoin`/`outerJoin`/`leftExistenceJoin` need no change below; the class
  // itself lives on the companion object (see there for why).
  private type ProbeContext = BroadcastRangeJoinExec.ProbeContext

  private def perPartition(
      relation: Broadcast[RangeRelation])(
      f: ProbeContext => Iterator[InternalRow]): RDD[InternalRow] = {
    val boundStreamKeys = BindReferences.bindReferences(streamedKeys, streamedOutput)
    val schema = left.output ++ right.output
    val numBuildFields = buildPlan.output.length
    val join = rangeJoin
    val side = buildSide
    streamedPlan.execute().mapPartitionsInternal { stream =>
      // relation.value is read here, inside the task closure, so each executor fetches
      // the index through the broadcast machinery (and shares it across the partitions
      // that closure runs over) instead of the driver baking the materialized index
      // into the closure that gets shipped out. PointScan.forJoin is passed by-name into
      // makeCandidateFn, so it is only evaluated - and only throws for a non-partial-range
      // join - inside the point-index branches that actually need it, same as before. It
      // reads `join`/`side` (copied above), not `rangeJoin`/`buildSide` directly, so this
      // closure captures only those small values instead of the enclosing plan node.
      val candidateFn = BroadcastRangeJoinExec.makeCandidateFn(
        relation.value, boundStreamKeys, PointScan.forJoin(join, side))
      f(new BroadcastRangeJoinExec.ProbeContext(
        stream,
        Predicate.create(condition.get, schema).eval _,
        new JoinedRow,
        new GenericInternalRow(numBuildFields),
        candidateFn,
        side))
    }
  }

  private def innerJoin(relation: Broadcast[RangeRelation]): RDD[InternalRow] = {
    perPartition(relation) { p =>
      p.stream.flatMap { row =>
        p.candidates(row).flatMap { buildRow =>
          val joined = p.join(row, buildRow)
          if (p.condition(joined)) Iterator.single(joined) else Iterator.empty
        }
      }
    }
  }

  /**
   * Emit each candidate the condition accepts. When none were, emit the stream
   * row once with the build side padded by nulls.
   *
   * The loop test checks `streamRow` before `stream.hasNext`. A whole-stage
   * stream reuses one `UnsafeRow`, and `hasNext` fills that buffer. It must
   * not run while the current row is still being joined.
   */
  private def outerJoin(relation: Broadcast[RangeRelation]): RDD[InternalRow] = {
    perPartition(relation) { p =>
      new Iterator[InternalRow] {
        private var streamRow: InternalRow = null
        private var matches: Iterator[InternalRow] = Iterator.empty
        private var foundMatch: Boolean = false
        private var resultRow: InternalRow = null

        private def findNext(): Boolean = {
          while (streamRow != null || p.stream.hasNext) {
            if (streamRow == null) {
              streamRow = p.stream.next()
              foundMatch = false
              matches = p.candidates(streamRow)
            }
            while (matches.hasNext) {
              val joined = p.join(streamRow, matches.next())
              if (p.condition(joined)) {
                foundMatch = true
                resultRow = joined
                return true
              }
            }
            if (!foundMatch) {
              resultRow = p.join(streamRow, p.nullBuild)
              streamRow = null
              return true
            }
            streamRow = null
          }
          false
        }

        override def hasNext: Boolean = resultRow != null || findNext()

        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }
  }

  /** Semi keeps a stream row that matches. Anti keeps one that does not. */
  private def leftExistenceJoin(
      relation: Broadcast[RangeRelation],
      exists: Boolean): RDD[InternalRow] = {
    perPartition(relation) { p =>
      p.stream.filter { row =>
        val matched = p.candidates(row).exists { buildRow =>
          p.condition(p.join(row, buildRow))
        }
        matched == exists
      }
    }
  }

  private def genResultProjection: UnsafeProjection = joinType match {
    case LeftSemi | LeftAnti =>
      UnsafeProjection.create(output, output)
    case _ =>
      UnsafeProjection.create(output, (left.output ++ right.output).map(_.withNullability(true)))
  }

  override def doExecute(): RDD[InternalRow] = {
    if (!BroadcastRangeJoinExec.supports(joinType, buildSide)) {
      throw SparkException.internalError(
        s"BroadcastRangeJoin does not support $joinType with $buildSide")
    }
    val relation = buildPlan.executeBroadcast[RangeRelation]()
    val joined = joinType match {
      case _: InnerLike => innerJoin(relation)
      case LeftOuter | RightOuter => outerJoin(relation)
      case LeftSemi => leftExistenceJoin(relation, exists = true)
      case LeftAnti => leftExistenceJoin(relation, exists = false)
      case t =>
        throw SparkException.internalError(
          s"BroadcastRangeJoin should not take $t as the JoinType")
    }
    val numOutputRows = longMetric("numOutputRows")
    joined.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  override def supportCodegen: Boolean = {
    BroadcastRangeJoinExec.supports(joinType, buildSide)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }

  private def multipleOutputForOneInput: Boolean = joinType match {
    case _: InnerLike | LeftOuter | RightOuter => true
    case _ => false
  }

  override def needCopyResult: Boolean = {
    streamedPlan.asInstanceOf[CodegenSupport].needCopyResult || multipleOutputForOneInput
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    joinType match {
      case _: InnerLike => codegenInner(ctx, input)
      case LeftOuter | RightOuter => codegenOuter(ctx, input)
      case LeftSemi => codegenLeftExistence(ctx, input, exists = true)
      case LeftAnti => codegenLeftExistence(ctx, input, exists = false)
      case t =>
        throw SparkException.internalError(
          s"BroadcastRangeJoin code-gen should not take $t as the JoinType")
    }
  }

  private def prepareBroadcast(ctx: CodegenContext): String = {
    val broadcast = buildPlan.executeBroadcast[RangeRelation]()
    val broadcastTerm = ctx.addReferenceObj("broadcast", broadcast)
    val cls = classOf[RangeRelation].getName
    ctx.addMutableState(cls, "rangeRelation",
      v => s"$v = ($cls) $broadcastTerm.value();", forceInline = true)
  }

  /**
   * Box each streamed range key. The index compares the same boxed values
   * `RangeIndex.getValue` produces on the build side.
   */
  private def genProbeKeys(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (String, Seq[String]) = {
    ctx.currentVars = input
    val keys = streamedKeys.map { key =>
      val bound = BindReferences.bindReference(key, streamedOutput)
      val ev = bound.genCode(ctx)
      val name = ctx.freshName("probeKey")
      val javaType = CodeGenerator.javaType(key.dataType)
      val boxed = CodeGenerator.boxedType(key.dataType)
      val valueExpr = if (javaType == boxed) {
        s"${ev.value}"
      } else {
        s"$boxed.valueOf(${ev.value})"
      }
      val code =
        s"""
           |${ev.code}
           |Object $name = ${ev.isNull} ? null : $valueExpr;
         """.stripMargin
      (code, name)
    }
    (keys.map(_._1).mkString("\n"), keys.map(_._2))
  }

  /**
   * Probe the broadcast index. The index kind and scan direction are fixed
   * when the plan is built, so generated code calls the concrete index
   * method directly. Keys either stay boxed per `genProbeKeys`, or arrive as
   * primitive longs from `genLongProbeKeys` with a null term each that the
   * long indexes cannot see; the call then yields the shared empty scan for
   * a null key. Generated code does not allocate a key array. An interval
   * probe owns its walk stack.
   */
  private def genCandidateIterator(
      ctx: CodegenContext,
      relationTerm: String,
      keyVars: Seq[String],
      nullVars: Seq[String]): (String, String) = {
    val matches = ctx.freshName("matches")
    val iterCls = classOf[Iterator[InternalRow]].getName
    // Dispatches on RangeIndexKind, the same source `requiredChildDistribution` uses to
    // decide which structure got built, instead of re-deriving "point or interval" from
    // rangeJoin a second time.
    val call = RangeIndexKind(rangeJoin) match {
      case PointIndexKind =>
        val points = (if (usesLongKeys) classOf[LongPointIndex] else classOf[PointIndex]).getName
        s"(($points) $relationTerm).${pointScan.methodName}(${keyVars.head})"
      case IntervalIndexKind =>
        val intervals =
          (if (usesLongKeys) classOf[LongIntervalIndex] else classOf[IntervalIndex]).getName
        // A point probe has one key. Reuse it as both ends so it is evaluated once.
        val high = if (keyVars.length == 1) keyVars.head else keyVars(1)
        s"(($intervals) $relationTerm).overlapping(${keyVars.head}, $high)"
    }
    val code = if (nullVars.isEmpty) {
      s"$iterCls $matches = $call;"
    } else {
      val emptyScan = s"${classOf[RowSlice].getName}$$.MODULE$$.empty()"
      s"$iterCls $matches = ${nullVars.mkString(" || ")} ? ($iterCls) $emptyScan : $call;"
    }
    (matches, code)
  }

  /**
   * Keys that map to one order-preserving long (`RangeIndex.longKey`) route
   * to the long indexes: build and probe keys then cross as raw longs, with
   * no boxing on either side, and every other supported key stays boxed.
   * One test picks both the `RangeRelationBroadcastMode` index family and
   * the generated probe code, so they cannot disagree.
   */
  private[this] lazy val usesLongKeys: Boolean = RangeIndex.canMapKeysToLong(buildKeys)

  /**
   * Evaluate each streamed key into a primitive long. Each key keeps the null
   * term of its expression; the candidate call turns a null into an empty scan.
   */
  private def genLongProbeKeys(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (String, Seq[String], Seq[String]) = {
    ctx.currentVars = input
    val keys = streamedKeys.map { key =>
      val bound = BindReferences.bindReference(key, streamedOutput)
      val ev = bound.genCode(ctx)
      val toLong = RangeIndex.requiredLongKey(key.dataType)
      val name = ctx.freshName("probeKey")
      val code =
        s"""
           |${ev.code}
           |long $name = ${ev.isNull} ? 0L : ${toLong.code(ev.value)};
         """.stripMargin
      (code, name, ev.isNull)
    }
    (keys.map(_._1).mkString("\n"), keys.map(_._2), keys.map(_._3))
  }

  /**
   * Shared init for every doConsume path: broadcasts the relation, evaluates the
   * streamed probe keys, and opens the candidate iterator over it.
   */
  private def prepareProbe(ctx: CodegenContext, input: Seq[ExprCode]): (String, String, String) = {
    val relationTerm = prepareBroadcast(ctx)
    val (keyCode, keyVars, nullVars) = if (usesLongKeys) {
      genLongProbeKeys(ctx, input)
    } else {
      val (code, vars) = genProbeKeys(ctx, input)
      (code, vars, Nil)
    }
    val (matches, iteratorCode) = genCandidateIterator(ctx, relationTerm, keyVars, nullVars)
    (keyCode, iteratorCode, matches)
  }

  private def orderResultVars(
      streamVars: Seq[ExprCode],
      buildVars: Seq[ExprCode]): Seq[ExprCode] = {
    buildSide match {
      case BuildLeft => buildVars ++ streamVars
      case BuildRight => streamVars ++ buildVars
    }
  }

  private def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (keyCode, iteratorCode, matches) = prepareProbe(ctx, input)
    val (buildRow, checkCondition, buildVars) =
      getJoinCondition(ctx, input, streamedPlan, buildPlan)
    val resultVars = orderResultVars(input, buildVars)
    val numOutput = metricTerm(ctx, "numOutputRows")

    s"""
       |$keyCode
       |$iteratorCode
       |while ($matches.hasNext()) {
       |  InternalRow $buildRow = (InternalRow) $matches.next();
       |  $checkCondition {
       |    $numOutput.add(1);
       |    ${consume(ctx, resultVars)}
       |  }
       |}
     """.stripMargin
  }

  private def codegenOuter(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (keyCode, iteratorCode, matches) = prepareProbe(ctx, input)
    val buildRow = ctx.freshName("buildRow")
    val (_, checkCondition, _) = getJoinCondition(
      ctx, input, streamedPlan, buildPlan, Some(buildRow))
    val buildVars = genOneSideJoinVars(ctx, buildRow, buildPlan, setDefaultValue = true)
    val resultVars = orderResultVars(input, buildVars)
    val found = ctx.freshName("foundMatch")
    val shouldOutput = ctx.freshName("shouldOutput")
    val numOutput = metricTerm(ctx, "numOutputRows")

    // One consume site, inside the loop. The last iteration pads the build side
    // when no candidate satisfied the original condition.
    s"""
       |$keyCode
       |$iteratorCode
       |boolean $found = false;
       |while ($matches.hasNext() || !$found) {
       |  InternalRow $buildRow = $matches.hasNext() ?
       |    (InternalRow) $matches.next() : null;
       |  boolean $shouldOutput = false;
       |  if ($buildRow != null) {
       |    $checkCondition {
       |      $shouldOutput = true;
       |      $found = true;
       |    }
       |  } else {
       |    $shouldOutput = true;
       |    $found = true;
       |  }
       |  if ($shouldOutput) {
       |    $numOutput.add(1);
       |    ${consume(ctx, resultVars)}
       |  }
       |}
     """.stripMargin
  }

  private def codegenLeftExistence(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      exists: Boolean): String = {
    val (keyCode, iteratorCode, matches) = prepareProbe(ctx, input)
    val (buildRow, checkCondition, _) =
      getJoinCondition(ctx, input, streamedPlan, buildPlan)
    val found = ctx.freshName("foundMatch")
    val numOutput = metricTerm(ctx, "numOutputRows")

    s"""
       |$keyCode
       |$iteratorCode
       |boolean $found = false;
       |while (!$found && $matches.hasNext()) {
       |  InternalRow $buildRow = (InternalRow) $matches.next();
       |  $checkCondition {
       |    $found = true;
       |  }
       |}
       |if ($found == $exists) {
       |  $numOutput.add(1);
       |  ${consume(ctx, input)}
       |}
     """.stripMargin
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)
}

object BroadcastRangeJoinExec {

  /**
   * Stream-preserving combinations. Left outer, left semi, and left anti broadcast the
   * right side. Right outer broadcasts the left side. Inner may broadcast either side.
   */
  def supports(joinType: JoinType, buildSide: BuildSide): Boolean = (joinType, buildSide) match {
    case (_: InnerLike, _) => true
    case (RightOuter, BuildLeft) => true
    case (LeftOuter | LeftSemi | LeftAnti, BuildRight) => true
    case _ => false
  }

  /**
   * Create candidate probe function for a broadcast relation and bound probe keys.
   * Constructed once per partition in `perPartition`, avoiding per-row relation dispatch.
   * Lives here rather than on [[BroadcastRangeJoinExec]] so the closure that calls it
   * does not capture the enclosing plan node. `pointScan` is by-name so it is only
   * forced - and only throws for a non-partial-range join - inside the point-index
   * branches below, matching the laziness `perPartition` relies on.
   */
  private def makeCandidateFn(
      relation: RangeRelation,
      boundStreamKeys: Seq[Expression],
      pointScan: => PointScan): InternalRow => Iterator[InternalRow] = {
    val isOneKey = boundStreamKeys.length == 1
    val lowExpr = boundStreamKeys.head
    val highExpr = if (isOneKey) null else boundStreamKeys(1)

    relation match {
      case longs: LongIntervalIndex => row =>
        val low = lowExpr.eval(row)
        if (low == null) {
          Iterator.empty
        } else {
          val high = if (isOneKey) low else highExpr.eval(row)
          if (high == null) {
            Iterator.empty
          } else {
            longs.overlapping(longs.toLong(low), longs.toLong(high))
          }
        }
      case points: LongPointIndex =>
        val scan = pointScan(points)
        row =>
          val low = lowExpr.eval(row)
          if (low == null) Iterator.empty else scan(points.toLong(low))
      case intervals: IntervalIndex => row =>
        val low = lowExpr.eval(row)
        if (low == null) {
          Iterator.empty
        } else {
          val high = if (isOneKey) low else highExpr.eval(row)
          intervals.overlapping(low, high)
        }
      case points: PointIndex =>
        val scan = pointScan(points)
        row =>
          val low = lowExpr.eval(row)
          if (low == null) Iterator.empty else scan(low)
    }
  }

  /**
   * Broadcast relation, bound probe keys, and the original condition for one
   * streamed partition. The joined row is reused; the caller copies it. Nested here
   * (not inside [[BroadcastRangeJoinExec]]) so constructing it does not implicitly
   * capture the enclosing plan node through a synthetic outer reference.
   */
  private class ProbeContext(
      val stream: Iterator[InternalRow],
      val condition: InternalRow => Boolean,
      joinedRow: JoinedRow,
      val nullBuild: InternalRow,
      candidateFn: InternalRow => Iterator[InternalRow],
      buildSide: BuildSide) {

    def candidates(row: InternalRow): Iterator[InternalRow] = candidateFn(row)

    def join(streamRow: InternalRow, buildRow: InternalRow): InternalRow = buildSide match {
      case BuildRight => joinedRow(streamRow, buildRow)
      case BuildLeft => joinedRow(buildRow, streamRow)
    }
  }
}
