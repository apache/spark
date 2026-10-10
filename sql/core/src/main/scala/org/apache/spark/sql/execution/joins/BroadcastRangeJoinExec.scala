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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.{PartialRangeJoin, RangeJoin}
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
 * [[PointIndex]] and scans the side of the bound the build side holds. Either probe
 * returns a superset. A candidate is kept only when the original join condition is
 * true, so inclusivity stays on the predicate. Heavy overlap can make one probe
 * return almost every build row.
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

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override lazy val requiredChildDistribution: Seq[Distribution] = {
    val mode = RangeBroadcastMode(
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
        throw new IllegalArgumentException(
          s"BroadcastRangeJoin should not take $t as the JoinType")
    }
  }

  // The preserved side is streamed, so its partitioning and ordering pass through.
  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  // Output columns are not expressions, so `references` would drop a streamed column
  // that the join returns but the condition does not read.
  override def usedInputs: AttributeSet = AttributeSet(streamedPlan.output)

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

  /** Build side holds the lower bound, so points at or below the stream key match. */
  private def buildHoldsLowerBound(partial: PartialRangeJoin): Boolean =
    partial.leftIsLower == (buildSide == BuildLeft)

  private def candidates(
      relation: RangeRelation,
      boundStreamKeys: Seq[Expression],
      streamRow: InternalRow): Iterator[InternalRow] = {
    val low = boundStreamKeys.head.eval(streamRow)
    if (low == null) return Iterator.empty
    relation match {
      case intervals: IntervalIndex =>
        val high = if (boundStreamKeys.length == 1) low else boundStreamKeys(1).eval(streamRow)
        if (high == null) Iterator.empty else intervals.overlapping(low, high)
      case points: PointIndex =>
        rangeJoin match {
          case partial: PartialRangeJoin if buildHoldsLowerBound(partial) =>
            points.upTo(low)
          case _: PartialRangeJoin =>
            points.from(low)
          case _ =>
            throw new IllegalStateException(
              s"Point index is only built for a partial range, got $rangeJoin.")
        }
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    require(BroadcastRangeJoinExec.supports(joinType, buildSide),
      s"BroadcastRangeJoin does not support $joinType with $buildSide")
    val relation = buildPlan.executeBroadcast[RangeRelation]()
    streamPreservingJoin(relation)
  }

  private def streamPreservingJoin(relation: Broadcast[RangeRelation]): RDD[InternalRow] = {
    val boundStreamKeys = BindReferences.bindReferences(streamedKeys, streamedOutput)
    val numBuildFields = buildPlan.output.length

    val resultRdd = streamedPlan.execute().mapPartitions { stream =>
      val boundCondition = Predicate.create(condition.get, left.output ++ right.output).eval _
      new Iterator[InternalRow] {
        private[this] val index = relation.value
        private[this] val joinedRow = new JoinedRow
        private[this] val buildNulls = new GenericInternalRow(numBuildFields)
        private[this] var matchIterator: Iterator[InternalRow] = Iterator.empty

        // current row from stream side
        private var streamRow: InternalRow = null
        // the next row to emit, or null if not yet found
        private var resultRow: InternalRow = null
        private var foundMatch: Boolean = false

        /**
         * Advance until the next output row, or the stream is exhausted.
         *
         * The loop test is ordered on purpose. A codegen stream reuses one
         * `UnsafeRow`, and `hasNext` fills that buffer with the next row. It must
         * not run while `streamRow` is still being joined, so `streamRow != null`
         * is checked first and short-circuits the call.
         *
         * Inner emits each candidate the condition accepts. Outer does too, then one
         * null-padded stream row when none were. Semi emits the stream row once.
         * Anti emits it only when nothing was accepted. A null bound is unknown,
         * so it is not a match.
         */
        private def findNextMatch(): Boolean = {
          while (streamRow != null || stream.hasNext) {
            if (streamRow == null) {
              streamRow = stream.next()
              foundMatch = false
              matchIterator = candidates(index, boundStreamKeys, streamRow)
            }

            var matched = false
            while (matchIterator.hasNext && !matched) {
              val buildRow = matchIterator.next()
              val joined = buildSide match {
                case BuildRight => joinedRow(streamRow, buildRow)
                case BuildLeft => joinedRow(buildRow, streamRow)
              }
              if (boundCondition(joined)) {
                foundMatch = true
                joinType match {
                  case LeftSemi =>
                    resultRow = streamRow
                    streamRow = null
                    return true
                  case LeftAnti =>
                    matchIterator = Iterator.empty
                  case _ =>
                    resultRow = joined
                    matched = true
                }
              }
            }
            if (matched) {
              return true
            }
            val emitUnmatched = joinType match {
              case LeftAnti | LeftOuter | RightOuter => !foundMatch
              case _ => false
            }
            if (emitUnmatched) {
              resultRow = joinType match {
                case LeftAnti => streamRow
                case _ =>
                  buildSide match {
                    case BuildRight => joinedRow(streamRow, buildNulls)
                    case BuildLeft => joinedRow(buildNulls, streamRow)
                  }
              }
              streamRow = null
              return true
            }
            streamRow = null
          }
          false
        }

        override def hasNext: Boolean = {
          resultRow != null || findNextMatch()
        }

        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }

    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  private[this] def genResultProjection: UnsafeProjection = joinType match {
    case LeftSemi | LeftAnti =>
      UnsafeProjection.create(output, output)
    case _ =>
      UnsafeProjection.create(
        output, (left.output ++ right.output).map(_.withNullability(true)))
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
        throw new IllegalArgumentException(
          s"BroadcastRangeJoin code-gen should not take $t as the JoinType")
    }
  }

  private def addRelation(ctx: CodegenContext): String = {
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
   * Probe the broadcast index. The index kind and scan direction are fixed when
   * the plan is built, so generated code calls [[IntervalIndex.overlapping]],
   * [[PointIndex.upTo]], or [[PointIndex.from]] directly. A null key yields the
   * empty iterator those methods already return. Generated code does not
   * allocate a key array. An interval probe owns its walk stack.
   */
  private def genCandidateIterator(
      ctx: CodegenContext,
      relationTerm: String,
      keyVars: Seq[String]): (String, String) = {
    val matches = ctx.freshName("matches")
    val iterCls = classOf[Iterator[InternalRow]].getName
    val call = rangeJoin match {
      case partial: PartialRangeJoin =>
        val points = classOf[PointIndex].getName
        val scan = if (buildHoldsLowerBound(partial)) "upTo" else "from"
        s"(($points) $relationTerm).$scan(${keyVars.head})"
      case _ =>
        val intervals = classOf[IntervalIndex].getName
        // A point probe has one key. Reuse it as both ends so it is evaluated once.
        val high = if (keyVars.length == 1) keyVars.head else keyVars(1)
        s"(($intervals) $relationTerm).overlapping(${keyVars.head}, $high)"
    }
    val code = s"$iterCls $matches = $call;"
    (matches, code)
  }

  /**
   * Shared init for every doConsume path: broadcasts the relation, evaluates the
   * streamed probe keys, and opens the candidate iterator over it.
   */
  private def prepareProbe(ctx: CodegenContext, input: Seq[ExprCode]): (String, String, String) = {
    val relationTerm = addRelation(ctx)
    val (keyCode, keyVars) = genProbeKeys(ctx, input)
    val (matches, iteratorCode) = genCandidateIterator(ctx, relationTerm, keyVars)
    (keyCode, iteratorCode, matches)
  }

  private def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (keyCode, iteratorCode, matches) = prepareProbe(ctx, input)
    val (buildRow, checkCondition, buildVars) =
      getJoinCondition(ctx, input, streamedPlan, buildPlan)
    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
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
    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
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
}
