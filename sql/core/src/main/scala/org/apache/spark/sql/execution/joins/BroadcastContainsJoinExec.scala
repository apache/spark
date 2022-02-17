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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

@DeveloperApi
case class BroadcastContainsJoinExec(
    leftKey: Expression,
    rightKey: Expression,
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    checkWildcards: Boolean,
    condition: Option[Expression])
    extends BaseJoinExec
    with CodegenSupport {

  override def leftKeys: Seq[Expression] = Seq(leftKey)

  override def rightKeys: Seq[Expression] = Seq(rightKey)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "emptyInputRows" -> SQLMetrics.createMetric(sparkContext, "number of empty input rows"))

  val numOutputRows = longMetric("numOutputRows")
  val emptyInputRows = longMetric("emptyInputRows")

  /** BuildRight means the right relation <=> the broadcast relation. */
  val (streamedPlan, streamedKeys, buildPlan, buildKeys) = buildSide match {
    case BuildRight => (left, leftKeys, right, rightKeys)
    case BuildLeft => (right, rightKeys, left, leftKeys)
  }

  @transient protected lazy val buildBoundKeys = bindReferences(buildKeys, buildPlan.output)

  @transient protected lazy val streamedBoundKeys =
    bindReferences(streamedKeys, streamedPlan.output)

  override def requiredChildDistribution: Seq[Distribution] = {
    // TODO support isNullAwareAntiJoin parameter
    val isNullAwareAntiJoin = false
    val mode = TokenTreeBroadcastMode(buildBoundKeys, checkWildcards, isNullAwareAntiJoin)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  private[this] def genResultProjection: UnsafeProjection = joinType match {
    case LeftExistence(j) =>
      UnsafeProjection.create(output, output)
    case other =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection
        .create(output, (streamedPlan.output ++ buildPlan.output).map(_.withNullability(true)))
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastContainsJoin should not take $x as the JoinType")
    }
  }

  @transient private lazy val boundCondition = {
    if (condition.isDefined) {
      Predicate.create(condition.get, streamedPlan.output ++ buildPlan.output).eval _
    } else { (r: InternalRow) =>
      true
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastRelation = buildPlan.executeBroadcast[TreeRelation]()
    val resultRdd = streamedPlan.execute().mapPartitionsInternal { streamedIter =>
      val startTS = System.currentTimeMillis()
      logInfo("Start read broadcast relation")
      val relation = broadcastRelation.value
      logInfo(s"Got broadcast relation in ${System.currentTimeMillis() - startTS} ms")
      TaskContext.get().taskMetrics().incPeakExecutionMemory(relation.estimatedSize)
      join(streamedIter, relation)
    }
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  def join(input: Iterator[InternalRow], relation: TreeRelation): Iterator[InternalRow] = {
    (joinType, buildSide) match {
      case (_: InnerLike, _) =>
        innerJoin(input, relation)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) =>
        outerJoin(input, relation)
      case (LeftSemi, BuildRight) =>
        semiJoin(input, relation)
      case (LeftAnti, BuildRight) =>
        antiJoin(input, relation)
      case _ =>
        throw new SparkException(
          s"Not supported joinType:${joinType} with " +
            s"buildSide:${buildSide} for ContainsJoin")
    }
  }

  /**
   * The implementation for InnerJoin.
   */
  private def innerJoin(
      streamedIter: Iterator[InternalRow],
      relation: TreeRelation): Iterator[InternalRow] = {
    val streamedKeysGenerator = UnsafeProjection.create(streamedBoundKeys)

    streamedIter.flatMap { streamedRow =>
      val key = streamedKeysGenerator(streamedRow).getUTF8String(0)
      if (key != null) {
        val streamedKeyValue = key.toString
        relation.tree
          .doMatch(streamedKeyValue)
          .map(new JoinedRow(streamedRow, _))
          .filterNot(condition.isDefined && !boundCondition(_))
      } else {
        emptyInputRows += 1
        Set[InternalRow]()
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildRight
   *   RightOuter with BuildLeft
   */
  private def outerJoin(
      streamedIter: Iterator[InternalRow],
      relation: TreeRelation): Iterator[InternalRow] = {

    val streamedKeysGenerator = UnsafeProjection.create(streamedBoundKeys)

    val nulls = new GenericInternalRow(buildPlan.output.size)
    streamedIter.flatMap { streamedRow =>
      val key = streamedKeysGenerator(streamedRow).getUTF8String(0)
      if (key != null) {
        val streamedKeyValue = key.toString
        val matchedRows = relation.tree
          .doMatch(streamedKeyValue)
          .map(new JoinedRow(streamedRow, _))
          .filterNot(condition.isDefined && !boundCondition(_))
        if (matchedRows.size == 0) {
          Set(new JoinedRow(streamedRow, nulls))
        } else {
          matchedRows
        }
      } else {
        emptyInputRows += 1
        // outer join will return streamedRow and null
        Set(new JoinedRow(streamedRow, nulls))
      }
    }
  }

  private def semiJoin(
      streamedIter: Iterator[InternalRow],
      relation: TreeRelation): Iterator[InternalRow] = {
    if (relation.rowNumber == 0) {
      return Iterator.empty
    }

    val streamedKeysGenerator = UnsafeProjection.create(streamedBoundKeys)
    val joinedRow = new JoinedRow

    streamedIter.filter { streamedRow =>
      val key = streamedKeysGenerator(streamedRow).getUTF8String(0)
      if (key == null) {
        false
      } else {
        val streamedKeyValue = key.toString
        val matchedRows = relation.tree.doMatch(streamedKeyValue)
        matchedRows.nonEmpty && (condition.isEmpty || matchedRows.exists { row =>
          boundCondition(joinedRow(streamedRow, row))
        })
      }
    }
  }

  private def antiJoin(
      streamedIter: Iterator[InternalRow],
      relation: TreeRelation): Iterator[InternalRow] = {
    // If the right side is empty, AntiJoin simply returns the left side.
    if (relation.rowNumber == 0) {
      return streamedIter
    }

    val streamedKeysGenerator = UnsafeProjection.create(streamedBoundKeys)
    val joinedRow = new JoinedRow

    streamedIter.filter { streamedRow =>
      val key = streamedKeysGenerator(streamedRow).getUTF8String(0)
      if (key == null) {
        true
      } else {
        val streamedKeyValue = key.toString
        val matchedRows = relation.tree.doMatch(streamedKeyValue)
        matchedRows.isEmpty || (condition.isDefined && !matchedRows.exists { row =>
          boundCondition(joinedRow(streamedRow, row))
        })
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    joinType match {
      case _: InnerLike => codegenInner(ctx, input)
      case LeftOuter | RightOuter => codegenOuter(ctx, input)
      case LeftSemi => codegenSemi(ctx, input)
      case LeftAnti => codegenAnti(ctx, input)
      case x =>
        throw new IllegalArgumentException(s"Contains join should not take $x as the JoinType")
    }
  }

  // If the streaming side needs to copy result, this join plan needs to copy too. Otherwise,
  // this join plan only needs to copy result if it may output multiple rows for one input.
  override def needCopyResult: Boolean =
    streamedPlan.asInstanceOf[CodegenSupport].needCopyResult

  def prepareBroadcast(ctx: CodegenContext): (String, Long, Int) = {
    // create a name for TreeRelation
    val broadcastRelation = buildPlan.executeBroadcast[TreeRelation]()
    val broadcast = ctx.addReferenceObj("broadcast", broadcastRelation)
    val clsName = TreeRelation.getClass.getName.stripSuffix("$")

    // Inline mutable state since not many join operations in a task
    val treeTerm = ctx.addMutableState(
      DoubleArrayTrieTree.getClass.getName.stripSuffix("$"),
      "tokenTree",
      v => s"""
              | $v = (($clsName) $broadcast.value()).tree();
              | incPeakExecutionMemory((($clsName) $broadcast.value()).estimatedSize());
       """.stripMargin,
      forceInline = true)
    (treeTerm, broadcastRelation.value.estimatedSize, broadcastRelation.value.rowNumber)
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key
   * has any null in it or not.
   */
  protected def genStreamSideJoinKey(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (ExprCode, String) = {
    ctx.currentVars = input
    // generate the join key as UnsafeRow
    val ev = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
    (ev, s"${ev.value}.anyNull()")
  }

  /**
   * Generates the code for variable of build side.
   */
  private def genBuildSideVars(ctx: CodegenContext, matched: String): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = matched
    buildPlan.output.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      if (joinType.isInstanceOf[InnerLike]) {
        ev
      } else {
        // the variables are needed even there is no matched rows
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val javaType = CodeGenerator.javaType(a.dataType)
        val code = code"""
                         |boolean $isNull = true;
                         |$javaType $value = ${CodeGenerator.defaultValue(a.dataType)};
                         |if ($matched != null) {
                         |  ${ev.code}
                         |  $isNull = ${ev.isNull};
                         |  $value = ${ev.value};
                         |}
         """.stripMargin
        ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, a.dataType))
      }
    }
  }

  /**
   * Generate the (non-equi) condition used to filter joined rows. This is used in Inner, Left Semi
   * and Left Anti joins.
   */
  protected def getJoinCondition(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (String, String, Seq[ExprCode]) = {
    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      val skipRow = s"${ev.isNull} || !${ev.value}"
      s"""
         |$eval
         |${ev.code}
         |if (!($skipRow))
       """.stripMargin
    } else {
      ""
    }
    (matched, checkCondition, buildVars)
  }

  /**
   * Generates the code for Inner join.
   */
  protected def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (tokenTreeTerm, _, rowNumber) = prepareBroadcast(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, buildVars) = getJoinCondition(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }

    if (rowNumber == 0) {
      """
        |// If TreeRelation is empty, tree inner join simply returns nothing.
      """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val joinedRow = ctx.freshName("joinedRow")
      val arrayBufferCls = classOf[ArrayBuffer[InternalRow]].getName
      val iteratorCls = classOf[Iterator[InternalRow]].getName

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |if(${keyEv.value}.getUTF8String(0) != null) {
         |  // Assume streamedBoundKeys only has one string expression
         |  String streamedKeyValue = ${keyEv.value}.getUTF8String(0).toString();
         |  // find matches from TokenTree
         |  $arrayBufferCls $matches = $anyNull ? null : $tokenTreeTerm.doMatch(streamedKeyValue);
         |  if ($matches != null) {
         |    $iteratorCls it = $matches.iterator();
         |    while (it.hasNext()) {
         |      InternalRow $matched = (InternalRow) it.next();
         |      InternalRow $joinedRow = new JoinedRow(${keyEv.value}, $matched);
         |      $checkCondition {
         |        $numOutput.add(1);
         |        ${consume(ctx, resultVars)}
         |      }
         |    }
         |  }
         |}
     """.stripMargin
    }
  }

  /**
   * Generates the code for left or right outer join.
   */
  protected def codegenOuter(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (tokenTreeTerm, _, _) = prepareBroadcast(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val numOutput = metricTerm(ctx, "numOutputRows")

    // filter the output via condition
    val conditionPassed = ctx.freshName("conditionPassed")
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      s"""
         |boolean $conditionPassed = true;
         |${eval.trim}
         |if ($matched != null) {
         |  ${ev.code}
         |  $conditionPassed = !${ev.isNull} && ${ev.value};
         |}
       """.stripMargin
    } else {
      s"final boolean $conditionPassed = true;"
    }

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }

    val matches = ctx.freshName("matches")
    val joinedRow = ctx.freshName("joinedRow")
    val arrayBufferCls = classOf[ArrayBuffer[InternalRow]].getName
    val iteratorCls = classOf[Iterator[InternalRow]].getName
    val found = ctx.freshName("found")

    s"""
       |// generate join key for stream side
       |${keyEv.code}
       |
       |// find matches from TokenTree
       |$arrayBufferCls $matches = $anyNull || ${keyEv.value}.getUTF8String(0) == null ? null :
       |        $tokenTreeTerm.doMatch(${keyEv.value}.getUTF8String(0).toString());
       |boolean $found = false;
       |$iteratorCls it = $matches == null ? null : $matches.iterator();
       |// the last iteration of this loop is to emit an empty row if there is no matched rows.
       |while (it != null && it.hasNext() || !$found) {
       |  InternalRow $matched = it !=null && it.hasNext() ? (InternalRow) it.next() : null;
       |  InternalRow $joinedRow = new JoinedRow(${keyEv.value}, $matched);
       |  ${checkCondition.trim}
       |  if ($conditionPassed) {
       |    $found = true;
       |    $numOutput.add(1);
       |    ${consume(ctx, resultVars)}
       |  }
       |}
       """.stripMargin
  }

  /**
   * Generates the code for left semi join.
   */
  protected def codegenSemi(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (tokenTreeTerm, _, rowNumber) = prepareBroadcast(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _) = getJoinCondition(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (rowNumber == 0) {
      """
        |// If TreeRelation is empty, tree semi join simply returns nothing.
      """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val joinedRow = ctx.freshName("joinedRow")
      val arrayBufferCls = classOf[ArrayBuffer[InternalRow]].getName
      val iteratorCls = classOf[Iterator[InternalRow]].getName
      val found = ctx.freshName("found")

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |if(${keyEv.value}.getUTF8String(0) != null) {
         |  // find matches from TokenTree
         |  String streamedKeyValue = ${keyEv.value}.getUTF8String(0).toString();
         |  $arrayBufferCls $matches = $anyNull ? null : $tokenTreeTerm.doMatch(streamedKeyValue);
         |  if ($matches != null) {
         |    $iteratorCls it = $matches.iterator();
         |    boolean $found = false;
         |    while (!$found && it.hasNext()) {
         |      InternalRow $matched = (InternalRow) it.next();
         |      InternalRow $joinedRow = new JoinedRow(${keyEv.value}, $matched);
         |      $checkCondition {
         |        $found = true;
         |      }
         |    }
         |    if ($found) {
         |      $numOutput.add(1);
         |      ${consume(ctx, input)}
         |    }
         |  }
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for anti join.
   */
  protected def codegenAnti(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (tokenTreeTerm, _, rowNumber) = prepareBroadcast(ctx)
    val numOutput = metricTerm(ctx, "numOutputRows")
    if (rowNumber == 0) {
      return s"""
                |// If TreeRelation is empty, tree anti join simply returns the stream side.
                |$numOutput.add(1);
                |${consume(ctx, input)}
              """.stripMargin
    }

    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _) = getJoinCondition(ctx, input)

    val matches = ctx.freshName("matches")
    val joinedRow = ctx.freshName("joinedRow")
    val arrayBufferCls = classOf[ArrayBuffer[InternalRow]].getName
    val iteratorCls = classOf[Iterator[InternalRow]].getName
    val found = ctx.freshName("found")
    s"""
       |boolean $found = false;
       |// generate join key for stream side
       |${keyEv.code}
       |if(${keyEv.value}.getUTF8String(0) != null) {
       |  // Check if the TokenTree exists.
       |  String streamedKeyValue = ${keyEv.value}.getUTF8String(0).toString();
       |  $arrayBufferCls $matches = $anyNull ? null : $tokenTreeTerm.doMatch(streamedKeyValue);
       |  if ($matches != null) {
       |    $iteratorCls it = $matches.iterator();
       |    // Evaluate the condition.
       |    while (!$found && it.hasNext()) {
       |      InternalRow $matched = (InternalRow) it.next();
       |      InternalRow $joinedRow = new JoinedRow(${keyEv.value}, $matched);
       |      $checkCondition {
       |        $found = true;
       |      }
       |    }
       |  }
       |}
       |
       |if (!$found) {
       |  $numOutput.add(1);
       |  ${consume(ctx, input)}
       |}
       """.stripMargin
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
