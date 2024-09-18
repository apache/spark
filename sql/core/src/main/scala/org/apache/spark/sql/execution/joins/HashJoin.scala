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

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{CodegenSupport, ExplainUtils, RowIterator}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{BooleanType, IntegralType, LongType}

/**
 * @param relationTerm variable name for HashedRelation
 * @param keyIsUnique  indicate whether keys of HashedRelation known to be unique in code-gen time
 * @param isEmpty indicate whether it known to be EmptyHashedRelation in code-gen time
 */
private[joins] case class HashedRelationInfo(
    relationTerm: String,
    keyIsUnique: Boolean,
    isEmpty: Boolean)

trait HashJoin extends JoinCodegenSupport {
  def buildSide: BuildSide

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType ${buildSide} ($opId)".trim
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter | LeftSingle =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(s"HashJoin should not take $x as the JoinType")
    }
  }

  override def outputPartitioning: Partitioning = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike | RightOuter => right.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"OutputPart HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike | LeftOuter | LeftSingle | LeftSemi | LeftAnti | _: ExistenceJoin =>
          left.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building right side")
      }
  }

  override def outputOrdering: Seq[SortOrder] = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike | RightOuter => right.outputOrdering
        case x =>
          throw new IllegalArgumentException(
            s"OutputOrder HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike | LeftOuter | LeftSingle | LeftSemi | LeftAnti | _: ExistenceJoin =>
          left.outputOrdering
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building right side")
      }
  }

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.length == rightKeys.length &&
      leftKeys.map(_.dataType)
        .zip(rightKeys.map(_.dataType))
        .forall(types => DataTypeUtils.sameType(types._1, types._2)),
      "Join keys from two sides should have same length and types")
    buildSide match {
      case BuildLeft => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  @transient protected lazy val (buildOutput, streamedOutput) = {
    buildSide match {
      case BuildLeft => (left.output, right.output)
      case BuildRight => (right.output, left.output)
    }
  }

  @transient protected lazy val buildBoundKeys =
    bindReferences(HashJoin.rewriteKeyExpr(buildKeys), buildOutput)

  @transient protected lazy val streamedBoundKeys =
    bindReferences(HashJoin.rewriteKeyExpr(streamedKeys), streamedOutput)

  protected def buildSideKeyGenerator(): UnsafeProjection =
    UnsafeProjection.create(buildBoundKeys)

  protected def streamSideKeyGenerator(): UnsafeProjection =
    UnsafeProjection.create(streamedBoundKeys)

  @transient protected[this] lazy val boundCondition = if (condition.isDefined) {
    if ((joinType == FullOuter || joinType == LeftOuter) && buildSide == BuildLeft) {
      // Put join left side before right side.
      Predicate.create(condition.get, buildPlan.output ++ streamedPlan.output).eval _
    } else {
      Predicate.create(condition.get, streamedPlan.output ++ buildPlan.output).eval _
    }
  } else {
    (r: InternalRow) => true
  }

  protected def createResultProjection(): (InternalRow) => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamedPlan.output ++ buildPlan.output).map(_.withNullability(true)))
  }

  private def innerJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = {
    val joinRow = new JoinedRow
    val joinKeys = streamSideKeyGenerator()

    if (hashedRelation == EmptyHashedRelation) {
      Iterator.empty
    } else if (hashedRelation.keyIsUnique) {
      streamIter.flatMap { srow =>
        joinRow.withLeft(srow)
        val matched = hashedRelation.getValue(joinKeys(srow))
        if (matched != null) {
          Some(joinRow.withRight(matched)).filter(boundCondition)
        } else {
          None
        }
      }
    } else {
      streamIter.flatMap { srow =>
        joinRow.withLeft(srow)
        val matches = hashedRelation.get(joinKeys(srow))
        if (matches != null) {
          matches.map(joinRow.withRight).filter(boundCondition)
        } else {
          Seq.empty
        }
      }
    }
  }

  private def outerJoin(
      streamedIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      check: Int => Int): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow()
    val keyGenerator = streamSideKeyGenerator()
    val nullRow = new GenericInternalRow(buildPlan.output.length)

    if (hashedRelation.keyIsUnique) {
      streamedIter.map { currentRow =>
        val rowKey = keyGenerator(currentRow)
        joinedRow.withLeft(currentRow)
        val matched = hashedRelation.getValue(rowKey)
        if (matched != null && boundCondition(joinedRow.withRight(matched))) {
          joinedRow
        } else {
          joinedRow.withRight(nullRow)
        }
      }
    } else {
      streamedIter.flatMap { currentRow =>
        val rowKey = keyGenerator(currentRow)
        joinedRow.withLeft(currentRow)
        val buildIter = hashedRelation.get(rowKey)
        new RowIterator {
          private var found = false
          private var matches = 0
          override def advanceNext(): Boolean = {
            while (buildIter != null && buildIter.hasNext) {
              val nextBuildRow = buildIter.next()
              if (boundCondition(joinedRow.withRight(nextBuildRow))) {
                found = true
                matches = check(matches)
                return true
              }
            }
            if (!found) {
              joinedRow.withRight(nullRow)
              found = true
              matches = 0
              return true
            }
            false
          }
          override def getRow: InternalRow = joinedRow
        }.toScala
      }
    }
  }

  private def semiJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = {
    val joinKeys = streamSideKeyGenerator()
    val joinedRow = new JoinedRow

    if (hashedRelation == EmptyHashedRelation) {
      Iterator.empty
    } else if (hashedRelation.keyIsUnique) {
      streamIter.filter { current =>
        val key = joinKeys(current)
        lazy val matched = hashedRelation.getValue(key)
        !key.anyNull && matched != null &&
          (condition.isEmpty || boundCondition(joinedRow(current, matched)))
      }
    } else {
      streamIter.filter { current =>
        val key = joinKeys(current)
        lazy val buildIter = hashedRelation.get(key)
        !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
          (row: InternalRow) => boundCondition(joinedRow(current, row))
        })
      }
    }
  }

  private def existenceJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = {
    val joinKeys = streamSideKeyGenerator()
    val result = new GenericInternalRow(Array[Any](null))
    val joinedRow = new JoinedRow

    if (hashedRelation.keyIsUnique) {
      streamIter.map { current =>
        val key = joinKeys(current)
        lazy val matched = hashedRelation.getValue(key)
        val exists = !key.anyNull && matched != null &&
          (condition.isEmpty || boundCondition(joinedRow(current, matched)))
        result.setBoolean(0, exists)
        joinedRow(current, result)
      }
    } else {
      streamIter.map { current =>
        val key = joinKeys(current)
        lazy val buildIter = hashedRelation.get(key)
        val exists = !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
          (row: InternalRow) => boundCondition(joinedRow(current, row))
        })
        result.setBoolean(0, exists)
        joinedRow(current, result)
      }
    }
  }

  private def antiJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = {
    // If the right side is empty, AntiJoin simply returns the left side.
    if (hashedRelation == EmptyHashedRelation) {
      return streamIter
    }

    val joinKeys = streamSideKeyGenerator()
    val joinedRow = new JoinedRow

    if (hashedRelation.keyIsUnique) {
      streamIter.filter { current =>
        val key = joinKeys(current)
        lazy val matched = hashedRelation.getValue(key)
        key.anyNull || matched == null ||
          (condition.isDefined && !boundCondition(joinedRow(current, matched)))
      }
    } else {
      streamIter.filter { current =>
        val key = joinKeys(current)
        lazy val buildIter = hashedRelation.get(key)
        key.anyNull || buildIter == null || (condition.isDefined && !buildIter.exists {
          row => boundCondition(joinedRow(current, row))
        })
      }
    }
  }

  protected def join(
      streamedIter: Iterator[InternalRow],
      hashed: HashedRelation,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {

    val joinedIter = joinType match {
      case _: InnerLike =>
        innerJoin(streamedIter, hashed)
      case LeftOuter | RightOuter =>
        outerJoin(streamedIter, hashed, _ => 0)
      case LeftSingle =>
        val check: Int => Int = matches => {
          if (matches > 0) {
            throw QueryExecutionErrors.scalarSubqueryReturnsMultipleRows();
          }
          matches + 1
        }
        outerJoin(streamedIter, hashed, check)
      case LeftSemi =>
        semiJoin(streamedIter, hashed)
      case LeftAnti =>
        antiJoin(streamedIter, hashed)
      case _: ExistenceJoin =>
        existenceJoin(streamedIter, hashed)
      case x =>
        throw new IllegalArgumentException(
          s"HashJoin should not take $x as the JoinType")
    }

    val resultProj = createResultProjection()
    joinedIter.map { r =>
      numOutputRows += 1
      resultProj(r)
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    joinType match {
      case _: InnerLike => codegenInner(ctx, input)
      case LeftOuter | RightOuter | LeftSingle => codegenOuter(ctx, input)
      case LeftSemi => codegenSemi(ctx, input)
      case LeftAnti => codegenAnti(ctx, input)
      case _: ExistenceJoin => codegenExistence(ctx, input)
      case x =>
        throw new IllegalArgumentException(
          s"HashJoin should not take $x as the JoinType")
    }
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key
   * has any null in it or not.
   */
  protected def genStreamSideJoinKey(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (ExprCode, String) = {
    ctx.currentVars = input
    if (streamedBoundKeys.length == 1 && streamedBoundKeys.head.dataType == LongType) {
      // generate the join key as Long
      val ev = streamedBoundKeys.head.genCode(ctx)
      (ev, ev.isNull)
    } else {
      // generate the join key as UnsafeRow
      val ev = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
      (ev, s"${ev.value}.anyNull()")
    }
  }

  /**
   * Generates the code for Inner join.
   */
  protected def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, isEmptyHashedRelation) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, buildVars) = getJoinCondition(ctx, input, streamedPlan, buildPlan)
    val numOutput = metricTerm(ctx, "numOutputRows")

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }

    if (isEmptyHashedRelation) {
      """
        |// If HashedRelation is empty, hash inner join simply returns nothing.
      """.stripMargin
    } else if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |if ($matched != null) {
         |  $checkCondition {
         |    $numOutput.add(1);
         |    ${consume(ctx, resultVars)}
         |  }
         |}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ?
         |  null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |if ($matches != null) {
         |  while ($matches.hasNext()) {
         |    UnsafeRow $matched = (UnsafeRow) $matches.next();
         |    $checkCondition {
         |      $numOutput.add(1);
         |      ${consume(ctx, resultVars)}
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
    val HashedRelationInfo(relationTerm, keyIsUnique, _) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val matched = ctx.freshName("matched")
    val buildVars = genOneSideJoinVars(ctx, matched, buildPlan, setDefaultValue = true)
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

    if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |${checkCondition.trim}
         |if (!$conditionPassed) {
         |  $matched = null;
         |  // reset the variables those are already evaluated.
         |  ${buildVars.filter(_.code.isEmpty).map(v => s"${v.isNull} = true;").mkString("\n")}
         |}
         |$numOutput.add(1);
         |${consume(ctx, resultVars)}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")
      val matchesFound = ctx.freshName("matchesFound")
      val initSingleCounter = if (joinType == LeftSingle) {
        s"""
           |int $matchesFound = 0;""".stripMargin
      } else {
        ""
      }
      val evaluateSingleCheck = if (joinType == LeftSingle) {
        s"""
           |$matchesFound = $matchesFound +  1;
           |if ($matchesFound > 1) {
           |  throw QueryExecutionErrors.scalarSubqueryReturnsMultipleRows();
           |}
           |""".stripMargin
      } else {
        ""
      }

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |boolean $found = false;
         |${initSingleCounter}
         |// the last iteration of this loop is to emit an empty row if there is no matched rows.
         |while ($matches != null && $matches.hasNext() || !$found) {
         |  UnsafeRow $matched = $matches != null && $matches.hasNext() ?
         |    (UnsafeRow) $matches.next() : null;
         |  ${checkCondition.trim}
         |  if ($conditionPassed) {
         |    $found = true;
         |    $evaluateSingleCheck
         |    $numOutput.add(1);
         |    ${consume(ctx, resultVars)}
         |  }
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for left semi join.
   */
  protected def codegenSemi(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, isEmptyHashedRelation) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _) = getJoinCondition(ctx, input, streamedPlan, buildPlan)
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (isEmptyHashedRelation) {
      """
        |// If HashedRelation is empty, hash semi join simply returns nothing.
      """.stripMargin
    } else if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |if ($matched != null) {
         |  $checkCondition {
         |    $numOutput.add(1);
         |    ${consume(ctx, input)}
         |  }
         |}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |if ($matches != null) {
         |  boolean $found = false;
         |  while (!$found && $matches.hasNext()) {
         |    UnsafeRow $matched = (UnsafeRow) $matches.next();
         |    $checkCondition {
         |      $found = true;
         |    }
         |  }
         |  if ($found) {
         |    $numOutput.add(1);
         |    ${consume(ctx, input)}
         |  }
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for anti join.
   */
  protected def codegenAnti(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, isEmptyHashedRelation) = prepareRelation(ctx)
    val numOutput = metricTerm(ctx, "numOutputRows")
    if (isEmptyHashedRelation) {
      return s"""
                |// If HashedRelation is empty, hash anti join simply returns the stream side.
                |$numOutput.add(1);
                |${consume(ctx, input)}
              """.stripMargin
    }

    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _) = getJoinCondition(ctx, input, streamedPlan, buildPlan)

    if (keyIsUnique) {
      val found = ctx.freshName("found")
      s"""
         |boolean $found = false;
         |// generate join key for stream side
         |${keyEv.code}
         |// Check if the key has nulls.
         |if (!($anyNull)) {
         |  // Check if the HashedRelation exists.
         |  UnsafeRow $matched = (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |  if ($matched != null) {
         |    // Evaluate the condition.
         |    $checkCondition {
         |      $found = true;
         |    }
         |  }
         |}
         |if (!$found) {
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")
      s"""
         |boolean $found = false;
         |// generate join key for stream side
         |${keyEv.code}
         |// Check if the key has nulls.
         |if (!($anyNull)) {
         |  // Check if the HashedRelation exists.
         |  $iteratorCls $matches = ($iteratorCls)$relationTerm.get(${keyEv.value});
         |  if ($matches != null) {
         |    // Evaluate the condition.
         |    while (!$found && $matches.hasNext()) {
         |      UnsafeRow $matched = (UnsafeRow) $matches.next();
         |      $checkCondition {
         |        $found = true;
         |      }
         |    }
         |  }
         |}
         |if (!$found) {
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for existence join.
   */
  protected def codegenExistence(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, _) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")
    val existsVar = ctx.freshName("exists")

    val matched = ctx.freshName("matched")
    val buildVars = genOneSideJoinVars(ctx, matched, buildPlan, setDefaultValue = false)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      s"""
         |$eval
         |${ev.code}
         |$existsVar = !${ev.isNull} && ${ev.value};
       """.stripMargin
    } else {
      s"$existsVar = true;"
    }

    val resultVar = input ++ Seq(ExprCode.forNonNullValue(
      JavaCode.variable(existsVar, BooleanType)))

    if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |boolean $existsVar = false;
         |if ($matched != null) {
         |  $checkCondition
         |}
         |$numOutput.add(1);
         |${consume(ctx, resultVar)}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |boolean $existsVar = false;
         |if ($matches != null) {
         |  while (!$existsVar && $matches.hasNext()) {
         |    UnsafeRow $matched = (UnsafeRow) $matches.next();
         |    $checkCondition
         |  }
         |}
         |$numOutput.add(1);
         |${consume(ctx, resultVar)}
       """.stripMargin
    }
  }

  protected def prepareRelation(ctx: CodegenContext): HashedRelationInfo
}

object HashJoin extends CastSupport with SQLConfHelper {

  private def canRewriteAsLongType(keys: Seq[Expression]): Boolean = {
    // TODO: support BooleanType, DateType and TimestampType
    keys.forall(_.dataType.isInstanceOf[IntegralType]) &&
      keys.map(_.dataType.defaultSize).sum <= 8
  }

  /**
   * Try to rewrite the key as LongType so we can use getLong(), if they key can fit with a long.
   *
   * If not, returns the original expressions.
   */
  def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    assert(keys.nonEmpty)
    if (!canRewriteAsLongType(keys)) {
      return keys
    }

    var keyExpr: Expression = if (keys.head.dataType != LongType) {
      cast(keys.head, LongType)
    } else {
      keys.head
    }
    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(cast(e, LongType), Literal((1L << bits) - 1)))
    }
    keyExpr :: Nil
  }

  /**
   * Extract a given key which was previously packed in a long value using its index to
   * determine the number of bits to shift
   */
  def extractKeyExprAt(keys: Seq[Expression], index: Int): Expression = {
    assert(canRewriteAsLongType(keys))
    // jump over keys that have a higher index value than the required key
    if (keys.size == 1) {
      assert(index == 0)
      Cast(
        child = BoundReference(0, LongType, nullable = false),
        dataType = keys(index).dataType,
        timeZoneId = Option(conf.sessionLocalTimeZone),
        ansiEnabled = false)
    } else {
      val shiftedBits =
        keys.slice(index + 1, keys.size).map(_.dataType.defaultSize * 8).sum
      val mask = (1L << (keys(index).dataType.defaultSize * 8)) - 1
      // build the schema for unpacking the required key
      val castChild = BitwiseAnd(
        ShiftRightUnsigned(BoundReference(0, LongType, nullable = false), Literal(shiftedBits)),
        Literal(mask))
      Cast(
        child = castChild,
        dataType = keys(index).dataType,
        timeZoneId = Option(conf.sessionLocalTimeZone),
        ansiEnabled = false)
    }
  }
}
