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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.metric.SQLMetrics

/** Base physical plan for [[FilterExec]] and [[StopAfterExec]]. */
abstract class FilterExecBase extends UnaryExecNode with CodegenSupport with PredicateHelper {

  val child: SparkPlan

  // The [[IsNotNulls]] predicates included in filtering condition.
  protected val notNullPreds: Seq[Expression]

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private lazy val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  /** A helper method to split [[IsNotNull]] predicates out of the condition */
  protected def splitNullPredicate(condition: Expression): (Seq[Expression], Seq[Expression]) = {
    splitConjunctivePredicates(condition).partition {
      case IsNotNull(a: NullIntolerant) if a.references.subsetOf(child.outputSet) => true
      case _ => false
    }
  }

  /**
   * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
   */
  private def genPredicate(
      ctx: CodegenContext,
      c: Expression,
      in: Seq[ExprCode],
      attrs: Seq[Attribute]): String = {
    val bound = BindReferences.bindReference(c, attrs)
    val evaluated = evaluateRequiredVariables(child.output, in, c.references)

    // Generate the code for the predicate.
    val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
    val nullCheck = if (bound.nullable) {
      s"${ev.isNull} || "
    } else {
      s""
    }

    s"""
       |$evaluated
       |${ev.code}
       |if (${nullCheck}!${ev.value}) continue;
     """.stripMargin
  }

  // To generate the predicates we will follow this algorithm.
  // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
  // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
  // generate that check *before* the predicate. After all of these predicates, we will generate
  // the remaining IsNotNull checks that were not part of other predicates.
  // This has the property of not doing redundant IsNotNull checks and taking better advantage of
  // short-circuiting, not loading attributes until they are needed.
  // This is very perf sensitive.
  // TODO: revisit this. We can consider reordering predicates as well.
  protected def genOtherPredicate(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      otherPreds: Seq[Expression],
      generatedNullChecks: Array[Boolean]): String = {

    otherPreds.map { c =>
      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}
        if (idx != -1 && !generatedNullChecks(idx)) {
          generatedNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          genPredicate(ctx, notNullPreds(idx), input, child.output)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(ctx, c, input, output)}
       """.stripMargin.trim
    }.mkString("\n")
  }

  /** A helper method used to generate codes for [[IsNotNull]]. */
  private def genNullChecks(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      generatedNullChecks: Array[Boolean]): String = {
    notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedNullChecks(idx)) {
        genPredicate(ctx, c, input, child.output)
      } else {
        ""
      }
    }.mkString("\n")
  }

  /**
   * An abstract method used to generate codes for the predicates other than [[IsNotNull]].
   * [[FilterExec]] and [[StopAfterExec]] implement this method.
   */
  protected def genCodeForPredicates(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      generatedNullChecks: Array[Boolean]): String

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")
    ctx.currentVars = input

    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val generated = genCodeForPredicates(ctx, input, generatedIsNotNullChecks)
    val nullChecks = genNullChecks(ctx, input, generatedIsNotNullChecks)

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = "false"
      }
      ev
    }

    s"""
       |$generated
       |$nullChecks
       |$numOutput.add(1);
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

/** Physical plan for Filter. */
case class FilterExec(val condition: Expression, val child: SparkPlan)
    extends FilterExecBase {

  // Split out all the IsNotNulls from condition.
  private val (_notNullPreds, otherPreds) = splitNullPredicate(condition)

  protected override val notNullPreds = _notNullPreds

  protected def genCodeForPredicates(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      generatedNullChecks: Array[Boolean]): String = {
    genOtherPredicate(ctx, input, otherPreds, generatedNullChecks)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { iter =>
      val predicate = newPredicate(condition, child.output)
      iter.filter { row =>
        val r = predicate(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }
}

/**
 *  Specific physical plan for Filter. Different to [[FilterExec]], this plan is specified for
 *  filtering on sorted data. If the data is sorted by a key, filters on the key could stop as
 *  soon as the data is out of range. For example, when we do filtering on a data sorted by a
 *  column called "id". The filter condition WHERE id < 10 should stop as soon as the first
 *  row with the column "id" equal to or more than 10 is seen.
 */
case class StopAfterExec(
    stopAfterCondition: Expression,
    otherCondition: Option[Expression],
    child: SparkPlan) extends FilterExecBase {

  protected val (_notNullPreds, otherCondNotNullConds) = if (otherCondition.isDefined) {
    splitNullPredicate(otherCondition.get)
  } else {
    (Seq[Expression](), Seq[Expression]())
  }

  protected override val notNullPreds = _notNullPreds

  val stopAfterConditions = splitConjunctivePredicates(stopAfterCondition)

  protected def genCodeForPredicates(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      generatedNullChecks: Array[Boolean]): String = {
    val condPassed = ctx.freshName("condPassed")
    ctx.addMutableState(s"boolean", condPassed, s"$condPassed = true;")
    s"""
       |if (!$condPassed) break;
       |$condPassed = false;
       |${genOtherPredicate(ctx, input, stopAfterConditions, generatedNullChecks)}
       |$condPassed = true;
       |${genOtherPredicate(ctx, input, otherCondNotNullConds, generatedNullChecks)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { iter =>
      val stopPredicate = newPredicate(stopAfterCondition, child.output)
      val predicate = if (otherCondition.isDefined) {
        newPredicate(otherCondition.get, child.output)
      } else {
        (_: InternalRow) => true
      }
      iter.takeWhile { row =>
        stopPredicate(row)
      }.filter { row =>
        val r = predicate(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }
}

object FilterExecBase extends PredicateHelper {
  /**
   * A helper function which determines whether a physical plan for filtering can be executed
   * by a [[StopAfterExec]]. If the child output is sorted, we can determine to stop filtering
   * as early as possible.
   * E.g, when we do filtering with "WHERE id < 10" on a child sorted by id in ascending. Once
   * the condition is false, it guarantees that no more data will satisfy the condition and we
   * can stop this filtering early.
   *
   * We can apply [[StopAfterExec]] only if the expression of the first [[SortOrder]] is used
   * in a [[BinaryComparison]] to compare with a [[NonNullLiteral]].
   *
   * This function returns a [[Tuple]]. The first element is the [[Option]] value of the
   * condition the [[StopAfterExec]] depends on to stop early. If we can't apply
   * [[StopAfterExec]], it is [[None]]. The second element is the [[Option]] of remaining
   * filtering conditions.
   */
  def getStopAfterCondition(
      condition: Expression,
      child: SparkPlan): (Option[Expression], Option[Expression]) = {

    // Take the first expression that the child output is ordered by.
    // E.g., if the child.outputOrdering is [a, b, c + 1, d], the expression is [a].
    //       if the child.outputOrdering is [a + 1, b], the expression is [a + 1].
    // This expression must be non-nullable.
    // TODO: Can we utilize `NullOrdering` to adapt for nullable expression?
    val orderingExpression = child.outputOrdering.take(1).map(_.child).filter(!_.nullable)
    if (orderingExpression.isEmpty) {
      return (None, Some(condition))
    }

    val (binaryComparison, nonBinaryConds) = splitConjunctivePredicates(condition)
      .partition(_.isInstanceOf[BinaryComparison])

    // Normalize binary comparison. E.g., 1 < id will be reordered to id > 1.
    val normalized = normalizePredicate(binaryComparison)

    // Extract stop after conditions which satisfy:
    // 1. [[BinaryComparison]] between first [[SortOrder]] expression and a [[NonNullLiteral]].
    // 2. Deterministic.
    val (stopAfterConditions, remainingConds) = normalized.partition { expr =>
      val isNonNullLiteral = expr.asInstanceOf[BinaryComparison].right match {
        case v @ NonNullLiteral(_, _) => true
        case _ => false
      }
      expr.asInstanceOf[BinaryComparison].left.semanticEquals(orderingExpression(0)) &&
        isNonNullLiteral && expr.deterministic
    }

    val sameDirections = isAllowedPredicatesInSameDirection(stopAfterConditions)

    if (stopAfterConditions.isEmpty || !sameDirections) {
      return (None, Some(condition))
    } else {
      val finalStopCond = if (child.outputOrdering(0).isAscending) {
        // If the first sort order expression is in ascending order.
        stopAfterConditions.head match {
          case _: LessThan => Some(stopAfterConditions.reduce(And))
          case _: LessThanOrEqual => Some(stopAfterConditions.reduce(And))
          case _ => None
        }
      } else {
        // If the first sort order expression is in descending order.
        stopAfterConditions.head match {
          case _: GreaterThan => Some(stopAfterConditions.reduce(And))
          case _: GreaterThanOrEqual => Some(stopAfterConditions.reduce(And))
          case _ => None
        }
      }
      (finalStopCond, (nonBinaryConds ++ remainingConds).reduceOption(And))
    }
  }

  /**
   * Normalizes a sequence of predicates to move literal to right child of predicate.
   */
  private def normalizePredicate(predicates: Seq[Expression]): Seq[Expression] = {
    // TODO: Extract attribute from simple arithmetic expression.
    // E.g., a + 1 > 5 can be normalized to a > 4.
    // We can add normalization like this to make [[StopAfterExec]] more flexible.
    predicates.map { expr =>
      expr match {
        case GreaterThan(v @ NonNullLiteral(_, _), r) => LessThan(r, v)
        case LessThan(v @ NonNullLiteral(_, _), r) => GreaterThan(r, v)
        case GreaterThanOrEqual(v @ NonNullLiteral(_, _), r) => LessThanOrEqual(v, r)
        case LessThanOrEqual(v @ NonNullLiteral(_, _), r) => GreaterThanOrEqual(v, r)
        case _ => expr
      }
    }
  }

  /**
   *  The binary comparison must be the same direction. I.e.,
   *    1. All comparison are LessThan or LessThanOrEqual.
   *    2. All comparison are GreaterThan or GreaterThanOrEqual.
   */
  private def isAllowedPredicatesInSameDirection(predicates: Seq[Expression]): Boolean = {
    if (predicates.length > 0) {
      predicates.head match {
        case _: LessThan | _: LessThanOrEqual =>
          predicates.tail.collect {
            case _: GreaterThan | _: GreaterThanOrEqual => true
            case _: EqualTo | _: EqualNullSafe => true
          }.isEmpty
        case _: GreaterThan | _: GreaterThanOrEqual =>
          predicates.tail.collect {
            case _: LessThan | _: LessThanOrEqual => true
            case _: EqualTo | _: EqualNullSafe => true
          }.isEmpty
        case _ => false
      }
    } else {
      false
    }
  }
}

/**
 * Replaces [[FilterExec]] in a query plan with [[StopAfterExec]] if its child plan is satisfying
 * the necessary requirements.
 */
case object ApplyStopAfter extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case f @ FilterExec(condition, child) =>
      val (stopAfterCond, remainingCond) = FilterExecBase.getStopAfterCondition(condition, child)
      if (stopAfterCond.isDefined) {
        StopAfterExec(stopAfterCond.get, remainingCond, child)
      } else {
        f
      }
  }
}
