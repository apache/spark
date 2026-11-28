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

package org.apache.spark.sql.execution.columnar.udf

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.internal.LogKeys.JOIN_CONDITION
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, EvalColumnarUDFs, Filter, Join, LogicalPlan, Project, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, COLUMNAR_UDF, JOIN}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Extracts the calls of columnar user-defined functions from the operators and evaluates them
 * in an [[EvalColumnarUDFs]] below the operator, as `ExtractPythonUDFs` does for Python UDFs.
 *
 * The calls of one operator that need no other call share an [[EvalColumnarUDFs]]. A call whose
 * argument is another call gets its own, above that of its argument. An argument that is not
 * an attribute is computed by a [[Project]] below, so that the arguments of the calls in an
 * [[EvalColumnarUDFs]] are all columns of its child.
 *
 * Two kinds of calls cannot be evaluated below their operator, and are moved above it first,
 * as `ExtractPythonUDFFromJoinCondition` and `ExtractPythonUDFFromAggregate` do:
 *  - a call in a join condition that reads both sides of the join is moved to a [[Filter]]
 *    above the join, which is possible for an inner join only;
 *  - a call in the result of an [[Aggregate]] that reads aggregate functions or grouping
 *    expressions, or no column at all, is moved to a [[Project]] above the aggregate.
 */
object ExtractColumnarUDFs extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery is rewritten into a join later, and goes through this rule then.
    case s: Subquery if s.correlated => plan

    case _ if !plan.containsPattern(COLUMNAR_UDF) => plan

    case _ =>
      val prepared = extractFromAggregate(extractFromJoinCondition(plan))
      prepared.transformUpWithPruning(_.containsPattern(COLUMNAR_UDF)) {
        case e: EvalColumnarUDFs => e
        case p: LogicalPlan => extract(p)
      }
  }

  // The pattern of an expression includes the plan of a subquery inside it. The calls of that
  // plan are evaluated there, so the expressions themselves are searched.
  private def hasColumnarUDF(e: Expression): Boolean =
    e.containsPattern(COLUMNAR_UDF) && e.exists(_.isInstanceOf[ColumnarUDF])

  // ---------------------------------------------------------------------------------------
  // Calls in a join condition that read both sides
  // ---------------------------------------------------------------------------------------

  private def hasUDFOverBothSides(e: Expression, join: Join): Boolean = e.exists {
    case udf: ColumnarUDF => !canEvaluate(udf, join.left) && !canEvaluate(udf, join.right)
    case _ => false
  }

  private def extractFromJoinCondition(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(COLUMNAR_UDF, JOIN)) {
      case join @ Join(_, _, joinType, Some(condition), _)
          if hasUDFOverBothSides(condition, join) =>
        if (!joinType.isInstanceOf[InnerLike]) {
          // Evaluating a part of the condition after the join changes the result of a join
          // that is not an inner join.
          throw QueryCompilationErrors.useColumnarUDFInJoinConditionUnsupportedError(joinType)
        }
        val (udfConditions, otherConditions) =
          splitConjunctivePredicates(condition).partition(hasUDFOverBothSides(_, join))
        val newCondition = if (otherConditions.isEmpty) {
          logWarning(log"The join condition:${MDC(JOIN_CONDITION, condition)} " +
            log"of the join plan contains columnar UDFs only, " +
            log"so it will be moved out and the join plan will become a cross join.")
          None
        } else {
          Some(otherConditions.reduceLeft(And))
        }
        Filter(udfConditions.reduceLeft(And), join.copy(condition = newCondition))
    }
  }

  // ---------------------------------------------------------------------------------------
  // Calls in the result of an aggregate that can only be evaluated after it
  // ---------------------------------------------------------------------------------------

  /** Whether the aggregate computes the expression: an aggregate function or a grouping key. */
  private def belongsToAggregate(e: Expression, agg: Aggregate): Boolean = {
    e.isInstanceOf[AggregateExpression] || agg.groupingExpressions.exists(_.semanticEquals(e))
  }

  private def hasUDFOverAggregate(e: Expression, agg: Aggregate): Boolean = e.exists {
    case udf: ColumnarUDF => udf.references.isEmpty || udf.exists(belongsToAggregate(_, agg))
    case _ => false
  }

  private def extractFromAggregate(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(COLUMNAR_UDF, AGGREGATE)) {
      case agg: Aggregate if agg.aggregateExpressions.exists(hasUDFOverAggregate(_, agg)) =>
        val projectList = mutable.ArrayBuffer[NamedExpression]()
        val aggregateExpressions = mutable.ArrayBuffer[NamedExpression]()
        // Equal expressions share an alias, so that equal calls over them stay equal.
        val aliases = mutable.HashMap[Expression, NamedExpression]()
        agg.aggregateExpressions.foreach { expr =>
          if (hasUDFOverAggregate(expr, agg)) {
            // The aggregate computes what the calls read, and the project above evaluates
            // the calls.
            val newExpr = expr.transformDown {
              case e: Expression if belongsToAggregate(e, agg) =>
                val named = e match {
                  case n: NamedExpression => n
                  case other if other.deterministic =>
                    aliases.getOrElseUpdate(other.canonicalized, Alias(other, "agg")())
                  case other => Alias(other, "agg")()
                }
                if (!aggregateExpressions.exists(_.exprId == named.exprId)) {
                  aggregateExpressions += named
                }
                named.toAttribute
            }
            projectList += newExpr.asInstanceOf[NamedExpression]
          } else {
            aggregateExpressions += expr
            projectList += expr.toAttribute
          }
        }
        Project(projectList.toSeq, agg.copy(aggregateExpressions = aggregateExpressions.toSeq))
    }
  }

  // ---------------------------------------------------------------------------------------
  // Calls that are evaluated below their operator
  // ---------------------------------------------------------------------------------------

  /** The calls that can be evaluated now: those whose arguments contain no other call. */
  private def collectEvaluableUDFs(e: Expression): Seq[ColumnarUDF] = e match {
    case udf: ColumnarUDF if !udf.children.exists(hasColumnarUDF) => Seq(udf)
    case other => other.children.flatMap(collectEvaluableUDFs)
  }

  /**
   * Equal calls of a deterministic function are evaluated once. The calls of a function that is
   * not deterministic are kept apart, by their `resultId`.
   */
  private def canonicalizeDeterministic(u: ColumnarUDF): ColumnarUDF = {
    if (u.deterministic) u.canonicalized.asInstanceOf[ColumnarUDF] else u
  }

  private def extract(plan: LogicalPlan): LogicalPlan = {
    // An `ExpressionSet` keeps every expression that is not deterministic, so one call of such
    // a function that the operator holds twice, as when a `Column` is selected twice, is in it
    // twice. It is one call, with one `resultId`, and is evaluated once.
    val udfs = ExpressionSet(plan.expressions.flatMap(collectEvaluableUDFs))
      .filter(_.references.subsetOf(plan.inputSet))
      .toSeq.asInstanceOf[Seq[ColumnarUDF]].distinct
    if (udfs.isEmpty) {
      plan
    } else {
      val attributeMap = mutable.HashMap[ColumnarUDF, Expression]()
      val newChildren = plan.children.map { child =>
        // A call that reads no column can be evaluated over any child, and the first one does.
        val validUdfs = udfs.filter { udf =>
          udf.references.subsetOf(child.outputSet) &&
            !attributeMap.contains(canonicalizeDeterministic(udf))
        }
        if (validUdfs.nonEmpty) {
          // The arguments that are not attributes of the child are computed below. Equal
          // arguments share a column, unless they are not deterministic.
          val argAliases = mutable.ArrayBuffer[Alias]()
          val sharedArgs = mutable.HashMap[Expression, Alias]()
          def newAlias(arg: Expression): Alias = {
            val alias = Alias(arg, s"columnarUDFArg${argAliases.size}")()
            argAliases += alias
            alias
          }
          val udfsOverColumns = validUdfs.map { udf =>
            val newArgs = udf.children.map {
              case a: Attribute => a
              case arg if arg.deterministic =>
                sharedArgs.getOrElseUpdate(arg.canonicalized, newAlias(arg)).toAttribute
              case arg => newAlias(arg).toAttribute
            }
            udf.withNewChildren(newArgs).asInstanceOf[ColumnarUDF]
          }
          val input = if (argAliases.isEmpty) {
            child
          } else {
            Project(child.output ++ argAliases, child)
          }
          val resultAttrs = validUdfs.zipWithIndex.map { case (u, i) =>
            AttributeReference(s"columnarUDF$i", u.dataType, u.nullable)()
          }
          attributeMap ++= validUdfs.map(canonicalizeDeterministic).zip(resultAttrs)
          EvalColumnarUDFs(udfsOverColumns, resultAttrs, input)
        } else {
          child
        }
      }
      udfs.map(canonicalizeDeterministic).filterNot(attributeMap.contains).foreach { udf =>
        throw SparkException.internalError(
          s"Invalid columnar function call $udf, requires attributes from more than one child.")
      }

      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case u: ColumnarUDF => attributeMap.getOrElse(canonicalizeDeterministic(u), u)
      }

      // Extract the calls whose arguments were calls.
      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) {
        Project(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }
}
