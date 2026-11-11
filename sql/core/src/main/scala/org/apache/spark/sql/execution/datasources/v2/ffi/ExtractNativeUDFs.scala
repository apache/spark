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
package org.apache.spark.sql.execution.datasources.v2.ffi

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, JOIN, NATIVE_UDF}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Plans the calls of native functions into [[EvalNativeUDFs]] below the operators that use their
 * results, one per level of nesting, shared by the sibling calls over the same child. Python UDFs
 * in their arguments are left to the Python rules, which run next and plan them below. Calls over
 * aggregates go above the aggregate, and cross-side calls of inner join conditions above the join.
 */
object ExtractNativeUDFs extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery is rewritten as a join and revisits this rule later.
    case subquery: Subquery if subquery.correlated => plan
    case _ if !plan.containsPattern(NATIVE_UDF) => plan
    case _ =>
      checkNoCallInLambda(plan)
      val prepared = fromGroupingKeys(overAggregates(fromJoinConditions(plan)))
      prepared.transformUpWithPruning(_.containsPattern(NATIVE_UDF)) {
        case evaluation: EvalNativeUDFs => evaluation
        case other => extract(other)
      }
  }

  private def checkNoCallInLambda(plan: LogicalPlan): Unit = plan.foreach { node =>
    node.expressions.foreach(_.foreach {
      case lambda: LambdaFunction =>
        lambda.function.collectFirst { case udf: NativeUDF => udf }.foreach { udf =>
          throw QueryCompilationErrors.nativeUdfInLambdaUnsupportedError(udf.name)
        }
      case _ =>
    })
  }

  private def hasCall(expression: Expression): Boolean = expression.containsPattern(NATIVE_UDF)

  private def needsBothSides(expression: Expression, join: Join): Boolean = expression.exists {
    case udf: NativeUDF => !canEvaluate(udf, join.left) && !canEvaluate(udf, join.right)
    case _ => false
  }

  /** Moves the conjuncts of an inner join condition with calls that need both sides above it. */
  private def fromJoinConditions(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(NATIVE_UDF, JOIN)) {
      case join @ Join(_, _, joinType, Some(condition), _) if needsBothSides(condition, join) =>
        if (!joinType.isInstanceOf[InnerLike]) {
          throw QueryCompilationErrors.nativeUdfInJoinConditionUnsupportedError(joinType)
        }
        val (withCalls, others) =
          splitConjunctivePredicates(condition).partition(needsBothSides(_, join))
        Filter(withCalls.reduceLeft(And), join.copy(condition = others.reduceLeftOption(And)))
    }
  }

  private def belongsToAggregate(expression: Expression, grouping: ExpressionSet): Boolean = {
    expression.isInstanceOf[AggregateExpression] || grouping.contains(expression)
  }

  private def hasCallOverAggregate(expression: Expression, grouping: ExpressionSet): Boolean = {
    expression.exists {
      case udf: NativeUDF => udf.references.isEmpty || udf.exists(belongsToAggregate(_, grouping))
      case _ => false
    }
  }

  /** Evaluates the calls over the aggregates or the grouping keys of an aggregate above it. */
  private def overAggregates(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(NATIVE_UDF, AGGREGATE)) {
      case aggregate: Aggregate =>
        val grouping = ExpressionSet(aggregate.groupingExpressions)
        if (!aggregate.aggregateExpressions.exists(hasCallOverAggregate(_, grouping))) {
          aggregate
        } else {
          val projectList = ArrayBuffer.empty[NamedExpression]
          val aggregateList = ArrayBuffer.empty[NamedExpression]
          aggregate.aggregateExpressions.foreach { expression =>
            if (hasCallOverAggregate(expression, grouping)) {
              projectList += expression.transformDown {
                case child if belongsToAggregate(child, grouping) =>
                  val alias = child match {
                    case named: NamedExpression => named
                    case other => Alias(other, "agg")()
                  }
                  aggregateList += alias
                  alias.toAttribute
              }.asInstanceOf[NamedExpression]
            } else {
              aggregateList += expression
              projectList += expression.toAttribute
            }
          }
          Project(projectList.toSeq, aggregate.copy(aggregateExpressions = aggregateList.toSeq))
        }
    }
  }

  private def key(udf: NativeUDF): Expression = if (udf.deterministic) udf.canonicalized else udf

  /** Evaluates the calls in the grouping keys of an aggregate below it. */
  private def fromGroupingKeys(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(NATIVE_UDF, AGGREGATE)) {
      case aggregate: Aggregate if aggregate.groupingExpressions.exists(hasCall) =>
        val projectList = ArrayBuffer.empty[NamedExpression]
        val keys = mutable.HashMap.empty[Expression, Attribute]
        val groupingExpressions = aggregate.groupingExpressions.map(_.transformDown {
          case udf: NativeUDF =>
            keys.getOrElseUpdate(key(udf), {
              val alias = Alias(udf, "groupingNativeUDF")()
              projectList += alias
              alias.toAttribute
            })
        })
        val aggregateExpressions = aggregate.aggregateExpressions.map(_.transformUp {
          case udf: NativeUDF if udf.deterministic => keys.getOrElse(key(udf), udf)
        }.asInstanceOf[NamedExpression])
        aggregate.copy(
          groupingExpressions = groupingExpressions,
          aggregateExpressions = aggregateExpressions,
          child = Project(projectList.toSeq ++ aggregate.child.output, aggregate.child))
    }
  }

  /** The calls whose arguments have no call, which can be evaluated first. */
  private def evaluableCalls(expression: Expression): Seq[NativeUDF] = expression match {
    case udf: NativeUDF if !udf.children.exists(hasCall) => Seq(udf)
    case other => other.children.flatMap(evaluableCalls)
  }

  /** Evaluates the calls of an operator below it, and the calls in their results above them. */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = ExpressionSet(plan.expressions.flatMap(evaluableCalls))
      .filter(_.references.subsetOf(plan.inputSet))
      .toSeq.asInstanceOf[Seq[NativeUDF]]
    if (udfs.isEmpty) {
      plan
    } else {
      val byChild = udfs.groupBy { udf =>
        plan.children.indexWhere(child => udf.references.subsetOf(child.outputSet))
      }
      byChild.get(-1).foreach { udf =>
        throw SparkException.internalError(
          s"The native function call $udf needs attributes of more than one child.")
      }
      val results = mutable.HashMap.empty[Expression, Attribute]
      val newChildren = plan.children.zipWithIndex.map { case (child, i) =>
        byChild.get(i).map { calls =>
          val resultAttrs = calls.zipWithIndex.map { case (udf, j) =>
            AttributeReference(s"nativeUDF$j", udf.dataType)()
          }
          results ++= calls.map(key).zip(resultAttrs)
          EvalNativeUDFs(calls, resultAttrs, child)
        }.getOrElse(child)
      }
      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case udf: NativeUDF => results.getOrElse(key(udf), udf)
      }
      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) Project(plan.output, newPlan) else newPlan
    }
  }
}
