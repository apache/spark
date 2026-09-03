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

package org.apache.spark.sql.execution.externalUDF

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.JOIN_CONDITION
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, EXTERNAL_UDF, JOIN}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.StaticSQLConf

/**
 * Converts each scalar external UDF expression into a separate logical evaluation node.
 * Join-condition handling mirrors `ExtractPythonUDFFromJoinCondition`.
 *
 * TODO(SPARK-55278): Add an external UDF equivalent of `ExtractPythonUDFFromLambda`.
 * TODO(SPARK-55278): Revisit sharing placement logic with the Python UDF extractors after
 * external UDF planning semantics stabilize.
 */
private[sql] object PlanExternalUDFs
    extends Rule[LogicalPlan] with Logging with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery is rewritten as a join and revisits this rule later.
    case subquery: Subquery if subquery.correlated => plan
    case _ if !conf.getConf(StaticSQLConf.UNIFIED_UDF_EXECUTION_ENABLED) =>
      if (plan.containsPattern(EXTERNAL_UDF)) {
        throw QueryCompilationErrors.externalUDFsDisabledError(
          StaticSQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key)
      }
      plan
    case _ =>
      var preparedPlan = extractExternalUDFFromJoinCondition(plan)
      preparedPlan = extractExternalUDFFromAggregate(preparedPlan)
      preparedPlan = extractGroupingExternalUDFFromAggregate(preparedPlan)
      preparedPlan.transformUpWithPruning(_.containsPattern(EXTERNAL_UDF)) {
        // These nodes already own their external UDF expressions.
        case udfPlan: ExternalUDF => udfPlan
        case other => extract(other)
      }
  }

  private def hasUnevaluableExternalUDF(expression: Expression, join: Join): Boolean = {
    expression.exists {
      case udf: ExternalUserDefinedFunction =>
        !canEvaluate(udf, join.left) && !canEvaluate(udf, join.right)
      case _ => false
    }
  }

  private def extractExternalUDFFromJoinCondition(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(EXTERNAL_UDF, JOIN)) {
      case join @ Join(_, _, joinType, Some(condition), _)
          if hasUnevaluableExternalUDF(condition, join) =>
        if (!joinType.isInstanceOf[InnerLike]) {
          // Match `PYTHON_UDF_IN_ON_CLAUSE`: moving a cross-side UDF to a post-join filter
          // changes the semantics of non-inner joins.
          throw QueryCompilationErrors.useExternalUDFInJoinConditionUnsupportedError(joinType)
        }

        val (udfConditions, otherConditions) = splitConjunctivePredicates(condition)
          .partition(hasUnevaluableExternalUDF(_, join))
        val newCondition = if (otherConditions.isEmpty) {
          logWarning(log"The join condition:${MDC(JOIN_CONDITION, condition)} " +
            log"of the join plan contains external UDFs only, " +
            log"so it will be moved out and the join plan will become a cross join.")
          None
        } else {
          Some(otherConditions.reduceLeft(And))
        }
        Filter(udfConditions.reduceLeft(And), join.copy(condition = newCondition))
    }
  }

  private def belongsToAggregate(
      expression: Expression,
      groupingExpressions: ExpressionSet): Boolean = {
    expression.isInstanceOf[AggregateExpression] ||
      groupingExpressions.contains(expression)
  }

  private def hasExternalUDFOverAggregate(
      expression: Expression,
      groupingExpressions: ExpressionSet): Boolean = {
    expression.exists {
      case udf: ExternalUserDefinedFunction =>
        udf.references.isEmpty || udf.exists(belongsToAggregate(_, groupingExpressions))
      case _ => false
    }
  }

  private def extractExternalUDFFromAggregate(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(EXTERNAL_UDF, AGGREGATE)) {
      case aggregate: Aggregate =>
        val groupingExpressions = ExpressionSet(aggregate.groupingExpressions)
        if (!aggregate.aggregateExpressions.exists(
            hasExternalUDFOverAggregate(_, groupingExpressions))) {
          aggregate
        } else {
          val projectExpressions = ArrayBuffer.empty[NamedExpression]
          val aggregateExpressions = ArrayBuffer.empty[NamedExpression]
          aggregate.aggregateExpressions.foreach { expression =>
            if (hasExternalUDFOverAggregate(expression, groupingExpressions)) {
              val newExpression = expression.transformDown {
                case child: Expression if belongsToAggregate(child, groupingExpressions) =>
                  val alias = child match {
                    case named: NamedExpression => named
                    case other => Alias(other, "agg")()
                  }
                  aggregateExpressions += alias
                  alias.toAttribute
              }
              projectExpressions += newExpression.asInstanceOf[NamedExpression]
            } else {
              aggregateExpressions += expression
              projectExpressions += expression.toAttribute
            }
          }
          Project(
            projectExpressions.toSeq,
            aggregate.copy(aggregateExpressions = aggregateExpressions.toSeq))
        }
    }
  }

  private def hasExternalUDF(expression: Expression): Boolean = {
    expression.exists(_.isInstanceOf[ExternalUserDefinedFunction])
  }

  private def extractGroupingExternalUDFFromAggregate(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(EXTERNAL_UDF, AGGREGATE)) {
      case aggregate: Aggregate if aggregate.groupingExpressions.exists(hasExternalUDF) =>
        val projectExpressions = ArrayBuffer.empty[NamedExpression]
        val groupingExpressions = ArrayBuffer.empty[Expression]
        val attributeMap = ArrayBuffer.empty[
          (ExternalUserDefinedFunction, NamedExpression)]

        def mappedAttribute(udf: ExternalUserDefinedFunction): Option[NamedExpression] = {
          attributeMap.collectFirst {
            case (candidate, attribute) if sameUDF(candidate, udf) => attribute
          }
        }

        aggregate.groupingExpressions.foreach { expression =>
          if (hasExternalUDF(expression)) {
            val newExpression = expression.transformDown {
              case udf: ExternalUserDefinedFunction =>
                assert(udf.udfDeterministic,
                  "Non-deterministic external UDFs should not appear in grouping expressions")
                mappedAttribute(udf).getOrElse {
                  val alias = Alias(udf, "groupingExternalUDF")()
                  projectExpressions += alias
                  attributeMap += ((udf, alias.toAttribute))
                  alias.toAttribute
                }
            }
            groupingExpressions += newExpression
          } else {
            groupingExpressions += expression
          }
        }

        val aggregateExpressions = aggregate.aggregateExpressions.map { expression =>
          expression.transformUp {
            case udf: ExternalUserDefinedFunction if udf.udfDeterministic =>
              mappedAttribute(udf).getOrElse(udf)
          }.asInstanceOf[NamedExpression]
        }
        aggregate.copy(
          groupingExpressions = groupingExpressions.toSeq,
          aggregateExpressions = aggregateExpressions,
          child = Project((projectExpressions ++ aggregate.child.output).toSeq, aggregate.child))
    }
  }

  private def containsExternalUDF(expression: Expression): Boolean = {
    expression.exists(_.isInstanceOf[ExternalUserDefinedFunction])
  }

  private def isEvaluable(udf: ExternalUserDefinedFunction): Boolean = {
    !udf.children.exists(containsExternalUDF)
  }

  private def sameUDF(
      left: ExternalUserDefinedFunction,
      right: ExternalUserDefinedFunction): Boolean = {
    if (left.deterministic && right.deterministic) {
      val normalizedPayload = Array.emptyByteArray
      left.payload.sameElements(right.payload) &&
        left.copy(payload = normalizedPayload).semanticEquals(
          right.copy(payload = normalizedPayload))
    } else {
      left.resultId == right.resultId
    }
  }

  private def collectEvaluableUDFs(
      expression: Expression): Iterator[ExternalUserDefinedFunction] = {
    expression match {
      case udf: ExternalUserDefinedFunction if isEvaluable(udf) => Iterator.single(udf)
      case other => other.children.iterator.flatMap(collectEvaluableUDFs)
    }
  }

  private def collectEvaluableUDF(plan: LogicalPlan): Option[ExternalUserDefinedFunction] = {
    val inputSet = plan.inputSet
    plan.expressions.iterator
      .flatMap(collectEvaluableUDFs)
      .find(_.references.subsetOf(inputSet))
  }

  /** Converts one UDF expression to a node and recursively converts the remaining UDFs. */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    collectEvaluableUDF(plan) match {
      case None => plan
      case Some(udf) =>
        val childIndex = plan.children.indexWhere { child =>
          udf.references.subsetOf(child.outputSet)
        }
        if (childIndex < 0) {
          throw QueryCompilationErrors.externalUDFWithMultipleChildrenUnsupportedError(udf)
        }

        val resultAttr = AttributeReference("externalUDF", udf.dataType, udf.nullable)()
        val newChildren = plan.children.zipWithIndex.map { case (child, index) =>
          if (index == childIndex) {
            ExecuteExternalUDF(udf, resultAttr, child)
          } else {
            child
          }
        }
        val rewritten = plan.withNewChildren(newChildren).transformExpressions {
          case candidate: ExternalUserDefinedFunction if candidate.resultId == udf.resultId =>
            resultAttr
        }

        val newPlan = extract(rewritten)
        if (newPlan.output != plan.output) {
          Project(plan.output, newPlan)
        } else {
          newPlan
        }
    }
  }
}
