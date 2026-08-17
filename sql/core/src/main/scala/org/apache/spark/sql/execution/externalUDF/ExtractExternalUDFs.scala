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

import org.apache.spark.{SparkConf, SparkException, SparkUnsupportedOperationException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.JOIN_CONDITION
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.PythonUDF.isScalarPythonUDF
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, EXTERNAL_UDF, JOIN, PYTHON_UDF}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * Extracts scalar external UDF expressions into logical evaluation nodes.
 *
 * Each [[EvalExternalUDF]] represents one worker session. A worker that does
 * not advertise UDF chaining receives one UDF call per node. A worker that
 * supports chaining may evaluate unary nested chains and fuse independent
 * UDF roots that use the same worker specification.
 *
 * When unified UDF execution and legacy Python UDF conversion are enabled,
 * scalar [[PythonUDF]] expressions are converted to
 * [[ExternalUserDefinedFunction]] expressions before extraction.
 */
private[sql] class ExtractExternalUDFs(sparkConf: SparkConf)
    extends Rule[LogicalPlan] with Logging with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery is rewritten as a join and revisits this rule later.
    case subquery: Subquery if subquery.correlated => plan
    case _ if !conf.getConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED) =>
      if (plan.containsPattern(EXTERNAL_UDF)) {
        throw new SparkUnsupportedOperationException(
          errorClass = "UNSUPPORTED_FEATURE.EXTERNAL_UDF",
          messageParameters = Map(
            "config" -> SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key))
      }
      plan
    case _ =>
      val externalPlan = convertPythonUDFs(plan)
      val joinPlan = extractExternalUDFFromJoinCondition(externalPlan)
      val aggregatePlan = extractExternalUDFFromAggregate(joinPlan)
      val groupingPlan = extractGroupingExternalUDFFromAggregate(aggregatePlan)
      groupingPlan.transformUpWithPruning(_.containsPattern(EXTERNAL_UDF)) {
        // These nodes already own their external UDF expressions.
        case udfPlan: ExternalUDF => udfPlan
        case other => extract(other)
      }
  }

  private def convertPythonUDFs(plan: LogicalPlan): LogicalPlan = {
    val convertPythonUDF =
      conf.getConf(SQLConf.UNIFIED_UDF_EXECUTION_CONVERT_PYTHON_UDF_ENABLED)
    plan.transformUpWithPruning(_.containsPattern(PYTHON_UDF)) { operator =>
      operator.transformExpressionsUpWithPruning(_.containsPattern(PYTHON_UDF)) {
        case udf: PythonUDF if isScalarPythonUDF(udf) =>
          if (!convertPythonUDF) {
            throw new SparkUnsupportedOperationException(
              errorClass = "UNSUPPORTED_FEATURE.PYTHON_UDF_TO_EXTERNAL_UDF",
              messageParameters = Map(
                "config" ->
                  SQLConf.UNIFIED_UDF_EXECUTION_CONVERT_PYTHON_UDF_ENABLED.key))
          }
          ExternalUserDefinedFunction(
            name = Some(udf.name),
            workerSpec = PythonUDFWorkerSpecification.fromPythonFunction(udf.func, sparkConf),
            payload = udf.func.command.toArray,
            dataType = udf.dataType,
            children = udf.children,
            inputTypes = None,
            udfDeterministic = udf.udfDeterministic,
            udfNullable = udf.nullable,
            resultId = udf.resultId)
      }
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

  private def belongsToAggregate(expression: Expression, aggregate: Aggregate): Boolean = {
    expression.isInstanceOf[AggregateExpression] ||
      aggregate.groupingExpressions.exists(_.semanticEquals(expression))
  }

  private def hasExternalUDFOverAggregate(
      expression: Expression,
      aggregate: Aggregate): Boolean = {
    expression.exists {
      case udf: ExternalUserDefinedFunction =>
        udf.references.isEmpty || udf.exists(belongsToAggregate(_, aggregate))
      case _ => false
    }
  }

  private def extractExternalUDFFromAggregate(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithPruning(_.containsAllPatterns(EXTERNAL_UDF, AGGREGATE)) {
      case aggregate: Aggregate
          if aggregate.aggregateExpressions.exists(
            hasExternalUDFOverAggregate(_, aggregate)) =>
        val projectExpressions = ArrayBuffer.empty[NamedExpression]
        val aggregateExpressions = ArrayBuffer.empty[NamedExpression]
        aggregate.aggregateExpressions.foreach { expression =>
          if (hasExternalUDFOverAggregate(expression, aggregate)) {
            val newExpression = expression.transformDown {
              case child: Expression if belongsToAggregate(child, aggregate) =>
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

  private def supportsUDFChaining(workerSpec: UDFWorkerSpecification): Boolean = {
    workerSpec.hasCapabilities &&
      workerSpec.getCapabilities.hasSupportsUdfChaining &&
      workerSpec.getCapabilities.getSupportsUdfChaining
  }

  private def containsExternalUDF(expression: Expression): Boolean = {
    expression.exists(_.isInstanceOf[ExternalUserDefinedFunction])
  }

  /** Returns whether `udf` can be evaluated in one worker session. */
  @scala.annotation.tailrec
  private def isEvaluable(udf: ExternalUserDefinedFunction): Boolean = {
    udf.children match {
      case Seq(child: ExternalUserDefinedFunction)
          if supportsUDFChaining(udf.workerSpec) && child.workerSpec == udf.workerSpec =>
        isEvaluable(child)
      case children =>
        !children.exists(containsExternalUDF)
    }
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

  private def collectEvaluableUDFs(expression: Expression): Seq[ExternalUserDefinedFunction] = {
    expression match {
      case udf: ExternalUserDefinedFunction if isEvaluable(udf) => Seq(udf)
      case other => other.children.flatMap(collectEvaluableUDFs)
    }
  }

  /** Selects the UDF roots that can share the next worker session. */
  private def collectSessionUDFs(plan: LogicalPlan): Seq[ExternalUserDefinedFunction] = {
    val candidates = plan.expressions
      .flatMap(collectEvaluableUDFs)
      .filter(_.references.subsetOf(plan.inputSet))

    val distinct = ArrayBuffer.empty[ExternalUserDefinedFunction]
    candidates.foreach { candidate =>
      if (!distinct.exists(sameUDF(_, candidate))) {
        distinct += candidate
      }
    }

    distinct.headOption.toSeq.flatMap { first =>
      if (supportsUDFChaining(first.workerSpec)) {
        distinct.filter(_.workerSpec == first.workerSpec)
      } else {
        Seq(first)
      }
    }
  }

  /** Extracts one worker session and recursively extracts the remaining UDFs. */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = collectSessionUDFs(plan)
    if (udfs.isEmpty) {
      plan
    } else {
      val udfToAttribute = ArrayBuffer.empty[(ExternalUserDefinedFunction, Attribute)]

      def resultFor(udf: ExternalUserDefinedFunction): Option[Attribute] = {
        udfToAttribute.collectFirst {
          case (candidate, result) if sameUDF(candidate, udf) => result
        }
      }

      val newChildren = plan.children.map { child =>
        val validUdfs = udfs.filter(_.references.subsetOf(child.outputSet))
        if (validUdfs.isEmpty) {
          child
        } else {
          val resultAttrs = validUdfs.zipWithIndex.map { case (udf, index) =>
            AttributeReference(s"externalUDF$index", udf.dataType, udf.nullable)()
          }
          val evaluation = EvalExternalUDF(
            validUdfs.head.workerSpec, validUdfs, resultAttrs, child)
          udfToAttribute ++= validUdfs.zip(resultAttrs)
          evaluation
        }
      }

      udfs.filter(resultFor(_).isEmpty).foreach { udf =>
        throw SparkException.internalError(
          s"Invalid external UDF $udf, requires attributes from more than one child.")
      }

      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case udf: ExternalUserDefinedFunction => resultFor(udf).getOrElse(udf)
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
