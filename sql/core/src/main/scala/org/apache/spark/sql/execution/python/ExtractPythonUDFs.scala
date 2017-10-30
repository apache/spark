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

package org.apache.spark.sql.execution.python

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}


/**
 * Extracts all the Python UDFs in logical aggregate, which depends on aggregate expression or
 * grouping key, evaluate them after aggregate.
 */
object ExtractPythonUDFFromAggregate extends Rule[LogicalPlan] {

  /**
   * Returns whether the expression could only be evaluated within aggregate.
   */
  private def belongAggregate(e: Expression, agg: Aggregate): Boolean = {
    e.isInstanceOf[AggregateExpression] ||
      agg.groupingExpressions.exists(_.semanticEquals(e))
  }

  private def hasPythonUdfOverAggregate(expr: Expression, agg: Aggregate): Boolean = {
    expr.find {
      e => e.isInstanceOf[PythonUDF] && e.find(belongAggregate(_, agg)).isDefined
    }.isDefined
  }

  private def extract(agg: Aggregate): LogicalPlan = {
    val projList = new ArrayBuffer[NamedExpression]()
    val aggExpr = new ArrayBuffer[NamedExpression]()
    agg.aggregateExpressions.foreach { expr =>
      if (hasPythonUdfOverAggregate(expr, agg)) {
        // Python UDF can only be evaluated after aggregate
        val newE = expr transformDown {
          case e: Expression if belongAggregate(e, agg) =>
            val alias = e match {
              case a: NamedExpression => a
              case o => Alias(e, "agg")()
            }
            aggExpr += alias
            alias.toAttribute
        }
        projList += newE.asInstanceOf[NamedExpression]
      } else {
        aggExpr += expr
        projList += expr.toAttribute
      }
    }
    // There is no Python UDF over aggregate expression
    Project(projList, agg.copy(aggregateExpressions = aggExpr))
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case agg: Aggregate if agg.aggregateExpressions.exists(hasPythonUdfOverAggregate(_, agg)) =>
      extract(agg)
  }
}


/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 *
 * Only extracts the PythonUDFs that could be evaluated in Python (the single child is PythonUDFs
 * or all the children could be evaluated in JVM).
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
object ExtractPythonUDFs extends Rule[SparkPlan] with PredicateHelper {

  private def hasPythonUDF(e: Expression): Boolean = {
    e.find(_.isInstanceOf[PythonUDF]).isDefined
  }

  private def canEvaluateInPython(e: PythonUDF): Boolean = {
    e.children match {
      // single PythonUDF child could be chained and evaluated in Python
      case Seq(u: PythonUDF) => canEvaluateInPython(u)
      // Python UDF can't be evaluated directly in JVM
      case children => !children.exists(hasPythonUDF)
    }
  }

  private def collectEvaluatableUDF(expr: Expression): Seq[PythonUDF] = expr match {
    case udf: PythonUDF if canEvaluateInPython(udf) => Seq(udf)
    case e => e.children.flatMap(collectEvaluatableUDF)
  }

  def apply(plan: SparkPlan): SparkPlan = plan transformUp {
    // FlatMapGroupsInPandas can be evaluated directly in python worker
    // Therefore we don't need to extract the UDFs
    case plan: FlatMapGroupsInPandasExec => plan
    case plan: SparkPlan => extract(plan)
  }

  private def pickUDFIntoMap(
      expr: Expression,
      condition: Expression,
      exprMap: mutable.HashMap[PythonUDF, Seq[Expression]]): Unit = {
    expr.foreachUp {
      case udf: PythonUDF => exprMap.update(udf, exprMap.getOrElse(udf, Seq()) :+ condition)
      case _ =>
    }
  }

  private def updateConditionForUDFInBranch(
      branches: Seq[(Expression, Expression)],
      exprMap: mutable.HashMap[PythonUDF, Seq[Expression]]): Unit = {
    branches.foreach(branch => pickUDFIntoMap(branch._2, branch._1, exprMap))
  }

  private def updateConditionForUDFInElse(
      branches: Seq[(Expression, Expression)],
      elseValue: Expression,
      exprMap: mutable.HashMap[PythonUDF, Seq[Expression]]): Unit = {
    assert(branches.length > 0)

    val elseCond = branches.map(_._1).reduce(Or)
    pickUDFIntoMap(elseValue, Not(elseCond), exprMap)
  }

  private def updateConditionForUDFInCaseWhen(
      branches: Seq[(Expression, Expression)],
      elseValue: Option[Expression],
      exprMap: mutable.HashMap[PythonUDF, Seq[Expression]]): Unit = {
    updateConditionForUDFInBranch(branches, exprMap)
    elseValue.foreach { elseExpr =>
      updateConditionForUDFInElse(branches, elseExpr, exprMap)
    }
  }

  /**
   * Extracts the conditions associated with PythonUDFs.
   * Not all PythonUDFs need to be evaluated. For example, for a case when expression like
   * `when(x > 1, pyUDF(x)).when(x > 2, pyUDF2(x))`, we don't need to evaluate two PythonUDFs
   * for every row. Besides performance effect, under some cases, early evaluation of all
   * PythonUDFs can cause failure, e.g., a PythonUDF that should divide by an expression when
   * the value of expression is more than zero.
   *
   * Returns a map in which the value of a PythonUDF key is the sequence of boolean expressions
   * that are the requirement to run the PythonUDF.
   */
  private def extractConditionForUDF(
      expressions: Seq[Expression],
      udfs: Seq[PythonUDF]): mutable.HashMap[PythonUDF, Seq[Expression]] = {
    val conditionMap = mutable.HashMap[PythonUDF, Seq[Expression]]()
    expressions.map { expr =>
      expr.foreachUp {
        case e @ CaseWhenCodegen(branches, elseValue)
            if branches.exists(x => hasPythonUDF(x._2)) ||
              elseValue.map(hasPythonUDF).getOrElse(false) =>
          updateConditionForUDFInCaseWhen(branches, elseValue, conditionMap)
        case e @ CaseWhen(branches, elseValue)
            if branches.exists(x => hasPythonUDF(x._2)) ||
              elseValue.map(hasPythonUDF).getOrElse(false) =>
          updateConditionForUDFInCaseWhen(branches, elseValue, conditionMap)
        case If(predicate, trueValue, falseValue)
            if hasPythonUDF(trueValue) || hasPythonUDF(falseValue) =>
          pickUDFIntoMap(trueValue, predicate, conditionMap)
          pickUDFIntoMap(falseValue, Not(predicate), conditionMap)
        case _ =>
      }
    }
    conditionMap
  }

  /**
   * Extract all the PythonUDFs from the current operator and evaluate them before the operator.
   */
  private def extract(plan: SparkPlan): SparkPlan = {
    val udfs = plan.expressions.flatMap(collectEvaluatableUDF)
      // ignore the PythonUDF that come from second/third aggregate, which is not used
      .filter(udf => udf.references.subsetOf(plan.inputSet))
    if (udfs.isEmpty) {
      // If there aren't any, we are done.
      plan
    } else {
      val udfConditionMap = extractConditionForUDF(plan.expressions, udfs)

      val attributeMap = mutable.HashMap[PythonUDF, Expression]()
      val splitFilter = trySplitFilter(plan)
      // Rewrite the child that has the input required for the UDF
      val newChildren = splitFilter.children.map { child =>
        // Pick the UDF we are going to evaluate
        val validUdfs = udfs.filter { udf =>
          // Check to make sure that the UDF can be evaluated with only the input of this child.
          udf.references.subsetOf(child.outputSet)
        }
        // If any UDFs to evaluate are used with conditional expressions.
        val foundConditionalUdfs = validUdfs.exists(udfConditionMap.contains(_))

        if (validUdfs.nonEmpty) {
          if (validUdfs.exists(_.pythonUdfType == PythonUdfType.PANDAS_GROUPED_UDF)) {
            throw new IllegalArgumentException("Can not use grouped vectorized UDFs")
          }

          val resultAttrs = udfs.zipWithIndex.map { case (u, i) =>
            AttributeReference(s"pythonUDF$i", u.dataType)()
          }

          val evaluation = validUdfs.partition(_.pythonUdfType == PythonUdfType.PANDAS_UDF) match {
            case (vectorizedUdfs, plainUdfs) if plainUdfs.isEmpty =>
              ArrowEvalPythonExec(vectorizedUdfs, child.output ++ resultAttrs, child)
            case (vectorizedUdfs, plainUdfs) if vectorizedUdfs.isEmpty && !foundConditionalUdfs =>
              BatchEvalPythonExec(plainUdfs, child.output ++ resultAttrs, child)
            case (vectorizedUdfs, plainUdfs) if vectorizedUdfs.isEmpty =>
              BatchOptEvalPythonExec(plainUdfs, child.output ++ resultAttrs, child,
                udfConditionMap.toMap)
            case _ =>
              throw new IllegalArgumentException("Can not mix vectorized and non-vectorized UDFs")
          }

          attributeMap ++= validUdfs.zip(resultAttrs)
          evaluation
        } else {
          child
        }
      }
      // Other cases are disallowed as they are ambiguous or would require a cartesian
      // product.
      udfs.filterNot(attributeMap.contains).foreach { udf =>
        sys.error(s"Invalid PythonUDF $udf, requires attributes from more than one child.")
      }

      val rewritten = splitFilter.withNewChildren(newChildren).transformExpressions {
        case p: PythonUDF if attributeMap.contains(p) =>
          attributeMap(p)
      }

      // extract remaining python UDFs recursively
      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) {
        // Trim away the new UDF value if it was only used for filtering or something.
        ProjectExec(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }

  // Split the original FilterExec to two FilterExecs. Only push down the first few predicates
  // that are all deterministic.
  private def trySplitFilter(plan: SparkPlan): SparkPlan = {
    plan match {
      case filter: FilterExec =>
        val (candidates, containingNonDeterministic) =
          splitConjunctivePredicates(filter.condition).span(_.deterministic)
        val (pushDown, rest) = candidates.partition(!hasPythonUDF(_))
        if (pushDown.nonEmpty) {
          val newChild = FilterExec(pushDown.reduceLeft(And), filter.child)
          FilterExec((rest ++ containingNonDeterministic).reduceLeft(And), newChild)
        } else {
          filter
        }
      case o => o
    }
  }
}
