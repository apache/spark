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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{EvalColumnarUDFs, LogicalPlan, Project, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COLUMNAR_UDF

/**
 * Extracts the calls of columnar user-defined functions from the operators and evaluates them
 * in an [[EvalColumnarUDFs]] below the operator, as `ExtractPythonUDFs` does for Python UDFs.
 *
 * The calls of one operator that need no other call share an [[EvalColumnarUDFs]]. A call whose
 * argument is another call gets its own, above that of its argument. An argument that is not
 * an attribute is computed by a [[Project]] below, so that the arguments of the calls in an
 * [[EvalColumnarUDFs]] are all columns of its child.
 */
object ExtractColumnarUDFs extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery is rewritten into a join later, and goes through this rule then.
    case s: Subquery if s.correlated => plan

    case _ => plan.transformUpWithPruning(_.containsPattern(COLUMNAR_UDF)) {
      case e: EvalColumnarUDFs => e
      case p: LogicalPlan => extract(p)
    }
  }

  private def hasColumnarUDF(e: Expression): Boolean = e.containsPattern(COLUMNAR_UDF)

  /** The calls that can be evaluated now: those whose arguments contain no other call. */
  private def collectEvaluableUDFs(e: Expression): Seq[ColumnarUDF] = e match {
    case udf: ColumnarUDF if !udf.children.exists(hasColumnarUDF) =>
      // A call over an aggregate or a window function cannot move below its operator.
      val overAggregate = udf.exists { e =>
        e.isInstanceOf[AggregateExpression] || e.isInstanceOf[WindowExpression]
      }
      if (overAggregate) {
        Nil
      } else {
        Seq(udf)
      }
    case other => other.children.flatMap(collectEvaluableUDFs)
  }

  private def canonicalizeDeterministic(u: ColumnarUDF): ColumnarUDF = {
    if (u.deterministic) u.canonicalized.asInstanceOf[ColumnarUDF] else u
  }

  private def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = ExpressionSet(plan.expressions.flatMap(collectEvaluableUDFs))
      .filter(_.references.subsetOf(plan.inputSet))
      .toSeq.asInstanceOf[Seq[ColumnarUDF]]
    if (udfs.isEmpty) {
      plan
    } else {
      val attributeMap = mutable.HashMap[ColumnarUDF, Expression]()
      val newChildren = plan.children.map { child =>
        val validUdfs = udfs.filter(_.references.subsetOf(child.outputSet))
        if (validUdfs.nonEmpty) {
          // The arguments that are not attributes of the child are computed below.
          val argAliases = mutable.LinkedHashMap[Expression, Alias]()
          val udfsOverColumns = validUdfs.map { udf =>
            val newArgs = udf.children.map {
              case a: Attribute => a
              case arg =>
                argAliases.getOrElseUpdate(
                  arg.canonicalized, Alias(arg, s"columnarUDFArg${argAliases.size}")()).toAttribute
            }
            udf.withNewChildren(newArgs).asInstanceOf[ColumnarUDF]
          }
          val input = if (argAliases.isEmpty) {
            child
          } else {
            Project(child.output ++ argAliases.values, child)
          }
          val resultAttrs = validUdfs.zipWithIndex.map { case (u, i) =>
            AttributeReference(s"columnarUDF$i", u.dataType, u.nullable)()
          }
          attributeMap ++= validUdfs.map(canonicalizeDeterministic).zip(resultAttrs)
          val evaluation = EvalColumnarUDFs(udfsOverColumns, resultAttrs, input)
          if (argAliases.isEmpty) {
            evaluation
          } else {
            // Drop the columns of the arguments.
            Project(child.output ++ resultAttrs, evaluation)
          }
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
