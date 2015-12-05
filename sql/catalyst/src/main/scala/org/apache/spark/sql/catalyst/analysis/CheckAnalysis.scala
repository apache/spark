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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * Throws user facing errors when passed invalid queries that fail to analyze.
 */
trait CheckAnalysis {

  /**
   * Override to provide additional checks for correct analysis.
   * These rules will be evaluated after our built-in check rules.
   */
  val extendedCheckRules: Seq[LogicalPlan => Unit] = Nil

  protected def failAnalysis(msg: String): Nothing = {
    throw new AnalysisException(msg)
  }

  protected def containsMultipleGenerators(exprs: Seq[Expression]): Boolean = {
    exprs.flatMap(_.collect {
      case e: Generator => e
    }).length > 1
  }

  def checkAnalysis(plan: LogicalPlan): Unit = {
    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    plan.foreachUp {
      case p if p.analyzed => // Skip already analyzed sub-plans

      case operator: LogicalPlan =>
        operator transformExpressionsUp {
          case a: Attribute if !a.resolved =>
            val from = operator.inputSet.map(_.name).mkString(", ")
            a.failAnalysis(s"cannot resolve '${a.prettyString}' given input columns $from")

          case e: Expression if e.checkInputDataTypes().isFailure =>
            e.checkInputDataTypes() match {
              case TypeCheckResult.TypeCheckFailure(message) =>
                e.failAnalysis(
                  s"cannot resolve '${e.prettyString}' due to data type mismatch: $message")
            }

          case c: Cast if !c.resolved =>
            failAnalysis(
              s"invalid cast from ${c.child.dataType.simpleString} to ${c.dataType.simpleString}")

          case WindowExpression(UnresolvedWindowFunction(name, _), _) =>
            failAnalysis(
              s"Could not resolve window function '$name'. " +
              "Note that, using window functions currently requires a HiveContext")

          case w @ WindowExpression(windowFunction, windowSpec) if windowSpec.validate.nonEmpty =>
            // The window spec is not valid.
            val reason = windowSpec.validate.get
            failAnalysis(s"Window specification $windowSpec is not valid because $reason")
        }

        operator match {
          case f: Filter if f.condition.dataType != BooleanType =>
            failAnalysis(
              s"filter expression '${f.condition.prettyString}' " +
                s"of type ${f.condition.dataType.simpleString} is not a boolean.")

          case j @ Join(_, _, _, Some(condition)) if condition.dataType != BooleanType =>
            failAnalysis(
              s"join condition '${condition.prettyString}' " +
                s"of type ${condition.dataType.simpleString} is not a boolean.")

          case j @ Join(_, _, _, Some(condition)) =>
            def checkValidJoinConditionExprs(expr: Expression): Unit = expr match {
              case p: Predicate =>
                p.asInstanceOf[Expression].children.foreach(checkValidJoinConditionExprs)
              case e if e.dataType.isInstanceOf[BinaryType] =>
                failAnalysis(s"binary type expression ${e.prettyString} cannot be used " +
                  "in join conditions")
              case e if e.dataType.isInstanceOf[MapType] =>
                failAnalysis(s"map type expression ${e.prettyString} cannot be used " +
                  "in join conditions")
              case _ => // OK
            }

            checkValidJoinConditionExprs(condition)

          case Aggregate(groupingExprs, aggregateExprs, child) =>
            def checkValidAggregateExpression(expr: Expression): Unit = expr match {
              case _: AggregateExpression => // OK
              case e: Attribute if !groupingExprs.exists(_.semanticEquals(e)) =>
                failAnalysis(
                  s"expression '${e.prettyString}' is neither present in the group by, " +
                    s"nor is it an aggregate function. " +
                    "Add to group by or wrap in first() if you don't care which value you get.")
              case e if groupingExprs.exists(_.semanticEquals(e)) => // OK
              case e if e.references.isEmpty => // OK
              case e => e.children.foreach(checkValidAggregateExpression)
            }

            def checkValidGroupingExprs(expr: Expression): Unit = expr.dataType match {
              case BinaryType =>
                failAnalysis(s"binary type expression ${expr.prettyString} cannot be used " +
                  "in grouping expression")
              case m: MapType =>
                failAnalysis(s"map type expression ${expr.prettyString} cannot be used " +
                  "in grouping expression")
              case _ => // OK
            }

            aggregateExprs.foreach(checkValidAggregateExpression)
            groupingExprs.foreach(checkValidGroupingExprs)

          case Sort(orders, _, _) =>
            orders.foreach { order =>
              if (!RowOrdering.isOrderable(order.dataType)) {
                failAnalysis(
                  s"sorting is not supported for columns of type ${order.dataType.simpleString}")
              }
            }

          case s @ SetOperation(left, right) if left.output.length != right.output.length =>
            failAnalysis(
              s"${s.nodeName} can only be performed on tables with the same number of columns, " +
               s"but the left table has ${left.output.length} columns and the right has " +
               s"${right.output.length}")

          case _ => // Fallbacks to the following checks
        }

        operator match {
          case o if o.children.nonEmpty && o.missingInput.nonEmpty =>
            val missingAttributes = o.missingInput.mkString(",")
            val input = o.inputSet.mkString(",")

            failAnalysis(
              s"resolved attribute(s) $missingAttributes missing from $input " +
                s"in operator ${operator.simpleString}")

          case p @ Project(exprs, _) if containsMultipleGenerators(exprs) =>
            failAnalysis(
              s"""Only a single table generating function is allowed in a SELECT clause, found:
                 | ${exprs.map(_.prettyString).mkString(",")}""".stripMargin)

          // Special handling for cases when self-join introduce duplicate expression ids.
          case j @ Join(left, right, _, _) if left.outputSet.intersect(right.outputSet).nonEmpty =>
            val conflictingAttributes = left.outputSet.intersect(right.outputSet)
            failAnalysis(
              s"""
                 |Failure when resolving conflicting references in Join:
                 |$plan
                 |Conflicting attributes: ${conflictingAttributes.mkString(",")}
                 |""".stripMargin)

          case o if !o.resolved =>
            failAnalysis(
              s"unresolved operator ${operator.simpleString}")

          case o if o.expressions.exists(!_.deterministic) &&
            !o.isInstanceOf[Project] && !o.isInstanceOf[Filter] =>
            failAnalysis(
              s"""nondeterministic expressions are only allowed in Project or Filter, found:
                 | ${o.expressions.map(_.prettyString).mkString(",")}
                 |in operator ${operator.simpleString}
             """.stripMargin)

          case _ => // Analysis successful!
        }
    }
    extendedCheckRules.foreach(_(plan))

    plan.foreach(_.setAnalyzed())
  }
}
