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
  self: Analyzer =>

  /**
   * Override to provide additional checks for correct analysis.
   * These rules will be evaluated after our built-in check rules.
   */
  val extendedCheckRules: Seq[LogicalPlan => Unit] = Nil

  protected def failAnalysis(msg: String): Nothing = {
    throw new AnalysisException(msg)
  }

  def containsMultipleGenerators(exprs: Seq[Expression]): Boolean = {
    exprs.flatMap(_.collect {
      case e: Generator => true
    }).length >= 1
  }

  def checkAnalysis(plan: LogicalPlan): Unit = {
    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    plan.foreachUp {
      case operator: LogicalPlan =>
        operator transformExpressionsUp {
          case a: Attribute if !a.resolved =>
            if (operator.childrenResolved) {
              a match {
                case UnresolvedAttribute(nameParts) =>
                  // Throw errors for specific problems with get field.
                  operator.resolveChildren(nameParts, resolver, throwErrors = true)
              }
            }

            val from = operator.inputSet.map(_.name).mkString(", ")
            a.failAnalysis(s"cannot resolve '${a.prettyString}' given input columns $from")

          case c: Cast if !c.resolved =>
            failAnalysis(
              s"invalid cast from ${c.child.dataType.simpleString} to ${c.dataType.simpleString}")

          case b: BinaryExpression if !b.resolved =>
            failAnalysis(
              s"invalid expression ${b.prettyString} " +
                s"between ${b.left.simpleString} and ${b.right.simpleString}")
        }

        operator match {
          case f: Filter if f.condition.dataType != BooleanType =>
            failAnalysis(
              s"filter expression '${f.condition.prettyString}' " +
                s"of type ${f.condition.dataType.simpleString} is not a boolean.")

          case Aggregate(groupingExprs, aggregateExprs, child) =>
            def checkValidAggregateExpression(expr: Expression): Unit = expr match {
              case _: AggregateExpression => // OK
              case e: Attribute if !groupingExprs.contains(e) =>
                failAnalysis(
                  s"expression '${e.prettyString}' is neither present in the group by, " +
                    s"nor is it an aggregate function. " +
                    "Add to group by or wrap in first() if you don't care which value you get.")
              case e if groupingExprs.contains(e) => // OK
              case e if e.references.isEmpty => // OK
              case e => e.children.foreach(checkValidAggregateExpression)
            }

            val cleaned = aggregateExprs.map(_.transform {
              // Should trim aliases around `GetField`s. These aliases are introduced while
              // resolving struct field accesses, because `GetField` is not a `NamedExpression`.
              // (Should we just turn `GetField` into a `NamedExpression`?)
              case Alias(g, _) => g
            })

            cleaned.foreach(checkValidAggregateExpression)

          case _ => // Fallbacks to the following checks
        }

        operator match {
          case o if o.children.nonEmpty && o.missingInput.nonEmpty =>
            val missingAttributes = o.missingInput.mkString(",")
            val input = o.inputSet.mkString(",")

            failAnalysis(
              s"resolved attribute(s) $missingAttributes missing from $input " +
                s"in operator ${operator.simpleString}")

          case o if !o.resolved =>
            failAnalysis(
              s"unresolved operator ${operator.simpleString}")

          case p @ Project(exprs, _) if containsMultipleGenerators(exprs) =>
            failAnalysis(
              s"""Only a single table generating function is allowed in a SELECT clause, found:
                 | ${exprs.map(_.prettyString).mkString(",")}""".stripMargin)


          case _ => // Analysis successful!
        }
    }
    extendedCheckRules.foreach(_(plan))
  }
}
