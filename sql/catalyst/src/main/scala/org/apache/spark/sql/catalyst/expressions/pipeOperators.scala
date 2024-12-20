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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Represents an expression when used with a SQL pipe operator.
 * We use this to check invariants about whether aggregate functions may exist in these expressions.
 * @param child The child expression.
 * @param isAggregate Whether the pipe operator is |> AGGREGATE.
 *                    If true, the child expression must contain at least one aggregate function.
 *                    If false, the child expression must not contain any aggregate functions.
 * @param clause The clause of the pipe operator. This is used to generate error messages.
 */
case class PipeExpression(child: Expression, isAggregate: Boolean, clause: String)
  extends UnaryExpression with RuntimeReplaceable {
  override def withNewChildInternal(newChild: Expression): Expression =
    PipeExpression(newChild, isAggregate, clause)
  override lazy val replacement: Expression = {
    val firstAggregateFunction: Option[AggregateFunction] = findFirstAggregate(child)
    if (isAggregate && firstAggregateFunction.isEmpty) {
      throw QueryCompilationErrors.pipeOperatorAggregateExpressionContainsNoAggregateFunction(child)
    } else if (!isAggregate) {
      firstAggregateFunction.foreach { a =>
        throw QueryCompilationErrors.pipeOperatorContainsAggregateFunction(a, clause)
      }
    }
    child
  }

  /** Returns the first aggregate function in the given expression, or None if not found. */
  private def findFirstAggregate(e: Expression): Option[AggregateFunction] = e match {
    case a: AggregateFunction =>
      Some(a)
    case _: WindowExpression =>
      // Window functions are allowed in these pipe operators, so do not traverse into children.
      None
    case _ =>
      e.children.flatMap(findFirstAggregate).headOption
  }
}

object PipeOperators {
  // These are definitions of query result clauses that can be used with the pipe operator.
  val aggregateClause = "AGGREGATE"
  val clusterByClause = "CLUSTER BY"
  val distributeByClause = "DISTRIBUTE BY"
  val extendClause = "EXTEND"
  val limitClause = "LIMIT"
  val offsetClause = "OFFSET"
  val orderByClause = "ORDER BY"
  val selectClause = "SELECT"
  val setClause = "SET"
  val sortByClause = "SORT BY"
  val sortByDistributeByClause = "SORT BY ... DISTRIBUTE BY ..."
  val windowClause = "WINDOW"
}
