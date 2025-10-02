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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.errors.QueryCompilationErrors

object UnsupportedExpressionInOperatorValidation {

  /**
   * Check that `expression` is allowed to exist in `operator`'s expression tree. Here, we avoid
   * validating [[AggregateExpression]]s when they are under a [[Sort]] or [[Filter]] on top of
   * [[Aggregate]] as the transformation to attributes is delayed to specific resolvers (if
   * needed). Example:
   *
   * {{{ SELECT SUM(col1) + 1 FROM VALUES(1) ORDER BY SUM(col1) + 1 }}}
   *
   * In this case, ordering [[AggregateExpression]] in `SUM(col1) + 1` should be allowed here
   * as its parent expression `SUM(col1) + 1` will be transformed to attribute later in
   * [[GroupingAndAggregateExpressionsExtractor.extractReferencedGroupingAndAggregateExpressions]]
   * and thus at the end of [[Sort]] resolution we won't have any [[AggregateExpression]]s in its
   * ordering expression tree. On the other side, here is a query that should fail:
   *
   * {{{ SELECT col1 FROM VALUES(1) ORDER BY MAX(col1) }}}
   *
   * `MAX(col1)` can't be extracted as the child of the [[Sort]] is [[Project]] and thus we fail
   * during validation in [[Resolver.validateResolvedOperatorGenerically]].
   */
  def isExpressionInUnsupportedOperator(
      expression: Expression,
      operator: LogicalPlan,
      isSortOnTopOfAggregate: Boolean,
      isFilterOnTopOfAggregate: Boolean): Boolean = {
    expression match {
      case _: WindowExpression => operator.isInstanceOf[Window]
      case _: AggregateExpression =>
        !(operator.isInstanceOf[Project] ||
        operator.isInstanceOf[Aggregate] ||
        operator.isInstanceOf[Window] ||
        operator.isInstanceOf[CollectMetrics] ||
        isSortOnTopOfAggregate ||
        isFilterOnTopOfAggregate ||
        onlyInLateralSubquery(operator))
      case _: Generator =>
        !(operator.isInstanceOf[Generate] ||
        operator.isInstanceOf[BaseEvalPythonUDTF])
      case _ =>
        false
    }
  }

  private def onlyInLateralSubquery(operator: LogicalPlan): Boolean = {
    operator.isInstanceOf[LateralJoin] && {
      // TODO: check if we are resolving a lateral join condition once lateral join is supported.
      throw QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature(
        s"${operator.getClass} operator resolution"
      )
    }
  }
}
