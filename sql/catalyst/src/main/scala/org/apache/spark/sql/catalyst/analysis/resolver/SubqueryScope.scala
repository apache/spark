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

import java.util.{ArrayDeque, ArrayList}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}

/**
 * The [[SubqueryScope]] is managed through the whole resolution process of a given
 * [[SubqueryExpression]] plan.
 *
 * The reason why we need this scope is that [[AggregateExpression]]s with [[OuterReference]]s are
 * handled in a special way. Consider this query:
 *
 * {{{
 * -- t1.col2 is an outer reference
 * SELECT col1 FROM VALUES (1, 2) t1 GROUP BY col1 HAVING (
 *   SELECT * FROM VALUES (1, 2) t2 WHERE t2.col2 == MAX(t1.col2)
 * )
 * }}}
 *
 * During the [[Exists]] resolution inside the HAVING clause we encounter "t1.col2" name, which is
 * resolved to an [[OuterReference]]. There's an [[AggregateExpression]] on top of it. This whole
 * expression is not local to the subquery, and thus it belongs to an outer [[Aggregate]]
 * operator below the `HAVING` clause. We need top pull it up outside of the subquery, and insert
 * it in the [[Aggregate]] operator. So the resolution order is as follows:
 *  - Resolve "t1.col2" to an [[OuterReference]] in [[ExpressionResolver.resolveAttribute]];
 *  - Resolve the [[AggregateExpression]] in [[AggregateExpressionResolver.resolve]];
 *  - Detect an outer reference below the aggregate expression, cut the whole subtree with outer
 *    references stripped away, alias it and insert it in this [[SubqueryScope]];
 *  - Replace the aggregate expression with an [[OuterReference]] to the [[AttributeReference]] from
 *    that artificial [[Alias]];
 *  - When the resolution of the [[SubqueryExpression]] is finished,
 *    [[SubqueryRegistry.popScope]] merges the lower scope to the upper one, and all the
 *    outer aggregate expression references are appended to the common
 *    [[lowerAliasedOuterAggregateExpressions]] list.
 *  - Finally, the resolution of the `HAVING` clause can insert the missing aggregate expression
 *    into the lower [[Aggregate]] operator. During this process we must call
 *    [[ExpressionIdAssigner.mapExpression]] on the new alias, because this auto-generated alias
 *    is new to the query plan, so that [[ExpressionIdAssigner]] remembers it.
 *
 * Notes:
 *  - Spark only supports outer aggregates in the subqueries inside `HAVING`;
 *  - The subtree under a given [[AggregateExpression]] can be arbitrary, but must contain either
 *    local or outer references, the mixed set is disallowed.
 *  - We can have several subquery expressions in HAVING clause, that's why we append outer
 *    aggregate expressions from lower scopes in [[mergeChildScope]].
 *
 * @param aggregateExpressionsExtractor [[GroupingAndAggregateExpressionsExtractor]] object defined
 *   in case the outer parent operator is a partially resolved HAVING. Used to extract the
 *   aggregate expressions from the outer [[Aggregate]] operator.
 */
class SubqueryScope(
    val aggregateExpressionsExtractor: Option[GroupingAndAggregateExpressionsExtractor] = None) {
  private val outerAliasedAggregateExpressions = new ArrayList[Alias]
  private val lowerAliasedOuterAggregateExpressions = new ArrayList[Alias]

  /**
   * Add an `alias` for the outer aggregate expression. [[Alias]] is either fetched from the outer
   * [[Aggregate]] or auto-generated and will be later inserted into the outer [[Aggregate]]
   * operator.
   */
  def addAliasForOuterAggregateExpression(alias: Alias): Unit = {
    outerAliasedAggregateExpressions.add(alias)
  }

  /**
   * Get the outer aggregate expression aliased from the lower subquery scope.
   */
  def getLowerOuterAggregateExpressionAliases: Seq[Alias] = {
    lowerAliasedOuterAggregateExpressions.asScala.toSeq
  }

  /**
   * Merge `childScope` by extending our `lowerAliasedOuterAggregateExpressions` with
   * `childScope.outerAliasedAggregateExpressions`.
   */
  def mergeChildScope(childScope: SubqueryScope): Unit = {
    lowerAliasedOuterAggregateExpressions.addAll(childScope.outerAliasedAggregateExpressions)
  }
}

/**
 * The [[SubqueryRegistry]] manages the stack of [[SubqueryScope]]s during the resolution of
 * the whole SQL query. Every new [[SubqueryScope]] has its own isolated scope.
 */
class SubqueryRegistry {
  private val stack = new ArrayDeque[SubqueryScope]
  stack.push(new SubqueryScope)

  /**
   * Get the current [[SubqueryScope]].
   */
  def currentScope: SubqueryScope = stack.peek()

  /**
   * Push a new scope to the stack. This is used by the [[SubqueryExpressionResolver]]
   * to create a new scope for each [[SubqueryExpression]].
   */
  def pushScope(
      parentOperator: LogicalPlan,
      autoGeneratedAliasProvider: AutoGeneratedAliasProvider): Unit = {
    val groupingAndAggregateExpressionsExtractor = parentOperator match {
      case Filter(_, aggregate: Aggregate) =>
        Some(new GroupingAndAggregateExpressionsExtractor(aggregate, autoGeneratedAliasProvider))
      case other => None
    }

    stack.push(
      new SubqueryScope(
        aggregateExpressionsExtractor = groupingAndAggregateExpressionsExtractor
      )
    )
  }

  /*
   * Pop the current scope from the stack and merge it with the parent scope.
   */
  def popScope(): Unit = {
    val childScope = stack.pop()
    currentScope.mergeChildScope(childScope)
  }
}
