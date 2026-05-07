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

import java.util.{ArrayDeque, HashMap}

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}

/**
 * The [[SubqueryScope]] is managed through the whole resolution process of a given
 * [[SubqueryExpression]] plan.
 *
 * The reason why we need this scope is that:
 * 1. [[AggregateExpression]]s with [[OuterReference]]s are handled in a special way.
 * Consider this query:
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
 * operator below the `HAVING` clause. We need to pull it up outside of the subquery, and insert it
 * in the [[Aggregate]] operator. So the resolution order is as follows:
 *  - Resolve "t1.col2" to an [[OuterReference]] in [[ExpressionResolver.resolveAttribute]];
 *  - Resolve the [[AggregateExpression]] in [[AggregateExpressionResolver.resolve]];
 *  - Detect an outer reference below the aggregate expression, cut the whole subtree with outer
 *    references stripped away and:
 *      - if the expression already exists in this [[SubqueryScope]] replace it with an
 *      [[OuterReference]] to an already extracted expression;
 *      - if the expression doesn't exist, alias it, insert it in this [[SubqueryScope]] and
 *      replace the aggregate expression with an [[OuterReference]] to the [[AttributeReference]]
 *      to the newly created alias.
 *  - Finally, the resolution of the `HAVING` clause can insert the missing aggregate expression
 *    into the lower [[Aggregate]] operator. During this process we must call
 *    [[ExpressionIdAssigner.mapExpression]] on the new alias, because this auto-generated alias
 *    is new to the query plan, so that [[ExpressionIdAssigner]] remembers it.
 *
 * 2. We need to track the outer scope levels of outer references for supporting nested correlated
 *    subqueries. Consider this query:
 * {{{
 *    SELECT *
 *    FROM t1
 *    WHERE t1.a = (
 *      SELECT MAX(t2.a)
 *      FROM t2
 *      WHERE t2.a = (
 *        SELECT MAX(t3.a)
 *        FROM t3
 *        WHERE t3.b > t2.b AND t3.c > t1.c
 *   ) AND t2.b > t1.b
 * }}}
 *   We get the [[outerScopeLevel]] map for the outer references during attributes resolution
 *   which indicates how many subquery levels we need to go up to find the attribute reference.
 *   We need the outerScopeLevel map when resolving the subquery expressions, where we collect
 *   the outer references and wrap the ones cannot be resolved in the current subquery scope or
 *   its parent scopes with [[OuterScopeReference]]. More details can be found in
 *   `SubqueryExpressionResolver.processOuterReferences`.
 *
 * Notes:
 *  - Spark only supports outer aggregates in the subqueries inside `HAVING`;
 *  - The subtree under a given [[AggregateExpression]] can be arbitrary, but must contain either
 *    local or outer references, the mixed set is disallowed.
 *  - We can have several subquery expressions in HAVING clause, that's why we append outer
 *    aggregate expressions from lower scopes in [[mergeChildScope]].
 *  - Spark does not support outer aggregates in the subqueries inside `HAVING` with
 *    nested correlations. E.g.:
 *    SELECT max(col1) FROM t1 GROUP BY col1 HAVING (
 *      SELECT * FROM t2 WHERE t2.col2 > (
 *        SELECT max(t3.col3) FROM t3 WHERE t3.col3 < max(t1.col1))
 *    is not supported.
 *
 * @param aggregateExpressionsExtractor [[GroupingAndAggregateExpressionsExtractor]] object defined
 *   in case the outer parent operator is a partially resolved HAVING. Used to extract the
 *   aggregate expressions from the outer [[Aggregate]] operator.
 */
class SubqueryScope(
    val aggregateExpressionsExtractor: Option[GroupingAndAggregateExpressionsExtractor] = None,
    private val outerOperatorResolutionContext: OperatorResolutionContext =
      new OperatorResolutionContext,
    private val outerReferenceScopeLevels: HashMap[ExprId, Int] = new HashMap[ExprId, Int]) {

  /**
   * Adds an aliased aggregate expression from currently resolved subquery scope to the outer
   * operator's context in order for it to be pushed down to the outer [[Aggregate]].
   */
  def addExtractedAggregateExpression(alias: Alias): Unit = {
    outerOperatorResolutionContext.addSubqueryAggregateExpression(alias)
  }

  /**
   * For a given aggregate expression from current subquery scopes returns an [[Alias]] of a
   * semantically equivalent aggregate expression that was already extracted from another subquery,
   * if such exists.
   */
  def getExtractedAggregateExpression(expression: Expression): Option[Alias] = {
    outerOperatorResolutionContext.getSubqueryAggregateExpression(expression)
  }

  /**
   * Records the scope level of an outer reference expression. If the scope level is 1, it means
   * the outer reference is referencing the attributes from the parent scope of current subquery
   * scope. If the scope level is beyond 1, it means the current subquery has nested correlations.
   */
  def addOuterReferenceScopeLevel(exprId: ExprId, level: Int): Unit = {
    outerReferenceScopeLevels.put(exprId, level)
  }

  /**
   * Returns the scope level of an outer reference expression if it exists. If the scope level is 1,
   * it means the outer reference is referencing the attributes from the parent scope of current
   * subquery scope. If the scope level is beyond 1, it means the current subquery has nested
   * correlations.
   */
  def getOuterReferenceScopeLevel(exprId: ExprId): Option[Int] = {
    Option(outerReferenceScopeLevels.get(exprId))
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
      autoGeneratedAliasProvider: AutoGeneratedAliasProvider,
      outerOperatorResolutionContext: OperatorResolutionContext): Unit = {
    val groupingAndAggregateExpressionsExtractor = parentOperator match {
      case Filter(_, aggregate: Aggregate) =>
        Some(new GroupingAndAggregateExpressionsExtractor(aggregate, autoGeneratedAliasProvider))
      case other => None
    }

    stack.push(
      new SubqueryScope(
        aggregateExpressionsExtractor = groupingAndAggregateExpressionsExtractor,
        outerOperatorResolutionContext = outerOperatorResolutionContext
      )
    )
  }

  /*
   * Pop the current scope from the stack and merge it with the parent scope.
   */
  def popScope(): Unit = {
    stack.pop()
  }
}
