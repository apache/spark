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

import java.util.ArrayDeque

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

/**
 * Tracks resolved aggregate context within a single query block (a SELECT block). Sibling
 * operators of the same query block - HAVING and ORDER BY - read this scope to locate the
 * [[Aggregate]] resolved by the SELECT and look up grouping-expression aliases.
 *
 * For example, `SELECT a AS b FROM t GROUP BY a ORDER BY a` records the base aggregate so that
 * the ORDER BY can be resolved against it:
 *
 * {{{
 * Sort [b ASC NULLS FIRST]
 * +- Aggregate [a], [a AS b]
 * }}}
 *
 * Lifetime is bound to the query block. [[QueryBlockScopeStack]] pushes a fresh scope at every
 * query-block boundary - per-CTE definition, [[SubqueryAlias]], [[View]] body, set-operation
 * child, join child, scalar subquery - and pops it when the boundary closes.
 */
class QueryBlockScope {

  /**
   * The [[Aggregate]] resolved inside the current query block. Either the resolved [[Aggregate]]
   * itself or the base [[Aggregate]] constructed when resolving lateral column references.
   */
  private var baseAggregate: Option[Aggregate] = None

  /**
   * Aliases present in the [[Aggregate]] of this query block. If the [[Aggregate]] has lateral
   * column references, this list contains both the aggregate aliases and all aliases from
   * artificially inserted [[Project]] nodes.
   */
  private var aggregateListAliases: Seq[Alias] = Seq.empty

  def getBaseAggregate: Option[Aggregate] = baseAggregate

  def getAggregateListAliases: Seq[Alias] = aggregateListAliases

  /**
   * Register the resolved [[Aggregate]] of this query block with its top-level aliases.
   */
  def registerBaseAggregate(aggregate: Aggregate, aliases: Seq[Alias] = Seq.empty): Unit = {
    baseAggregate = Some(aggregate)
    aggregateListAliases = aliases
  }

  /** Clear the registered [[Aggregate]] for this query block. */
  def clearAggregateState(): Unit = {
    baseAggregate = None
    aggregateListAliases = Seq.empty
  }
}

/**
 * Stack of [[QueryBlockScope]]s that mirrors the query-block nesting of the resolved plan. Pushed
 * at query-block boundaries (per-CTE definition, SubqueryAlias, View body, set-op child, join
 * child, scalar subquery) and popped when the boundary closes. The constructor seeds the stack
 * with a root scope corresponding to the top-level query.
 */
class QueryBlockScopeStack {
  private val stack = new ArrayDeque[QueryBlockScope]
  stack.push(new QueryBlockScope)

  /**
   * Return the innermost [[QueryBlockScope]].
   */
  def current: QueryBlockScope = {
    if (stack.isEmpty) {
      throw SparkException.internalError("No current QueryBlockScope")
    }
    stack.peek()
  }

  /**
   * Push a fresh [[QueryBlockScope]] onto the stack when entering a new query block.
   */
  def push(): Unit = {
    stack.push(new QueryBlockScope)
  }

  /**
   * Pop the innermost [[QueryBlockScope]] when leaving a query block. The root scope (created by
   * the constructor) cannot be popped.
   */
  def pop(): QueryBlockScope = {
    if (stack.size <= 1) {
      throw SparkException.internalError("Cannot pop the root QueryBlockScope")
    }
    stack.pop()
  }
}
