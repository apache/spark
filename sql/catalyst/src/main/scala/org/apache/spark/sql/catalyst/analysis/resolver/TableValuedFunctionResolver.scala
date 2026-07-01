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

import org.apache.spark.sql.catalyst.analysis.{FunctionResolution, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Resolver class that resolves [[UnresolvedTableValuedFunction]] operators.
 */
class TableValuedFunctionResolver(
    resolver: Resolver,
    expressionResolver: ExpressionResolver,
    functionResolution: FunctionResolution) {

  /**
   * Resolves [[UnresolvedTableValuedFunction]] operator by replacing it with an arbitrary
   * [[LogicalPlan]].
   *
   * The resolution happens in several steps:
   *  - Resolve all the function arguments using [[ExpressionResolver]].
   *  - Resolve the function itself using [[FunctionResolution]] getting a [[LogicalPlan]].
   *  - Recursively resolve the resulting [[LogicalPlan]].
   *
   * Examples:
   *  - `range(...)` resolves to [[Range]] operator.
   *  {{{
   *  SELECT * FROM range(10)
   *
   *  == Parsed Logical Plan ==
   *  'Project [*]
   *  +- 'UnresolvedTableValuedFunction [range], [10], false
   *
   *  == Analyzed Logical Plan ==
   *  id: bigint
   *  Project [id#0]
   *  +- Range (0, 10, step=1)
   *  }}}
   *
   *  - `explode(...)` resolves to [[Generate]] operator with [[OneRowRelation]].
   *  {{{
   *  SELECT * FROM explode(array(1, 2, 3))
   *
   *  == Parsed Logical Plan ==
   *  'Project [*]
   *  +- 'UnresolvedTableValuedFunction [explode], ['array(1, 2, 3)], false
   *
   *  == Analyzed Logical Plan ==
   *  col: int
   *  Project [col#0]
   *  +- Generate explode(array(1, 2, 3)), false, [col#0]
   *     +- OneRowRelation
   *  }}}
   */
  def resolve(unresolvedTVF: UnresolvedTableValuedFunction): LogicalPlan = {
    val resolvedArguments = unresolvedTVF.functionArgs.map(
      expressionResolver.resolveExpressionTreeInOperator(_, unresolvedTVF)
    )
    val unresolvedTVFWithResolvedArguments =
      unresolvedTVF.copy(functionArgs = resolvedArguments)
    val resolvedTVF =
      functionResolution.resolveTableValuedFunction(unresolvedTVFWithResolvedArguments)
    resolver.resolve(resolvedTVF)
  }
}
