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

import org.apache.spark.sql.catalyst.expressions.PipeOperator

/**
 * Resolves [[PipeOperator]] node by resolving its child plan.
 */
class PipeOperatorResolver(resolver: Resolver)
    extends TreeNodeResolver[PipeOperator, PipeOperator] {

  /**
   * Resolve [[PipeOperator]] by resolving its child and returning a new [[PipeOperator]] with the
   * resolved child. For example, given the following query:
   *
   * {{{
   *   FROM VALUES (1, 2) AS t(col1, col2)
   *   |> SELECT col1, col2
   *   |> WHERE col1 > 0;
   * }}}
   *
   * The parsed plan looks like:
   *
   * {{{
   *   'Filter ('col1 > 0)
   *   +- 'PipeOperator
   *      +- 'Project ['col1, 'col2]
   *         +- 'SubqueryAlias t
   *            +- LocalRelation [col1#0, col2#1]
   * }}}
   *
   * After resolution, the [[PipeOperator]] is preserved with its resolved child:
   *
   * {{{
   *   Filter (col1#0 > 0)
   *   +- PipeOperator
   *      +- Project [col1#0, col2#1]
   *         +- SubqueryAlias t
   *            +- LocalRelation [col1#0, col2#1]
   * }}}
   */
  override def resolve(unresolvedPipeOperator: PipeOperator): PipeOperator = {
    val resolvedChild = resolver.resolve(unresolvedPipeOperator.child)
    unresolvedPipeOperator.copy(child = resolvedChild)
  }
}
