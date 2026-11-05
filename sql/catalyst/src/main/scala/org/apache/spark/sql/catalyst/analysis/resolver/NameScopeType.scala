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

/**
 * Indicates the type of a [[NameScope]] in the [[NameScopeStack]].
 *
 * - [[Intermediate]]: a regular scope used for operators within the same query block (e.g.,
 *   [[Project]], [[Filter]]). On pop, its state (hidden output, aliases) is propagated to the
 *   parent scope so that subsequent resolution steps can see the accumulated state.
 * - [[SubqueryRoot]]: the scope is the root of a subquery. Used by
 *   [[NameScopeStack.resolveMultipartName]] to detect the nearest outer scope for correlated
 *   reference resolution. Subquery root scopes do not propagate state upward on pop because
 *   the subquery is an independent query block whose internal state should not leak to the parent.
 * - [[Opaque]]: an opaque boundary that prevents outer reference resolution from crossing into
 *   the scope body. Used for views, UDFs, and procedures which should be resolved in isolation
 *   from the enclosing query context. Like [[SubqueryRoot]], opaque scopes do not propagate
 *   state upward on pop.
 */
sealed trait NameScopeType

object NameScopeType {
  case object Intermediate extends NameScopeType
  case object SubqueryRoot extends NameScopeType
  case object Opaque extends NameScopeType
}
