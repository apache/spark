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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * The [[ResolverExtension]] is a main interface for single-pass analysis extensions in Catalyst.
 * External code that needs specific node types to be resolved has to implement this trait and
 * inject the implementation into the [[Analyzer.singlePassResolverExtensions]].
 */
trait ResolverExtension {

  /**
   * Resolve the operator if it's supported by this extension. This method is called by the
   * single-pass [[Resolver]] on all the configured extensions when it exhausted its match list
   * for the known node types.
   *
   *  - The implementation can rely on children being resolved.
   *  - The implementation can introduce new unresolved subtrees, but has to invoke `resolver` on
   *    them.
   */
  def resolveOperator(
      operator: LogicalPlan,
      resolver: TreeNodeResolver[LogicalPlan, LogicalPlan]): Option[LogicalPlan]
}
