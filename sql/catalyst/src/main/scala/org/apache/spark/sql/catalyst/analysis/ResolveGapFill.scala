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

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedGapFill}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_GAP_FILL
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Rejects [[UnresolvedGapFill]] until the operator is implemented.
 *
 * When `spark.sql.gapFillRelationOperator.enabled` is off, it fails at once, so a disabled
 * operator is reported before any error in its input. When it is on, it waits for the child to
 * resolve, so an error in the child (such as a missing table) is still reported first.
 */
object ResolveGapFill extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(UNRESOLVED_GAP_FILL), ruleId) {
    case g: UnresolvedGapFill if !conf.getConf(SQLConf.GAP_FILL_ENABLED) || g.childrenResolved =>
      throw QueryCompilationErrors.gapFillNotSupportedError()
  }
}
