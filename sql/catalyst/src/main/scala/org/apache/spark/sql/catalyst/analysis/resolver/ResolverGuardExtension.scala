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

import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Result of a [[ResolverGuardExtension.checkOperator]] call. */
sealed trait GuardExtensionResult

object GuardExtensionResult {

  /** The operator is supported by the single-pass analyzer. */
  case object Supported extends GuardExtensionResult

  /** The operator is not supported by the single-pass analyzer. */
  case object NotSupported extends GuardExtensionResult
}

/**
 * Extension interface for the resolver guard. Allows external code to register additional
 * operator guard checks without modifying [[ResolverGuard]] directly.
 */
trait ResolverGuardExtension {

  /** Check whether the given operator is supported by the single-pass analyzer. */
  def checkOperator(
      operator: LogicalPlan,
      tracker: Option[QueryPlanningTracker]): GuardExtensionResult
}
