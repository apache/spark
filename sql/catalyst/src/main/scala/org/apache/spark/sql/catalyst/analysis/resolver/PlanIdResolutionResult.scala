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
 * Outcome of resolving a plan-id-tagged reference: resolved to a candidate, the plan id matched but
 * the column did not resolve there, or no node matched the plan id at all.
 */
sealed trait PlanIdResolutionResult

object PlanIdResolutionResult {
  /** The reference resolved to a single candidate at the matched target. */
  case class Resolved(nameTarget: NameTarget) extends PlanIdResolutionResult

  /**
   * A target node matched the plan id, but no column resolved against it (the column was dropped or
   * shadowed above the tagged node), so the reference falls through to normal name resolution.
   */
  case object MatchedWithoutCandidate extends PlanIdResolutionResult

  /**
   * No plan node with the reference's plan id is visible in the current subtree -- a genuinely
   * illegal cross-DataFrame reference (e.g. `df1.select(df2.a)`), as distinct from
   * [[MatchedWithoutCandidate]] where a node did match.
   */
  case object NoTarget extends PlanIdResolutionResult
}
