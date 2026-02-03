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
 * The result of [[ResolverRunner]] operation. Can be one of the following:
 * - [[ResolverRunnerResultResolvedPlan]]: The plan was properly resolved by the single-pass
 *   Analyzer.
 * - [[ResolverRunnerResultPlanNotSupported]]: The plan is not supported and cannot be processed
 *   by the single-pass Analyzer. We would fall back to the fixed-point Analyzer for this plan.
 * - [[ResolverRunnerResultUnrecoverableException]]: An unrecoverable exception occurred during
 *   the single-pass Analysis, which prevents to use the fixed-point Analyzer as a fallback. We
 *   should just propagate it to the user from the [[HybridAnalyzer]].
 */
sealed trait ResolverRunnerResult

case class ResolverRunnerResultResolvedPlan(plan: LogicalPlan) extends ResolverRunnerResult

case class ResolverRunnerResultPlanNotSupported(reason: String) extends ResolverRunnerResult

case class ResolverRunnerResultUnrecoverableException(exception: Throwable)
    extends ResolverRunnerResult
