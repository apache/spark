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
 * The result of [[CatalogObjectGuard]] check. Can be one of the following:
 * - [[CatalogObjectGuardResultPlanSupported]]: The plan is supported and can be fully processed by
 *   the single-pass Analyzer.
 * - [[CatalogObjectGuardResultPlanNotSupported]]: The plan is not supported and cannot be processed
 *   by the single-pass Analyzer. We would fall back to the fixed-point Analyzer for this plan.
 * - [[CatalogObjectGuardResultUnrecoverableException]]: An unrecoverable exception occurred during
 *   the check, which prevents to use the fixed-point Analyzer as a fall back. We should just
 *   propagate it to the user from the [[HybridAnalyzer]].
 */
sealed trait CatalogObjectGuardResult

case class CatalogObjectGuardResultPlanSupported() extends CatalogObjectGuardResult

case class CatalogObjectGuardResultPlanNotSupported(reason: String) extends CatalogObjectGuardResult

case class CatalogObjectGuardResultUnrecoverableException(exception: Throwable)
    extends CatalogObjectGuardResult
