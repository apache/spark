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

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Parameters for name resolution in [[NameScope]] and [[NameScopeStack]].
 *
 * @param canLaterallyReferenceColumn Whether lateral column references are allowed.
 * @param canReferenceAggregateExpressionAliases Whether aggregate expression aliases can be
 *  referenced.
 * @param canResolveNameByHiddenOutput Whether hidden output can be used for name resolution.
 * @param canResolveNameByHiddenOutputInSubquery Whether hidden output can be used in subquery
 *  context.
 * @param shouldPreferHiddenOutput Whether hidden output should be preferred over main output.
 * @param canReferenceAggregatedAccessOnlyAttributes Whether aggregated access only attributes can
 *  be referenced.
 * @param resolvingView Whether we are currently resolving a view (including nested views).
 * @param resolvingExecuteImmediate Whether we are currently resolving an EXECUTE IMMEDIATE.
 * @param referredTempVariableNames The names of the temporary variables that are referred to in
 *  the view that we are currently resolving.
 * @param extractValueExtractionKey Extraction expression for [[UnresolvedExtractValue]] if one is
 * currently being resolved, None otherwise.
 */
case class NameResolutionParameters(
    canLaterallyReferenceColumn: Boolean = false,
    canReferenceAggregateExpressionAliases: Boolean = false,
    canResolveNameByHiddenOutput: Boolean = false,
    canResolveNameByHiddenOutputInSubquery: Boolean = false,
    shouldPreferHiddenOutput: Boolean = false,
    canReferenceAggregatedAccessOnlyAttributes: Boolean = false,
    resolvingView: Boolean = false,
    resolvingExecuteImmediate: Boolean = false,
    referredTempVariableNames: Seq[Seq[String]] = Seq.empty,
    extractValueExtractionKey: Option[Expression] = None) {}
