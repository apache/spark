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
 * This is an addon to [[ResolverGuard]] functionality for features that cannot be determined by
 * only looking at the unresolved plan. [[Resolver]] will throw this control-flow exception
 * when it encounters some explicitly unsupported feature. Later behavior depends on the value of
 * [[HybridAnalyzer.checkSupportedSinglePassFeatures]] flag:
 *  - If it is true: It will later be caught by [[HybridAnalyzer]] to abort single-pass
 *    analysis without comparing single-pass and fixed-point results. The motivation for this
 *    feature is the same as for the [[ResolverGuard]] - we want to have an explicit allowlist of
 *    unimplemented features that we are aware of, and `UNSUPPORTED_SINGLE_PASS_ANALYZER_FEATURE`
 *    will signal us the rest of the gaps.
 *  - If it is false: It will be thrown by the [[HybridAnalyzer]] in order to get better sense
 *    of coverage.
 *
 * For example, [[UnresolvedRelation]] can be intermediately resolved by [[ResolveRelations]] as
 * [[UnresolvedCatalogRelation]] or a [[View]] (among all others). Say that for now the views
 * are not implemented, and we are aware of that, so [[ExplicitlyUnsupportedResolverFeature]] will
 * be thrown in the middle of the single-pass analysis to abort it.
 */
class ExplicitlyUnsupportedResolverFeature(reason: String)
    extends Exception(
      s"The single-pass analyzer cannot process this query or command because it does not yet " +
      s"support $reason."
    ) {
  override def getStackTrace(): Array[StackTraceElement] = new Array[StackTraceElement](0)
  override def fillInStackTrace(): Throwable = this
}

/**
 * This object contains all the metadata on explicitly unsupported resolver features.
 */
object ExplicitlyUnsupportedResolverFeature {
  val OPERATORS = Set(
    "org.apache.spark.sql.catalyst.plans.logical.View",
    "org.apache.spark.sql.catalyst.streaming.StreamingRelationV2",
    "org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation",
    "org.apache.spark.sql.execution.streaming.StreamingRelation"
  )
}
