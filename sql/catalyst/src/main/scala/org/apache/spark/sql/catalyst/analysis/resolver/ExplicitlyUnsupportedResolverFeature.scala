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
 * when it encounters some explicitly unsupported feature. Like a [[ResolverGuard]] rejection, it
 * marks a known coverage gap rather than a single-pass failure: [[HybridAnalyzer]] falls back to
 * the fixed-point result and records `resolutionGap` as the single-pass resolution gap. The
 * `ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_EXPLICITLY_UNSUPPORTED_FEATURE_IN_*` flags expose it as a
 * failure instead. `reason` only appears in the exception message, so unlike `resolutionGap` it
 * may carry details that must not be logged unredacted.
 *
 * For example, [[UnresolvedRelation]] can be intermediately resolved by [[ResolveRelations]] as
 * [[UnresolvedCatalogRelation]] or a [[View]] (among all others). Say that for now the views
 * are not implemented, and we are aware of that, so [[ExplicitlyUnsupportedResolverFeature]] will
 * be thrown in the middle of the single-pass analysis to abort it.
 */
class ExplicitlyUnsupportedResolverFeature(val resolutionGap: ResolutionGap, reason: String)
    extends Exception(
      s"The single-pass analyzer cannot process this query or command because it does not yet " +
      s"support $reason."
    ) {
  def this(resolutionGap: ResolutionGap) = this(resolutionGap, resolutionGap.name)

  override def getStackTrace(): Array[StackTraceElement] = new Array[StackTraceElement](0)
  override def fillInStackTrace(): Throwable = this
}

/**
 * This object contains all the metadata on explicitly unsupported resolver features.
 */
object ExplicitlyUnsupportedResolverFeature {
  val OPERATORS = Set(
    "org.apache.spark.sql.catalyst.streaming.StreamingRelationV2",
    "org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation",
    "org.apache.spark.sql.execution.streaming.StreamingRelation",
    "org.apache.spark.sql.metricview.logical.MetricViewPlaceholder",
    "org.apache.spark.sql.metricview.logical.ResolvedMetricView"
  )
}
