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

package org.apache.spark.sql.execution.analysis

import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_HINT
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.analysis.ResolveCacheHints.NO_RESULT_CACHE_TAG

case class ResolveCacheHints(session: SparkSession) extends Rule[LogicalPlan] {
  private lazy val cacheManager = session.sharedState.cacheManager

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(UNRESOLVED_HINT)) {
      case hint @ UnresolvedHint(hintName, parameters, child) =>
        hintName.toUpperCase(Locale.ROOT) match {
          case "RESULT_CACHE" =>
            if (parameters.nonEmpty) {
              throw QueryCompilationErrors.invalidCacheHintParameterError(hintName, parameters)
            }
            if (cacheManager.lookupCachedData(child).isEmpty) {
              cacheManager.cacheQuery(session, child, None)
            }
            child.unsetTagValue(NO_RESULT_CACHE_TAG)
            child
          case "NO_RESULT_CACHE" =>
            if (parameters.nonEmpty) {
              throw QueryCompilationErrors.invalidCacheHintParameterError(hintName, parameters)
            }
            child.setTagValue(NO_RESULT_CACHE_TAG, true)
            child
          case "RESULT_UNCACHE" =>
            if (parameters.nonEmpty) {
              throw QueryCompilationErrors.invalidCacheHintParameterError(hintName, parameters)
            }
            if (cacheManager.lookupCachedData(child).nonEmpty) {
              cacheManager.uncacheQuery(session, child, cascade = false, blocking = true)
            }
            child
          case _ => hint
        }
    }
  }
}

object ResolveCacheHints {
  /**
   * A tag for telling the `CacheManager` to skip the cached plan when true.
   */
  val NO_RESULT_CACHE_TAG: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("NO_RESULT_CACHE")
}
