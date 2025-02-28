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

import org.apache.spark.sql.catalyst.analysis.RelationResolution
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.CatalogManager

/**
 * The [[BridgedRelationMetadataProvider]] is a [[RelationMetadataProvider]] that just reuses
 * resolved metadata from the [[AnalyzerBridgeState]]. This is used in the single-pass [[Resolver]]
 * to avoid duplicate catalog/table lookups in dual-run mode, so metadata is simply reused from the
 * fixed-point [[Analyzer]] run. We strictly rely on the [[AnalyzerBridgeState]] to avoid any
 * blocking calls here.
 */
class BridgedRelationMetadataProvider(
    override val catalogManager: CatalogManager,
    override val relationResolution: RelationResolution,
    analyzerBridgeState: AnalyzerBridgeState
) extends RelationMetadataProvider {
  override val relationsWithResolvedMetadata = new RelationsWithResolvedMetadata
  updateRelationsWithResolvedMetadata()

  /**
   * We update relations on each [[resolve]] call, because relation IDs might have changed.
   * This can happen for the nested views, since catalog name may differ, and expanded table name
   * will differ for the same [[UnresolvedRelation]].
   *
   * See [[ViewResolver.resolve]] for more info on how SQL configs are propagated to nested views).
   */
  override def resolve(unresolvedPlan: LogicalPlan): Unit = {
    updateRelationsWithResolvedMetadata()
  }

  private def updateRelationsWithResolvedMetadata(): Unit = {
    analyzerBridgeState.relationsWithResolvedMetadata.forEach(
      (unresolvedRelation, relationWithResolvedMetadata) => {
        relationsWithResolvedMetadata.put(
          relationIdFromUnresolvedRelation(unresolvedRelation),
          relationWithResolvedMetadata
        )
      }
    )
  }
}
