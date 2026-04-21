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

import java.util.HashMap

import org.apache.spark.sql.catalyst.analysis.{RelationResolution, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}
import org.apache.spark.util.ArrayImplicits._

/**
 * [[RelationMetadataProvider]] provides relations with resolved metadata based on the
 * corresponding [[UnresolvedRelation]]s. It is used by [[Resolver]] to replace
 * [[UnresolvedRelation]] with a specific [[LogicalPlan]] with resolved metadata, e.g. with
 * [[UnresolvedCatalogRelation]] or [[View]].
 */
trait RelationMetadataProvider extends LookupCatalog {
  type RelationsWithResolvedMetadata = HashMap[RelationId, LogicalPlan]

  /**
   * [[relationResolution]] is used by the [[RelationMetadataProvider]] to expand relation
   * identifiers in [[relationIdFromUnresolvedRelation]].
   */
  protected val relationResolution: RelationResolution

  /**
   * [[relationsWithResolvedMetadata]] is a map from relation ID to the specific [[LogicalPlan]]
   * with resolved metadata, like [[UnresolvedCatalogRelation]] or [[View]]. It's filled by the
   * specific [[RelationMetadataProvider]] implementation and is queried in
   * [[getRelationWithResolvedMetadata]].
   */
  protected val relationsWithResolvedMetadata: RelationsWithResolvedMetadata

  /**
   * Resolve metadata for the given `unresolvedPlan`. This method is called once per unresolved
   * logical plan by the [[Resolver]] (for each SQL query/ DataFrame program and for each
   * nested [[View]] operator).
   */
  def resolve(unresolvedPlan: LogicalPlan): Unit

  /**
   * Get the [[LogicalPlan]] with resolved metadata for the given [[UnresolvedRelation]].
   *
   * [[java.util.HashMap]] returns `null` if the key is not found, so we wrap it in an [[Option]].
   */
  def getRelationWithResolvedMetadata(
      unresolvedRelation: UnresolvedRelation): Option[LogicalPlan] = {
    Option(
      relationsWithResolvedMetadata.get(
        relationIdFromUnresolvedRelation(unresolvedRelation)
      )
    )
  }

  /**
   * Returns the [[RelationId]] for the given [[UnresolvedRelation]]. Here we use
   * [[relationResolution]] to expand the [[UnresolvedRelation]] identifier fully, so that our
   * [[RelationId]] uniquely identifies the [[unresolvedRelation]].
   *
   * This method is public, because it's used in [[MetadataResolverSuite]].
   */
  def relationIdFromUnresolvedRelation(unresolvedRelation: UnresolvedRelation): RelationId = {
    val identifier = unresolvedRelation.multipartIdentifier
    val isGlobalTempView = identifier.length == 2 &&
      catalogManager.v1SessionCatalog.isGlobalTempViewDB(identifier.head)
    val isTempView = !isGlobalTempView &&
      relationResolution.lookupTempView(identifier).isDefined
    val multipartIdentifier =
      if (isTempView) {
        expandSessionTemporaryViewIdentifier(identifier)
      } else {
        val expanded = relationResolution.expandIdentifier(identifier)
        expanded match {
          case CatalogAndIdentifier(catalog, ident) =>
            Seq(catalog.name()) ++ ident.namespace().toImmutableArraySeq ++ Seq(ident.name())
          case _ =>
            // expandIdentifier returns bare names at top level (not inside views).
            // Fall back to current catalog/namespace to produce a unique key.
            Seq(catalogManager.currentCatalog.name()) ++
              catalogManager.currentNamespace.toImmutableArraySeq ++
              identifier
        }
      }

    RelationId(
      multipartIdentifier = multipartIdentifier,
      options = unresolvedRelation.options,
      isStreaming = unresolvedRelation.isStreaming,
      timeTravelSpec = None
    )
  }

  /**
   * Expands a session temporary view identifier to `system.session.<name>` to avoid
   * [[RelationId]] collisions with persistent tables of the same name in the
   * [[relationsWithResolvedMetadata]] map.
   */
  private def expandSessionTemporaryViewIdentifier(
      multipartIdentifier: Seq[String]): Seq[String] = {
    if (multipartIdentifier.length >= 3 &&
        multipartIdentifier.head.equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
        multipartIdentifier(1).equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)) {
      Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE) ++
        multipartIdentifier.drop(2)
    } else if (multipartIdentifier.length >= 2 &&
        multipartIdentifier.head.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)) {
      Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE) ++
        multipartIdentifier.drop(1)
    } else {
      Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE) ++
        multipartIdentifier
    }
  }
}
