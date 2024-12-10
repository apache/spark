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

import java.util.{ArrayDeque, HashMap}

import org.apache.spark.sql.catalyst.analysis.{withPosition, RelationResolution, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}
import org.apache.spark.util.ArrayImplicits._

/**
 * The [[MetadataResolver]] performs relation metadata resolution based on the unresolved plan
 * at the start of the analysis phase. Usually it does RPC calls to some table catalog and to table
 * metadata itself.
 *
 * [[RelationsWithResolvedMetadata]] is a map from relation ID to the relations with resolved
 * metadata. It's produced by [[resolve]] and is used later in [[Resolver]] to replace
 * [[UnresolvedRelation]]s.
 *
 * This object is one-shot per SQL query or DataFrame program resolution.
 */
class MetadataResolver(
    override val catalogManager: CatalogManager,
    relationResolution: RelationResolution,
    override val extensions: Seq[ResolverExtension] = Seq.empty)
    extends LookupCatalog
    with DelegatesResolutionToExtensions {
  private val resolvedRelations = new MetadataResolver.RelationsWithResolvedMetadata

  /**
   * This is the optional [[AnalyzerBridgeState]] from fixed-point [[Analyzer]] which is used to
   * reuse the cached relations. This field is set once in [[resolve]] prior to the metadata
   * resolution.
   */
  private var analyzerBridgeState: Option[AnalyzerBridgeState] = None

  /**
   * Returns the [[RelationId]] for the given [[UnresolvedRelation]]. Used by this class to store
   * the relations with resolved metadata in a map, and later by [[Resolver]] to lookup those
   * relations based on the [[UnresolvedRelation]] from the unresolved logical plan.
   */
  def relationIdFromUnresolvedRelation(unresolvedRelation: UnresolvedRelation): RelationId = {
    relationResolution.expandIdentifier(unresolvedRelation.multipartIdentifier) match {
      case CatalogAndIdentifier(catalog, ident) =>
        RelationId(
          multipartIdentifier =
            Seq(catalog.name()) ++ ident.namespace().toImmutableArraySeq ++ Seq(ident.name()),
          options = unresolvedRelation.options,
          isStreaming = unresolvedRelation.isStreaming
        )
      case _ =>
        withPosition(unresolvedRelation) {
          unresolvedRelation.tableNotFound(unresolvedRelation.multipartIdentifier)
        }
    }
  }

  /**
   * Resolves the relation metadata for `unresolvedPlan`. Usually this involves several blocking
   * calls for the [[UnresolvedRelation]]s present in that tree. In the end we return a map of
   * relation IDs to relations with resolved metadata. This map will be used to resolve the plan
   * in single-pass by the [[Resolver]].
   */
  def resolve(unresolvedPlan: LogicalPlan, bridgeState: Option[AnalyzerBridgeState] = None)
      : MetadataResolver.RelationsWithResolvedMetadata = {
    analyzerBridgeState = bridgeState

    lookupMetadata(unresolvedPlan)
    resolvedRelations
  }

  /**
   * Lookup metadata for all the unresolved relations in the given `unresolvedPlan` and
   * fill the [[resolvedRelations]]. If the generic metadata resolution using [[RelationResolution]]
   * wasn't successful, we resort to using [[extensions]]. Otherwise, we fail with an exception.
   *
   * If [[analyzerBridgeState]] is available, we _strictly_ use the
   * relations from fixed-point [[Analyzer]]. This is done to avoid any blocking calls in dual-run
   * mode (`ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER`) after the fixed-point analysis
   * is complete to avoid any latency issues. Otherwise (normally), this method is blocking.
   */
  private def lookupMetadata(root: LogicalPlan): Unit =
    traverseLogicalPlanTree(root) { unresolvedPlan =>
      unresolvedPlan match {
        case unresolvedRelation: UnresolvedRelation =>
          val relationWithResolvedMetadata = analyzerBridgeState match {
            case Some(analyzerBridgeState) =>
              Option(analyzerBridgeState.relationsWithResolvedMetadata.get(unresolvedRelation))
            case None =>
              resolveRelation(unresolvedRelation).orElse {
                // In case the generic metadata resolution returned `None`, we try to check if any
                // of the [[extensions]] matches this `unresolvedRelation`, and resolve it using
                // that extension.
                tryDelegateResolutionToExtension(unresolvedRelation)
              }
          }

          relationWithResolvedMetadata match {
            case Some(relationWithResolvedMetadata) =>
              resolvedRelations.put(
                relationIdFromUnresolvedRelation(unresolvedRelation),
                relationWithResolvedMetadata
              )
            case None =>
              handleUnsupportedOperator(unresolvedRelation)
          }
        case _ =>
      }
    }

  /**
   * Resolves the metadata for the given unresolved relation and returns a relation with the
   * resolved metadata. This method is blocking.
   */
  private def resolveRelation(unresolvedRelation: UnresolvedRelation): Option[LogicalPlan] =
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      relationResolution.resolveRelation(
        u = unresolvedRelation
      )
    }

  /**
   * Traverse the logical plan tree from `root` in a pre-order DFS manner and apply `visitor` to
   * each node.
   */
  private def traverseLogicalPlanTree(root: LogicalPlan)(visitor: LogicalPlan => Unit) = {
    val stack = new ArrayDeque[Either[LogicalPlan, Expression]]
    stack.push(Left(root))

    while (!stack.isEmpty) {
      stack.pop() match {
        case Left(logicalPlan) =>
          visitor(logicalPlan)

          for (child <- logicalPlan.children) {
            stack.push(Left(child))
          }
          for (expression <- logicalPlan.expressions) {
            stack.push(Right(expression))
          }
        case Right(expression) =>
          for (child <- expression.children) {
            stack.push(Right(child))
          }

          expression match {
            case planExpression: PlanExpression[_] =>
              planExpression.plan match {
                case plan: LogicalPlan =>
                  stack.push(Left(plan))
                case _ =>
              }
            case _ =>
          }
      }
    }
  }

  /**
   * Handles the case when the operator is not supported by the [[MetadataResolver]]. We throw an
   * [[unresolvedRelation.tableNotFound]] exception.
   */
  private def handleUnsupportedOperator(unresolvedRelation: UnresolvedRelation): Nothing = {
    withPosition(unresolvedRelation) {
      unresolvedRelation.tableNotFound(unresolvedRelation.multipartIdentifier)
    }
  }
}

object MetadataResolver {
  type RelationsWithResolvedMetadata = HashMap[RelationId, LogicalPlan]
}
