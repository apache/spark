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

import java.util.ArrayDeque

import scala.collection.mutable

import com.databricks.sql.managedcatalog.DeltaSharingKind

import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, UnresolvedRelation, ViewResolution}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The [[ViewResolver]] resolves view plans that were already reconstructed by [[SessionCatalog]]
 * from the view text and view metadata (schema, configs).
 */
class ViewResolver(
    resolver: Resolver,
    catalogManager: CatalogManager,
    tracker: Option[QueryPlanningTracker] = None)
    extends TreeNodeResolver[View, View]
    with ResolverMetricTracker // EDGE
    {
  private val cteRegistry = resolver.getCteRegistry
  private val sourceUnresolvedRelationStack = new ArrayDeque[UnresolvedRelation]
  private val viewResolutionContextStack = new ArrayDeque[ViewResolutionContext]

  /**
   * Get current [[ViewResolutionContext]], if we are resolving a view, or None otherwise.
   */
  def getViewResolutionContext: Option[ViewResolutionContext] =
    if (viewResolutionContextStack.isEmpty) {
      None
    } else {
      Some(viewResolutionContextStack.peek())
    }

  /**
   * This method preserves the resolved [[UnresolvedRelation]] for the further view resolution
   * process.
   *
   * [[sourceUnresolvedRelationStack]] is used to save the [[UnresolvedRelation]] after its
   * resolution by [[Resolver.resolveRelation]], since [[View]] that was produced from this
   * [[UnresolvedRelation]] needs [[UnresolvedRelation.options]] for its resolution.
   * We pop from the [[sourceUnresolvedRelationStack]] after the `body` is executed. The stack is
   * necessary, since [[withSourceUnresolvedRelation]] calls might be nested:
   * [[UnresolvedRelation]] -> [[View]]
   *   ...
   *     [[UnresolvedRelation]] -> [[View]]
   *       ...
   */
  def withSourceUnresolvedRelation(unresolvedRelation: UnresolvedRelation)(
      body: => LogicalPlan): LogicalPlan = {
    sourceUnresolvedRelationStack.push(unresolvedRelation)
    try {
      body
    } finally {
      sourceUnresolvedRelationStack.pop()
    }
  }

  /**
   * Resolve the `unresolvedView` and its underlying plan. This method uses parent [[Resolver]] to
   * resolve the view child. [[View]] resolution consists of the following steps:
   *   - Check if the view has any row-level security policies or column masking rules applied. If
   *     so, throw [[ExplicitlyUnsupportedResolverFeature]].
   *   - Check if the view is a materialized view. If so, throw
   *     [[ExplicitlyUnsupportedResolverFeature]].
   *   - Check if the single-pass resolver fully supports the view plan using the [[ResolverGuard]].
   *     Throw [[ExplicitlyUnsupportedResolverFeature]] if the view plan is not supported.
   *   - Set the [[ViewResolutionContext]] for the view plan resolution.
   *   - Replace the necessary configurations in [[SQLConf]] with those that were stored with the
   *     view.
   *   - Resolve the view plan using parent [[Resolver]].
   *   - Sets the views child as analyzed in order to avoid recursing into it using rewrite rules.
   *   - Create a new [[CatalogTable]] description for the resolved view based on the original
   *     [[UnresolvedRelation.options]], original [[CatalogTable]] description and used
   *     [[ViewResolutionContext]].
   *   - Return the resolved [[View]] with the resolved child and a new [[CatalogTable]]
   *     description.
   */
  // BEGIN-EDGE
  override def resolve(unresolvedView: View): View = recordProfile("resolve") {
    /* // END-EDGE
  override def resolve(unresolvedView: View): View = {
     */ // EDGE

    RestrictRowLevelSecurityFeature(unresolvedView)
    checkMaterializedView(unresolvedView) // EDGE

    val (resolvedChild, usedViewResolutionContext) = withViewResolutionContext(unresolvedView) {
      SQLConf.withExistingConf(
        // BEGIN-EDGE
        ViewResolution.getViewResolutionConf(unresolvedView.desc, unresolvedView.isTempView)
        /* // END-EDGE
        View.effectiveSQLConf(unresolvedView.desc.viewSQLConfigs, unresolvedView.isTempView)
         */ // EDGE
      ) {
        checkResolverGuard(unresolvedView)

        cteRegistry.pushScope(isRoot = true, isOpaque = true)

        try {
          resolver.lookupMetadataAndResolve(unresolvedView.child)
        } finally {
          cteRegistry.popScope()
        }
      }
    }

    resolvedChild.setAnalyzed()

    val options = if (sourceUnresolvedRelationStack.isEmpty()) {
      CaseInsensitiveStringMap.empty()
    } else {
      sourceUnresolvedRelationStack.peek().options
    }

    // BEGIN-EDGE
    View(
      desc = ViewResolution.createResolvedViewDescription(
        sourceDescription = unresolvedView.desc,
        unresolvedRelationOptions = options,
        deltaSharingKind = usedViewResolutionContext.deltaSharingKind,
        referredBuiltInFunctionNames = usedViewResolutionContext.referredBuiltInFunctionNames,
        referredExternalFunctionNames = usedViewResolutionContext.referredExternalFunctionNames,
        checkAnalysis = () => {}
      ),
      isTempView = unresolvedView.isTempView,
      child = resolvedChild,
      options = options
    )
    /* // END-EDGE
    unresolvedView.copy(child = resolvedChild, options = options)
     */ // EDGE
  }

  /**
   * Execute `body` with a fresh [[ViewResolutionContext]] specifically constructed for
   * `unresolvedView` resolution. The context is popped back to the previous one after the
   * `body` is executed, because views may be nested.
   */
  private def withViewResolutionContext(unresolvedView: View)(
      body: => LogicalPlan): (LogicalPlan, ViewResolutionContext) = {
    AnalysisContext.withAnalysisContext(unresolvedView.desc) {
      val currentAnalysisContext = AnalysisContext.get

      val prevContext = if (viewResolutionContextStack.isEmpty()) {
        ViewResolutionContext(
          nestedViewDepth = 0,
          maxNestedViewDepth = conf.maxNestedViewDepth
        )
      } else {
        viewResolutionContextStack.peek()
      }

      val viewResolutionContext = prevContext.copy(
        nestedViewDepth = prevContext.nestedViewDepth + 1,
        referredTempVariableNames = unresolvedView.desc.viewReferredTempVariableNames,
        // BEGIN-EDGE
        deltaSharingKind = prevContext.deltaSharingKind.orElse(
          unresolvedView.desc.deltaSharingKind
        ),
        referredBuiltInFunctionNames = currentAnalysisContext.referredBuiltinFunctionNames,
        referredExternalFunctionNames = currentAnalysisContext.referredExternalFunctionNames,
        collation = unresolvedView.desc.collation,
        // END-EDGE
        catalogAndNamespace = Some(unresolvedView.desc.viewCatalogAndNamespace)
      )
      viewResolutionContext.validate(unresolvedView)

      viewResolutionContextStack.push(viewResolutionContext)
      try {
        (body, viewResolutionContext)
      } finally {
        viewResolutionContextStack.pop()
      }
    }
  }
  // BEGIN-EDGE

  private def checkMaterializedView(unresolvedView: View): Unit = {
    if (unresolvedView.desc.isMaterializedView) {
      throw new ExplicitlyUnsupportedResolverFeature("Materialized views")
    }
  }
  // END-EDGE

  private def checkResolverGuard(unresolvedView: View): Unit = {
    val resolverGuard = new ResolverGuard(catalogManager, tracker = tracker)
    resolverGuard(unresolvedView).planUnsupportedReason match {
      case Some(reason) =>
        throw new ExplicitlyUnsupportedResolverFeature(s"View body is not supported: $reason")
      case None =>
    }
  }
}

/**
 * The [[ViewResolutionContext]] consists of data, which is specific to the specific view plan
 * resolution. This data is also propagated to the subviews.
 *
 * @param nestedViewDepth Current nested view depth. Cannot exceed the `maxNestedViewDepth`.
 * @param maxNestedViewDepth Maximum allowed nested view depth. Configured in the upper context
 * @param referredTempVariableNames All the temporary variables referred in the view.
 *   based on [[SQLConf.MAX_NESTED_VIEW_DEPTH]].
 * // BEGIN-EDGE
 * @param deltaSharingKind The delta sharing kind must be propagated to subviews because we must
 *   validate views beneath a shared view.
 * @param referredBuiltinFunctionNames All the built-in functions referred in the view.
 * @param referredExternalFunctionNames All the external functions referred in the view.
 * // END-EDGE
 * @param collation View's default collation if explicitly set.
 * @param catalogAndNamespace Catalog and camespace under which the [[View]] was created.
 */
case class ViewResolutionContext(
    nestedViewDepth: Int,
    maxNestedViewDepth: Int,
    // BEGIN-EDGE
    deltaSharingKind: Option[DeltaSharingKind] = None,
    referredTempVariableNames: Seq[Seq[String]] = Seq.empty,
    referredBuiltInFunctionNames: mutable.Set[String] = mutable.Set.empty,
    referredExternalFunctionNames: mutable.Set[String] = mutable.Set.empty,
    // END-EDGE
    collation: Option[String] = None,
    catalogAndNamespace: Option[Seq[String]] = None) {
  def validate(unresolvedView: View): Unit = {
    if (nestedViewDepth > maxNestedViewDepth) {
      throw QueryCompilationErrors.viewDepthExceedsMaxResolutionDepthError(
        unresolvedView.desc.identifier,
        maxNestedViewDepth,
        unresolvedView
      )
    }
  }
}
