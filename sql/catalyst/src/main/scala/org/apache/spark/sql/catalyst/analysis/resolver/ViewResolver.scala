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

import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, UnresolvedRelation}
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
    catalogManager: CatalogManager)
    extends TreeNodeResolver[View, View]
    with ResolverMetricTracker
    {
  private val cteRegistry = resolver.getCteRegistry
  private val queryBlockScopes = resolver.getQueryBlockScopes
  private val nestedObjectRegistry = resolver.getNestedObjectRegistry
  private val scopes = resolver.getNameScopes
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
   *   - Check if the single-pass resolver fully supports the view plan using the [[ResolverGuard]],
   *     both before and after the view plan is preprocessed by the [[PlanPreprocessor]]. Throw
   *     [[ExplicitlyUnsupportedResolverFeature]] if the view plan is not supported.
   *   - Set the [[ViewResolutionContext]] for the view plan resolution.
   *   - Replace the necessary configurations in [[SQLConf]] with those that were stored with the
   *     view.
   *   - Resolve the view plan using parent [[Resolver]] in an opaque [[NameScope]], so that outer
   *     references from the view body cannot leak into the enclosing query.
   *   - Sets the views child as analyzed in order to avoid recursing into it using rewrite rules.
   *   - Return the resolved [[View]] with the resolved child and the original
   *     [[UnresolvedRelation.options]].
   */
  override def resolve(unresolvedView: View): View = {
    val (resolvedChild, usedViewResolutionContext) = withViewResolutionContext(unresolvedView) {
      SQLConf.withExistingConf(
        View.effectiveSQLConf(
          configs = unresolvedView.desc.viewSQLConfigs,
          isTempView = unresolvedView.isTempView,
          createSparkVersion = unresolvedView.desc.createVersion)
      ) {
        checkResolverGuard(unresolvedView)
        val planPreprocessor =
          new PlanPreprocessor(resolver, resolveExpressionIdentifiersInRootScope = false)
        val preprocessedView = planPreprocessor.substitutePlan(unresolvedView)
        checkResolverGuard(preprocessedView)

        cteRegistry.pushScope(isRoot = true, isOpaque = true)
        queryBlockScopes.push()
        scopes.pushScope(scopeType = NameScopeType.Opaque)

        try {
          resolver.lookupMetadataAndResolve(unresolvedView.child)
        } finally {
          popViewBodyScopeIntoOuter()
          queryBlockScopes.pop()
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

    unresolvedView.copy(child = resolvedChild, options = options)
  }

  /**
   * Execute `body` with a fresh [[ViewResolutionContext]] specifically constructed for
   * `unresolvedView` resolution. The context is popped back to the previous one after the
   * `body` is executed, because views may be nested.
   */
  private def withViewResolutionContext(unresolvedView: View)(
      body: => LogicalPlan): (LogicalPlan, ViewResolutionContext) = {
    AnalysisContext.withAnalysisContext(unresolvedView.desc) {
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
        catalogAndNamespace = Some(unresolvedView.desc.viewCatalogAndNamespace)
      )
      viewResolutionContext.validate(unresolvedView)

      viewResolutionContextStack.push(viewResolutionContext)
      nestedObjectRegistry.pushScope(
        defaultCollation = unresolvedView.desc.collation,
        referredTempVariableNames = unresolvedView.desc.viewReferredTempVariableNames
      )
      try {
        (body, viewResolutionContext)
      } finally {
        nestedObjectRegistry.popScope()
        viewResolutionContextStack.pop()
      }
    }
  }
  /**
   * Pops the view body's isolated scope and re-establishes the outer scope with the view body's
   * output and plan-id targets.
   *
   * The view body resolves in its own opaque [[NameScope]], but a DataFrame can persist a
   * plan-id-tagged plan as a temp view and then reference that plan's column from outside the view,
   * so the view body's [[PlanIdTargetRegistry]] must reach the outer scope. For example:
   *
   * {{{
   * val df1 = spark.range(2)
   * df1.createOrReplaceTempView("v")
   * val df2 = spark.table("v")
   * df2.select(df1("id"))
   * }}}
   *
   * `df1("id")` carries `df1`'s plan id, but `df1` now sits inside the `v` view body; its target
   * is recorded there, so carrying that registry out lets the outer `select` resolve the reference.
   */
  private def popViewBodyScopeIntoOuter(): Unit = {
    val viewOutput = scopes.current.output
    val viewPlanIdTargetRegistry = scopes.current.planIdTargetRegistry
    scopes.popScope()
    scopes.overwriteCurrent(
      output = Some(viewOutput),
      planIdTargetRegistry = Some(viewPlanIdTargetRegistry)
    )
  }

  private def checkResolverGuard(unresolvedPlan: LogicalPlan): Unit = {
    ResolverGuard(
      catalogManager = catalogManager,
      resolverExtensions = resolver.extensions
    )(unresolvedPlan).planUnsupportedReason match {
      case Some(reason) =>
        throw new ExplicitlyUnsupportedResolverFeature(
          ResolutionGap.ViewBody,
          s"View body is not supported: $reason"
        )
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
 *   based on [[SQLConf.MAX_NESTED_VIEW_DEPTH]].
 * @param catalogAndNamespace Catalog and namespace under which the [[View]] was created.
 */
case class ViewResolutionContext(
    nestedViewDepth: Int,
    maxNestedViewDepth: Int,
    catalogAndNamespace: Option[Seq[String]] = None) {
  def validate(unresolvedView: View): Unit = {
    if (nestedViewDepth > maxNestedViewDepth) {
      throw QueryCompilationErrors.viewDepthExceedsMaxResolutionDepthError(
        unresolvedView.desc.fullIdent,
        maxNestedViewDepth,
        unresolvedView
      )
    }
  }
}
