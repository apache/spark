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
class ViewResolver(resolver: Resolver, catalogManager: CatalogManager)
    extends TreeNodeResolver[View, View] {
  private val cteRegistry = resolver.getCteRegistry
  private val sourceUnresolvedRelationStack = new ArrayDeque[UnresolvedRelation]
  private val viewResolutionContextStack = new ArrayDeque[ViewResolutionContext]

  def getCatalogAndNamespace: Option[Seq[String]] =
    if (viewResolutionContextStack.isEmpty) {
      None
    } else {
      viewResolutionContextStack.peek().catalogAndNamespace
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
   *   - Check if the single-pass resolver fully supports the view plan using the [[ResolverGuard]].
   *     Throw [[ExplicitlyUnsupportedResolverFeature]] if the view plan is not supported.
   *   - Set the [[ViewResolutionContext]] for the view plan resolution.
   *   - Replace the necessary configurations in [[SQLConf]] with those that were stored with the
   *     view.
   *   - Resolve the view plan using parent [[Resolver]].
   *   - Create a new [[CatalogTable]] description for the resolved view based on the original
   *     [[UnresolvedRelation.options]], original [[CatalogTable]] description and used
   *     [[ViewResolutionContext]].
   *   - Return the resolved [[View]] with the resolved child and a new [[CatalogTable]]
   *     description.
   */
  override def resolve(unresolvedView: View): View = {
    checkResolverGuard(unresolvedView)

    val (resolvedChild, usedViewResolutionContext) = withViewResolutionContext(unresolvedView) {
      SQLConf.withExistingConf(
        View.effectiveSQLConf(unresolvedView.desc.viewSQLConfigs, unresolvedView.isTempView)
      ) {
        cteRegistry.withNewScope(isRoot = true, isOpaque = true) {
          resolver.lookupMetadataAndResolve(unresolvedView.child)
        }
      }
    }
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
      try {
        (body, viewResolutionContext)
      } finally {
        viewResolutionContextStack.pop()
      }
    }
  }

  private def checkResolverGuard(unresolvedView: View): Unit = {
    val resolverGuard = new ResolverGuard(catalogManager)
    if (!resolverGuard(unresolvedView)) {
      throw new ExplicitlyUnsupportedResolverFeature("View body is not supported")
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
 * @param catalogAndNamespace Catalog and camespace under which the [[View]] was created.
 */
case class ViewResolutionContext(
    nestedViewDepth: Int,
    maxNestedViewDepth: Int,
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
