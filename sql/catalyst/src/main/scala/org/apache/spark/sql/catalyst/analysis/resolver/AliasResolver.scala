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

import org.apache.spark.sql.catalyst.analysis.{AliasResolution, MultiAlias, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}

/**
 * Resolver class that resolves unresolved aliases and handles user-specified aliases.
 */
class AliasResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[UnresolvedAlias, Expression]
    with ResolvesExpressionChildren {
  private val scopes = expressionResolver.getNameScopes

  /**
   * Resolves [[UnresolvedAlias]] by resolving its child and computing the alias name by calling
   * [[AliasResolution]] on the result. After resolving it, we assign a correct exprId to the
   * resulting [[Alias]]. Here we allow inner aliases to persist until the end of single-pass
   * resolution, after which they will be removed in the post-processing phase.
   */
  override def resolve(unresolvedAlias: UnresolvedAlias): NamedExpression =
    scopes.current.lcaRegistry.withNewLcaScope {
      val aliasWithResolvedChildren =
        withResolvedChildren(unresolvedAlias, expressionResolver.resolve _)
          .asInstanceOf[UnresolvedAlias]

      val resolvedAlias =
        AliasResolution.resolve(aliasWithResolvedChildren).asInstanceOf[NamedExpression]

      resolvedAlias match {
        case multiAlias: MultiAlias =>
          throw new ExplicitlyUnsupportedResolverFeature(
            s"unsupported expression: ${multiAlias.getClass.getName}"
          )
        case alias: Alias =>
          expressionResolver.getExpressionIdAssigner.mapExpression(alias)
      }
    }

  /**
   * Handle already resolved [[Alias]] nodes, i.e. user-specified aliases. Here we only need to
   * resolve its children and afterwards reassign exprId to the resulting [[Alias]].
   */
  def handleResolvedAlias(alias: Alias): Alias = {
    val resolvedAlias = scopes.current.lcaRegistry.withNewLcaScope {
      val aliasWithResolvedChildren =
        withResolvedChildren(alias, expressionResolver.resolve _).asInstanceOf[Alias]

      expressionResolver.getExpressionIdAssigner.mapExpression(aliasWithResolvedChildren)
    }

    collapseAlias(resolvedAlias)
  }

  /**
   * In case where there are two explicit [[Alias]]es, one on top of the other, remove the bottom
   * one. For the example bellow:
   *
   * - df.select($"column".as("alias_1").as("alias_2"))
   *
   * the plan is:
   *
   *   Project[
   *     Alias("alias_2")(
   *       Alias("alias_1")(id)
   *     )
   *   ]( ... )
   *
   * and after the `collapseAlias` call (removing the bottom one) it would be:
   *
   *   Project[
   *     Alias("alias_2")(id)
   *   ]( ... )
   */
  private def collapseAlias(alias: Alias): Alias =
    alias.child match {
      case innerAlias: Alias =>
        val metadata = if (alias.metadata.isEmpty) {
          None
        } else {
          Some(alias.metadata)
        }
        alias.copy(child = innerAlias.child)(
          exprId = alias.exprId,
          qualifier = alias.qualifier,
          explicitMetadata = metadata,
          nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
        )
      case _ => alias
    }
}
