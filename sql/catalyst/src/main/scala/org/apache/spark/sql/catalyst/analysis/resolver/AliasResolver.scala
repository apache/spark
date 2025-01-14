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

import org.apache.spark.sql.catalyst.analysis.{AliasResolution, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Cast,
  CreateNamedStruct,
  Expression,
  NamedExpression
}

/**
 * Resolver class that resolves unresolved aliases and handles user-specified aliases.
 */
class AliasResolver(expressionResolver: ExpressionResolver, scopes: NameScopeStack)
    extends TreeNodeResolver[UnresolvedAlias, Expression]
    with ResolvesExpressionChildren {

  /**
   * Resolves [[UnresolvedAlias]] by handling two specific cases:
   *  - Alias(CreateNamedStruct(...)) - instead of calling [[CreateNamedStructResolver]] which will
   *  clean up its inner aliases, we manually resolve [[CreateNamedStruct]]'s children, because we
   *  need to preserve inner aliases until after the alias name is computed. This is a hack because
   *  fixed-point analyzer computes [[Alias]] name before removing inner aliases.
   *  - Alias(...) - recursively call [[ExpressionResolver]] to resolve the child expression.
   *
   * After the children are resolved, call [[AliasResolution]] to compute the alias name. Finally,
   * clean up inner aliases from [[CreateNamedStruct]].
   */
  override def resolve(unresolvedAlias: UnresolvedAlias): NamedExpression = {
    val aliasWithResolvedChildren = withResolvedChildren(
      unresolvedAlias, {
        case createNamedStruct: CreateNamedStruct =>
          withResolvedChildren(createNamedStruct, expressionResolver.resolve)
        case other => expressionResolver.resolve(other)
      }
    )

    val resolvedAlias =
      AliasResolution.resolve(aliasWithResolvedChildren).asInstanceOf[NamedExpression]

    scopes.top.addAlias(resolvedAlias.name)
    AliasResolver.cleanupAliases(resolvedAlias)
  }

  /**
   * Handle already resolved [[Alias]] nodes, i.e. user-specified aliases. We disallow stacking
   * of [[Alias]] nodes by collapsing them so that only the top node remains.
   *
   * For an example query like:
   *
   * {{{ SELECT 1 AS a }}}
   *
   * parsed plan will be:
   *
   * Project [Alias(1, a)]
   * +- OneRowRelation
   *
   */
  def handleResolvedAlias(alias: Alias): Alias = {
    val aliasWithResolvedChildren = withResolvedChildren(alias, expressionResolver.resolve)
    scopes.top.addAlias(aliasWithResolvedChildren.name)
    AliasResolver.collapseAlias(aliasWithResolvedChildren)
  }
}

object AliasResolver {

  /**
   * For a query like:
   *
   * {{{ SELECT STRUCT(1 AS a, 2 AS b) AS st }}}
   *
   * After resolving [[CreateNamedStruct]] the plan will be:
   *     CreateNamedStruct(Seq("a", Alias(1, "a"), "b", Alias(2, "b")))
   *
   * For a query like:
   *
   * {{{ df.select($"col1".cast("int").cast("double")) }}}
   *
   * After resolving top-most [[Alias]] the plan will be:
   *     Alias(Cast(Alias(Cast(col1, int), col1)), double), col1)
   *
   * Both examples contain inner aliases that are not expected in the analyzed logical plan,
   * therefore need to be removed. However, in both examples inner aliases are necessary in order
   * for the outer alias to compute its name. To achieve this, we delay removal of inner aliases
   * until after the outer alias name is computed.
   *
   * For cases where there are no dependencies on inner alias, inner alias should be removed by the
   * resolver that produces it.
   */
  private def cleanupAliases(namedExpression: NamedExpression): NamedExpression =
    namedExpression
      .withNewChildren(namedExpression.children.map {
        case cast @ Cast(alias: Alias, _, _, _) =>
          cast.copy(child = alias.child)
        case createNamedStruct: CreateNamedStruct =>
          CreateNamedStructResolver.cleanupAliases(createNamedStruct)
        case other => other
      })
      .asInstanceOf[NamedExpression]

  /**
   * If an [[Alias]] node appears on top of another [[Alias]], remove the bottom one. Here we don't
   * handle a case where a node of different type appears between two [[Alias]] nodes: in this
   * case, removal of inner alias (if it is unnecessary) should be handled by respective node's
   * resolver, in order to preserve the bottom-up contract.
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
