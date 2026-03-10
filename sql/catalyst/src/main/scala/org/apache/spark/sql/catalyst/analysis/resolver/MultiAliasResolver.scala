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

import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions.{Alias, Generator, NamedExpression}

/**
 * Resolver class that resolves [[MultiAlias]] expressions.
 *
 * [[MultiAlias]] was originally created for [[Generator]]s that could have multiple outputs
 * (e.g. posexplode). This resolver handles the resolution of [[MultiAlias]] children,
 * collapses inner aliases, validates that the child is a [[Generator]], and converts
 * the result to [[AliasedGenerator]] when appropriate.
 */
class MultiAliasResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[MultiAlias, NamedExpression]
    with ResolvesExpressionChildren
    with ConvertsToAliasedGenerator {

  override protected val scopes: NameScopeStack = expressionResolver.getNameScopes
  override protected val traversals: ExpressionTreeTraversalStack =
    expressionResolver.getExpressionTreeTraversals
  override protected val expressionIdAssigner: ExpressionIdAssigner =
    expressionResolver.getExpressionIdAssigner

  /**
   * Resolve [[MultiAlias]] nodes, i.e. user-specified multi alias. Here we only
   * need to resolve its child and afterwards collapse inner aliases and validate.
   *
   * In the end try to convert [[MultiAlias]] over [[Generator]] into [[AliasedGenerator]].
   * It must always happen, because
   *  - Child is always [[Generator]] (see [[validateMultiAliasHasGenerator]])
   *  - Child is resolved (except [[GeneratorOuter]] wrapper)
   *  For more info see [[tryCreateAliasedGenerator]].
   */
  override def resolve(multiAlias: MultiAlias): NamedExpression = {
    scopes.current.lcaRegistry.withNewLcaScope(
      aliasKind = AliasKind.Explicit
    ) {
      val resolvedMultiAlias =
        withResolvedChildren(multiAlias, expressionResolver.resolve _)
          .asInstanceOf[MultiAlias]
      val collapsedMultiAlias = collapseMultiAlias(resolvedMultiAlias)

      validateMultiAliasHasGenerator(collapsedMultiAlias)

      tryCreateAliasedGenerator(collapsedMultiAlias)
    }
  }

  /**
   * Check if [[MultiAlias]] wraps [[Generator]] and throws an exception otherwise. [[MultiAlias]]
   * was originally created for [[Generator]]s that could have multiple outputs (e.g. posexplode).
   */
  private def validateMultiAliasHasGenerator(multiAlias: MultiAlias): Unit = {
    multiAlias.child match {
      case _: Generator =>
      case _: AliasedGenerator =>
      case child =>
        multiAlias.failAnalysis(
          errorClass = "MULTI_ALIAS_WITHOUT_GENERATOR",
          messageParameters = Map(
            "expr" -> toSQLExpr(child),
            "names" -> multiAlias.names.mkString(", ")
          )
        )
    }
  }

  /**
   * Collapse nested aliases under a [[MultiAlias]]. Similar to [[AliasResolver.collapseAlias]],
   * this removes any inner [[Alias]] or [[MultiAlias]] nodes, keeping only the outermost
   * [[MultiAlias]] with the innermost non-alias expression as its child.
   */
  private def collapseMultiAlias(multiAlias: MultiAlias): MultiAlias =
    multiAlias.child match {
      case innerAlias: Alias =>
        multiAlias.copy(child = innerAlias.child)
      case innerMultiAlias: MultiAlias =>
        multiAlias.copy(child = innerMultiAlias.child)
      case _ => multiAlias
    }
}
