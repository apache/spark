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

import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, Expression}

/**
 * Resolves [[CreateNamedStruct]] nodes by recursively resolving children. If [[CreateNamedStruct]]
 * is not directly under an [[Alias]], removes aliases from struct fields. Otherwise, let
 * [[AliasResolver]] handle the removal.
 */
class CreateNamedStructResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[CreateNamedStruct, Expression]
    with ResolvesExpressionChildren {

  override def resolve(createNamedStruct: CreateNamedStruct): Expression = {
    val createNamedStructWithResolvedChildren =
      withResolvedChildren(createNamedStruct, expressionResolver.resolve)
    CreateNamedStructResolver.cleanupAliases(createNamedStructWithResolvedChildren)
  }
}

object CreateNamedStructResolver {

  /**
   * For a query like:
   *
   * {{{ SELECT STRUCT(1 AS a, 2 AS b) }}}
   *
   * [[CreateNamedStruct]] will be: CreateNamedStruct(Seq("a", Alias(1, "a"), "b", Alias(2, "b")))
   *
   * Because inner aliases are not expected in the analyzed logical plan, we need to remove them
   * here. However, we only do so if [[CreateNamedStruct]] is not directly under an [[Alias]], in
   * which case the removal will be handled by [[AliasResolver]]. This is because in single-pass,
   * [[Alias]] is resolved after [[CreateNamedStruct]] and in order to compute the correct output
   * name, it needs to know complete structure of the child.
   */
  def cleanupAliases(createNamedStruct: CreateNamedStruct): CreateNamedStruct = {
    createNamedStruct
      .withNewChildren(createNamedStruct.children.map {
        case a: Alias if a.metadata.isEmpty => a.child
        case other => other
      })
      .asInstanceOf[CreateNamedStruct]
  }
}
