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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}
import org.apache.spark.sql.types.Metadata

/**
 * Helper methods for collecting and replacing aliases.
 */
trait AliasHelper {

  protected def getAliasMap(plan: Project): AttributeMap[Alias] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> Alias(a + b, c)).
    getAliasMap(plan.projectList)
  }

  protected def getAliasMap(plan: Aggregate): AttributeMap[Alias] = {
    // Find all the aliased expressions in the aggregate list that don't include any actual
    // AggregateExpression or PythonUDF, and create a map from the alias to the expression
    val aliasMap = plan.aggregateExpressions.collect {
      case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
        (a.toAttribute, a)
    }
    AttributeMap(aliasMap)
  }

  protected def getAliasMap(exprs: Seq[NamedExpression]): AttributeMap[Alias] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> Alias(a + b, c)).
    AttributeMap(exprs.collect { case a: Alias => (a.toAttribute, a) })
  }

  /**
   * Replace all attributes, that reference an alias, with the aliased expression
   */
  protected def replaceAlias(
      expr: Expression,
      aliasMap: AttributeMap[Alias]): Expression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId,
    trimAliases(expr.transformUp {
      case a: Attribute => aliasMap.getOrElse(a, a)
    })
  }

  /**
   * Replace all attributes, that reference an alias, with the aliased expression,
   * but keep the name of the outermost attribute.
   */
  protected def replaceAliasButKeepName(
     expr: NamedExpression,
     aliasMap: AttributeMap[Alias]): NamedExpression = {
    expr match {
      // We need to keep the `Alias` if we replace a top-level Attribute, so that it's still a
      // `NamedExpression`. We also need to keep the name of the original Attribute.
      case a: Attribute => aliasMap.get(a).map(_.withName(a.name)).getOrElse(a)
      case o =>
        // Use transformUp to prevent infinite recursion when the replacement expression
        // redefines the same ExprId.
        o.mapChildren(_.transformUp {
          case a: Attribute => aliasMap.get(a).map(_.child).getOrElse(a)
        }).asInstanceOf[NamedExpression]
    }
  }

  protected def trimAliases(e: Expression): Expression = e match {
    // The children of `CreateNamedStruct` may use `Alias` to carry metadata and we should not
    // trim them.
    case c: CreateNamedStruct => c.mapChildren {
      case a: Alias if a.metadata != Metadata.empty => a
      case other => trimAliases(other)
    }
    case a @ Alias(child, _) => trimAliases(child)
    case MultiAlias(child, _) => trimAliases(child)
    case other => other.mapChildren(trimAliases)
  }

  protected def trimNonTopLevelAliases[T <: Expression](e: T): T = {
    val res = e match {
      case a: Alias =>
        val metadata = if (a.metadata == Metadata.empty) {
          None
        } else {
          Some(a.metadata)
        }
        a.copy(child = trimAliases(a.child))(
          exprId = a.exprId,
          qualifier = a.qualifier,
          explicitMetadata = metadata,
          nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)
      case a: MultiAlias =>
        a.copy(child = trimAliases(a.child))
      case other => trimAliases(other)
    }

    res.asInstanceOf[T]
  }
}
