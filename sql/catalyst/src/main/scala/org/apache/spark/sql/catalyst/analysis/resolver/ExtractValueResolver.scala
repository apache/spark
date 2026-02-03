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

import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExtractValue}

/**
 * Resolver for [[UnresolvedExtractValue]]. Resolve the [[UnresolvedExtractValue]] by resolving its
 * extraction key and child.
 *
 * [[UnresolvedExtractValue]] is constructed in parser, when indexing an
 * array or a map with a key. For example, in the following query:
 *
 * {{{
 * SELECT col1.a, col2[0], col3['a']
 * FROM VALUES (named_struct('a', 1, 'b', 2), array(1,2,3), map('a', 1, 'b', 2));
 * }}}
 *
 * -  `col1.a` is parsed as [[UnresolvedAttribute]]
 * -  `col2[0]` is parsed as [[UnresolvedExtractValue]] with `col2` as child and `0` as extraction
 *    key.
 * -  `col3['a']` is parsed as [[UnresolvedExtractValue]] with `col3` as child and `'a'` as
 *    extraction key.
 */
class ExtractValueResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[UnresolvedExtractValue, Expression]
    with CollectsWindowSourceExpressions
    with CoercesExpressionTypes {
  protected val windowResolutionContextStack = expressionResolver.getWindowResolutionContextStack
  private val traversals = expressionResolver.getExpressionTreeTraversals
  private val expressionResolutionContextStack =
    expressionResolver.getExpressionResolutionContextStack

  /**
   * Resolves [[UnresolvedExtractValue]] by first resolving its extraction key and then its child.
   * After resolving extraction key, we put the resolved key in current resolution context in order
   * to allow [[NameScope]] to resolve attributes that are inside the [[ExtractValue]] expression.
   *
   * Handle the resolved [[ExtractValue]] by type coercing it and collecting it for window
   * resolution, if needed.
   */
  def resolve(unresolvedExtractValue: UnresolvedExtractValue): Expression = {
    val resolvedExtractionKey = expressionResolver.resolve(unresolvedExtractValue.extraction)

    expressionResolutionContextStack.peek().extractValueExtractionKey = Some(resolvedExtractionKey)
    val resolvedChild = try {
      expressionResolver.resolve(unresolvedExtractValue.child)
    } finally {
      expressionResolutionContextStack.peek().extractValueExtractionKey = None
    }

    val resolvedExtractValue = ExtractValue.apply(
      child = resolvedChild,
      extraction = resolvedExtractionKey,
      resolver = conf.resolver
    )

    resolvedExtractValue match {
      case extractValue: ExtractValue => handleResolvedExtractValue(extractValue)
      case other => other
    }
  }

  /**
   * Coerces recursive types ([[ExtractValue]] expressions) in a bottom up manner and collects
   * attribute references required for window resolution. For example:
   *
   * {{{
   * CREATE OR REPLACE TABLE t(col MAP<BIGINT, DOUBLE>);
   * SELECT col.field FROM t;
   * }}}
   *
   * In this example we need to cast inner field from `String` to `BIGINT`, thus analyzed plan
   * should look like:
   *
   * {{{
   * Project [col#x[cast(field as bigint)] AS field#x]
   * +- SubqueryAlias spark_catalog.default.t
   *    +- Relation spark_catalog.default.t[col#x] parquet
   * }}}
   *
   * This is needed to stay compatible with the fixed-point implementation.
   */
  def handleResolvedExtractValue(extractValue: ExtractValue): Expression = {
    extractValue.transformUp {
      case attributeReference: AttributeReference =>
        collectWindowSourceExpression(
          expression = attributeReference,
          parentOperator = traversals.current.parentOperator
        )
        coerceExpressionTypes(attributeReference, traversals.current)
      case field =>
        coerceExpressionTypes(field, traversals.current)
    }
  }
}
