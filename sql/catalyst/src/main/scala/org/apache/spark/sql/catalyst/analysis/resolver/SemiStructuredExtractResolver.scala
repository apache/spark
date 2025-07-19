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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SemiStructuredExtract}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.VariantType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Resolver for [[SemiStructuredExtract]]. Resolves [[SemiStructuredExtract]] by resolving its
 * children, replacing it with the proper semi-structured field extraction method and applying type
 * coercion to the result.
 */
class SemiStructuredExtractResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[SemiStructuredExtract, Expression]
    with ResolvesExpressionChildren
    with CoercesExpressionTypes {

  private val timezoneAwareExpressionResolver =
    expressionResolver.getTimezoneAwareExpressionResolver

  /**
   * Resolves children and replaces [[SemiStructuredExtract]] expressions with the proper
   * semi-structured field extraction method depending on column type. In case the column is of
   * [[VariantType]], applies timezone to the result of the previous step.
   *
   * Currently only JSON is supported as an extraction method. An important distinction here with
   * other JSON extraction methods is that the extraction fields provided here should be
   * case-insensitive, unless explicitly stated through quoting.
   *
   * After replacing with proper extraction method, apply type coercion to the result.
   */
  override def resolve(semiStructuredExtract: SemiStructuredExtract): Expression = {
    val semiStructuredExtractWithResolvedChildren =
      withResolvedChildren(semiStructuredExtract, expressionResolver.resolve _)
        .asInstanceOf[SemiStructuredExtract]

    val semiStructuredExtractWithProperExtractionMethod =
      semiStructuredExtractWithResolvedChildren.child.dataType match {
        case _: VariantType =>
          val extractResult = VariantGet(
            child = semiStructuredExtractWithResolvedChildren.child,
            path = Literal(UTF8String.fromString(semiStructuredExtractWithResolvedChildren.field)),
            targetType = VariantType,
            failOnError = true
          )
          timezoneAwareExpressionResolver.resolve(extractResult)
        case _ =>
          throw new AnalysisException(
            errorClass = "COLUMN_IS_NOT_VARIANT_TYPE",
            messageParameters = Map.empty
          )
      }

    coerceExpressionTypes(
      expression = semiStructuredExtractWithProperExtractionMethod,
      expressionTreeTraversal = expressionResolver.getExpressionTreeTraversals.current
    )
  }
}
