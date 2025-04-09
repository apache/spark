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

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, TimeZoneAwareExpression}

/**
 * Resolves [[TimeZoneAwareExpressions]] by applying the session's local timezone.
 *
 * This class is responsible for resolving [[TimeZoneAwareExpression]]s by first resolving their
 * children and then applying the session's local timezone. Additionally, ensures that any tags from
 * the original expression are preserved during the resolution process.
 *
 * @constructor Creates a new TimezoneAwareExpressionResolver with the given expression resolver.
 * @param expressionResolver The [[ExpressionResolver]] used to resolve child expressions.
 */
class TimezoneAwareExpressionResolver(expressionResolver: TreeNodeResolver[Expression, Expression])
    extends TreeNodeResolver[TimeZoneAwareExpression, Expression]
    with ResolvesExpressionChildren {
  val typeCoercionResolver = new TypeCoercionResolver(this)

  /**
   * Resolves a [[TimeZoneAwareExpression]] by resolving its children and applying a timezone.
   * If the resolved expression is a [[Cast]], type coercion is not needed. Otherwise we apply
   * [[TypeCoercionResolver]] to the resolved expression.
   *
   * @param unresolvedTimezoneExpression The [[TimeZoneAwareExpression]] to resolve.
   * @return A resolved [[Expression]] with the session's local timezone applied, and optionally
   *   type coerced.
   */
  override def resolve(unresolvedTimezoneExpression: TimeZoneAwareExpression): Expression = {
    val expressionWithResolvedChildren =
      withResolvedChildren(unresolvedTimezoneExpression, expressionResolver.resolve _)
    withResolvedTimezone(expressionWithResolvedChildren, conf.sessionLocalTimeZone) match {
      case cast: Cast => cast
      case other => typeCoercionResolver.resolve(other)
    }
  }

  /**
   * Applies a timezone to a [[TimeZoneAwareExpression]] while preserving original tags.
   *
   * This method is particularly useful for cases like resolving [[Cast]] expressions where tags
   * such as [[USER_SPECIFIED_CAST]] need to be preserved.
   *
   * @param expression The [[TimeZoneAwareExpression]] to apply the timezone to.
   * @param timeZoneId The timezone ID to apply.
   * @return A new [[TimeZoneAwareExpression]] with the specified timezone and original tags.
   */
  def withResolvedTimezone(expression: Expression, timeZoneId: String): Expression =
    expression match {
      case timezoneExpression: TimeZoneAwareExpression if timezoneExpression.timeZoneId.isEmpty =>
        val withTimezone = timezoneExpression.withTimeZone(timeZoneId)
        withTimezone.copyTagsFrom(timezoneExpression)
        withTimezone
      case other => other
    }
}
