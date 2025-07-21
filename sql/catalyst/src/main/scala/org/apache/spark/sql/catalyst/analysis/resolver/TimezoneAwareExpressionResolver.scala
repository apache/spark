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

import org.apache.spark.sql.catalyst.expressions.{
  Cast,
  DefaultStringProducingExpression,
  Expression,
  TimeZoneAwareExpression
}
import org.apache.spark.sql.types.StringType

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
class TimezoneAwareExpressionResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[TimeZoneAwareExpression, Expression]
    with ResolvesExpressionChildren
    with CoercesExpressionTypes {

  private val traversals = expressionResolver.getExpressionTreeTraversals

  /**
   * Resolves a [[TimeZoneAwareExpression]] by resolving its children, applying a timezone
   * and calling [[coerceExpressionTypes]] on the result. If the expression is a [[Cast]], we apply
   * [[collapseCast]] to the result.
   *
   * @param unresolvedTimezoneExpression The [[TimeZoneAwareExpression]] to resolve.
   * @return A resolved [[Expression]] with the session's local timezone applied, and optionally
   *   type coerced.
   */
  override def resolve(unresolvedTimezoneExpression: TimeZoneAwareExpression): Expression = {
    val expressionWithResolvedChildren =
      withResolvedChildren(unresolvedTimezoneExpression, expressionResolver.resolve _)
    val expressionWithResolvedChildrenAndTimeZone = TimezoneAwareExpressionResolver.resolveTimezone(
      expressionWithResolvedChildren,
      traversals.current.sessionLocalTimeZone
    )
    coerceExpressionTypes(
      expression = expressionWithResolvedChildrenAndTimeZone,
      expressionTreeTraversal = traversals.current
    ) match {
      case cast: Cast if traversals.current.defaultCollation.isDefined =>
        tryCollapseCast(cast, traversals.current.defaultCollation.get)
      case other =>
        other
    }
  }

  /**
   * When a [[View]] has a custom default collation, we update the [[Cast]]'s [[DataType]] by
   * replacing all occurrences of the companion object [[StringType]] with [[StringType]] with the
   * default collation.
   *
   * Special case:
   * Before resolution, if the [[Cast]]'s child is a [[DefaultStringProducingExpression]] and the
   * custom default collation is not `UTF8_BINARY`, then the [[DefaultStringProducingExpression]]
   * will be wrapped with a [[Cast]] to [[StringType]] with the default collation. Additionally,
   * if the currently resolving [[Cast]] is also a [[Cast]] to the companion object [[StringType]],
   * we update it to a [[Cast]] to [[StringType]] with the default collation as well.
   *
   * This results in two identical [[Cast]]s, which is redundant-so we can remove one of them.
   * We are doing this to have the same plan as the one from the fixed-point analyzer.
   *
   * Example query for the special case:
   * {{{
   * CREATE VIEW v
   * DEFAULT COLLATION UNICODE
   * AS SELECT current_database()::STRING AS c1
   * }}}
   *
   * Plan for the `AS query` part:
   * Project [cast(c1#2 as string collate UNICODE) AS c1#3]
   * +- Project [cast(current_schema() as string collate UNICODE) AS c1#2]
   *    +- OneRowRelation
   */
  private def tryCollapseCast(cast: Cast, defaultCollation: String): Cast = {
    cast.child match {
      case childCast: Cast if shouldCollapseInnerCast(cast, defaultCollation) =>
        cast.copy(child = childCast.child)
      case _ =>
        cast
    }
  }

  private def shouldCollapseInnerCast(cast: Cast, defaultCollation: String): Boolean = {
    StringType(defaultCollation) != StringType && {
      cast.child match {
        case _ @Cast(_: DefaultStringProducingExpression, innerDataType, _, _)
            if cast.dataType == innerDataType =>
          true
        case _ =>
          false
      }
    }
  }
}

object TimezoneAwareExpressionResolver {

  /**
   * Applies a timezone to a [[TimeZoneAwareExpression]] while preserving original tags.
   *
   * Method is applied recursively to all the nested [[TimeZoneAwareExpression]]s which lack a
   * timezone until we find one which has it. This is because sometimes type coercion rules (or
   * other code) can produce multiple [[Cast]]s on top of an expression. For example:
   *
   * {{{ SELECT NANVL(1, null); }}}
   *
   * Plan:
   *
   * {{{
   * Project [nanvl(cast(1 as double), cast(cast(null as int) as double)) AS nanvl(1, NULL)#0]
   * +- OneRowRelation
   * }}}
   *
   * As it can be seen, there are multiple nested [[Cast]] nodes and timezone should be applied to
   * all of them.
   *
   * This method is particularly useful for cases like resolving [[Cast]] expressions where tags
   * such as [[USER_SPECIFIED_CAST]] need to be preserved.
   *
   * @param expression The [[TimeZoneAwareExpression]] to apply the timezone to.
   * @param timeZoneId The timezone ID to apply.
   * @return A new [[TimeZoneAwareExpression]] with the specified timezone and original tags.
   */
  def resolveTimezone(expression: Expression, timeZoneId: String): Expression = {
    expression match {
      case timezoneExpression: TimeZoneAwareExpression if timezoneExpression.timeZoneId.isEmpty =>
        val childrenWithTimeZone = timezoneExpression.children.map { child =>
          resolveTimezone(child, timeZoneId)
        }
        val withNewChildren = timezoneExpression
          .withNewChildren(childrenWithTimeZone)
          .asInstanceOf[TimeZoneAwareExpression]
        val withTimezone = withNewChildren.withTimeZone(timeZoneId)
        withTimezone.copyTagsFrom(timezoneExpression)
        withTimezone
      case other => other
    }
  }
}
