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

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}

/**
 * [[TypeCoercionResolver]] is used by other resolvers to uniformly apply type coercions to all
 * expressions. [[TypeCoercionResolver]] takes in a sequence of type coercion transformations that
 * should be applied to an expression and applies them in order. Finally, [[TypeCoercionResolver]]
 * applies timezone to expression's children, as a child could be replaced with Cast(child, type),
 * therefore [[Cast]] resolution is needed. Timezone is applied only on children that have been
 * re-instantiated by [[TypeCoercionResolver]], because otherwise children have already been
 * resolved.
 */
class TypeCoercionResolver(
    timezoneAwareExpressionResolver: TimezoneAwareExpressionResolver,
    typeCoercionRules: Seq[Expression => Expression])
    extends TreeNodeResolver[Expression, Expression] {

  override def resolve(expression: Expression): Expression = {
    val oldChildren = expression.children

    val withTypeCoercion = typeCoercionRules.foldLeft(expression) {
      case (expr, rule) => rule.apply(expr)
    }

    val newChildren = withTypeCoercion.children.zip(oldChildren).map {
      case (newChild: Cast, oldChild) if !newChild.eq(oldChild) =>
        timezoneAwareExpressionResolver.withResolvedTimezone(newChild, conf.sessionLocalTimeZone)
      case (newChild, _) => newChild
    }
    withTypeCoercion.withNewChildren(newChildren)
  }
}
