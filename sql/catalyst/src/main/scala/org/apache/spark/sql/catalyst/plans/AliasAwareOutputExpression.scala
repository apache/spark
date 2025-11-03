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

package org.apache.spark.sql.catalyst.plans

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Empty2Null, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.internal.SQLConf

/**
 * A trait that provides functionality to handle aliases in the `outputExpressions`.
 */
trait AliasAwareOutputExpression extends SQLConfHelper {
  protected val aliasCandidateLimit = conf.getConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT)
  protected def outputExpressions: Seq[NamedExpression]
  /**
   * This method can be used to strip expression which does not affect the result, for example:
   * strip the expression which is ordering agnostic for output ordering.
   */
  protected def strip(expr: Expression): Expression = expr

  // Build an `Expression` -> `Attribute` alias map.
  // There can be multiple alias defined for the same expressions but it doesn't make sense to store
  // more than `aliasCandidateLimit` attributes for an expression. In those cases the old logic
  // handled only the last alias so we need to make sure that we give precedence to that.
  // If the `outputExpressions` contain simple attributes we need to add those too to the map.
  @transient
  private lazy val aliasMap = {
    val aliases = mutable.Map[Expression, mutable.ArrayBuffer[Attribute]]()
    outputExpressions.reverse.foreach {
      case a @ Alias(child, _) =>
        val buffer = aliases.getOrElseUpdate(strip(child).canonicalized, mutable.ArrayBuffer.empty)
        if (buffer.size < aliasCandidateLimit) {
          buffer += a.toAttribute
        }
      case _ =>
    }
    outputExpressions.foreach {
      case a: Attribute if aliases.contains(a.canonicalized) =>
        val buffer = aliases(a.canonicalized)
        if (buffer.size < aliasCandidateLimit) {
          buffer += a
        }
      case _ =>
    }
    aliases
  }

  protected def hasAlias: Boolean = aliasMap.nonEmpty

  /**
   * Return a stream of expressions in which the original expression is projected with `aliasMap`.
   */
  protected def projectExpression(expr: Expression): LazyList[Expression] = {
    val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))
    expr.multiTransformDown {
      // Mapping with aliases
      case e: Expression if aliasMap.contains(e.canonicalized) =>
        aliasMap(e.canonicalized).toSeq ++ (if (e.containsChild.nonEmpty) Seq(e) else Seq.empty)

      // Prune if we encounter an attribute that we can't map and it is not in output set.
      // This prune will go up to the closest `multiTransformDown()` call and returns `Stream.empty`
      // there.
      case a: Attribute if !outputSet.contains(a) => Seq.empty
    }
  }
}

/**
 * A trait that handles aliases in the `orderingExpressions` to produce `outputOrdering` that
 * satisfies ordering requirements.
 */
trait AliasAwareQueryOutputOrdering[T <: QueryPlan[T]]
  extends AliasAwareOutputExpression { self: QueryPlan[T] =>
  protected def orderingExpressions: Seq[SortOrder]

  override protected def strip(expr: Expression): Expression = expr match {
    case e: Empty2Null => strip(e.child)
    case _ => expr
  }

  override final def outputOrdering: Seq[SortOrder] = {
    val newOrdering: Iterator[Option[SortOrder]] = if (hasAlias) {
      // Take the first `SortOrder`s only until they can be projected.
      // E.g. we have child ordering `Seq(SortOrder(a), SortOrder(b))` then
      // if only `a AS x` can be projected then we can return Seq(SortOrder(x))`
      // but if only `b AS y` can be projected we can't return `Seq(SortOrder(y))`.
      orderingExpressions.iterator.map { sortOrder =>
        val orderingSet = mutable.Set.empty[Expression]
        val sameOrderings = sortOrder.children.to(LazyList)
          .flatMap(projectExpression)
          .filter(e => orderingSet.add(e.canonicalized))
          .take(aliasCandidateLimit)
        if (sameOrderings.nonEmpty) {
          Some(sortOrder.copy(child = sameOrderings.head,
            sameOrderExpressions = sameOrderings.tail))
        } else {
          None
        }
      }
    } else {
      // Make sure the returned ordering are valid (only reference output attributes of the current
      // plan node). Same as above (the if branch), we take the first ordering expressions that are
      // all valid.
      val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))
      orderingExpressions.iterator.map { order =>
        val validChildren = order.children.filter(_.references.subsetOf(outputSet))
        if (validChildren.nonEmpty) {
          Some(order.copy(child = validChildren.head, sameOrderExpressions = validChildren.tail))
        } else {
          None
        }
      }
    }
    newOrdering.takeWhile(_.isDefined).flatten.toSeq
  }
}
