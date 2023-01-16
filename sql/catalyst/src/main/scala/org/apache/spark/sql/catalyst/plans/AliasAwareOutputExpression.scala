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
  private val aliasCandidateLimit = conf.getConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT)
  protected def outputExpressions: Seq[NamedExpression]
  /**
   * This method can be used to strip expression which does not affect the result, for example:
   * strip the expression which is ordering agnostic for output ordering.
   */
  protected def strip(expr: Expression): Expression = expr

  // Split the alias map into 2 maps, the first contains `Expression` -> `Attribute` mappings where
  // any children of the `Expression` contains any other mapping. This because during
  // `normalizeExpression()` we will need to handle those maps separately and don't stop generating
  // alternatives at the `Expression` but we also need to traverse down to its children.
  private lazy val (exprAliasMap, attrAliasMap) = {
    val aliases = mutable.Map[Expression, mutable.ListBuffer[Attribute]]()
    // Add aliases to the map. If multiple alias is defined for a source attribute then add all.
    outputExpressions.foreach {
      case a @ Alias(child, _) =>
        // This prepend is needed to make the first element of the `ListBuffer` point to the last
        // occurrence of an aliased child. This is to keep the previous behavior and give precedence
        // the last Alias during `normalizeExpression()` to avoid any kind of regression.
        a.toAttribute +=:
          aliases.getOrElseUpdate(strip(child.canonicalized), mutable.ListBuffer.empty)
      case _ =>
    }
    // Append identity mapping of an attribute to the map if both the attribute and its aliased
    // version can be found in `outputExpressions`.
    outputExpressions.foreach {
      case a: Attribute if aliases.contains(a.canonicalized) => aliases(a.canonicalized) += a
      case _ =>
    }

    aliases.partition { case (expr, _) => expr.children.exists(_.exists(aliases.contains)) }
  }

  protected def hasAlias: Boolean = attrAliasMap.nonEmpty

  /**
   * Return a set of Expression which normalize the original expression to the aliased.
   * @param pruneFunc used to prune the alias-replaced expression whose references are not the
   *                 subset of output
   */
  protected def normalizeExpression(
      expr: Expression,
      pruneFunc: (Expression, AttributeSet) => Option[Expression]): Seq[Expression] = {
    val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))
    expr.multiTransformDown {
      case e: Expression if exprAliasMap.contains(e.canonicalized) =>
        (exprAliasMap(e.canonicalized) :+ e).toStream
      case e: Expression if attrAliasMap.contains(e.canonicalized) =>
        attrAliasMap(e.canonicalized).toStream
    }.flatMap { candidate =>
      if (candidate.references.subsetOf(outputSet)) {
        Some(candidate)
      } else {
        pruneFunc(candidate, outputSet)
      }
    }.take(aliasCandidateLimit)
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
    if (hasAlias) {
      orderingExpressions.map { sortOrder =>
        val normalized = normalizeExpression(sortOrder, (replacedExpr, outputExpressionSet) => {
          assert(replacedExpr.isInstanceOf[SortOrder])
          val sortOrder = replacedExpr.asInstanceOf[SortOrder]
          val pruned = sortOrder.children.filter { child =>
            child.references.subsetOf(outputExpressionSet)
          }
          if (pruned.isEmpty) {
            None
          } else {
            // All expressions after pruned are semantics equality, so just use head to build a new
            // SortOrder and use tail as the sameOrderExpressions.
            Some(SortOrder(pruned.head, sortOrder.direction, sortOrder.nullOrdering, pruned.tail))
          }
        })
        sortOrder.copy(sameOrderExpressions =
          normalized.flatMap(_.asInstanceOf[SortOrder].children))
      }
    } else {
      orderingExpressions
    }
  }
}
