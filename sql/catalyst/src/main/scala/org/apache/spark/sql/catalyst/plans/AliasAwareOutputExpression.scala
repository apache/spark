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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Empty2Null, Expression, ExpressionSet, NamedExpression, SortOrder}
import org.apache.spark.sql.internal.SQLConf

/**
 * A trait that provides functionality to handle aliases in the `outputExpressions`.
 */
trait AliasAwareOutputExpression extends SQLConfHelper {
  private val aliasCandidateLimit = conf.getConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT)
  private var _hasAlias = false
  protected def outputExpressions: Seq[NamedExpression]

  /**
   * This method is used to strip expression which does not affect the result, for example:
   * strip the expression which is ordering agnostic for output ordering.
   */
  protected def strip(expr: Expression): Expression = expr

  protected lazy val aliasMap: Map[Expression, ArrayBuffer[Attribute]] = {
    if (aliasCandidateLimit < 1) {
      Map.empty
    } else {
      val outputExpressionSet = AttributeSet(outputExpressions.map(_.toAttribute))
      val exprWithAliasMap = new mutable.HashMap[Expression, ArrayBuffer[Attribute]]()

      def updateAttrWithAliasMap(key: Expression, target: Attribute): Unit = {
        val aliasArray = exprWithAliasMap.getOrElseUpdate(
          strip(key).canonicalized, new ArrayBuffer[Attribute]())
        // pre-filter if the number of alias exceed candidate limit
        if (aliasArray.size < aliasCandidateLimit) {
          aliasArray.append(target)
        }
      }

      outputExpressions.foreach {
        case a @ Alias(child, _) =>
          _hasAlias = true
          updateAttrWithAliasMap(child, a.toAttribute)
        case a: Attribute if outputExpressionSet.contains(a) =>
          updateAttrWithAliasMap(a, a)
        case _ =>
      }
      exprWithAliasMap.toMap
    }
  }

  protected def hasAlias: Boolean = {
    aliasMap
    _hasAlias
  }

  /**
   * Return a set of Expression which normalize the original expression to the aliased.
   */
  protected def normalizeExpression(expr: Expression): Seq[Expression] = {
    val normalizedCandidates = expr.multiTransformDown {
      case e: Expression if aliasMap.contains(e.canonicalized) =>
        val candidates = aliasMap(e.canonicalized)
        (candidates :+ e).toStream
    }.take(aliasCandidateLimit)

    if (normalizedCandidates.isEmpty) {
      expr :: Nil
    } else {
      normalizedCandidates.toSeq
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
    if (hasAlias) {
      orderingExpressions.map { sortOrder =>
        val normalized = normalizeExpression(sortOrder)
        assert(normalized.forall(_.isInstanceOf[SortOrder]))
        val pruned = ExpressionSet(normalized.flatMap {
          case s: SortOrder => s.children.filter(_.references.subsetOf(outputSet))
        })
        if (pruned.isEmpty) {
          sortOrder
        } else {
          // All expressions after pruned are semantics equality, so just use head to build a new
          // SortOrder and use tail as the sameOrderExpressions.
          SortOrder(pruned.head, sortOrder.direction, sortOrder.nullOrdering, pruned.tail.toSeq)
        }
      }
    } else {
      orderingExpressions
    }
  }
}
