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

  private lazy val aliasMap: Map[Expression, ArrayBuffer[Attribute]] = {
    if (aliasCandidateLimit < 1) {
      Map.empty
    } else {
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
          updateAttrWithAliasMap(child, a.toAttribute)
        case _ =>
      }
      exprWithAliasMap.toMap
    }
  }

  protected def hasAlias: Boolean = aliasMap.nonEmpty

  /**
   * Return a set of Expression which normalize the original expression to the aliased.
   * @param pruneFunc used to prune the alias-replaced expression whose references are not the
   *                 subset of output
   */
  protected def normalizeExpression(
      expr: Expression,
      pruneFunc: (Expression, AttributeSet) => Option[Expression]): Seq[Expression] = {
    val normalizedCandidates = new mutable.HashSet[Expression]()
    normalizedCandidates.add(expr)
    val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))

    def pruneCandidate(candidate: Expression): Option[Expression] = {
      if (candidate.references.subsetOf(outputSet)) {
        Some(candidate)
      } else {
        pruneFunc(candidate, outputSet)
      }
    }

    // Stop loop if the size of candidates exceed limit
    for ((origin, aliases) <- aliasMap if normalizedCandidates.size <= aliasCandidateLimit) {
      for (alias <- aliases if normalizedCandidates.size <= aliasCandidateLimit) {
        val localCandidates = normalizedCandidates.toArray
        for (candidate <- localCandidates if normalizedCandidates.size <= aliasCandidateLimit) {
          var hasOtherAlias = false
          val newCandidate = candidate.transformDown {
            case e: Expression if e.canonicalized == origin => alias
            case e if aliasMap.contains(e.canonicalized) =>
              hasOtherAlias = true
              e
          }

          if (!candidate.fastEquals(newCandidate)) {
            if (!hasOtherAlias) {
              // If there is no other alias, we can do eagerly pruning to speed up
              pruneCandidate(newCandidate).foreach(e => normalizedCandidates.add(e))
            } else {
              // We can not do pruning at this branch because we may miss replace alias with
              // other sub-expression. We can only prune after all alias replaced.
              normalizedCandidates.add(newCandidate)
            }
          }
        }
      }
    }

    val pruned = normalizedCandidates.flatMap { candidate =>
      pruneCandidate(candidate)
    }
    if (pruned.isEmpty) {
      expr :: Nil
    } else {
      pruned.toSeq
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
