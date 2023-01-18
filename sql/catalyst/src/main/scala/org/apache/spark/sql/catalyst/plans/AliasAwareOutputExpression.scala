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
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.trees.MultiTransformHelper
import org.apache.spark.sql.internal.SQLConf

/**
 * A trait that provides functionality to handle aliases in the `outputExpressions`.
 */
trait AliasAwareOutputExpression extends SQLConfHelper with MultiTransformHelper {
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
   */
  protected def normalizeExpression(expr: Expression): Seq[Expression] = {
    val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))

    def f: PartialFunction[Expression, Stream[Expression]] = {
      // Mapping with aliases
      case e: Expression if exprAliasMap.contains(e.canonicalized) =>
        (exprAliasMap(e.canonicalized) :+ e).toStream
      case e: Expression if attrAliasMap.contains(e.canonicalized) =>
        attrAliasMap(e.canonicalized).toStream

      // Prune if we encounter an attribute that we can't map and it is not in output set.
      // This prune will go up to the closest `multiTransformDown()` call and returns `Stream.empty`
      // there.
      case a: Attribute if !outputSet.contains(a) => Stream.empty

      // Remove `PartitioningCollection` elements that are expressions and contain an attribute that
      // can't be mapped and the node's output set doesn't contain the attribute.
      // To achieve this we need to "restart" `multiTransformDown()` for each expression child and
      // filter out empty streams due to the above attribute pruning case.
      // The child streams can be then combined using `generateChildrenSeq()` into one stream as
      // `multiTransformDown()` would also do (but without filtering empty streams).
      case p: PartitioningCollection =>
        val childrenStreams = p.partitionings.map {
          case e: Expression => e.multiTransformDown(f).asInstanceOf[Stream[Partitioning]]
          case o => Stream(o)
        }.filter(_.nonEmpty)
        generateChildrenSeq(childrenStreams).flatMap {
          case Nil => None
          // We might have an expression type partitioning that doesn't need
          // `PartitioningCollection`
          case (p: Expression) :: Nil => Some(p)
          case p :: Nil => Some(PartitioningCollection(Seq(p)))
          case ps => Some(PartitioningCollection(ps))
        }

      // Filter `SortOrder` children similarly to `PartitioningCollection` elements
      case s: SortOrder =>
        val childrenStreams = s.children.map(_.multiTransformDown(f)).filter(_.nonEmpty)
        generateChildrenSeq(childrenStreams)
          .filter(_.nonEmpty)
          .map(es => s.copy(child = es.head, sameOrderExpressions = es.tail))
    }

    expr.multiTransformDown(f).take(aliasCandidateLimit)
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
      orderingExpressions.flatMap { sortOrder =>
        val normalized = normalizeExpression(sortOrder)
        val allOrderingExpressions = normalized.flatMap(_.asInstanceOf[SortOrder].children)
        if (allOrderingExpressions.isEmpty) {
          None
        } else {
          Some(sortOrder.copy(child = allOrderingExpressions.head,
            sameOrderExpressions = allOrderingExpressions.tail))
        }
      }
    } else {
      orderingExpressions
    }
  }
}
