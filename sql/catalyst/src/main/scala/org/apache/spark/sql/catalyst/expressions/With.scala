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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, TreePattern, WITH_EXPRESSION}
import org.apache.spark.sql.types.DataType

/**
 * An expression holder that keeps a list of common expressions and allow the actual expression to
 * reference these common expressions. The common expressions are guaranteed to be evaluated only
 * once even if it's referenced more than once. This is similar to CTE but is expression-level.
 */
case class With(child: Expression, defs: Seq[CommonExpressionDef])
  extends Expression with Unevaluable {
  // We do not allow creating a With expression with an AggregateExpression that contains a
  // reference to a common expression defined in that scope (note that it can contain another With
  // expression with a common expression ref of the inner With). This is to prevent the creation of
  // a dangling CommonExpressionRef after rewriting it in RewriteWithExpression.
  assert(!With.childContainsUnsupportedAggExpr(this))

  override val nodePatterns: Seq[TreePattern] = Seq(WITH_EXPRESSION)
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def children: Seq[Expression] = child +: defs
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(child = newChildren.head, defs = newChildren.tail.map(_.asInstanceOf[CommonExpressionDef]))
  }

  /**
   * Builds a map of ids (originally assigned ids -> canonicalized ids) to be re-assigned during
   * canonicalization.
   */
  private lazy val canonicalizationIdMap: Map[Long, Long] = {
    // Start numbering after taking into account all nested With expression id maps.
    var currentId = child.map {
      case w: With => w.canonicalizationIdMap.size
      case _ => 0L
    }.sum
    defs.map { d =>
      currentId += 1
      d.id.id -> currentId
    }.toMap
  }

  /**
   * Canonicalize by re-assigning all ids in CommonExpressionRef's and CommonExpressionDef's
   * starting from 0. This uses [[canonicalizationIdMap]], which contains all mappings for
   * CommonExpressionDef's defined in this scope.
   * Note that this takes into account nested With expressions by sharing a numbering scope (see
   * [[canonicalizationIdMap]].
   */
  override lazy val canonicalized: Expression = copy(
    child = child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
      case r: CommonExpressionRef if !r.id.canonicalized =>
        r.copy(id = r.id.canonicalize(canonicalizationIdMap))
    }.canonicalized,
    defs = defs.map {
      case d: CommonExpressionDef if !d.id.canonicalized =>
        d.copy(id = d.id.canonicalize(canonicalizationIdMap)).canonicalized
          .asInstanceOf[CommonExpressionDef]
      case d => d.canonicalized.asInstanceOf[CommonExpressionDef]
    }
  )
}

object With {
  /**
   * Helper function to create a [[With]] statement with an arbitrary number of common expressions.
   * Note that the number of arguments in `commonExprs` should be the same as the number of
   * arguments taken by `replaced`.
   *
   * @param commonExprs list of common expressions
   * @param replaced    closure that defines the common expressions in the main expression
   * @return the expression returned by replaced with its arguments replaced by commonExprs in order
   */
  def apply(commonExprs: Expression*)(replaced: Seq[Expression] => Expression): With = {
    val commonExprDefs = commonExprs.map(CommonExpressionDef(_))
    val commonExprRefs = commonExprDefs.map(new CommonExpressionRef(_))
    With(replaced(commonExprRefs), commonExprDefs)
  }

  private[sql] def childContainsUnsupportedAggExpr(withExpr: With): Boolean = {
    lazy val commonExprIds = withExpr.defs.map(_.id).toSet
    withExpr.child.exists {
      case agg: AggregateExpression =>
        // Check that the aggregate expression does not contain a reference to a common expression
        // in the outer With expression (it is ok if it contains a reference to a common expression
        // for a nested With expression).
        agg.exists {
          case r: CommonExpressionRef => commonExprIds.contains(r.id)
          case _ => false
        }
      case _ => false
    }
  }
}

case class CommonExpressionId(id: Long = CommonExpressionId.newId, canonicalized: Boolean = false) {
  /**
   * Re-assign to a canonicalized id based on idMap. If it is not found in idMap, the id is defined
   * in an outer scope and will be replaced later.
   */
  def canonicalize(idMap: Map[Long, Long]): CommonExpressionId = {
    if (idMap.contains(id)) {
      copy(id = idMap(id), canonicalized = true)
    } else {
      this
    }
  }
}

object CommonExpressionId {
  private[sql] val curId = new java.util.concurrent.atomic.AtomicLong()
  def newId: Long = curId.getAndIncrement()
}

/**
 * A wrapper of common expression to carry the id.
 */
case class CommonExpressionDef(child: Expression, id: CommonExpressionId = new CommonExpressionId())
  extends UnaryExpression with Unevaluable {
  override def dataType: DataType = child.dataType
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

/**
 * A reference to the common expression by its id. Only resolved common expressions can be
 * referenced, so that we can determine the data type and nullable of the reference node.
 */
case class CommonExpressionRef(id: CommonExpressionId, dataType: DataType, nullable: Boolean)
  extends LeafExpression with Unevaluable {
  def this(exprDef: CommonExpressionDef) = this(exprDef.id, exprDef.dataType, exprDef.nullable)
  override val nodePatterns: Seq[TreePattern] = Seq(COMMON_EXPR_REF)
}
