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

import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.logical.{CoGroup, LogicalPlan, MapGroups}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType


/**
 * An expression that has to be resolved against a scope of resolved attributes.
 */
case class ScopedExpression(expr: Expression, scope: Seq[Attribute])
  extends Expression with Unevaluable {
  override def children: Seq[Expression] = expr +: scope
  override def dataType: DataType = expr.dataType
  override def nullable: Boolean = expr.nullable
  override def prettyName: String = "scoped"
  override def sql: String = s"$prettyName(${expr.sql}, $scope)"
  override lazy val resolved: Boolean = expr.resolved

  def mapScope(f: Expression => Expression): Expression = {
    // similar to mapChildren, but only for the scope children
    if (scope.nonEmpty) {
      this.copy(scope = scope.map(f).map(_.asInstanceOf[Attribute]))
    } else {
      this
    }
  }

  override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): Expression = {
    val scope = children.tail
    assert(scope.forall(_.isInstanceOf[Attribute]), "Scope children have to be attributes")
    copy(expr = children.head, scope = scope.map(_.asInstanceOf[Attribute]))
  }
}

/**
 * Restricts the scope of resolving some expressions.
 */
object ScopeExpressions extends Rule[LogicalPlan] {
  private def scopeOrder(scope: Seq[Attribute])(sortOrder: SortOrder): SortOrder = {
    sortOrder match {
      case so if so.child.isInstanceOf[ScopedExpression] => so
      case so => so.copy(
        child = ScopedExpression(so.child, scope),
        sameOrderExpressions = so.sameOrderExpressions.map(soe => ScopedExpression(soe, scope))
      )
    }
  }

  private def isNotScoped(sortOrder: SortOrder): Boolean =
    !sortOrder.child.isInstanceOf[ScopedExpression]

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    // SPARK-42199: sort order of MapGroups must be scoped to their dataAttributes
    case mg: MapGroups if mg.dataOrder.exists(isNotScoped) =>
      mg.copy(dataOrder = mg.dataOrder.map(scopeOrder(mg.dataAttributes)))

    // SPARK-42199: sort order of CoGroups must be scoped to their respective dataAttributes
    case cg: CoGroup if Seq(cg.leftOrder, cg.rightOrder).exists(_.exists(isNotScoped)) =>
      val scopedLeftOrder = cg.leftOrder.map(scopeOrder(cg.leftAttr))
      val scopedRightOrder = cg.rightOrder.map(scopeOrder(cg.rightAttr))
      cg.copy(leftOrder = scopedLeftOrder, rightOrder = scopedRightOrder)
  }
}

/**
 * Resolves expressions against their scope of attributes.
 */
class ResolveScopedExpression(val resolver: Resolver) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressions {
    case se: ScopedExpression if se.resolved => se.expr
    case se @ ScopedExpression(expr, attributes) =>
      val resolved = expr.transformDown {
        case u@UnresolvedAttribute(nameParts) =>
          attributes.resolve(nameParts, resolver).getOrElse(u)
      }
      if (resolved.fastEquals(expr)) {
        se
      } else {
        resolved
      }
  }
}

