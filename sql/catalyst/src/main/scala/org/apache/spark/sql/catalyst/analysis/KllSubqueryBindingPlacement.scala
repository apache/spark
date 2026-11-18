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

package org.apache.spark.sql.catalyst.analysis

import java.util.IdentityHashMap

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical._

/** Shared placement boundaries for newly extracted and already bound independent subqueries. */
private[sql] final class KllSubqueryBindingPlacement {
  private val forwarding = new IdentityHashMap[Project, java.lang.Boolean]
  private val safeComputed = new IdentityHashMap[Project, java.lang.Boolean]

  private def forwardsAttributes(project: Project): Boolean = forwarding.computeIfAbsent(project,
    p => p.projectList.forall(_.isInstanceOf[Attribute])).booleanValue()

  private def safeProjection(project: Project): Boolean = safeComputed.computeIfAbsent(project,
    p => p.projectList.forall(e => e.deterministic && !e.throwable)).booleanValue()

  def mayDescend(project: Project, references: AttributeSet): Boolean =
    forwardsAttributes(project) ||
      (safeProjection(project) && references.subsetOf(project.child.outputSet))

  def destination(child: LogicalPlan, references: AttributeSet): LogicalPlan = child match {
    case alias: SubqueryAlias => destination(alias.child, references)
    case projection: Project if mayDescend(projection, references) =>
      destination(projection.child, references)
    case inner: Join if inner.joinType.isInstanceOf[InnerLike] && inner.hint == JoinHint.NONE =>
      val next = if (references.nonEmpty && references.subsetOf(inner.right.outputSet)) {
        inner.right
      } else inner.left
      destination(next, references)
    case other => other
  }

  def project(
      child: LogicalPlan,
      bindings: Seq[NamedExpression],
      uses: Map[ExprId, AttributeSet]): LogicalPlan = {
    if (bindings.isEmpty) child else child match {
      case alias: SubqueryAlias => alias.copy(child = project(alias.child, bindings, uses))
      case projection: Project if forwardsAttributes(projection) =>
        projection.copy(projectList = projection.projectList ++ bindings.map(_.toAttribute),
          child = project(projection.child, bindings, uses))
      case projection: Project if safeProjection(projection) =>
        val (lower, here) = bindings.partition(b => mayDescend(projection, uses(b.exprId)))
        val updated = if (lower.isEmpty) projection else {
          projection.copy(projectList = projection.projectList ++ lower.map(_.toAttribute),
            child = project(projection.child, lower, uses))
        }
        if (here.isEmpty) updated else Project(updated.output ++ here, updated)
      case inner: Join if inner.joinType.isInstanceOf[InnerLike] && inner.hint == JoinHint.NONE =>
        val rightOutput = inner.right.outputSet
        val (right, left) = bindings.partition { binding =>
          val references = uses(binding.exprId)
          references.nonEmpty && references.subsetOf(rightOutput)
        }
        inner.copy(left = project(inner.left, left, uses),
          right = project(inner.right, right, uses))
      case _ => Project(child.output ++ bindings, child)
    }
  }
}
