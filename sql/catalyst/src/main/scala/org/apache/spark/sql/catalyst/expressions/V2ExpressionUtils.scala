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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, IdentityTransform, NamedReference, NullOrdering => V2NullOrdering, SortDirection => V2SortDirection, SortValue}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * A utility class that converts public connector expressions into Catalyst expressions.
 */
private[sql] object V2ExpressionUtils extends SQLConfHelper {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

  def resolveRef[T <: NamedExpression](
      ref: NamedReference,
      plan: LogicalPlan,
      resolver: Resolver = conf.resolver): T = {
    plan.resolve(ref.fieldNames, resolver) match {
      case Some(namedExpr) =>
        namedExpr.asInstanceOf[T]
      case None =>
        val name = ref.fieldNames.toSeq.quoted
        val outputString = plan.output.map(_.name).mkString(",")
        throw QueryCompilationErrors.cannotResolveAttributeError(name, outputString)
    }
  }

  def resolveRefs[T <: NamedExpression](
      refs: Seq[NamedReference],
      plan: LogicalPlan,
      resolver: Resolver = conf.resolver): Seq[T] = {
    refs.map(ref => resolveRef[T](ref, plan, resolver))
  }

  def toCatalyst(
      expr: V2Expression,
      query: LogicalPlan,
      resolver: Resolver = conf.resolver): Expression = {
    expr match {
      case SortValue(child, direction, nullOrdering) =>
        val catalystChild = toCatalyst(child, query, resolver)
        SortOrder(catalystChild, toCatalyst(direction), toCatalyst(nullOrdering), Seq.empty)
      case IdentityTransform(ref) =>
        resolveRef[NamedExpression](ref, query, resolver)
      case ref: FieldReference =>
        resolveRef[NamedExpression](ref, query, resolver)
      case _ =>
        throw new AnalysisException(s"$expr is not currently supported")
    }
  }

  private def toCatalyst(direction: V2SortDirection): SortDirection = direction match {
    case V2SortDirection.ASCENDING => Ascending
    case V2SortDirection.DESCENDING => Descending
  }

  private def toCatalyst(nullOrdering: V2NullOrdering): NullOrdering = nullOrdering match {
    case V2NullOrdering.NULLS_FIRST => NullsFirst
    case V2NullOrdering.NULLS_LAST => NullsLast
  }
}
