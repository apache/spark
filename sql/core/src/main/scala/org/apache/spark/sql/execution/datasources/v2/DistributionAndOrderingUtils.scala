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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, Expression, NamedExpression, NullOrdering, NullsFirst, NullsLast, SortDirection, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionByExpression, Sort}
import org.apache.spark.sql.connector.distributions.{ClusteredDistribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, IdentityTransform, NullOrdering => V2NullOrdering, SortDirection => V2SortDirection, SortValue}
import org.apache.spark.sql.connector.write.{RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.internal.SQLConf

object DistributionAndOrderingUtils {

  def prepareQuery(write: Write, query: LogicalPlan, conf: SQLConf): LogicalPlan = write match {
    case write: RequiresDistributionAndOrdering =>
      val resolver = conf.resolver

      val distribution = write.requiredDistribution match {
        case d: OrderedDistribution =>
          d.ordering.map(e => toCatalyst(e, query, resolver))
        case d: ClusteredDistribution =>
          d.clustering.map(e => toCatalyst(e, query, resolver))
        case _: UnspecifiedDistribution =>
          Array.empty[Expression]
      }

      val queryWithDistribution = if (distribution.nonEmpty) {
        val numShufflePartitions = conf.numShufflePartitions
        // the conversion to catalyst expressions above produces SortOrder expressions
        // for OrderedDistribution and generic expressions for ClusteredDistribution
        // this allows RepartitionByExpression to pick either range or hash partitioning
        RepartitionByExpression(distribution, query, numShufflePartitions)
      } else {
        query
      }

      val ordering = write.requiredOrdering.toSeq
        .map(e => toCatalyst(e, query, resolver))
        .asInstanceOf[Seq[SortOrder]]

      val queryWithDistributionAndOrdering = if (ordering.nonEmpty) {
        Sort(ordering, global = false, queryWithDistribution)
      } else {
        queryWithDistribution
      }

      queryWithDistributionAndOrdering

    case _ =>
      query
  }

  private def toCatalyst(
      expr: V2Expression,
      query: LogicalPlan,
      resolver: Resolver): Expression = {

    // we cannot perform the resolution in the analyzer since we need to optimize expressions
    // in nodes like OverwriteByExpression before constructing a logical write
    def resolve(ref: FieldReference): NamedExpression = {
      query.resolve(ref.parts, resolver) match {
        case Some(attr) => attr
        case None => throw new AnalysisException(s"Cannot resolve '$ref' using ${query.output}")
      }
    }

    expr match {
      case SortValue(child, direction, nullOrdering) =>
        val catalystChild = toCatalyst(child, query, resolver)
        SortOrder(catalystChild, toCatalyst(direction), toCatalyst(nullOrdering), Seq.empty)
      case IdentityTransform(ref) =>
        resolve(ref)
      case ref: FieldReference =>
        resolve(ref)
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
