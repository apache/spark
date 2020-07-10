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

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, PredicateHelper, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, RepartitionByExpression, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.distributions.{ClusteredDistribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, IdentityTransform, NullOrdering, SortDirection, SortValue}
import org.apache.spark.sql.connector.write.{LogicalWriteInfoImpl, RequiresDistributionAndOrdering, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}

/**
 * A rule that constructs [[Write]]s.
 *
 * This rule does resolution in the optimizer because some nodes like [[OverwriteByExpression]]
 * must undergo the expression optimization before we can construct a logical write.
 */
object V2Writes extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case a @ AppendData(r: DataSourceV2Relation, query, options, _, None) =>
      val writeBuilder = newWriteBuilder(r.table, query, options)
      val write = writeBuilder.build()
      a.copy(write = Some(write), query = addDistributionAndOrdering(write, query))

    case o @ OverwriteByExpression(r: DataSourceV2Relation, deleteExpr, query, options, _, None) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val filters = splitConjunctivePredicates(deleteExpr).flatMap { p =>
        val filter = DataSourceStrategy.translateFilter(p, supportNestedPredicatePushdown = true)
        if (filter.isEmpty) {
          throw new AnalysisException(s"Cannot translate expression to source filter: $p")
        }
        filter
      }.toArray

      val table = r.table
      val writeBuilder = newWriteBuilder(table, query, options)
      val write = writeBuilder match {
        case builder: SupportsTruncate if isTruncate(filters) =>
          builder.truncate().build()
        case builder: SupportsOverwrite =>
          builder.overwrite(filters).build()
        case _ =>
          throw new SparkException(s"Table does not support overwrite by expression: $table")
      }

      o.copy(write = Some(write), query = addDistributionAndOrdering(write, query))

    case o @ OverwritePartitionsDynamic(r: DataSourceV2Relation, query, options, _, None) =>
      val table = r.table
      val writeBuilder = newWriteBuilder(table, query, options)
      val write = writeBuilder match {
        case builder: SupportsDynamicOverwrite =>
          builder.overwriteDynamicPartitions().build()
        case _ =>
          throw new SparkException(s"Table does not support dynamic partition overwrite: $table")
      }
      o.copy(write = Some(write), query = addDistributionAndOrdering(write, query))
  }

  private def newWriteBuilder(
      table: Table,
      query: LogicalPlan,
      writeOptions: Map[String, String]): WriteBuilder = {

    val info = LogicalWriteInfoImpl(
      queryId = UUID.randomUUID().toString,
      query.schema,
      writeOptions.asOptions)
    table.asWritable.newWriteBuilder(info)
  }

  private def isTruncate(filters: Array[Filter]): Boolean = {
    filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue]
  }

  private def addDistributionAndOrdering(
      write: Write,
      query: LogicalPlan): LogicalPlan = write match {

    case write: RequiresDistributionAndOrdering =>
      val sqlConf = SQLConf.get
      val resolver = sqlConf.resolver

      val distribution = write.requiredDistribution match {
        case d: OrderedDistribution =>
          d.ordering.map(e => toCatalyst(e, query, resolver))
        case d: ClusteredDistribution =>
          d.clustering.map(e => toCatalyst(e, query, resolver))
        case _: UnspecifiedDistribution =>
          Array.empty[catalyst.expressions.Expression]
      }

      val queryWithDistribution = if (distribution.nonEmpty) {
        val numShufflePartitions = sqlConf.numShufflePartitions
        // the conversion to catalyst expressions above produces SortOrder expressions
        // for OrderedDistribution and generic expressions for ClusteredDistribution
        // this allows RepartitionByExpression to pick either range or hash partitioning
        RepartitionByExpression(distribution, query, numShufflePartitions)
      } else {
        query
      }

      val ordering = write.requiredOrdering.toSeq
          .map(e => toCatalyst(e, query, resolver))
          .asInstanceOf[Seq[catalyst.expressions.SortOrder]]

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
      expr: Expression,
      query: LogicalPlan,
      resolver: Resolver): catalyst.expressions.Expression = {
    def resolve(ref: FieldReference): NamedExpression = {
      // this part is controversial as we perform resolution in the optimizer
      // we cannot perform this step in the analyzer since we need to optimize expressions
      // in nodes like OverwriteByExpression before constructing a logical write
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
        throw new RuntimeException(s"$expr is not currently supported")
    }
  }

  private def toCatalyst(direction: SortDirection): catalyst.expressions.SortDirection = {
    direction match {
      case SortDirection.ASCENDING => catalyst.expressions.Ascending
      case SortDirection.DESCENDING => catalyst.expressions.Descending
    }
  }

  private def toCatalyst(nullOrdering: NullOrdering): catalyst.expressions.NullOrdering = {
    nullOrdering match {
      case NullOrdering.NULLS_FIRST => catalyst.expressions.NullsFirst
      case NullOrdering.NULLS_LAST => catalyst.expressions.NullsLast
    }
  }
}
