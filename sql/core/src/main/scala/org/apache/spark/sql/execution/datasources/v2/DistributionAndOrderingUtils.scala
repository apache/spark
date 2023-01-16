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

import org.apache.spark.sql.catalyst.analysis.{AnsiTypeCoercion, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SortOrder, TransformExpression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RebalancePartitions, RepartitionByExpression, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.FunctionCatalog
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.connector.distributions._
import org.apache.spark.sql.connector.write.{RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.errors.QueryCompilationErrors

object DistributionAndOrderingUtils {

  def prepareQuery(
      write: Write,
      query: LogicalPlan,
      funCatalogOpt: Option[FunctionCatalog]): LogicalPlan = write match {
    case write: RequiresDistributionAndOrdering =>
      val numPartitions = write.requiredNumPartitions()

      val distribution = write.requiredDistribution match {
        case d: OrderedDistribution =>
          toCatalystOrdering(d.ordering(), query, funCatalogOpt)
            .map(e => resolveTransformExpression(e).asInstanceOf[SortOrder])
        case d: ClusteredDistribution =>
          d.clustering.map(e => toCatalyst(e, query, funCatalogOpt))
            .map(e => resolveTransformExpression(e)).toSeq
        case _: UnspecifiedDistribution => Seq.empty[Expression]
      }

      val queryWithDistribution = if (distribution.nonEmpty) {
        val optNumPartitions = if (numPartitions > 0) Some(numPartitions) else None
        // the conversion to catalyst expressions above produces SortOrder expressions
        // for OrderedDistribution and generic expressions for ClusteredDistribution
        // this allows RebalancePartitions/RepartitionByExpression to pick either
        // range or hash partitioning
        if (write.distributionStrictlyRequired()) {
          RepartitionByExpression(distribution, query, optNumPartitions)
        } else {
          RebalancePartitions(distribution, query, optNumPartitions)
        }
      } else if (numPartitions > 0) {
        throw QueryCompilationErrors.numberOfPartitionsNotAllowedWithUnspecifiedDistributionError()
      } else {
        query
      }

      val ordering = toCatalystOrdering(write.requiredOrdering, query, funCatalogOpt)
      val queryWithDistributionAndOrdering = if (ordering.nonEmpty) {
        Sort(
          ordering.map(e => resolveTransformExpression(e).asInstanceOf[SortOrder]),
          global = false,
          queryWithDistribution)
      } else {
        queryWithDistribution
      }

      // Apply typeCoercionRules since the converted expression from TransformExpression
      // implemented ImplicitCastInputTypes
      typeCoercionRules.foldLeft(queryWithDistributionAndOrdering)((plan, rule) => rule(plan))
    case _ =>
      query
  }

  private def resolveTransformExpression(expr: Expression): Expression = expr.transform {
    case TransformExpression(scalarFunc: ScalarFunction[_], arguments, Some(numBuckets)) =>
      V2ExpressionUtils.resolveScalarFunction(scalarFunc, Seq(Literal(numBuckets)) ++ arguments)
    case TransformExpression(scalarFunc: ScalarFunction[_], arguments, None) =>
      V2ExpressionUtils.resolveScalarFunction(scalarFunc, arguments)
  }

  private def typeCoercionRules: List[Rule[LogicalPlan]] = if (conf.ansiEnabled) {
    AnsiTypeCoercion.typeCoercionRules
  } else {
    TypeCoercion.typeCoercionRules
  }
}
