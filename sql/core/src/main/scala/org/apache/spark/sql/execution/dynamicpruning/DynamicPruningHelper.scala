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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.AggUtils

trait DynamicPruningHelper {

  def planBloomFilterLogicalPlan(
      buildQuery: LogicalPlan,
      buildKeys: Seq[Expression],
      index: Int): Aggregate = {
    val rowCount = buildQuery.stats.rowCount
    val bloomFilterAgg = if (rowCount.exists(_.longValue > 0L)) {
      new BloomFilterAggregate(new XxHash64(buildKeys(index)), Literal(rowCount.get.longValue))
    } else {
      new BloomFilterAggregate(new XxHash64(buildKeys(index)))
    }

    Aggregate(Nil, Seq(Alias(bloomFilterAgg.toAggregateExpression(), "bloomFilter")()), buildQuery)
  }

  def planBloomFilterPhysicalPlan(
      bfLogicalPlan: Aggregate,
      reusedShuffleExchange: Option[SparkPlan]): Option[SparkPlan] = {
    val physicalAggregation = PhysicalAggregation.unapply(bfLogicalPlan)
    if (reusedShuffleExchange.nonEmpty && physicalAggregation.nonEmpty) {
      val (groupingExps, aggExps, resultExps, _) = physicalAggregation.get
      val bfPhysicalPlan = AggUtils.planAggregateWithoutDistinct(
        groupingExps,
        aggExps.map(_.asInstanceOf[AggregateExpression]),
        resultExps,
        reusedShuffleExchange.get).headOption
      bfPhysicalPlan.foreach(_.setLogicalLink(bfLogicalPlan))
      bfPhysicalPlan
    } else {
      None
    }
  }
}
