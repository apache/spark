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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Statistics}


object AggregateEstimation {
  import EstimationUtils._

  /**
   * Estimate the number of output rows based on column stats of group-by columns, and propagate
   * column stats for aggregate expressions.
   */
  def estimate(agg: Aggregate): Option[Statistics] = {
    val childStats = agg.child.stats
    // Check if we have column stats for all group-by columns.
    val colStatsExist = agg.groupingExpressions.forall { e =>
      e.isInstanceOf[Attribute] &&
        childStats.attributeStats.get(e.asInstanceOf[Attribute]).exists(_.hasCountStats)
    }
    if (rowCountsExist(agg.child) && colStatsExist) {
      // Multiply distinct counts of group-by columns. This is an upper bound, which assumes
      // the data contains all combinations of distinct values of group-by columns.
      var outputRows: BigInt = agg.groupingExpressions.foldLeft(BigInt(1))(
        (res, expr) => {
          val columnStat = childStats.attributeStats(expr.asInstanceOf[Attribute])
          val distinctCount = columnStat.distinctCount.get
          val distinctValue: BigInt = if (columnStat.nullCount.get > 0) {
            distinctCount + 1
          } else {
            distinctCount
          }
          res * distinctValue
        })

      outputRows = if (agg.groupingExpressions.isEmpty) {
        // If there's no group-by columns, the output is a single row containing values of aggregate
        // functions: aggregated results for non-empty input or initial values for empty input.
        1
      } else {
        // Here we set another upper bound for the number of output rows: it must not be larger than
        // child's number of rows.
        outputRows.min(childStats.rowCount.get)
      }

      val aliasStats = EstimationUtils.getAliasStats(
        agg.expressions, childStats.attributeStats, outputRows)

      val outputAttrStats = getOutputMap(
        AttributeMap(childStats.attributeStats.toSeq ++ aliasStats), agg.output)
      Some(Statistics(
        sizeInBytes = getOutputSize(agg.output, outputRows, outputAttrStats),
        rowCount = Some(outputRows),
        attributeStats = outputAttrStats))
    } else {
      None
    }
  }
}
