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

package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the aggregates that we can push down to the data sources.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * An aggregate information for data sources.
 *
 * @since 2.0.0
 */
case class Aggregate(aggregateFuncs: Seq[AggregateFunc], groupingColumns: Seq[String])

/**
 * An aggregation function for data sources.
 *
 * @since 2.0.0
 */
abstract class AggregateFunc

/**
 * A function that computes a minimum value in a given column.
 *
 * @since 2.0.0
 */
case class Min(column: String) extends AggregateFunc

/**
 * A function that computes a maximum value in a given column.
 *
 * @since 2.0.0
 */
case class Max(column: String) extends AggregateFunc

object Aggregate {

  // Returns an empty Aggregate
  def empty: Aggregate = Aggregate(Seq.empty[AggregateFunc], Seq.empty[String])

  // Returns true if given aggregation expressions can be pushed down into data sources
  def canSupportPreAggregate(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.forall { p =>
      // Currently, we only support the restricted form of aggregation functions to push down.
      // We will accept more aggregation push downs step-by-step.
      val aggFunc = p.aggregateFunction
      aggFunc.children.size == 1 && aggFunc.dataType == aggFunc.children.head.dataType
    }
  }
}
