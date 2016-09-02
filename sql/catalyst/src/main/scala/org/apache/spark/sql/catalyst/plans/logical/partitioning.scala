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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}

/**
 * Performs a physical redistribution of the data.  Used when the consumer of the query
 * result have expectations about the distribution and ordering of partitioned input data.
 */
abstract class RedistributeData extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class SortPartitions(sortExpressions: Seq[SortOrder], child: LogicalPlan)
  extends RedistributeData with NonSQLPlan

/**
 * This method repartitions data using [[Expression]]s into `numPartitions`, and receives
 * information about the number of partitions during execution. Used when a specific ordering or
 * distribution is expected by the consumer of the query result. Use [[Repartition]] for RDD-like
 * `coalesce` and `repartition`.
 * If `numPartitions` is not specified, the number of partitions will be the number set by
 * `spark.sql.shuffle.partitions`.
 */
case class RepartitionByExpression(
    partitionExpressions: Seq[Expression],
    child: LogicalPlan,
    numPartitions: Option[Int] = None) extends RedistributeData {
  numPartitions match {
    case Some(n) => require(n > 0, s"Number of partitions ($n) must be positive.")
    case None => // Ok
  }

  override def sql: String = {
    s"${child.sql} DISTRIBUTE BY ${partitionExpressions.map(_.sql).mkString(", ")}"
  }
}
