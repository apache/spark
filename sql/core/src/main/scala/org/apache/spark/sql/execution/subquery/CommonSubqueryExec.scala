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

package org.apache.spark.sql.execution.subquery

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{LeafExecNode, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.Utils

private[sql] case class CommonSubqueryExec(
    attributes: Seq[Attribute],
    @transient subquery: CommonSubquery)
  extends LeafExecNode {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(subquery.child)

  override def argString: String = Utils.truncatedString(attributes, "[", ", ", "]")

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = attributes

  override def outputPartitioning: Partitioning = subquery.child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = subquery.child.outputOrdering

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val subqueryOutput = subquery.computedOutput
    numOutputRows += subquery.numRows
    subqueryOutput
  }
}
