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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * Similar to [[SubqueryAdaptiveBroadcastExec]], this node is used to store the
 * initial physical plan of dynamic bloom filter pruning subquery filters when enabling both AQE
 * and dynamic bloom filter pruning.
 */
case class SubqueryAdaptiveShuffleExec(
    name: String,
    @transient buildPlan: LogicalPlan,
    buildKeys: Seq[Expression],
    child: SparkPlan) extends BaseSubqueryExec with UnaryExecNode {

  protected override def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError("SubqueryAdaptiveShuffleExec")
  }

  protected override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    copy(name = "dpp", buildKeys = keys, child = child.canonicalized)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SubqueryAdaptiveShuffleExec =
    copy(child = newChild)
}
