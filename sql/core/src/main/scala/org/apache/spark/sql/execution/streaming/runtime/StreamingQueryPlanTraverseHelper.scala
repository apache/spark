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

package org.apache.spark.sql.execution.streaming.runtime

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

/**
 * This is a utility object placing methods to traverse the query plan for streaming query.
 * This is used for patterns of traversal which are repeated in multiple places.
 */
object StreamingQueryPlanTraverseHelper {
  def collectFromUnfoldedLatestExecutedPlan[B](
      lastExecution: IncrementalExecution)(pf: PartialFunction[SparkPlan, B]): Seq[B] = {
    val executedPlan = lastExecution.executedPlan

    collectWithUnfolding(executedPlan)(pf)
  }

  private def collectWithUnfolding[B](executedPlan: SparkPlan)(
      pf: PartialFunction[SparkPlan, B]): Seq[B] = {
    executedPlan.flatMap {
      // InMemoryTableScanExec is a node to represent a cached plan. The node has underlying
      // actual executed plan, which we should traverse to collect the required information.
      case s: InMemoryTableScanExec => collectWithUnfolding(s.relation.cachedPlan)(pf)
      case p if pf.isDefinedAt(p) => Seq(pf(p))
      case _ => Seq.empty[B]
    }
  }
}
