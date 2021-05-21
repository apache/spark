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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.optimizer.PropagateEmptyRelationAdvanced
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.joins.HashedRelationWithAllNullKeys

/**
 * A helper class to provide a AQE side `PropagateEmptyRelationAdvanced` rule.
 */
object PropagateEmptyRelationAdvancedHelper {

  private def isRelationWithAllNullKeys(plan: LogicalPlan) = plan match {
    case LogicalQueryStage(_, stage: BroadcastQueryStageExec)
      if stage.resultOption.get().isDefined =>
      stage.broadcast.relationFuture.get().value == HashedRelationWithAllNullKeys
    case _ => false
  }

  private def checkRowCount(plan: LogicalPlan, hasRow: Boolean): Boolean = plan match {
    case LogicalQueryStage(_, stage: QueryStageExec) if stage.resultOption.get().isDefined =>
      stage.getRuntimeStatistics.rowCount match {
        case Some(count) => hasRow == (count > 0)
        case _ => false
      }
    case _ => false
  }

  lazy val propagateEmptyRelationAdvanced: PropagateEmptyRelationAdvanced = {
    PropagateEmptyRelationAdvanced(Some(checkRowCount), Some(isRelationWithAllNullKeys))
  }
}
