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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.streaming.{StreamingRelation, StreamingRelationV2}
import org.apache.spark.sql.sources.v2.TableCapability.{CONTINUOUS_READ, MICRO_BATCH_READ}

/**
 * This rules adds some basic table capability check for streaming scan, without knowing the actual
 * streaming execution mode.
 */
object V2StreamingScanSupportCheck extends (LogicalPlan => Unit) {
  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): Unit = {
    val streamingSources = plan.collect {
      case r: StreamingRelationV2 => r.table
    }
    val v1StreamingRelations = plan.collect {
      case r: StreamingRelation => r
    }

    if ((streamingSources ++ v1StreamingRelations).length > 1) {
      val allSupportsMicroBatch = streamingSources.forall(_.supports(MICRO_BATCH_READ))
      // v1 streaming data source only supports micro-batch.
      val allSupportsContinuous = streamingSources.forall(_.supports(CONTINUOUS_READ)) &&
        v1StreamingRelations.nonEmpty
      if (!allSupportsMicroBatch && !allSupportsContinuous) {
        throw new AnalysisException(
          "The streaming sources in a query do not have a common supported execution mode.")
      }
    }
  }
}
