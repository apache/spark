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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.sources.v2.TableCapability._
import org.apache.spark.sql.types.BooleanType

object V2WriteSupportCheck extends (LogicalPlan => Unit) {
  import DataSourceV2Implicits._

  def failAnalysis(msg: String): Unit = throw new AnalysisException(msg)

  override def apply(plan: LogicalPlan): Unit = plan foreach {
    case AppendData(rel: DataSourceV2Relation, _, _) if !rel.table.supports(BATCH_WRITE) =>
      failAnalysis(s"Table does not support append in batch mode: ${rel.table}")

    case OverwritePartitionsDynamic(rel: DataSourceV2Relation, _, _)
      if !rel.table.supports(BATCH_WRITE) || !rel.table.supports(OVERWRITE_DYNAMIC) =>
      failAnalysis(s"Table does not support dynamic overwrite in batch mode: ${rel.table}")

    case OverwriteByExpression(rel: DataSourceV2Relation, expr, _, _) =>
      expr match {
        case Literal(true, BooleanType) =>
          if (!rel.table.supports(BATCH_WRITE) ||
              !rel.table.supportsAny(TRUNCATE, OVERWRITE_BY_FILTER)) {
            failAnalysis(
              s"Table does not support truncate in batch mode: ${rel.table}")
          }
        case _ =>
          if (!rel.table.supports(BATCH_WRITE) || !rel.table.supports(OVERWRITE_BY_FILTER)) {
            failAnalysis(s"Table does not support overwrite expression ${expr.sql} " +
                s"in batch mode: ${rel.table}")
          }
      }

    case _ => // OK
  }
}
