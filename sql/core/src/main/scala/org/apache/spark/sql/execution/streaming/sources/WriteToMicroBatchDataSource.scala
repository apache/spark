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

package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2

/**
 * The logical plan for writing data to a micro-batch stream.
 *
 * Note that this logical plan does not have a corresponding physical plan, as it will be converted
 * to [[WriteToDataSourceV2]] with [[MicroBatchWrite]] before execution.
 */
case class WriteToMicroBatchDataSource(write: StreamingWrite, query: LogicalPlan)
  extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil

  def createPlan(batchId: Long): WriteToDataSourceV2 = {
    WriteToDataSourceV2(new MicroBatchWrite(batchId, write), query)
  }
}
