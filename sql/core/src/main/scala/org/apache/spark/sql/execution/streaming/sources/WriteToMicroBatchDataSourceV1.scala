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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

/**
 * Marker node to represent a DSv1 sink on streaming query.
 *
 * Despite this is expected to be the top node, this node should behave like "pass-through"
 * since the DSv1 codepath on microbatch execution handles sink operation separately.
 *
 * This node is eliminated in streaming specific optimization phase, which means there is no
 * matching physical node.
 */
case class WriteToMicroBatchDataSourceV1(
    catalogTable: Option[CatalogTable],
    sink: Sink,
    query: LogicalPlan,
    queryId: String,
    writeOptions: Map[String, String],
    outputMode: OutputMode,
    batchId: Option[Long] = None)
  extends UnaryNode {

  override def child: LogicalPlan = query

  override def output: Seq[Attribute] = query.output

  def withNewBatchId(batchId: Long): WriteToMicroBatchDataSourceV1 = {
    copy(batchId = Some(batchId))
  }

  override protected def withNewChildInternal(
      newChild: LogicalPlan): WriteToMicroBatchDataSourceV1 = copy(query = newChild)
}
