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

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode, V2StreamingWriteCommand}
import org.apache.spark.sql.streaming.OutputMode

/**
 * The logical plan for writing data to a micro-batch stream.
 *
 * Note that this logical plan does not have a corresponding physical plan, as it will be
 * converted to
 * [[org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2 WriteToDataSourceV2]]
 * with [[MicroBatchWrite]] before execution.
 *
 * When the write target is backed by a transactional catalog, it is created as a
 * [[org.apache.spark.sql.catalyst.analysis.V2TableReference V2TableReference]].
 * This is then resolved by ResolveRelations as a
 * [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation DataSourceV2Relation]]
 * for each micro-batch.
 *
 * For non-transactional catalogs, the write target is pre-resolved as a
 * [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation DataSourceV2Relation]].
 */
case class WriteToMicroBatchDataSource(
    relation: NamedRelation,
    query: LogicalPlan,
    queryId: String,
    writeOptions: Map[String, String],
    outputMode: OutputMode,
    batchId: Option[Long] = None)
  extends UnaryNode with V2StreamingWriteCommand {

  override def child: LogicalPlan = query
  override def output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String =
    s"WriteToMicroBatchDataSource ${relation.name}"

  override def table: NamedRelation = relation

  override def withNewTable(newTable: NamedRelation): WriteToMicroBatchDataSource =
    copy(relation = newTable)

  def withNewBatchId(batchId: Long): WriteToMicroBatchDataSource = {
    copy(batchId = Some(batchId))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): WriteToMicroBatchDataSource =
    copy(query = newChild)
}
