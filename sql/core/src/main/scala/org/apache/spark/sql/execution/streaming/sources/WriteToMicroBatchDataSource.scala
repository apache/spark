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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, StreamingV2WriteCommand, UnaryNode}
import org.apache.spark.sql.streaming.OutputMode

/**
 * The logical plan for writing data to a micro-batch stream.
 *
 * Note that this logical plan does not have a corresponding physical plan, as it will be converted
 * to [[org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2 WriteToDataSourceV2]]
 * with [[MicroBatchWrite]] before execution.
 *
 * [[relation]] starts as [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation]] when the
 * sink has a catalog+identifier (transactional catalogs), or as a resolved
 * [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation]] for non-transactional
 * catalog-backed sinks and format-based sinks.
 * [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveRelations]]
 * resolves it to [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation]] during
 * each micro-batch analysis, going through the transaction-aware catalog when a transaction is
 * active.
 */
case class WriteToMicroBatchDataSource(
    relation: NamedRelation,
    query: LogicalPlan,
    queryId: String,
    writeOptions: Map[String, String],
    outputMode: OutputMode,
    batchId: Option[Long] = None)
  extends UnaryNode with StreamingV2WriteCommand {

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
