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

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolveSchemaEvolution}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode, WriteWithSchemaEvolution}
import org.apache.spark.sql.catalyst.trees.TreePattern.WRITE_TO_MICRO_BATCH_DATA_SOURCE
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableChange, TableWritePrivilege}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2CatalogAndIdentifier}
import org.apache.spark.sql.streaming.OutputMode

/**
 * The logical plan for writing data to a micro-batch stream.
 *
 * Note that this logical plan does not have a corresponding physical plan, as it will be converted
 * to [[org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2 WriteToDataSourceV2]]
 * with [[MicroBatchWrite]] before execution.
 */
case class WriteToMicroBatchDataSource(
    relation: Option[DataSourceV2Relation],
    sinkTable: SupportsWrite,
    query: LogicalPlan,
    queryId: String,
    writeOptions: Map[String, String],
    outputMode: OutputMode,
    override val withSchemaEvolution: Boolean,
    batchId: Option[Long] = None)
  extends UnaryNode with WriteWithSchemaEvolution {
  override def child: LogicalPlan = query
  override def output: Seq[Attribute] = Nil

  final override val nodePatterns = Seq(WRITE_TO_MICRO_BATCH_DATA_SOURCE)

  override def table: LogicalPlan = relation.getOrElse {
    throw new IllegalStateException(
      "Cannot access table for schema evolution: no DataSourceV2Relation is set.")
  }

  override lazy val schemaEvolutionEnabled: Boolean =
    withSchemaEvolution && relation.exists {
      case r: DataSourceV2Relation => r.autoSchemaEvolution
      case _ => false
    }

  override lazy val schemaEvolutionReady: Boolean =
    relation.exists(_.resolved) && query.resolved

  override def pendingSchemaChanges: Seq[TableChange] = {
    if (relation.isEmpty || !schemaEvolutionEnabled || !schemaEvolutionReady) {
      Seq.empty
    } else {
      val currentRelation = relation.get match {
        case r @ ExtractV2CatalogAndIdentifier(catalog, ident) =>
          // Loading the current table from the catalog ensures we don't use a stale schema.
          val currentTable = catalog.loadTable(ident)
          r.copy(
            table = currentTable,
            output = DataTypeUtils.toAttributes(currentTable.columns))
        case r => r
      }

      // TODO: Streaming writes don't go through TableOutputResolver to
      // reorder columns based on by-position vs. by-name semantic. It's up to the
      // connector to decide how to write the data when schemas don't match.
      // Here we assume by-position to detect new columns for schema evolution, but that
      // may not match what the connector expects.
      ResolveSchemaEvolution.computeSupportedSchemaChanges(
        currentRelation, query.schema, isByName = false).toSeq
    }
  }

  override val writePrivileges: Set[TableWritePrivilege] = Set(TableWritePrivilege.INSERT)

  override def withNewTable(newTable: NamedRelation): WriteToMicroBatchDataSource = {
    val newRelation = newTable.asInstanceOf[DataSourceV2Relation]
    copy(
      relation = Some(newRelation),
      sinkTable = newRelation.table.asInstanceOf[SupportsWrite])
  }

  def withNewBatchId(batchId: Long): WriteToMicroBatchDataSource = {
    copy(batchId = Some(batchId))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): WriteToMicroBatchDataSource =
    copy(query = newChild)
}
