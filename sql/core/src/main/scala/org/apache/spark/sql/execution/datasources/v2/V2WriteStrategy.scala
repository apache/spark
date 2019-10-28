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

import java.util.UUID

import org.apache.spark.sql.{AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.write.{SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.types.StructType

object V2WriteStrategy extends Strategy with PredicateHelper {
  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case AppendData(r: DataSourceV2Relation, query, writeOptions, _) =>
      val writeBuilder = newWriteBuilder(r.table, writeOptions, query.schema)
      writeBuilder match {
        case v1: V1WriteBuilder =>
          AppendDataExecV1(v1.buildForV1Write(), query) :: Nil
        case _ =>
          AppendDataExec(writeBuilder.buildForBatch(), planLater(query)) :: Nil
      }

    case OverwriteByExpression(r: DataSourceV2Relation, deleteExpr, query, writeOptions, _) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val filters = splitConjunctivePredicates(deleteExpr).map {
        filter => DataSourceStrategy.translateFilter(deleteExpr).getOrElse(
          throw new AnalysisException(s"Cannot translate expression to source filter: $filter"))
      }.toArray

      val writeBuilder = newWriteBuilder(r.table, writeOptions, query.schema)
      val configured = writeBuilder match {
        case builder: SupportsTruncate if isTruncate(filters) => builder.truncate()
        case builder: SupportsOverwrite => builder.overwrite(filters)
        case _ =>
          throw new IllegalArgumentException(
            s"Table does not support overwrite by expression: ${r.table.name}")
      }

      configured match {
        case v1: V1WriteBuilder =>
          OverwriteByExpressionExecV1(v1.buildForV1Write(), filters, query) :: Nil
        case _ =>
          OverwriteByExpressionExec(configured.buildForBatch(), filters, planLater(query)) :: Nil
      }

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, writeOptions, _) =>
      val writeBuilder = newWriteBuilder(r.table, writeOptions, query.schema)
      val configured = writeBuilder match {
        case builder: SupportsDynamicOverwrite =>
          builder.overwriteDynamicPartitions()
        case _ =>
          throw new IllegalArgumentException(
            s"Table does not support dynamic partition overwrite: ${r.table.name}")
      }
      OverwritePartitionsDynamicExec(configured.buildForBatch(), planLater(query)) :: Nil
  }

  def newWriteBuilder(
      table: Table,
      options: Map[String, String],
      inputSchema: StructType): WriteBuilder = {
    table.asWritable.newWriteBuilder(options.asOptions)
      .withInputDataSchema(inputSchema)
      .withQueryId(UUID.randomUUID().toString)
  }

  private def isTruncate(filters: Array[Filter]): Boolean = {
    filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue]
  }
}
