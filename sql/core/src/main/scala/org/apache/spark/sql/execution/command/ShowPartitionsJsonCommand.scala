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
package org.apache.spark.sql.execution.command

import org.json4s._
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{MetadataBuilder, StringType}
import org.apache.spark.sql.util.PartitioningUtils

/**
 * A command for users to list the partition names of a table as a JSON document. If a partition
 * spec is specified, only partitions matching the spec are returned. Otherwise all partitions are
 * returned.
 *
 * The output is a single row with a `json_metadata` column containing a JSON object of the form:
 * `{"partitions": ["col=val/col=val", ...]}`.
 *
 * This command is the AS JSON variant of [[ShowPartitionsCommand]] and is produced by the parser
 * when `SHOW PARTITIONS ... AS JSON` is issued. Only V1 (session catalog / Hive metastore) tables
 * are supported.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW PARTITIONS multi_part_name [partition_spec] AS JSON;
 * }}}
 */
case class ShowPartitionsJsonCommand(
    child: LogicalPlan,
    spec: Option[TablePartitionSpec],
    override val output: Seq[Attribute] = Seq(
      AttributeReference(
        "json_metadata",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "Partition list of the table").build())()))
    extends UnaryRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    child match {
      case ResolvedTable(_, _, t: V1Table, _) =>
        val catalog = sparkSession.sessionState.catalog
        val table = t.catalogTable
        val tableName = table.identifier

        if (table.partitionColumnNames.isEmpty) {
          throw QueryCompilationErrors.showPartitionNotAllowedOnTableNotPartitionedError(
            tableName.quotedString)
        }

        DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "SHOW PARTITIONS")

        val normalizedSpec = spec.map(partitionSpec =>
          PartitioningUtils.normalizePartitionSpec(
            partitionSpec,
            table.partitionSchema,
            tableName.quotedString,
            sparkSession.sessionState.conf.resolver))

        val partNames = catalog.listPartitionNames(tableName, normalizedSpec)
        Seq(Row(compact(render(JObject("partitions" ->
          JArray(partNames.map(JString(_)).toList))))))

      // ResolvedTempView and ResolvedPersistentView are currently not supported.
      case _ =>
        throw QueryCompilationErrors.showPartitionsAsJsonNotSupportedForV2TablesError()
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}
