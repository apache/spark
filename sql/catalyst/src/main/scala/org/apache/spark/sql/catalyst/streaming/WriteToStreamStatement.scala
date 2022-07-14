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

package org.apache.spark.sql.catalyst.streaming

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.streaming.OutputMode

/**
 * A statement for Stream writing. It contains all neccessary param and will be resolved in the
 * rule [[ResolveStreamWrite]].
 *
 * @param userSpecifiedName  Query name optionally specified by the user.
 * @param userSpecifiedCheckpointLocation  Checkpoint location optionally specified by the user.
 * @param useTempCheckpointLocation  Whether to use a temporary checkpoint location when the user
 *                                   has not specified one. If false, then error will be thrown.
 * @param recoverFromCheckpointLocation  Whether to recover query from the checkpoint location.
 *                                       If false and the checkpoint location exists, then error
 *                                       will be thrown.
 * @param sink  Sink to write the streaming outputs.
 * @param outputMode  Output mode for the sink.
 * @param hadoopConf  The Hadoop Configuration to get a FileSystem instance
 * @param isContinuousTrigger  Whether the statement is triggered by a continuous query or not.
 * @param inputQuery  The analyzed query plan from the streaming DataFrame.
 * @param catalogAndIdent Catalog and identifier for the sink, set when it is a V2 catalog table
 */
case class WriteToStreamStatement(
    userSpecifiedName: Option[String],
    userSpecifiedCheckpointLocation: Option[String],
    useTempCheckpointLocation: Boolean,
    recoverFromCheckpointLocation: Boolean,
    sink: Table,
    outputMode: OutputMode,
    hadoopConf: Configuration,
    isContinuousTrigger: Boolean,
    inputQuery: LogicalPlan,
    catalogAndIdent: Option[(TableCatalog, Identifier)] = None,
    catalogTable: Option[CatalogTable] = None) extends UnaryNode {

  override def isStreaming: Boolean = true

  override def output: Seq[Attribute] = Nil

  override def child: LogicalPlan = inputQuery

  override protected def withNewChildInternal(newChild: LogicalPlan): WriteToStreamStatement =
    copy(inputQuery = newChild)
}

