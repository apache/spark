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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

/**
 * Saves the results of `query` in to a data source.
 *
 * Note that this command is different from [[InsertIntoDataSourceCommand]]. This command will call
 * `CreatableRelationProvider.createRelation` to write out the data, while
 * [[InsertIntoDataSourceCommand]] calls `InsertableRelation.insert`. Ideally these 2 data source
 * interfaces should do the same thing, but as we've already published these 2 interfaces and the
 * implementations may have different logic, we have to keep these 2 different commands.
 */
case class SaveIntoDataSourceCommand(
    query: LogicalPlan,
    provider: String,
    partitionColumns: Seq[String],
    options: Map[String, String],
    mode: SaveMode) extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    DataSource(
      sparkSession,
      className = provider,
      partitionColumns = partitionColumns,
      options = options).write(mode, Dataset.ofRows(sparkSession, query))

    Seq.empty[Row]
  }
}
