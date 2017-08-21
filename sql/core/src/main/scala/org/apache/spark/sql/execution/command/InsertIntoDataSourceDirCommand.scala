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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._

/**
 * A command used to write the result of a query to a directory.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   INSERT OVERWRITE DIRECTORY (path=STRING)?
 *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
 *   SELECT ...
 * }}}
 */
case class InsertIntoDataSourceDirCommand(
    storage: CatalogStorageFormat,
    provider: Option[String],
    query: LogicalPlan) extends RunnableCommand {

  override def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(innerChildren.length == 1)
    assert(!storage.locationUri.isEmpty)
    assert(provider.isDefined)

    // Create the relation based on the input logical plan: `data`.
    val pathOption = storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
    val dataSource = DataSource(
      sparkSession,
      className = provider.get,
      options = storage.properties ++ pathOption,
      catalogTable = None)

    try {
      dataSource.writeAndRead(SaveMode.Overwrite, query)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to directory " + storage.locationUri.toString, ex)
        throw ex
    }

    Seq.empty[Row]
  }
}
