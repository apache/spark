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

import scala.util.control.NonFatal

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{CTEInChildren, CTERelationDef, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}

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
    dataSource: CreatableRelationProvider,
    options: Map[String, String],
    mode: SaveMode) extends LeafRunnableCommand with CTEInChildren {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    var relation: BaseRelation = null

    try {
      relation = dataSource.createRelation(
        sparkSession.sqlContext, mode, options, Dataset.ofRows(sparkSession, query))
    } catch {
      case e @ (_: NullPointerException | _: MatchError | _: ArrayIndexOutOfBoundsException) =>
        // These are some of the exceptions thrown by the data source API. We catch these
        // exceptions here and rethrow QueryCompilationErrors.externalDataSourceException to
        // provide a more friendly error message for the user. This list is not exhaustive.
        throw QueryCompilationErrors.externalDataSourceException(Some(e))
      case e: Throwable =>
        // For other exceptions, just rethrow it, since we don't have enough information to
        // provide a better error message for the user at the moment. We may want to further
        // improve the error message handling in the future.
        throw e
    }

    try {
      val logicalRelation = LogicalRelation(relation, toAttributes(relation.schema), None, false)
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, logicalRelation)
    } catch {
      case NonFatal(_) =>
        // some data source can not support return a valid relation, e.g. `KafkaSourceProvider`
    }

    Seq.empty[Row]
  }

  override def simpleString(maxFields: Int): String = {
    val redacted = conf.redactOptions(options)
    s"SaveIntoDataSourceCommand ${dataSource}, ${redacted}, ${mode}"
  }

  // Override `clone` since the default implementation will turn `CaseInsensitiveMap` to a normal
  // map.
  override def clone(): LogicalPlan = {
    SaveIntoDataSourceCommand(query.clone(), dataSource, options, mode)
  }

  override def withCTEDefs(cteDefs: Seq[CTERelationDef]): LogicalPlan = {
    copy(query = WithCTE(query, cteDefs))
  }
}
