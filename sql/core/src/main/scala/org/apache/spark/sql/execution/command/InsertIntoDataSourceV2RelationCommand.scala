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

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, OverwriteByExpressionExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.sources.v2.writer.SupportsSaveMode

/**
 * A command for writing data to a [[DataSourceV2Relation]]. Supports both overwriting and saving.
 */
case class InsertIntoDataSourceV2RelationCommand(
    relation: DataSourceV2Relation,
    query: LogicalPlan,
    partition: Map[String, Option[String]],
    overwrite: Boolean) extends RunnableCommand {
  protected override def innerChildren: Seq[QueryPlan[_]] = query :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

    val dsOptions = relation.options
    val data = sparkSession.sessionState.executePlan(query).executedPlan
    val writeTable = relation.table.asWritable
    val writeExec = if (overwrite) {
      val relation = DataSourceV2Relation.create(writeTable, dsOptions)
      OverwriteByExpressionExec(relation.table.asWritable, Array.empty, dsOptions, data)
    } else {
      writeTable.newWriteBuilder(dsOptions) match {
        case writeBuilder: SupportsSaveMode =>
          val write = writeBuilder.mode(SaveMode.ErrorIfExists)
            .withQueryId(UUID.randomUUID().toString)
            .withInputDataSchema(data.schema)
            .buildForBatch()
          WriteToDataSourceV2Exec(write, data)
        case _ => throw new SparkException(
          "Insert into data source v2 relation should support save mode or overwrite mode")
      }
    }

    writeExec.execute().count()

    Seq.empty[Row]
  }
}
