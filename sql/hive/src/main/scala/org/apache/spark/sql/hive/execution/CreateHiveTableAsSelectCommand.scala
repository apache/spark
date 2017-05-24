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

package org.apache.spark.sql.hive.execution

import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.command.RunnableCommand


/**
 * Create table and insert the query result into it.
 *
 * @param tableDesc the Table Describe, which may contains serde, storage handler etc.
 * @param query the query whose result will be insert into the new relation
 * @param mode SaveMode
 */
case class CreateHiveTableAsSelectCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    mode: SaveMode)
  extends RunnableCommand {

  private val tableIdentifier = tableDesc.identifier

  override def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (sparkSession.sessionState.catalog.tableExists(tableIdentifier)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableIdentifier has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"$tableIdentifier already exists.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      sparkSession.sessionState.executePlan(
        InsertIntoTable(
          UnresolvedRelation(tableIdentifier),
          Map(),
          query,
          overwrite = false,
          ifPartitionNotExists = false)).toRdd
    } else {
      // TODO ideally, we should get the output data ready first and then
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      assert(tableDesc.schema.isEmpty)
      sparkSession.sessionState.catalog.createTable(
        tableDesc.copy(schema = query.schema), ignoreIfExists = false)

      try {
        sparkSession.sessionState.executePlan(
          InsertIntoTable(
            UnresolvedRelation(tableIdentifier),
            Map(),
            query,
            overwrite = true,
            ifPartitionNotExists = false)).toRdd
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          sparkSession.sessionState.catalog.dropTable(tableIdentifier, ignoreIfNotExists = true,
            purge = false)
          throw e
      }
    }

    Seq.empty[Row]
  }

  override def argString: String = {
    s"[Database:${tableDesc.database}}, " +
    s"TableName: ${tableDesc.identifier.table}, " +
    s"InsertIntoHiveTable]"
  }
}
