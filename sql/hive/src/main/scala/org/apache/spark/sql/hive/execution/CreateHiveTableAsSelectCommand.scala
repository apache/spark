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

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{DataWritingCommand, LeafRunnableCommand}

/**
 * Create table and insert the query result into it.
 *
 * @param tableDesc the table description, which may contain serde, storage handler etc.
 * @param query the query whose result will be insert into the new relation
 * @param mode SaveMode
 */
case class CreateHiveTableAsSelectCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    outputColumnNames: Seq[String],
    mode: SaveMode)
  extends LeafRunnableCommand {
  assert(query.resolved)
  override def innerChildren: Seq[LogicalPlan] = query :: Nil

  protected val tableIdentifier = tableDesc.identifier

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val tableExists = catalog.tableExists(tableIdentifier)

    if (tableExists) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableIdentifier has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw QueryCompilationErrors.tableIdentifierExistsError(tableIdentifier)
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      val command = getWritingCommand(catalog, tableDesc, tableExists = true)
      val qe = sparkSession.sessionState.executePlan(command)
      qe.assertCommandExecuted()
    } else {
      tableDesc.storage.locationUri.foreach { p =>
        DataWritingCommand.assertEmptyRootPath(p, mode, sparkSession.sessionState.newHadoopConf)
      }
      // TODO ideally, we should get the output data ready first and then
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      val outputColumns = DataWritingCommand.logicalPlanOutputWithNames(query, outputColumnNames)
      val tableSchema = CharVarcharUtils.getRawSchema(
        outputColumns.toStructType, sparkSession.sessionState.conf)
      assert(tableDesc.schema.isEmpty)
      catalog.createTable(
        tableDesc.copy(schema = tableSchema), ignoreIfExists = false)

      try {
        // Read back the metadata of the table which was created just now.
        val createdTableMeta = catalog.getTableMetadata(tableDesc.identifier)
        val command = getWritingCommand(catalog, createdTableMeta, tableExists = false)
        val qe = sparkSession.sessionState.executePlan(command)
        qe.assertCommandExecuted()
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          catalog.dropTable(tableIdentifier, ignoreIfNotExists = true, purge = false)
          throw e
      }
    }

    Seq.empty[Row]
  }

  private def getWritingCommand(
      catalog: SessionCatalog,
      tableDesc: CatalogTable,
      tableExists: Boolean): DataWritingCommand = {
    // For CTAS, there is no static partition values to insert.
    val partition = tableDesc.partitionColumnNames.map(_ -> None).toMap
    InsertIntoHiveTable(
      tableDesc,
      partition,
      query,
      overwrite = if (tableExists) false else true,
      ifPartitionNotExists = false,
      outputColumnNames = outputColumnNames)
  }

  override def argString(maxFields: Int): String = {
    s"[Database: ${tableDesc.database}, " +
      s"TableName: ${tableDesc.identifier.table}]"
  }
}
