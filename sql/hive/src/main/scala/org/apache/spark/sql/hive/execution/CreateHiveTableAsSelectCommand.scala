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

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, OverwriteOptions}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.MetastoreRelation


/**
 * Create table and insert the query result into it.
 *
 * @param tableDesc the Table Describe, which may contains serde, storage handler etc.
 * @param query the query whose result will be insert into the new relation
 * @param ignoreIfExists allow continue working if it's already exists, otherwise
 *                      raise exception
 */
case class CreateHiveTableAsSelectCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    ignoreIfExists: Boolean)
  extends RunnableCommand {

  private val tableIdentifier = tableDesc.identifier

  override def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    lazy val metastoreRelation: MetastoreRelation = {
      import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
      import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
      import org.apache.hadoop.io.Text
      import org.apache.hadoop.mapred.TextInputFormat

      val withFormat =
        tableDesc.withNewStorage(
          inputFormat =
            tableDesc.storage.inputFormat.orElse(Some(classOf[TextInputFormat].getName)),
          outputFormat =
            tableDesc.storage.outputFormat
              .orElse(Some(classOf[HiveIgnoreKeyTextOutputFormat[Text, Text]].getName)),
          serde = tableDesc.storage.serde.orElse(Some(classOf[LazySimpleSerDe].getName)),
          compressed = tableDesc.storage.compressed)

      val withSchema = if (withFormat.schema.isEmpty) {
        // Hive doesn't support specifying the column list for target table in CTAS
        // However we don't think SparkSQL should follow that.
        tableDesc.copy(schema = query.output.toStructType)
      } else {
        withFormat
      }

      sparkSession.sessionState.catalog.createTable(withSchema, ignoreIfExists = false)

      // Get the Metastore Relation
      sparkSession.sessionState.catalog.lookupRelation(tableIdentifier) match {
        case r: MetastoreRelation => r
      }
    }
    // TODO ideally, we should get the output data ready first and then
    // add the relation into catalog, just in case of failure occurs while data
    // processing.
    if (sparkSession.sessionState.catalog.tableExists(tableIdentifier)) {
      if (ignoreIfExists) {
        // table already exists, will do nothing, to keep consistent with Hive
      } else {
        throw new AnalysisException(s"$tableIdentifier already exists.")
      }
    } else {
      try {
        sparkSession.sessionState.executePlan(InsertIntoTable(
          metastoreRelation, Map(), query, overwrite = OverwriteOptions(true),
          ifNotExists = false)).toRdd
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
