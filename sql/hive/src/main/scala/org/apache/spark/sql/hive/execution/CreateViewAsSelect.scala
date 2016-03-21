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

import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.{ HiveContext, HiveMetastoreTypes, SQLBuilder}

/**
 * Create Hive view on non-hive-compatible tables by specifying schema ourselves instead of
 * depending on Hive meta-store.
 */
private[hive] case class CreateViewAsSelect(
    tableDesc: CatalogTable,
    child: LogicalPlan,
    allowExisting: Boolean,
    orReplace: Boolean) extends RunnableCommand {

  private val childSchema = child.output

  assert(tableDesc.schema == Nil || tableDesc.schema.length == childSchema.length)
  assert(tableDesc.viewText.isDefined)

  private val tableIdentifier = tableDesc.name

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]

    hiveContext.sessionState.catalog.tableExists(tableIdentifier) match {
      case true if allowExisting =>
        // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
        // already exists.

      case true if orReplace =>
        // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
        hiveContext.sessionState.catalog.client.alertView(prepareTable(sqlContext))

      case true =>
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw new AnalysisException(s"View $tableIdentifier already exists. " +
          "If you want to update the view definition, please use ALTER VIEW AS or " +
          "CREATE OR REPLACE VIEW AS")

      case false =>
        hiveContext.sessionState.catalog.client.createView(prepareTable(sqlContext))
    }

    Seq.empty[Row]
  }

  private def prepareTable(sqlContext: SQLContext): CatalogTable = {
    val logicalPlan = if (tableDesc.schema.isEmpty) {
      child
    } else {
      val projectList = childSchema.zip(tableDesc.schema).map {
        case (attr, col) => Alias(attr, col.name)()
      }
      sqlContext.executePlan(Project(projectList, child)).analyzed
    }

    val viewText = new SQLBuilder(logicalPlan, sqlContext).toSQL

    val viewSchema: Seq[CatalogColumn] = {
      if (tableDesc.schema.isEmpty) {
        childSchema.map { a =>
          CatalogColumn(a.name, HiveMetastoreTypes.toMetastoreType(a.dataType))
        }
      } else {
        childSchema.zip(tableDesc.schema).map { case (a, col) =>
          CatalogColumn(
            col.name,
            HiveMetastoreTypes.toMetastoreType(a.dataType),
            nullable = true,
            col.comment)
        }
      }
    }

    tableDesc.copy(schema = viewSchema, viewText = Some(viewText))
  }
}
