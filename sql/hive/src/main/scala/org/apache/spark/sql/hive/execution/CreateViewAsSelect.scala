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

import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes, SQLBuilder}
import org.apache.spark.sql.hive.client.{HiveColumn, HiveTable}

/**
 * Create Hive view on non-hive-compatible tables by specifying schema ourselves instead of
 * depending on Hive meta-store.
 */
// TODO: Note that this class can NOT canonicalize the view SQL string entirely, which is different
// from Hive and may not work for some cases like create view on self join.
private[hive] case class CreateViewAsSelect(
    tableDesc: HiveTable,
    child: LogicalPlan,
    allowExisting: Boolean,
    orReplace: Boolean) extends RunnableCommand {

  private val childSchema = child.output

  assert(tableDesc.schema == Nil || tableDesc.schema.length == childSchema.length)
  assert(tableDesc.viewText.isDefined)

  val tableIdentifier = TableIdentifier(tableDesc.name, Some(tableDesc.database))

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]

    hiveContext.catalog.tableExists(tableIdentifier) match {
      case true if allowExisting =>
        // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
        // already exists.

      case true if orReplace =>
        // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
        hiveContext.catalog.client.alertView(prepareTable(sqlContext))

      case true =>
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw new AnalysisException(s"View $tableIdentifier already exists. " +
          "If you want to update the view definition, please use ALTER VIEW AS or " +
          "CREATE OR REPLACE VIEW AS")

      case false =>
        hiveContext.catalog.client.createView(prepareTable(sqlContext))
    }

    Seq.empty[Row]
  }

  private def prepareTable(sqlContext: SQLContext): HiveTable = {
    val expandedText = if (sqlContext.conf.canonicalView) {
      try rebuildViewQueryString(sqlContext) catch {
        case NonFatal(e) => wrapViewTextWithSelect
      }
    } else {
      wrapViewTextWithSelect
    }

    val viewSchema = {
      if (tableDesc.schema.isEmpty) {
        childSchema.map { attr =>
          HiveColumn(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), null)
        }
      } else {
        childSchema.zip(tableDesc.schema).map { case (attr, col) =>
          HiveColumn(col.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), col.comment)
        }
      }
    }

    tableDesc.copy(schema = viewSchema, viewText = Some(expandedText))
  }

  private def wrapViewTextWithSelect: String = {
    // When user specified column names for view, we should create a project to do the renaming.
    // When no column name specified, we still need to create a project to declare the columns
    // we need, to make us more robust to top level `*`s.
    val viewOutput = {
      val columnNames = childSchema.map(f => quote(f.name))
      if (tableDesc.schema.isEmpty) {
        columnNames.mkString(", ")
      } else {
        columnNames.zip(tableDesc.schema.map(f => quote(f.name))).map {
          case (name, alias) => s"$name AS $alias"
        }.mkString(", ")
      }
    }

    val viewText = tableDesc.viewText.get
    val viewName = quote(tableDesc.name)
    s"SELECT $viewOutput FROM ($viewText) $viewName"
  }

  private def rebuildViewQueryString(sqlContext: SQLContext): String = {
    val logicalPlan = if (tableDesc.schema.isEmpty) {
      child
    } else {
      val projectList = childSchema.zip(tableDesc.schema).map {
        case (attr, col) => Alias(attr, col.name)()
      }
      sqlContext.executePlan(Project(projectList, child)).analyzed
    }
    new SQLBuilder(logicalPlan, sqlContext).toSQL
  }

  // escape backtick with double-backtick in column name and wrap it with backtick.
  private def quote(name: String) = s"`${name.replaceAll("`", "``")}`"
}
