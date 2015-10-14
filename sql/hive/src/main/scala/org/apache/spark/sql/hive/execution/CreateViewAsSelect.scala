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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.hive.{HiveMetastoreTypes, HiveContext}
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hive.client.{HiveColumn, HiveTable}

/**
 * Create Hive view on non-hive-compatible tables by specifying schema ourselves instead of
 * depending on Hive meta-store.
 */
// TODO: Note that this class can NOT canonicalize the view SQL string entirely, which is different
// from Hive and may not work for some cases like create view on self join.
private[hive] case class CreateViewAsSelect(
    tableDesc: HiveTable,
    childSchema: Seq[Attribute],
    allowExisting: Boolean,
    orReplace: Boolean) extends RunnableCommand {

  assert(tableDesc.schema == Nil || tableDesc.schema.length == childSchema.length)
  assert(tableDesc.viewText.isDefined)

  val tableIdentifier = TableIdentifier(tableDesc.name, Some(tableDesc.database))

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]

    if (hiveContext.catalog.tableExists(tableIdentifier)) {
      if (allowExisting) {
        // view already exists, will do nothing, to keep consistent with Hive
      } else if (orReplace) {
        hiveContext.catalog.client.alertView(prepareTable())
      } else {
        throw new AnalysisException(s"View $tableIdentifier already exists. " +
          "If you want to update the view definition, please use ALTER VIEW AS or " +
          "CREATE OR REPLACE VIEW AS")
      }
    } else {
      hiveContext.catalog.client.createView(prepareTable())
    }

    Seq.empty[Row]
  }

  private def prepareTable(): HiveTable = {
    // setup column types according to the schema of child.
    val schema = if (tableDesc.schema == Nil) {
      childSchema.map { attr =>
        HiveColumn(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), null)
      }
    } else {
      childSchema.zip(tableDesc.schema).map { case (attr, col) =>
        HiveColumn(col.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), col.comment)
      }
    }

    val columnNames = childSchema.map(f => verbose(f.name))

    // When user specified column names for view, we should create a project to do the renaming.
    // When no column name specified, we still need to create a project to declare the columns
    // we need, to make us more robust to top level `*`s.
    val projectList = if (tableDesc.schema == Nil) {
      columnNames.mkString(", ")
    } else {
      columnNames.zip(tableDesc.schema.map(f => verbose(f.name))).map {
        case (name, alias) => s"$name AS $alias"
      }.mkString(", ")
    }

    val viewName = verbose(tableDesc.name)

    val expandedText = s"SELECT $projectList FROM (${tableDesc.viewText.get}) $viewName"

    tableDesc.copy(schema = schema, viewText = Some(expandedText))
  }

  // escape backtick with double-backtick in column name and wrap it with backtick.
  private def verbose(name: String) = s"`${name.replaceAll("`", "``")}`"
}
