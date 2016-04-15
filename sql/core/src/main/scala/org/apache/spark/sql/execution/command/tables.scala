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

import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}

/**
 * A command to create a table with the same definition of the given existing table.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   LIKE [other_db_name.]existing_table_name
 * }}}
 */
case class CreateTableLike(
    targetTable: TableIdentifier,
    sourceTable: TableIdentifier,
    ifNotExists: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    if (!catalog.tableExists(sourceTable)) {
      throw new AnalysisException(
        s"Source table in CREATE TABLE LIKE does not exist: '$sourceTable'")
    }
    if (catalog.isTemporaryTable(sourceTable)) {
      throw new AnalysisException(
        s"Source table in CREATE TABLE LIKE cannot be temporary: '$sourceTable'")
    }

    val tableToCreate = catalog.getTableMetadata(sourceTable).copy(
      identifier = targetTable,
      tableType = CatalogTableType.MANAGED_TABLE,
      createTime = System.currentTimeMillis,
      lastAccessTime = -1).withNewStorage(locationUri = None)

    catalog.createTable(tableToCreate, ifNotExists)
    Seq.empty[Row]
  }
}


// TODO: move the rest of the table commands from ddl.scala to this file

/**
 * A command to create a table.
 *
 * Note: This is currently used only for creating Hive tables.
 * This is not intended for temporary tables.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
 *   [(col1 data_type [COMMENT col_comment], ...)]
 *   [COMMENT table_comment]
 *   [PARTITIONED BY (col3 data_type [COMMENT col_comment], ...)]
 *   [CLUSTERED BY (col1, ...) [SORTED BY (col1 [ASC|DESC], ...)] INTO num_buckets BUCKETS]
 *   [SKEWED BY (col1, col2, ...) ON ((col_value, col_value, ...), ...)
 *   [STORED AS DIRECTORIES]
 *   [ROW FORMAT row_format]
 *   [STORED AS file_format | STORED BY storage_handler_class [WITH SERDEPROPERTIES (...)]]
 *   [LOCATION path]
 *   [TBLPROPERTIES (property_name=property_value, ...)]
 *   [AS select_statement];
 * }}}
 */
case class CreateTable(table: CatalogTable, ifNotExists: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.sessionState.catalog.createTable(table, ifNotExists)
    Seq.empty[Row]
  }

}


/**
 * A command that renames a table/view.
 *
 * The syntax of this command is:
 * {{{
 *    ALTER TABLE table1 RENAME TO table2;
 *    ALTER VIEW view1 RENAME TO view2;
 * }}}
 */
case class AlterTableRename(
    oldName: TableIdentifier,
    newName: TableIdentifier,
    isView: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    DDLUtils.verifyAlterTableType(catalog, oldName, isView)
    catalog.invalidateTable(oldName)
    catalog.renameTable(oldName, newName)
    Seq.empty[Row]
  }

}
