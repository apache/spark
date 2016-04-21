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

import java.io.File
import java.net.URI

import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, ExternalCatalog}
import org.apache.spark.util.Utils

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

/**
 * A command that loads data into a Hive table.
 *
 * The syntax of this command is:
 * {{{
 *  LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
 *  [PARTITION (partcol1=val1, partcol2=val2 ...)]
 * }}}
 */
case class LoadData(
    table: TableIdentifier,
    path: String,
    isLocal: Boolean,
    isOverwrite: Boolean,
    partition: Option[ExternalCatalog.TablePartitionSpec]) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    if (!catalog.tableExists(table)) {
      throw new AnalysisException(
        s"Table in LOAD DATA does not exist: '$table'")
    }

    val targetTable = catalog.getTableMetadataOption(table).getOrElse {
      throw new AnalysisException(
        s"Table in LOAD DATA cannot be temporary: '$table'")
    }

    if (DDLUtils.isDatasourceTable(targetTable)) {
      throw new AnalysisException(
        "LOAD DATA is not supported for datasource tables")
    }

    if (targetTable.partitionColumnNames.nonEmpty) {
      if (partition.isEmpty || targetTable.partitionColumnNames.size != partition.get.size) {
        throw new AnalysisException(
          "LOAD DATA to partitioned table must specify a specific partition of " +
          "the table by specifying values for all of the partitioning columns.")
      }

      partition.get.keys.foreach { colName =>
        if (!targetTable.partitionColumnNames.contains(colName)) {
          throw new AnalysisException(
            s"LOAD DATA to partitioned table specifies a non-existing partition column: '$colName'")
        }
      }
    } else {
      if (partition.nonEmpty) {
        throw new AnalysisException(
          "LOAD DATA to non-partitioned table cannot specify partition.")
      }
    }

    val loadPath =
      if (isLocal) {
        val uri = Utils.resolveURI(path)
        if (!new File(uri.getPath()).exists()) {
          throw new AnalysisException(s"LOAD DATA with non-existing path: $path")
        }
        uri
      } else {
        val uri = new URI(path)
        if (uri.getScheme() != null && uri.getAuthority() != null) {
          uri
        } else {
          // Follow Hive's behavior:
          // If no schema or authority is provided with non-local inpath,
          // we will use hadoop configuration "fs.default.name".
          val defaultFSConf = sqlContext.sparkContext.hadoopConfiguration.get("fs.default.name")
          val defaultFS = if (defaultFSConf == null) {
            new URI("")
          } else {
            new URI(defaultFSConf)
          }

          val scheme = if (uri.getScheme() != null) {
            uri.getScheme()
          } else {
            defaultFS.getScheme()
          }
          val authority = if (uri.getAuthority() != null) {
            uri.getAuthority()
          } else {
            defaultFS.getAuthority()
          }

          if (scheme == null) {
            throw new AnalysisException(
              "LOAD DATA with non-local path must specify URI Scheme.")
          }

          // Follow Hive's behavior:
          // If LOCAL is not specified, and the path is relative,
          // then the path is interpreted relative to "/user/<username>"
          val uriPath = uri.getPath()
          val absolutePath = if (uriPath != null && uriPath.startsWith("/")) {
            uriPath
          } else {
            s"/user/${System.getProperty("user.name")}/$uriPath"
          }
          new URI(scheme, authority, absolutePath, uri.getQuery(), uri.getFragment())
        }
      }

    if (partition.nonEmpty) {
      catalog.loadPartition(
        targetTable.identifier,
        loadPath.toString,
        partition.get,
        isOverwrite,
        holdDDLTime = false,
        inheritTableSpecs = true,
        isSkewedStoreAsSubdir = false)
    } else {
      catalog.loadTable(
        targetTable.identifier,
        loadPath.toString,
        isOverwrite,
        holdDDLTime = false)
    }
    Seq.empty[Row]
  }
}
