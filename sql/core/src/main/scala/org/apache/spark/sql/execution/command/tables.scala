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
import java.util.Date

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StringType, StructType}
import org.apache.spark.util.Utils

case class CreateTableAsSelectLogicalPlan(
  tableDesc: CatalogTable,
  child: LogicalPlan,
  allowExisting: Boolean) extends UnaryNode with Command {

  override def output: Seq[Attribute] = Seq.empty[Attribute]

  override lazy val resolved: Boolean =
    tableDesc.identifier.database.isDefined &&
      tableDesc.schema.nonEmpty &&
      tableDesc.storage.serde.isDefined &&
      tableDesc.storage.inputFormat.isDefined &&
      tableDesc.storage.outputFormat.isDefined &&
      childrenResolved
}

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

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
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
      tableType = CatalogTableType.MANAGED,
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

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.catalog.createTable(table, ifNotExists)
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

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
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
    partition: Option[TablePartitionSpec]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (!catalog.tableExists(table)) {
      throw new AnalysisException(s"Target table in LOAD DATA does not exist: '$table'")
    }
    val targetTable = catalog.getTableMetadataOption(table).getOrElse {
      throw new AnalysisException(s"Target table in LOAD DATA cannot be temporary: '$table'")
    }
    if (DDLUtils.isDatasourceTable(targetTable)) {
      throw new AnalysisException(s"LOAD DATA is not supported for datasource tables: '$table'")
    }
    if (targetTable.partitionColumnNames.nonEmpty) {
      if (partition.isEmpty) {
        throw new AnalysisException(s"LOAD DATA target table '$table' is partitioned, " +
          s"but no partition spec is provided")
      }
      if (targetTable.partitionColumnNames.size != partition.get.size) {
        throw new AnalysisException(s"LOAD DATA target table '$table' is partitioned, " +
          s"but number of columns in provided partition spec (${partition.get.size}) " +
          s"do not match number of partitioned columns in table " +
          s"(s${targetTable.partitionColumnNames.size})")
      }
      partition.get.keys.foreach { colName =>
        if (!targetTable.partitionColumnNames.contains(colName)) {
          throw new AnalysisException(s"LOAD DATA target table '$table' is partitioned, " +
            s"but the specified partition spec refers to a column that is not partitioned: " +
            s"'$colName'")
        }
      }
    } else {
      if (partition.nonEmpty) {
        throw new AnalysisException(s"LOAD DATA target table '$table' is not partitioned, " +
          s"but a partition spec was provided.")
      }
    }

    val loadPath =
      if (isLocal) {
        val uri = Utils.resolveURI(path)
        if (!new File(uri.getPath()).exists()) {
          throw new AnalysisException(s"LOAD DATA input path does not exist: $path")
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
          val defaultFSConf = sparkSession.sessionState.newHadoopConf().get("fs.default.name")
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
              s"LOAD DATA: URI scheme is required for non-local input paths: '$path'")
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

/**
 * Command that looks like
 * {{{
 *   DESCRIBE [EXTENDED|FORMATTED] table_name;
 * }}}
 */
case class DescribeTableCommand(table: TableIdentifier, isExtended: Boolean, isFormatted: Boolean)
  extends RunnableCommand {

  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference("col_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference("data_type", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build())(),
    AttributeReference("comment", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build())()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val catalog = sparkSession.sessionState.catalog

    if (catalog.isTemporaryTable(table)) {
      describeSchema(catalog.lookupRelation(table).schema, result)
    } else {
      val metadata = catalog.getTableMetadata(table)

      if (isExtended) {
        describeExtended(metadata, result)
      } else if (isFormatted) {
        describeFormatted(metadata, result)
      } else {
        describe(metadata, result)
      }
    }

    result
  }

  // Shows data columns and partitioned columns (if any)
  private def describe(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    describeSchema(table.schema, buffer)

    if (table.partitionColumns.nonEmpty) {
      append(buffer, "# Partition Information", "", "")
      append(buffer, s"# ${output(0).name}", output(1).name, output(2).name)
      describeSchema(table.partitionColumns, buffer)
    }
  }

  private def describeExtended(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    describe(table, buffer)

    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", table.toString, "")
  }

  private def describeFormatted(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    describe(table, buffer)

    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    append(buffer, "Database:", table.database, "")
    append(buffer, "Owner:", table.owner, "")
    append(buffer, "Create Time:", new Date(table.createTime).toString, "")
    append(buffer, "Last Access Time:", new Date(table.lastAccessTime).toString, "")
    append(buffer, "Location:", table.storage.locationUri.getOrElse(""), "")
    append(buffer, "Table Type:", table.tableType.name, "")

    append(buffer, "Table Parameters:", "", "")
    table.properties.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }

    append(buffer, "", "", "")
    append(buffer, "# Storage Information", "", "")
    table.storage.serde.foreach(serdeLib => append(buffer, "SerDe Library:", serdeLib, ""))
    table.storage.inputFormat.foreach(format => append(buffer, "InputFormat:", format, ""))
    table.storage.outputFormat.foreach(format => append(buffer, "OutputFormat:", format, ""))
    append(buffer, "Compressed:", if (table.storage.compressed) "Yes" else "No", "")
    append(buffer, "Num Buckets:", table.numBuckets.toString, "")
    append(buffer, "Bucket Columns:", table.bucketColumnNames.mkString("[", ", ", "]"), "")
    append(buffer, "Sort Columns:", table.sortColumnNames.mkString("[", ", ", "]"), "")

    append(buffer, "Storage Desc Parameters:", "", "")
    table.storage.serdeProperties.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }
  }

  private def describeSchema(schema: Seq[CatalogColumn], buffer: ArrayBuffer[Row]): Unit = {
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.toLowerCase, column.comment.orNull)
    }
  }

  private def describeSchema(schema: StructType, buffer: ArrayBuffer[Row]): Unit = {
    schema.foreach { column =>
      val comment =
        if (column.metadata.contains("comment")) column.metadata.getString("comment") else ""
      append(buffer, column.name, column.dataType.simpleString, comment)
    }
  }

  private def append(
      buffer: ArrayBuffer[Row], column: String, dataType: String, comment: String): Unit = {
    buffer += Row(column, dataType, comment)
  }
}


/**
 * A command for users to get tables in the given database.
 * If a databaseName is not given, the current database will be used.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW TABLES [(IN|FROM) database_name] [[LIKE] 'identifier_with_wildcards'];
 * }}}
 */
case class ShowTablesCommand(
    databaseName: Option[String],
    tableIdentifierPattern: Option[String]) extends RunnableCommand {

  // The result of SHOW TABLES has two columns, tableName and isTemporary.
  override val output: Seq[Attribute] = {
    AttributeReference("tableName", StringType, nullable = false)() ::
      AttributeReference("isTemporary", BooleanType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Since we need to return a Seq of rows, we will call getTables directly
    // instead of calling tables in sparkSession.
    val catalog = sparkSession.sessionState.catalog
    val db = databaseName.getOrElse(catalog.getCurrentDatabase)
    val tables =
      tableIdentifierPattern.map(catalog.listTables(db, _)).getOrElse(catalog.listTables(db))
    tables.map { t =>
      val isTemp = t.database.isEmpty
      Row(t.table, isTemp)
    }
  }
}


/**
 * A command for users to list the properties for a table If propertyKey is specified, the value
 * for the propertyKey is returned. If propertyKey is not specified, all the keys and their
 * corresponding values are returned.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW TBLPROPERTIES table_name[('propertyKey')];
 * }}}
 */
case class ShowTablePropertiesCommand(table: TableIdentifier, propertyKey: Option[String])
  extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = AttributeReference("value", StringType, nullable = false)() :: Nil
    propertyKey match {
      case None => AttributeReference("key", StringType, nullable = false)() :: schema
      case _ => schema
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog

    if (catalog.isTemporaryTable(table)) {
      Seq.empty[Row]
    } else {
      val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(table)

      propertyKey match {
        case Some(p) =>
          val propValue = catalogTable
            .properties
            .getOrElse(p, s"Table ${catalogTable.qualifiedName} does not have property: $p")
          Seq(Row(propValue))
        case None =>
          catalogTable.properties.map(p => Row(p._1, p._2)).toSeq
      }
    }
  }
}
