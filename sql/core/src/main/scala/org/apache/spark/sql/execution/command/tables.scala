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
import scala.util.control.NonFatal
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A command to create a MANAGED table with the same definition of the given existing table.
 * In the target table definition, the table comment is always empty but the column comments
 * are identical to the ones defined in the source table.
 *
 * The CatalogTable attributes copied from the source table are storage(inputFormat, outputFormat,
 * serde, compressed, properties), schema, provider, partitionColumnNames, bucketSpec.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   LIKE [other_db_name.]existing_table_name
 * }}}
 */
case class CreateTableLikeCommand(
    targetTable: TableIdentifier,
    sourceTable: TableIdentifier,
    ifNotExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)

    // Storage format
    val newStorage =
      if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
        val newPath = catalog.defaultTablePath(targetTable)
        CatalogStorageFormat.empty.copy(properties = Map("path" -> newPath))
      } else if (DDLUtils.isDatasourceTable(sourceTableDesc)) {
        val newPath = catalog.defaultTablePath(targetTable)
        val newSerdeProp =
          sourceTableDesc.storage.properties.filterKeys(_.toLowerCase != "path") ++
            Map("path" -> newPath)
        sourceTableDesc.storage.copy(
          locationUri = None,
          properties = newSerdeProp)
      } else {
        sourceTableDesc.storage.copy(
          locationUri = None,
          properties = sourceTableDesc.storage.properties)
      }

    val newProvider = if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else {
      sourceTableDesc.provider
    }

    val newTableDesc =
      CatalogTable(
        identifier = targetTable,
        tableType = CatalogTableType.MANAGED,
        storage = newStorage,
        schema = sourceTableDesc.schema,
        provider = newProvider,
        partitionColumnNames = sourceTableDesc.partitionColumnNames,
        bucketSpec = sourceTableDesc.bucketSpec)

    catalog.createTable(newTableDesc, ifNotExists)
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
case class CreateTableCommand(table: CatalogTable, ifNotExists: Boolean) extends RunnableCommand {

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
case class AlterTableRenameCommand(
    oldName: TableIdentifier,
    newName: String,
    isView: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    // If this is a temp view, just rename the view.
    // Otherwise, if this is a real table, we also need to uncache and invalidate the table.
    if (catalog.isTemporaryTable(oldName)) {
      catalog.renameTable(oldName, newName)
    } else {
      val table = catalog.getTableMetadata(oldName)
      DDLUtils.verifyAlterTableType(catalog, table, isView)
      val newTblName = TableIdentifier(newName, oldName.database)
      // If an exception is thrown here we can just assume the table is uncached;
      // this can happen with Hive tables when the underlying catalog is in-memory.
      val wasCached = Try(sparkSession.catalog.isCached(oldName.unquotedString)).getOrElse(false)
      if (wasCached) {
        try {
          sparkSession.catalog.uncacheTable(oldName.unquotedString)
        } catch {
          case NonFatal(e) => log.warn(e.toString, e)
        }
      }
      // For datasource tables, we also need to update the "path" serde property
      if (DDLUtils.isDatasourceTable(table) && table.tableType == CatalogTableType.MANAGED) {
        val newPath = catalog.defaultTablePath(newTblName)
        val newTable = table.withNewStorage(
          properties = table.storage.properties ++ Map("path" -> newPath))
        catalog.alterTable(newTable)
      }
      // Invalidate the table last, otherwise uncaching the table would load the logical plan
      // back into the hive metastore cache
      catalog.refreshTable(oldName)
      catalog.renameTable(oldName, newName)
      if (wasCached) {
        sparkSession.catalog.cacheTable(newTblName.unquotedString)
      }
    }
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
case class LoadDataCommand(
    table: TableIdentifier,
    path: String,
    isLocal: Boolean,
    isOverwrite: Boolean,
    partition: Option[TablePartitionSpec]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val targetTable = catalog.getTableMetadata(table)
    val tableIdentwithDB = targetTable.identifier.quotedString

    if (targetTable.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(s"Target table in LOAD DATA cannot be a view: $tableIdentwithDB")
    }
    if (DDLUtils.isDatasourceTable(targetTable)) {
      throw new AnalysisException(
        s"LOAD DATA is not supported for datasource tables: $tableIdentwithDB")
    }
    if (targetTable.partitionColumnNames.nonEmpty) {
      if (partition.isEmpty) {
        throw new AnalysisException(s"LOAD DATA target table $tableIdentwithDB is partitioned, " +
          s"but no partition spec is provided")
      }
      if (targetTable.partitionColumnNames.size != partition.get.size) {
        throw new AnalysisException(s"LOAD DATA target table $tableIdentwithDB is partitioned, " +
          s"but number of columns in provided partition spec (${partition.get.size}) " +
          s"do not match number of partitioned columns in table " +
          s"(s${targetTable.partitionColumnNames.size})")
      }
      partition.get.keys.foreach { colName =>
        if (!targetTable.partitionColumnNames.contains(colName)) {
          throw new AnalysisException(s"LOAD DATA target table $tableIdentwithDB is partitioned, " +
            s"but the specified partition spec refers to a column that is not partitioned: " +
            s"'$colName'")
        }
      }
    } else {
      if (partition.nonEmpty) {
        throw new AnalysisException(s"LOAD DATA target table $tableIdentwithDB is not " +
          s"partitioned, but a partition spec was provided.")
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
        inheritTableSpecs = true)
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
 * A command to truncate table.
 *
 * The syntax of this command is:
 * {{{
 *   TRUNCATE TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
 * }}}
 */
case class TruncateTableCommand(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec]) extends RunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentwithDB = table.identifier.quotedString

    if (table.tableType == CatalogTableType.EXTERNAL) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on external tables: $tableIdentwithDB")
    }
    if (table.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on views: $tableIdentwithDB")
    }
    val isDatasourceTable = DDLUtils.isDatasourceTable(table)
    if (isDatasourceTable && partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
        s"for tables created using the data sources API: $tableIdentwithDB")
    }
    if (table.partitionColumnNames.isEmpty && partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
        s"for tables that are not partitioned: $tableIdentwithDB")
    }
    val locations =
      if (isDatasourceTable) {
        Seq(table.storage.properties.get("path"))
      } else if (table.partitionColumnNames.isEmpty) {
        Seq(table.storage.locationUri)
      } else {
        catalog.listPartitions(table.identifier, partitionSpec).map(_.storage.locationUri)
      }
    val hadoopConf = spark.sessionState.newHadoopConf()
    locations.foreach { location =>
      if (location.isDefined) {
        val path = new Path(location.get)
        try {
          val fs = path.getFileSystem(hadoopConf)
          fs.delete(path, true)
          fs.mkdirs(path)
        } catch {
          case NonFatal(e) =>
            throw new AnalysisException(
              s"Failed to truncate table $tableIdentwithDB when removing data of the path: $path " +
                s"because of ${e.toString}")
        }
      }
    }
    // After deleting the data, invalidate the table to make sure we don't keep around a stale
    // file relation in the metastore cache.
    spark.sessionState.refreshTable(tableName.unquotedString)
    // Also try to drop the contents of the table from the columnar cache
    try {
      spark.sharedState.cacheManager.uncacheQuery(spark.table(table.identifier))
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table $tableIdentwithDB", e)
    }
    Seq.empty[Row]
  }
}

/**
 * Command that looks like
 * {{{
 *   DESCRIBE [EXTENDED|FORMATTED] table_name partitionSpec?;
 * }}}
 */
case class DescribeTableCommand(
    table: TableIdentifier,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean,
    isFormatted: Boolean)
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
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException(
          s"DESC PARTITION is not allowed on a temporary view: ${table.identifier}")
      }
      describeSchema(catalog.lookupRelation(table).schema, result)
    } else {
      val metadata = catalog.getTableMetadata(table)
      describeSchema(metadata.schema, result)

      describePartitionInfo(metadata, result)

      if (partitionSpec.isEmpty) {
        if (isExtended) {
          describeExtendedTableInfo(metadata, result)
        } else if (isFormatted) {
          describeFormattedTableInfo(metadata, result)
        }
      } else {
        if (DDLUtils.isDatasourceTable(metadata)) {
          throw new AnalysisException(
            s"DESC PARTITION is not allowed on a datasource table: ${table.identifier}")
        }
        val partition = catalog.getPartition(table, partitionSpec)
        if (isExtended) {
          describeExtendedDetailPartitionInfo(table, metadata, partition, result)
        } else if (isFormatted) {
          describeFormattedDetailPartitionInfo(table, metadata, partition, result)
          describeStorageInfo(metadata, result)
        }
      }
    }

    result
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (table.partitionColumnNames.nonEmpty) {
      append(buffer, "# Partition Information", "", "")
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
      describeSchema(table.partitionSchema, buffer)
    }
  }

  private def describeExtendedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", table.toString, "")
  }

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    append(buffer, "Database:", table.database, "")
    append(buffer, "Owner:", table.owner, "")
    append(buffer, "Create Time:", new Date(table.createTime).toString, "")
    append(buffer, "Last Access Time:", new Date(table.lastAccessTime).toString, "")
    append(buffer, "Location:", table.storage.locationUri.getOrElse(""), "")
    append(buffer, "Table Type:", table.tableType.name, "")
    table.stats.foreach(s => append(buffer, "Statistics:", s.simpleString, ""))

    append(buffer, "Table Parameters:", "", "")
    table.properties.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }

    describeStorageInfo(table, buffer)

    if (table.tableType == CatalogTableType.VIEW) describeViewInfo(table, buffer)
  }

  private def describeStorageInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Storage Information", "", "")
    metadata.storage.serde.foreach(serdeLib => append(buffer, "SerDe Library:", serdeLib, ""))
    metadata.storage.inputFormat.foreach(format => append(buffer, "InputFormat:", format, ""))
    metadata.storage.outputFormat.foreach(format => append(buffer, "OutputFormat:", format, ""))
    append(buffer, "Compressed:", if (metadata.storage.compressed) "Yes" else "No", "")
    describeBucketingInfo(metadata, buffer)

    append(buffer, "Storage Desc Parameters:", "", "")
    metadata.storage.properties.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }
  }

  private def describeViewInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# View Information", "", "")
    append(buffer, "View Original Text:", metadata.viewOriginalText.getOrElse(""), "")
    append(buffer, "View Expanded Text:", metadata.viewText.getOrElse(""), "")
  }

  private def describeBucketingInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    metadata.bucketSpec match {
      case Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames)) =>
        append(buffer, "Num Buckets:", numBuckets.toString, "")
        append(buffer, "Bucket Columns:", bucketColumnNames.mkString("[", ", ", "]"), "")
        append(buffer, "Sort Columns:", sortColumnNames.mkString("[", ", ", "]"), "")

      case _ =>
    }
  }

  private def describeExtendedDetailPartitionInfo(
      tableIdentifier: TableIdentifier,
      table: CatalogTable,
      partition: CatalogTablePartition,
      buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "Detailed Partition Information " + partition.toString, "", "")
  }

  private def describeFormattedDetailPartitionInfo(
      tableIdentifier: TableIdentifier,
      table: CatalogTable,
      partition: CatalogTablePartition,
      buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Partition Information", "", "")
    append(buffer, "Partition Value:", s"[${partition.spec.values.mkString(", ")}]", "")
    append(buffer, "Database:", table.database, "")
    append(buffer, "Table:", tableIdentifier.table, "")
    append(buffer, "Create Time:", "UNKNOWN", "")
    append(buffer, "Last Access Time:", "UNKNOWN", "")
    append(buffer, "Protect Mode:", "None", "")
    append(buffer, "Location:", partition.storage.locationUri.getOrElse(""), "")
    append(buffer, "Partition Parameters:", "", "")
    partition.parameters.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }
  }

  private def describeSchema(schema: StructType, buffer: ArrayBuffer[Row]): Unit = {
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.simpleString, column.getComment().orNull)
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
 * A command for users to list the properties for a table. If propertyKey is specified, the value
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

/**
 * A command to list the column names for a table. This function creates a
 * [[ShowColumnsCommand]] logical plan.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW COLUMNS (FROM | IN) table_identifier [(FROM | IN) database];
 * }}}
 */
case class ShowColumnsCommand(tableName: TableIdentifier) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("col_name", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTempViewOrPermanentTableMetadata(tableName)
    table.schema.map { c =>
      Row(c.name)
    }
  }
}

/**
 * A command to list the partition names of a table. If the partition spec is specified,
 * partitions that match the spec are returned. [[AnalysisException]] exception is thrown under
 * the following conditions:
 *
 * 1. If the command is called for a non partitioned table.
 * 2. If the partition spec refers to the columns that are not defined as partitioning columns.
 *
 * This function creates a [[ShowPartitionsCommand]] logical plan
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW PARTITIONS [db_name.]table_name [PARTITION(partition_spec)]
 * }}}
 */
case class ShowPartitionsCommand(
    tableName: TableIdentifier,
    spec: Option[TablePartitionSpec]) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("partition", StringType, nullable = false)() :: Nil
  }

  private def getPartName(spec: TablePartitionSpec, partColNames: Seq[String]): String = {
    partColNames.map { name =>
      PartitioningUtils.escapePathName(name) + "=" + PartitioningUtils.escapePathName(spec(name))
    }.mkString(File.separator)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString

    /**
     * Validate and throws an [[AnalysisException]] exception under the following conditions:
     * 1. If the table is not partitioned.
     * 2. If it is a datasource table.
     * 3. If it is a view.
     */
    if (table.tableType == VIEW) {
      throw new AnalysisException(s"SHOW PARTITIONS is not allowed on a view: $tableIdentWithDB")
    }

    if (table.partitionColumnNames.isEmpty) {
      throw new AnalysisException(
        s"SHOW PARTITIONS is not allowed on a table that is not partitioned: $tableIdentWithDB")
    }

    if (DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException(
        s"SHOW PARTITIONS is not allowed on a datasource table: $tableIdentWithDB")
    }

    /**
     * Validate the partitioning spec by making sure all the referenced columns are
     * defined as partitioning columns in table definition. An AnalysisException exception is
     * thrown if the partitioning spec is invalid.
     */
    if (spec.isDefined) {
      val badColumns = spec.get.keySet.filterNot(table.partitionColumnNames.contains)
      if (badColumns.nonEmpty) {
        val badCols = badColumns.mkString("[", ", ", "]")
        throw new AnalysisException(
          s"Non-partitioning column(s) $badCols are specified for SHOW PARTITIONS")
      }
    }

    val partNames = catalog.listPartitions(tableName, spec).map { p =>
      getPartName(p.spec, table.partitionColumnNames)
    }

    partNames.map(Row(_))
  }
}

case class ShowCreateTableCommand(table: TableIdentifier) extends RunnableCommand {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("createtab_stmt", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val tableMetadata = catalog.getTableMetadata(table)

    // TODO: unify this after we unify the CREATE TABLE syntax for hive serde and data source table.
    val stmt = if (DDLUtils.isDatasourceTable(tableMetadata)) {
      showCreateDataSourceTable(tableMetadata)
    } else {
      showCreateHiveTable(tableMetadata)
    }

    Seq(Row(stmt))
  }

  private def showCreateHiveTable(metadata: CatalogTable): String = {
    def reportUnsupportedError(features: Seq[String]): Unit = {
      throw new AnalysisException(
        s"Failed to execute SHOW CREATE TABLE against table ${metadata.identifier.quotedString}, " +
          "which is created by Hive and uses the following unsupported feature(s)\n" +
          features.map(" - " + _).mkString("\n")
      )
    }

    if (metadata.unsupportedFeatures.nonEmpty) {
      reportUnsupportedError(metadata.unsupportedFeatures)
    }

    val builder = StringBuilder.newBuilder

    val tableTypeString = metadata.tableType match {
      case EXTERNAL => " EXTERNAL TABLE"
      case VIEW => " VIEW"
      case MANAGED => " TABLE"
    }

    builder ++= s"CREATE$tableTypeString ${table.quotedString}"

    if (metadata.tableType == VIEW) {
      if (metadata.schema.nonEmpty) {
        builder ++= metadata.schema.map(_.name).mkString("(", ", ", ")")
      }
      builder ++= metadata.viewText.mkString(" AS\n", "", "\n")
    } else {
      showHiveTableHeader(metadata, builder)
      showHiveTableNonDataColumns(metadata, builder)
      showHiveTableStorageInfo(metadata, builder)
      showHiveTableProperties(metadata, builder)
    }

    builder.toString()
  }

  private def showHiveTableHeader(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val columns = metadata.schema.filterNot { column =>
      metadata.partitionColumnNames.contains(column.name)
    }.map(columnToDDLFragment)

    if (columns.nonEmpty) {
      builder ++= columns.mkString("(", ", ", ")\n")
    }

    metadata
      .comment
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  private def columnToDDLFragment(column: StructField): String = {
    val comment = column.getComment().map(escapeSingleQuotedString).map(" COMMENT '" + _ + "'")
    s"${quoteIdentifier(column.name)} ${column.dataType.catalogString}${comment.getOrElse("")}"
  }

  private def showHiveTableNonDataColumns(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.partitionColumnNames.nonEmpty) {
      val partCols = metadata.partitionSchema.map(columnToDDLFragment)
      builder ++= partCols.mkString("PARTITIONED BY (", ", ", ")\n")
    }

    if (metadata.bucketSpec.isDefined) {
      throw new UnsupportedOperationException(
        "Creating Hive table with bucket spec is not supported yet.")
    }
  }

  private def showHiveTableStorageInfo(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val storage = metadata.storage

    storage.serde.foreach { serde =>
      builder ++= s"ROW FORMAT SERDE '$serde'\n"

      val serdeProps = metadata.storage.properties.map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= serdeProps.mkString("WITH SERDEPROPERTIES (\n  ", ",\n  ", "\n)\n")
    }

    if (storage.inputFormat.isDefined || storage.outputFormat.isDefined) {
      builder ++= "STORED AS\n"

      storage.inputFormat.foreach { format =>
        builder ++= s"  INPUTFORMAT '${escapeSingleQuotedString(format)}'\n"
      }

      storage.outputFormat.foreach { format =>
        builder ++= s"  OUTPUTFORMAT '${escapeSingleQuotedString(format)}'\n"
      }
    }

    if (metadata.tableType == EXTERNAL) {
      storage.locationUri.foreach { uri =>
        builder ++= s"LOCATION '$uri'\n"
      }
    }
  }

  private def showHiveTableProperties(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.properties.nonEmpty) {
      val filteredProps = metadata.properties.filterNot {
        // Skips "EXTERNAL" property for external tables
        case (key, _) => key == "EXTERNAL" && metadata.tableType == EXTERNAL
      }

      val props = filteredProps.map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      if (props.nonEmpty) {
        builder ++= props.mkString("TBLPROPERTIES (\n  ", ",\n  ", "\n)\n")
      }
    }
  }

  private def showCreateDataSourceTable(metadata: CatalogTable): String = {
    val builder = StringBuilder.newBuilder

    builder ++= s"CREATE TABLE ${table.quotedString} "
    showDataSourceTableDataColumns(metadata, builder)
    showDataSourceTableOptions(metadata, builder)
    showDataSourceTableNonDataColumns(metadata, builder)

    builder.toString()
  }

  private def showDataSourceTableDataColumns(
      metadata: CatalogTable, builder: StringBuilder): Unit = {
    val columns = metadata.schema.fields.map(f => s"${quoteIdentifier(f.name)} ${f.dataType.sql}")
    builder ++= columns.mkString("(", ", ", ")\n")
  }

  private def showDataSourceTableOptions(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val props = metadata.properties

    builder ++= s"USING ${metadata.provider.get}\n"

    val dataSourceOptions = metadata.storage.properties.filterNot {
      case (key, value) =>
        // If it's a managed table, omit PATH option. Spark SQL always creates external table
        // when the table creation DDL contains the PATH option.
        key.toLowerCase == "path" && metadata.tableType == MANAGED
    }.map {
      case (key, value) => s"${quoteIdentifier(key)} '${escapeSingleQuotedString(value)}'"
    }

    if (dataSourceOptions.nonEmpty) {
      builder ++= "OPTIONS (\n"
      builder ++= dataSourceOptions.mkString("  ", ",\n  ", "\n")
      builder ++= ")\n"
    }
  }

  private def showDataSourceTableNonDataColumns(
      metadata: CatalogTable, builder: StringBuilder): Unit = {
    val partCols = metadata.partitionColumnNames
    if (partCols.nonEmpty) {
      builder ++= s"PARTITIONED BY ${partCols.mkString("(", ", ", ")")}\n"
    }

    metadata.bucketSpec.foreach { spec =>
      if (spec.bucketColumnNames.nonEmpty) {
        builder ++= s"CLUSTERED BY ${spec.bucketColumnNames.mkString("(", ", ", ")")}\n"

        if (spec.sortColumnNames.nonEmpty) {
          builder ++= s"SORTED BY ${spec.sortColumnNames.mkString("(", ", ", ")")}\n"
        }

        builder ++= s"INTO ${spec.numBuckets} BUCKETS\n"
      }
    }
  }

  private def escapeSingleQuotedString(str: String): String = {
    val builder = StringBuilder.newBuilder

    str.foreach {
      case '\'' => builder ++= s"\\\'"
      case ch => builder += ch
    }

    builder.toString()
  }
}
