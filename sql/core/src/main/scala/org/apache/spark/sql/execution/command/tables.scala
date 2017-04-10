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
import java.nio.file.FileSystems
import java.util.Date

import scala.collection.{GenMap, GenSeq}
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.control.NonFatal
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types._
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A command to create a table with the same definition of the given existing table.
 * In the target table definition, the table comment is always empty but the column comments
 * are identical to the ones defined in the source table.
 *
 * The CatalogTable attributes copied from the source table are storage(inputFormat, outputFormat,
 * serde, compressed, properties), schema, provider, partitionColumnNames, bucketSpec.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   LIKE [other_db_name.]existing_table_name [locationSpec]
 * }}}
 */
case class CreateTableLikeCommand(
    targetTable: TableIdentifier,
    sourceTable: TableIdentifier,
    location: Option[String],
    ifNotExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)

    val newProvider = if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else {
      sourceTableDesc.provider
    }

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType = if (location.isEmpty) CatalogTableType.MANAGED else CatalogTableType.EXTERNAL

    val newTableDesc =
      CatalogTable(
        identifier = targetTable,
        tableType = tblType,
        storage = sourceTableDesc.storage.copy(
          locationUri = location.map(CatalogUtils.stringToURI(_))),
        schema = sourceTableDesc.schema,
        provider = newProvider,
        partitionColumnNames = sourceTableDesc.partitionColumnNames,
        bucketSpec = sourceTableDesc.bucketSpec)

    catalog.createTable(newTableDesc, ifNotExists)
    Seq.empty[Row]
  }
}

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
case class CreateTableCommand(
    table: CatalogTable,
    ignoreIfExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.catalog.createTable(table, ignoreIfExists)
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
    newName: TableIdentifier,
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
      // Invalidate the table last, otherwise uncaching the table would load the logical plan
      // back into the hive metastore cache
      catalog.refreshTable(oldName)
      catalog.renameTable(oldName, newName)
      if (wasCached) {
        sparkSession.catalog.cacheTable(newName.unquotedString)
      }
    }
    Seq.empty[Row]
  }

}

/**
 * A command that add columns to a table
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
 * }}}
*/
case class AlterTableAddColumnsCommand(
    table: TableIdentifier,
    columns: Seq[StructField]) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val catalogTable = verifyAlterTableAddColumn(catalog, table)

    try {
      sparkSession.catalog.uncacheTable(table.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${table.quotedString}", e)
    }
    catalog.refreshTable(table)

    // make sure any partition columns are at the end of the fields
    val reorderedSchema = catalogTable.dataSchema ++ columns ++ catalogTable.partitionSchema
    catalog.alterTableSchema(
      table, catalogTable.schema.copy(fields = reorderedSchema.toArray))

    Seq.empty[Row]
  }

  /**
   * ALTER TABLE ADD COLUMNS command does not support temporary view/table,
   * view, or datasource table with text, orc formats or external provider.
   * For datasource table, it currently only supports parquet, json, csv.
   */
  private def verifyAlterTableAddColumn(
      catalog: SessionCatalog,
      table: TableIdentifier): CatalogTable = {
    val catalogTable = catalog.getTempViewOrPermanentTableMetadata(table)

    if (catalogTable.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"""
          |ALTER ADD COLUMNS does not support views.
          |You must drop and re-create the views for adding the new columns. Views: $table
         """.stripMargin)
    }

    if (DDLUtils.isDatasourceTable(catalogTable)) {
      DataSource.lookupDataSource(catalogTable.provider.get).newInstance() match {
        // For datasource table, this command can only support the following File format.
        // TextFileFormat only default to one column "value"
        // OrcFileFormat can not handle difference between user-specified schema and
        // inferred schema yet. TODO, once this issue is resolved , we can add Orc back.
        // Hive type is already considered as hive serde table, so the logic will not
        // come in here.
        case _: JsonFileFormat | _: CSVFileFormat | _: ParquetFileFormat =>
        case s =>
          throw new AnalysisException(
            s"""
              |ALTER ADD COLUMNS does not support datasource table with type $s.
              |You must drop and re-create the table for adding the new columns. Tables: $table
             """.stripMargin)
      }
    }
    catalogTable
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
          s"(${targetTable.partitionColumnNames.size})")
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
        val file = new File(uri.getPath)
        val exists = if (file.getAbsolutePath.contains("*")) {
          val fileSystem = FileSystems.getDefault
          val dir = file.getParentFile.getAbsolutePath
          if (dir.contains("*")) {
            throw new AnalysisException(
              s"LOAD DATA input path allows only filename wildcard: $path")
          }

          // Note that special characters such as "*" on Windows are not allowed as a path.
          // Calling `WindowsFileSystem.getPath` throws an exception if there are in the path.
          val dirPath = fileSystem.getPath(dir)
          val pathPattern = new File(dirPath.toAbsolutePath.toString, file.getName).toURI.getPath
          val safePathPattern = if (Utils.isWindows) {
            // On Windows, the pattern should not start with slashes for absolute file paths.
            pathPattern.stripPrefix("/")
          } else {
            pathPattern
          }
          val files = new File(dir).listFiles()
          if (files == null) {
            false
          } else {
            val matcher = fileSystem.getPathMatcher("glob:" + safePathPattern)
            files.exists(f => matcher.matches(fileSystem.getPath(f.getAbsolutePath)))
          }
        } else {
          new File(file.getAbsolutePath).exists()
        }
        if (!exists) {
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
          // we will use hadoop configuration "fs.defaultFS".
          val defaultFSConf = sparkSession.sessionState.newHadoopConf().get("fs.defaultFS")
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
        inheritTableSpecs = true,
        isSrcLocal = isLocal)
    } else {
      catalog.loadTable(
        targetTable.identifier,
        loadPath.toString,
        isOverwrite,
        isSrcLocal = isLocal)
    }

    // Refresh the metadata cache to ensure the data visible to the users
    catalog.refreshTable(targetTable.identifier)

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
    val tableIdentWithDB = table.identifier.quotedString

    if (table.tableType == CatalogTableType.EXTERNAL) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on external tables: $tableIdentWithDB")
    }
    if (table.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on views: $tableIdentWithDB")
    }
    if (table.partitionColumnNames.isEmpty && partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
        s"for tables that are not partitioned: $tableIdentWithDB")
    }
    if (partitionSpec.isDefined) {
      DDLUtils.verifyPartitionProviderIsHive(spark, table, "TRUNCATE TABLE ... PARTITION")
    }

    val partCols = table.partitionColumnNames
    val locations =
      if (partCols.isEmpty) {
        Seq(table.storage.locationUri)
      } else {
        val normalizedSpec = partitionSpec.map { spec =>
          PartitioningUtils.normalizePartitionSpec(
            spec,
            partCols,
            table.identifier.quotedString,
            spark.sessionState.conf.resolver)
        }
        val partLocations =
          catalog.listPartitions(table.identifier, normalizedSpec).map(_.storage.locationUri)

        // Fail if the partition spec is fully specified (not partial) and the partition does not
        // exist.
        for (spec <- partitionSpec if partLocations.isEmpty && spec.size == partCols.length) {
          throw new NoSuchPartitionException(table.database, table.identifier.table, spec)
        }

        partLocations
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
              s"Failed to truncate table $tableIdentWithDB when removing data of the path: $path " +
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
        log.warn(s"Exception when attempting to uncache table $tableIdentWithDB", e)
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
    isExtended: Boolean)
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
      if (metadata.schema.isEmpty) {
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        describeSchema(sparkSession.table(metadata.identifier).schema, result)
      } else {
        describeSchema(metadata.schema, result)
      }

      describePartitionInfo(metadata, result)

      if (partitionSpec.nonEmpty) {
        // Outputs the partition-specific info for the DDL command:
        // "DESCRIBE [EXTENDED|FORMATTED] table_name PARTITION (partitionVal*)"
        describeDetailedPartitionInfo(sparkSession, catalog, metadata, result)
      } else if (isExtended) {
        describeFormattedTableInfo(metadata, result)
      }
    }

    result
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (table.partitionColumnNames.nonEmpty) {
      append(buffer, "# Partition Information", "", "")
      describeSchema(table.partitionSchema, buffer)
    }
  }

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    // The following information has been already shown in the previous outputs
    val excludedTableInfo = Seq(
      "Partition Columns",
      "Schema"
    )
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    table.toLinkedHashMap.filterKeys(!excludedTableInfo.contains(_)).foreach {
      s => append(buffer, s._1, s._2, "")
    }
  }

  private def describeDetailedPartitionInfo(
      spark: SparkSession,
      catalog: SessionCatalog,
      metadata: CatalogTable,
      result: ArrayBuffer[Row]): Unit = {
    if (metadata.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"DESC PARTITION is not allowed on a view: ${table.identifier}")
    }
    DDLUtils.verifyPartitionProviderIsHive(spark, metadata, "DESC PARTITION")
    val partition = catalog.getPartition(table, partitionSpec)
    if (isExtended) describeFormattedDetailedPartitionInfo(table, metadata, partition, result)
  }

  private def describeFormattedDetailedPartitionInfo(
      tableIdentifier: TableIdentifier,
      table: CatalogTable,
      partition: CatalogTablePartition,
      buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Partition Information", "", "")
    append(buffer, "Database", table.database, "")
    append(buffer, "Table", tableIdentifier.table, "")
    partition.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
    append(buffer, "", "", "")
    append(buffer, "# Storage Information", "", "")
    table.bucketSpec match {
      case Some(spec) =>
        spec.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
      case _ =>
    }
    table.storage.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
  }

  private def describeSchema(schema: StructType, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
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
 *   SHOW TABLE EXTENDED [(IN|FROM) database_name] LIKE 'identifier_with_wildcards'
 *   [PARTITION(partition_spec)];
 * }}}
 */
case class ShowTablesCommand(
    databaseName: Option[String],
    tableIdentifierPattern: Option[String],
    isExtended: Boolean = false,
    partitionSpec: Option[TablePartitionSpec] = None) extends RunnableCommand {

  // The result of SHOW TABLES/SHOW TABLE has three basic columns: database, tableName and
  // isTemporary. If `isExtended` is true, append column `information` to the output columns.
  override val output: Seq[Attribute] = {
    val tableExtendedInfo = if (isExtended) {
      AttributeReference("information", StringType, nullable = false)() :: Nil
    } else {
      Nil
    }
    AttributeReference("database", StringType, nullable = false)() ::
      AttributeReference("tableName", StringType, nullable = false)() ::
      AttributeReference("isTemporary", BooleanType, nullable = false)() :: tableExtendedInfo
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Since we need to return a Seq of rows, we will call getTables directly
    // instead of calling tables in sparkSession.
    val catalog = sparkSession.sessionState.catalog
    val db = databaseName.getOrElse(catalog.getCurrentDatabase)
    if (partitionSpec.isEmpty) {
      // Show the information of tables.
      val tables =
        tableIdentifierPattern.map(catalog.listTables(db, _)).getOrElse(catalog.listTables(db))
      tables.map { tableIdent =>
        val database = tableIdent.database.getOrElse("")
        val tableName = tableIdent.table
        val isTemp = catalog.isTemporaryTable(tableIdent)
        if (isExtended) {
          val information = catalog.getTempViewOrPermanentTableMetadata(tableIdent).simpleString
          Row(database, tableName, isTemp, s"$information\n")
        } else {
          Row(database, tableName, isTemp)
        }
      }
    } else {
      // Show the information of partitions.
      //
      // Note: tableIdentifierPattern should be non-empty, otherwise a [[ParseException]]
      // should have been thrown by the sql parser.
      val tableIdent = TableIdentifier(tableIdentifierPattern.get, Some(db))
      val table = catalog.getTableMetadata(tableIdent).identifier
      val partition = catalog.getPartition(tableIdent, partitionSpec.get)
      val database = table.database.getOrElse("")
      val tableName = table.table
      val isTemp = catalog.isTemporaryTable(table)
      val information = partition.simpleString
      Seq(Row(database, tableName, isTemp, s"$information\n"))
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
case class ShowColumnsCommand(
    databaseName: Option[String],
    tableName: TableIdentifier) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("col_name", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val resolver = sparkSession.sessionState.conf.resolver
    val lookupTable = databaseName match {
      case None => tableName
      case Some(db) if tableName.database.exists(!resolver(_, db)) =>
        throw new AnalysisException(
          s"SHOW COLUMNS with conflicting databases: '$db' != '${tableName.database.get}'")
      case Some(db) => TableIdentifier(tableName.identifier, Some(db))
    }
    val table = catalog.getTempViewOrPermanentTableMetadata(lookupTable)
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

    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "SHOW PARTITIONS")

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

    val partNames = catalog.listPartitionNames(tableName, spec)
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
        s"Failed to execute SHOW CREATE TABLE against table/view ${metadata.identifier}, " +
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
      val props = metadata.properties.map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= props.mkString("TBLPROPERTIES (\n  ", ",\n  ", "\n)\n")
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
    builder ++= s"USING ${metadata.provider.get}\n"

    val dataSourceOptions = metadata.storage.properties.map {
      case (key, value) => s"${quoteIdentifier(key)} '${escapeSingleQuotedString(value)}'"
    } ++ metadata.storage.locationUri.flatMap { location =>
      if (metadata.tableType == MANAGED) {
        // If it's a managed table, omit PATH option. Spark SQL always creates external table
        // when the table creation DDL contains the PATH option.
        None
      } else {
        Some(s"path '${escapeSingleQuotedString(CatalogUtils.URIToString(location))}'")
      }
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


/**
 * Drops a table/view from the metastore and removes it if it is cached.
 *
 * The syntax of this command is:
 * {{{
 *   DROP TABLE [IF EXISTS] table_name;
 *   DROP VIEW [IF EXISTS] [db_name.]view_name;
 * }}}
 */
case class DropTableCommand(
    tableName: TableIdentifier,
    ifExists: Boolean,
    isView: Boolean,
    purge: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog

    if (!catalog.isTemporaryTable(tableName) && catalog.tableExists(tableName)) {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      catalog.getTableMetadata(tableName).tableType match {
        case CatalogTableType.VIEW if !isView =>
          throw new AnalysisException(
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
        case o if o != CatalogTableType.VIEW && isView =>
          throw new AnalysisException(
            s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead")
        case _ =>
      }
    }
    try {
      sparkSession.sharedState.cacheManager.uncacheQuery(sparkSession.table(tableName))
    } catch {
      case _: NoSuchTableException if ifExists =>
      case NonFatal(e) => log.warn(e.toString, e)
    }
    catalog.refreshTable(tableName)
    catalog.dropTable(tableName, ifExists, purge)
    Seq.empty[Row]
  }
}

/**
 * A command that sets table/view properties.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 *   ALTER VIEW view1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 * }}}
 */
case class AlterTableSetPropertiesCommand(
    tableName: TableIdentifier,
    properties: Map[String, String],
    isView: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView)
    // This overrides old properties
    val newTable = table.copy(properties = table.properties ++ properties)
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

/**
 * A command that unsets table/view properties.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 *   ALTER VIEW view1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 * }}}
 */
case class AlterTableUnsetPropertiesCommand(
    tableName: TableIdentifier,
    propKeys: Seq[String],
    ifExists: Boolean,
    isView: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView)
    if (!ifExists) {
      propKeys.foreach { k =>
        if (!table.properties.contains(k)) {
          throw new AnalysisException(
            s"Attempted to unset non-existent property '$k' in table '${table.identifier}'")
        }
      }
    }
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties)
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}


/**
 * A command to change the column for a table, only support changing the comment of a non-partition
 * column for now.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment]
 *   [FIRST | AFTER column_name];
 * }}}
 */
case class AlterTableChangeColumnCommand(
    tableName: TableIdentifier,
    columnName: String,
    newColumn: StructField) extends RunnableCommand {

  // TODO: support change column name/dataType/metadata/position.
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val resolver = sparkSession.sessionState.conf.resolver
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)

    // Find the origin column from schema by column name.
    val originColumn = findColumnByName(table.schema, columnName, resolver)
    // Throw an AnalysisException if the column name/dataType is changed.
    if (!columnEqual(originColumn, newColumn, resolver)) {
      throw new AnalysisException(
        "ALTER TABLE CHANGE COLUMN is not supported for changing column " +
          s"'${originColumn.name}' with type '${originColumn.dataType}' to " +
          s"'${newColumn.name}' with type '${newColumn.dataType}'")
    }

    val newSchema = table.schema.fields.map { field =>
      if (field.name == originColumn.name) {
        // Create a new column from the origin column with the new comment.
        addComment(field, newColumn.getComment)
      } else {
        field
      }
    }
    val newTable = table.copy(schema = StructType(newSchema))
    catalog.alterTable(newTable)

    Seq.empty[Row]
  }

  // Find the origin column from schema by column name, throw an AnalysisException if the column
  // reference is invalid.
  private def findColumnByName(
      schema: StructType, name: String, resolver: Resolver): StructField = {
    schema.fields.collectFirst {
      case field if resolver(field.name, name) => field
    }.getOrElse(throw new AnalysisException(
      s"Invalid column reference '$name', table schema is '${schema}'"))
  }

  // Add the comment to a column, if comment is empty, return the original column.
  private def addComment(column: StructField, comment: Option[String]): StructField = {
    comment.map(column.withComment(_)).getOrElse(column)
  }

  // Compare a [[StructField]] to another, return true if they have the same column
  // name(by resolver) and dataType.
  private def columnEqual(
                           field: StructField, other: StructField, resolver: Resolver): Boolean = {
    resolver(field.name, other.name) && field.dataType == other.dataType
  }
}

/**
 * A command that sets the serde class and/or serde properties of a table/view.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
 *   ALTER TABLE table [PARTITION spec] SET SERDEPROPERTIES serde_properties;
 * }}}
 */
case class AlterTableSerDePropertiesCommand(
    tableName: TableIdentifier,
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, String]],
    partSpec: Option[TablePartitionSpec])
  extends RunnableCommand {

  // should never happen if we parsed things correctly
  require(serdeClassName.isDefined || serdeProperties.isDefined,
    "ALTER TABLE attempted to set neither serde class name nor serde properties")

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    // For datasource tables, disallow setting serde or specifying partition
    if (partSpec.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException("Operation not allowed: ALTER TABLE SET " +
        "[SERDE | SERDEPROPERTIES] for a specific partition is not supported " +
        "for tables created with the datasource API")
    }
    if (serdeClassName.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException("Operation not allowed: ALTER TABLE SET SERDE is " +
        "not supported for tables created with the datasource API")
    }
    if (partSpec.isEmpty) {
      val newTable = table.withNewStorage(
        serde = serdeClassName.orElse(table.storage.serde),
        properties = table.storage.properties ++ serdeProperties.getOrElse(Map()))
      catalog.alterTable(newTable)
    } else {
      val spec = partSpec.get
      val part = catalog.getPartition(table.identifier, spec)
      val newPart = part.copy(storage = part.storage.copy(
        serde = serdeClassName.orElse(part.storage.serde),
        properties = part.storage.properties ++ serdeProperties.getOrElse(Map())))
      catalog.alterPartitions(table.identifier, Seq(newPart))
    }
    Seq.empty[Row]
  }

}

/**
 * Add Partition in ALTER TABLE: add the table partitions.
 *
 * An error message will be issued if the partition exists, unless 'ifNotExists' is true.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table ADD [IF NOT EXISTS] PARTITION spec1 [LOCATION 'loc1']
 *                                         PARTITION spec2 [LOCATION 'loc2']
 * }}}
 */
case class AlterTableAddPartitionCommand(
    tableName: TableIdentifier,
    partitionSpecsAndLocs: Seq[(TablePartitionSpec, Option[String])],
    ifNotExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE ADD PARTITION")
    val parts = partitionSpecsAndLocs.map { case (spec, location) =>
      val normalizedSpec = PartitioningUtils.normalizePartitionSpec(
        spec,
        table.partitionColumnNames,
        table.identifier.quotedString,
        sparkSession.sessionState.conf.resolver)
      // inherit table storage format (possibly except for location)
      CatalogTablePartition(normalizedSpec, table.storage.copy(
        locationUri = location.map(CatalogUtils.stringToURI(_))))
    }
    catalog.createPartitions(table.identifier, parts, ignoreIfExists = ifNotExists)
    Seq.empty[Row]
  }

}

/**
 * Alter a table partition's spec.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table PARTITION spec1 RENAME TO PARTITION spec2;
 * }}}
 */
case class AlterTableRenamePartitionCommand(
    tableName: TableIdentifier,
    oldPartition: TablePartitionSpec,
    newPartition: TablePartitionSpec)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE RENAME PARTITION")

    val normalizedOldPartition = PartitioningUtils.normalizePartitionSpec(
      oldPartition,
      table.partitionColumnNames,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver)

    val normalizedNewPartition = PartitioningUtils.normalizePartitionSpec(
      newPartition,
      table.partitionColumnNames,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver)

    catalog.renamePartitions(
      tableName, Seq(normalizedOldPartition), Seq(normalizedNewPartition))
    Seq.empty[Row]
  }

}

/**
 * Drop Partition in ALTER TABLE: to drop a particular partition for a table.
 *
 * This removes the data and metadata for this partition.
 * The data is actually moved to the .Trash/Current directory if Trash is configured,
 * unless 'purge' is true, but the metadata is completely lost.
 * An error message will be issued if the partition does not exist, unless 'ifExists' is true.
 * Note: purge is always false when the target is a view.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE];
 * }}}
 */
case class AlterTableDropPartitionCommand(
    tableName: TableIdentifier,
    specs: Seq[TablePartitionSpec],
    ifExists: Boolean,
    purge: Boolean,
    retainData: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE DROP PARTITION")

    val normalizedSpecs = specs.map { spec =>
      PartitioningUtils.normalizePartitionSpec(
        spec,
        table.partitionColumnNames,
        table.identifier.quotedString,
        sparkSession.sessionState.conf.resolver)
    }

    catalog.dropPartitions(
      table.identifier, normalizedSpecs, ignoreIfNotExists = ifExists, purge = purge,
      retainData = retainData)
    Seq.empty[Row]
  }

}


case class PartitionStatistics(numFiles: Int, totalSize: Long)

/**
 * Recover Partitions in ALTER TABLE: recover all the partition in the directory of a table and
 * update the catalog.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table RECOVER PARTITIONS;
 *   MSCK REPAIR TABLE table;
 * }}}
 */
case class AlterTableRecoverPartitionsCommand(
    tableName: TableIdentifier,
    cmd: String = "ALTER TABLE RECOVER PARTITIONS") extends RunnableCommand {

  // These are list of statistics that can be collected quickly without requiring a scan of the data
  // see https://github.com/apache/hive/blob/master/
  //   common/src/java/org/apache/hadoop/hive/common/StatsSetupConst.java
  val NUM_FILES = "numFiles"
  val TOTAL_SIZE = "totalSize"
  val DDL_TIME = "transient_lastDdlTime"

  private def getPathFilter(hadoopConf: Configuration): PathFilter = {
    // Dummy jobconf to get to the pathFilter defined in configuration
    // It's very expensive to create a JobConf(ClassUtil.findContainingJar() is slow)
    val jobConf = new JobConf(hadoopConf, this.getClass)
    val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
    new PathFilter {
      override def accept(path: Path): Boolean = {
        val name = path.getName
        if (name != "_SUCCESS" && name != "_temporary" && !name.startsWith(".")) {
          pathFilter == null || pathFilter.accept(path)
        } else {
          false
        }
      }
    }
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    if (table.partitionColumnNames.isEmpty) {
      throw new AnalysisException(
        s"Operation not allowed: $cmd only works on partitioned tables: $tableIdentWithDB")
    }

    if (table.storage.locationUri.isEmpty) {
      throw new AnalysisException(s"Operation not allowed: $cmd only works on table with " +
        s"location provided: $tableIdentWithDB")
    }

    val root = new Path(table.location)
    logInfo(s"Recover all the partitions in $root")
    val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val threshold = spark.conf.get("spark.rdd.parallelListingThreshold", "10").toInt
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val pathFilter = getPathFilter(hadoopConf)
    val partitionSpecsAndLocs = scanPartitions(spark, fs, pathFilter, root, Map(),
      table.partitionColumnNames, threshold, spark.sessionState.conf.resolver)
    val total = partitionSpecsAndLocs.length
    logInfo(s"Found $total partitions in $root")

    val partitionStats = if (spark.sqlContext.conf.gatherFastStats) {
      gatherPartitionStats(spark, partitionSpecsAndLocs, fs, pathFilter, threshold)
    } else {
      GenMap.empty[String, PartitionStatistics]
    }
    logInfo(s"Finished to gather the fast stats for all $total partitions.")

    addPartitions(spark, table, partitionSpecsAndLocs, partitionStats)
    // Updates the table to indicate that its partition metadata is stored in the Hive metastore.
    // This is always the case for Hive format tables, but is not true for Datasource tables created
    // before Spark 2.1 unless they are converted via `msck repair table`.
    spark.sessionState.catalog.alterTable(table.copy(tracksPartitionsInCatalog = true))
    catalog.refreshTable(tableName)
    logInfo(s"Recovered all partitions ($total).")
    Seq.empty[Row]
  }

  @transient private lazy val evalTaskSupport = new ForkJoinTaskSupport(new ForkJoinPool(8))

  private def scanPartitions(
      spark: SparkSession,
      fs: FileSystem,
      filter: PathFilter,
      path: Path,
      spec: TablePartitionSpec,
      partitionNames: Seq[String],
      threshold: Int,
      resolver: Resolver): GenSeq[(TablePartitionSpec, Path)] = {
    if (partitionNames.isEmpty) {
      return Seq(spec -> path)
    }

    val statuses = fs.listStatus(path, filter)
    val statusPar: GenSeq[FileStatus] =
      if (partitionNames.length > 1 && statuses.length > threshold || partitionNames.length > 2) {
        // parallelize the list of partitions here, then we can have better parallelism later.
        val parArray = statuses.par
        parArray.tasksupport = evalTaskSupport
        parArray
      } else {
        statuses
      }
    statusPar.flatMap { st =>
      val name = st.getPath.getName
      if (st.isDirectory && name.contains("=")) {
        val ps = name.split("=", 2)
        val columnName = ExternalCatalogUtils.unescapePathName(ps(0))
        // TODO: Validate the value
        val value = ExternalCatalogUtils.unescapePathName(ps(1))
        if (resolver(columnName, partitionNames.head)) {
          scanPartitions(spark, fs, filter, st.getPath, spec ++ Map(partitionNames.head -> value),
            partitionNames.drop(1), threshold, resolver)
        } else {
          logWarning(
            s"expected partition column ${partitionNames.head}, but got ${ps(0)}, ignoring it")
          Seq()
        }
      } else {
        logWarning(s"ignore ${new Path(path, name)}")
        Seq()
      }
    }
  }

  private def gatherPartitionStats(
      spark: SparkSession,
      partitionSpecsAndLocs: GenSeq[(TablePartitionSpec, Path)],
      fs: FileSystem,
      pathFilter: PathFilter,
      threshold: Int): GenMap[String, PartitionStatistics] = {
    if (partitionSpecsAndLocs.length > threshold) {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val serializableConfiguration = new SerializableConfiguration(hadoopConf)
      val serializedPaths = partitionSpecsAndLocs.map(_._2.toString).toArray

      // Set the number of parallelism to prevent following file listing from generating many tasks
      // in case of large #defaultParallelism.
      val numParallelism = Math.min(serializedPaths.length,
        Math.min(spark.sparkContext.defaultParallelism, 10000))
      // gather the fast stats for all the partitions otherwise Hive metastore will list all the
      // files for all the new partitions in sequential way, which is super slow.
      logInfo(s"Gather the fast stats in parallel using $numParallelism tasks.")
      spark.sparkContext.parallelize(serializedPaths, numParallelism)
        .mapPartitions { paths =>
          val pathFilter = getPathFilter(serializableConfiguration.value)
          paths.map(new Path(_)).map{ path =>
            val fs = path.getFileSystem(serializableConfiguration.value)
            val statuses = fs.listStatus(path, pathFilter)
            (path.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
          }
        }.collectAsMap()
    } else {
      partitionSpecsAndLocs.map { case (_, location) =>
        val statuses = fs.listStatus(location, pathFilter)
        (location.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
      }.toMap
    }
  }

  private def addPartitions(
      spark: SparkSession,
      table: CatalogTable,
      partitionSpecsAndLocs: GenSeq[(TablePartitionSpec, Path)],
      partitionStats: GenMap[String, PartitionStatistics]): Unit = {
    val total = partitionSpecsAndLocs.length
    var done = 0L
    // Hive metastore may not have enough memory to handle millions of partitions in single RPC,
    // we should split them into smaller batches. Since Hive client is not thread safe, we cannot
    // do this in parallel.
    val batchSize = 100
    partitionSpecsAndLocs.toIterator.grouped(batchSize).foreach { batch =>
      val now = System.currentTimeMillis() / 1000
      val parts = batch.map { case (spec, location) =>
        val params = partitionStats.get(location.toString).map {
          case PartitionStatistics(numFiles, totalSize) =>
            // This two fast stat could prevent Hive metastore to list the files again.
            Map(NUM_FILES -> numFiles.toString,
              TOTAL_SIZE -> totalSize.toString,
              // Workaround a bug in HiveMetastore that try to mutate a read-only parameters.
              // see metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java
              DDL_TIME -> now.toString)
        }.getOrElse(Map.empty)
        // inherit table storage format (possibly except for location)
        CatalogTablePartition(
          spec,
          table.storage.copy(locationUri = Some(location.toUri)),
          params)
      }
      spark.sessionState.catalog.createPartitions(tableName, parts, ignoreIfExists = true)
      done += parts.length
      logDebug(s"Recovered ${parts.length} partitions ($done/$total so far)")
    }
  }
}


/**
 * A command that sets the location of a table or a partition.
 *
 * For normal tables, this just sets the location URI in the table/partition's storage format.
 * For datasource tables, this sets a "path" parameter in the table/partition's serde properties.
 *
 * The syntax of this command is:
 * {{{
 *    ALTER TABLE table_name [PARTITION partition_spec] SET LOCATION "loc";
 * }}}
 */
case class AlterTableSetLocationCommand(
     tableName: TableIdentifier,
     partitionSpec: Option[TablePartitionSpec],
     location: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val locUri = CatalogUtils.stringToURI(location)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    partitionSpec match {
      case Some(spec) =>
        DDLUtils.verifyPartitionProviderIsHive(
          sparkSession, table, "ALTER TABLE ... SET LOCATION")
        // Partition spec is specified, so we set the location only for this partition
        val part = catalog.getPartition(table.identifier, spec)
        val newPart = part.copy(storage = part.storage.copy(locationUri = Some(locUri)))
        catalog.alterPartitions(table.identifier, Seq(newPart))
      case None =>
        // No partition spec is specified, so we set the location for the table itself
        catalog.alterTable(table.withNewStorage(locationUri = Some(locUri)))
    }
    Seq.empty[Row]
  }
}
