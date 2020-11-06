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

import java.net.{URI, URISyntaxException}
import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.collection.{GenMap, GenSeq}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileStatus, FileSystem, FsConstants, Path, PathFilter}
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope, AclEntryType, FsAction, FsPermission}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.DescribeCommandSchema
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIdentifier, CaseInsensitiveMap}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils, Utils}

/**
 * A command to create a table with the same definition of the given existing table.
 * In the target table definition, the table comment is always empty but the column comments
 * are identical to the ones defined in the source table.
 *
 * The CatalogTable attributes copied from the source table are storage(inputFormat, outputFormat,
 * serde, compressed, properties), schema, provider, partitionColumnNames, bucketSpec by default.
 *
 * Use "CREATE TABLE t1 LIKE t2 USING file_format" to specify new provider for t1.
 * For Hive compatibility, use "CREATE TABLE t1 LIKE t2 STORED AS hiveFormat"
 * to specify new file storage format (inputFormat, outputFormat, serde) for t1.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   LIKE [other_db_name.]existing_table_name
 *   [USING provider |
 *    [
 *     [ROW FORMAT row_format]
 *     [STORED AS file_format] [WITH SERDEPROPERTIES (...)]
 *    ]
 *   ]
 *   [locationSpec]
 *   [TBLPROPERTIES (property_name=property_value, ...)]
 * }}}
 */
case class CreateTableLikeCommand(
    targetTable: TableIdentifier,
    sourceTable: TableIdentifier,
    fileFormat: CatalogStorageFormat,
    provider: Option[String],
    properties: Map[String, String] = Map.empty,
    ifNotExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)
    val newProvider = if (provider.isDefined) {
      if (!DDLUtils.isHiveTable(provider)) {
        // check the validation of provider input, invalid provider will throw
        // AnalysisException, ClassNotFoundException, or NoClassDefFoundError
        DataSource.lookupDataSource(provider.get, sparkSession.sessionState.conf)
      }
      provider
    } else if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else if (fileFormat.inputFormat.isDefined) {
      Some(DDLUtils.HIVE_PROVIDER)
    } else {
      sourceTableDesc.provider
    }

    val newStorage = if (fileFormat.inputFormat.isDefined) {
      fileFormat
    } else {
      sourceTableDesc.storage.copy(locationUri = fileFormat.locationUri)
    }

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType = if (newStorage.locationUri.isEmpty) {
      CatalogTableType.MANAGED
    } else {
      CatalogTableType.EXTERNAL
    }

    val newTableDesc =
      CatalogTable(
        identifier = targetTable,
        tableType = tblType,
        storage = newStorage,
        schema = sourceTableDesc.schema,
        provider = newProvider,
        partitionColumnNames = sourceTableDesc.partitionColumnNames,
        bucketSpec = sourceTableDesc.bucketSpec,
        properties = properties,
        tracksPartitionsInCatalog = sourceTableDesc.tracksPartitionsInCatalog)

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
    val isTempView = catalog.isTemporaryTable(tableName)

    if (!isTempView && catalog.tableExists(tableName)) {
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

    if (isTempView || catalog.tableExists(tableName)) {
      try {
        sparkSession.sharedState.cacheManager.uncacheQuery(
          sparkSession.table(tableName), cascade = !isTempView)
      } catch {
        case NonFatal(e) => log.warn(e.toString, e)
      }
      catalog.refreshTable(tableName)
      catalog.dropTable(tableName, ifExists, purge)
    } else if (ifExists) {
      // no-op
    } else {
      throw new AnalysisException(s"Table or view not found: ${tableName.identifier}")
    }
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
        CommandUtils.uncacheTableOrView(sparkSession, oldName.unquotedString)
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
    colsToAdd: Seq[StructField]) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val catalogTable = verifyAlterTableAddColumn(sparkSession.sessionState.conf, catalog, table)

    CommandUtils.uncacheTableOrView(sparkSession, table.quotedString)
    catalog.refreshTable(table)

    SchemaUtils.checkColumnNameDuplication(
      (colsToAdd ++ catalogTable.schema).map(_.name),
      "in the table definition of " + table.identifier,
      conf.caseSensitiveAnalysis)
    DDLUtils.checkDataColNames(catalogTable, colsToAdd.map(_.name))

    catalog.alterTableDataSchema(table, StructType(catalogTable.dataSchema ++ colsToAdd))
    Seq.empty[Row]
  }

  /**
   * ALTER TABLE ADD COLUMNS command does not support temporary view/table,
   * view, or datasource table with text, orc formats or external provider.
   * For datasource table, it currently only supports parquet, json, csv, orc.
   */
  private def verifyAlterTableAddColumn(
      conf: SQLConf,
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
      DataSource.lookupDataSource(catalogTable.provider.get, conf).
        getConstructor().newInstance() match {
        // For datasource table, this command can only support the following File format.
        // TextFileFormat only default to one column "value"
        // Hive type is already considered as hive serde table, so the logic will not
        // come in here.
        case _: CSVFileFormat | _: JsonFileFormat | _: ParquetFileFormat =>
        case _: JsonDataSourceV2 | _: CSVDataSourceV2 |
             _: OrcDataSourceV2 | _: ParquetDataSourceV2 =>
        case s if s.getClass.getCanonicalName.endsWith("OrcFileFormat") =>
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
    // This overrides old properties and update the comment parameter of CatalogTable
    // with the newly added/modified comment since CatalogTable also holds comment as its
    // direct property.
    val newTable = table.copy(
      properties = table.properties ++ properties,
      comment = properties.get(TableCatalog.PROP_COMMENT).orElse(table.comment))
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
        if (!table.properties.contains(k) && k != TableCatalog.PROP_COMMENT) {
          throw new AnalysisException(
            s"Attempted to unset non-existent property '$k' in table '${table.identifier}'")
        }
      }
    }
    // If comment is in the table property, we reset it to None
    val tableComment = if (propKeys.contains(TableCatalog.PROP_COMMENT)) None else table.comment
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties, comment = tableComment)
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

    // Find the origin column from dataSchema by column name.
    val originColumn = findColumnByName(table.dataSchema, columnName, resolver)
    // Throw an AnalysisException if the column name/dataType is changed.
    if (!columnEqual(originColumn, newColumn, resolver)) {
      throw new AnalysisException(
        "ALTER TABLE CHANGE COLUMN is not supported for changing column " +
          s"'${originColumn.name}' with type '${originColumn.dataType}' to " +
          s"'${newColumn.name}' with type '${newColumn.dataType}'")
    }

    val newDataSchema = table.dataSchema.fields.map { field =>
      if (field.name == originColumn.name) {
        // Create a new column from the origin column with the new comment.
        addComment(field, newColumn.getComment)
      } else {
        field
      }
    }
    catalog.alterTableDataSchema(tableName, StructType(newDataSchema))

    Seq.empty[Row]
  }

  // Find the origin column from schema by column name, throw an AnalysisException if the column
  // reference is invalid.
  private def findColumnByName(
      schema: StructType, name: String, resolver: Resolver): StructField = {
    schema.fields.collectFirst {
      case field if resolver(field.name, name) => field
    }.getOrElse(throw new AnalysisException(
      s"Can't find column `$name` given table data columns " +
        s"${schema.fieldNames.mkString("[`", "`, `", "`]")}"))
  }

  // Add the comment to a column, if comment is empty, return the original column.
  private def addComment(column: StructField, comment: Option[String]): StructField =
    comment.map(column.withComment).getOrElse(column)

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
        locationUri = location.map(CatalogUtils.stringToURI)))
    }

    // Hive metastore may not have enough memory to handle millions of partitions in single RPC.
    // Also the request to metastore times out when adding lot of partitions in one shot.
    // we should split them into smaller batches
    val batchSize = conf.getConf(SQLConf.ADD_PARTITION_BATCH_SIZE)
    parts.toIterator.grouped(batchSize).foreach { batch =>
      catalog.createPartitions(table.identifier, batch, ignoreIfExists = ifNotExists)
    }

    if (table.stats.nonEmpty) {
      if (sparkSession.sessionState.conf.autoSizeUpdateEnabled) {
        val addedSize = CommandUtils.calculateMultipleLocationSizes(sparkSession, table.identifier,
          parts.map(_.storage.locationUri)).sum
        if (addedSize > 0) {
          val newStats = CatalogStatistics(sizeInBytes = table.stats.get.sizeInBytes + addedSize)
          catalog.alterTableStats(table.identifier, Some(newStats))
        }
      } else {
        catalog.alterTableStats(table.identifier, None)
      }
    }
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

    CommandUtils.updateTableStats(sparkSession, table)

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
    path: Path => {
      val name = path.getName
      if (name != "_SUCCESS" && name != "_temporary" && !name.startsWith(".")) {
        pathFilter == null || pathFilter.accept(path)
      } else {
        false
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
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fs = root.getFileSystem(hadoopConf)

    val threshold = spark.sparkContext.conf.get(RDD_PARALLEL_LISTING_THRESHOLD)
    val pathFilter = getPathFilter(hadoopConf)

    val evalPool = ThreadUtils.newForkJoinPool("AlterTableRecoverPartitionsCommand", 8)
    val partitionSpecsAndLocs: GenSeq[(TablePartitionSpec, Path)] =
      try {
        scanPartitions(spark, fs, pathFilter, root, Map(), table.partitionColumnNames, threshold,
          spark.sessionState.conf.resolver, new ForkJoinTaskSupport(evalPool)).seq
      } finally {
        evalPool.shutdown()
      }
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

  private def scanPartitions(
      spark: SparkSession,
      fs: FileSystem,
      filter: PathFilter,
      path: Path,
      spec: TablePartitionSpec,
      partitionNames: Seq[String],
      threshold: Int,
      resolver: Resolver,
      evalTaskSupport: ForkJoinTaskSupport): GenSeq[(TablePartitionSpec, Path)] = {
    if (partitionNames.isEmpty) {
      return Seq(spec -> path)
    }

    val statuses = fs.listStatus(path, filter)
    val statusPar: GenSeq[FileStatus] =
      if (partitionNames.length > 1 && statuses.length > threshold || partitionNames.length > 2) {
        // parallelize the list of partitions here, then we can have better parallelism later.
        val parArray = new ParVector(statuses.toVector)
        parArray.tasksupport = evalTaskSupport
        parArray.seq
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
            partitionNames.drop(1), threshold, resolver, evalTaskSupport)
        } else {
          logWarning(
            s"expected partition column ${partitionNames.head}, but got ${ps(0)}, ignoring it")
          Seq.empty
        }
      } else {
        logWarning(s"ignore ${new Path(path, name)}")
        Seq.empty
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
      val hadoopConf = spark.sessionState.newHadoopConf()
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
      val now = MILLISECONDS.toSeconds(System.currentTimeMillis())
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

    CommandUtils.updateTableStats(sparkSession, table)
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
    val normalizedSpec = partition.map { spec =>
      PartitioningUtils.normalizePartitionSpec(
        spec,
        targetTable.partitionColumnNames,
        tableIdentwithDB,
        sparkSession.sessionState.conf.resolver)
    }

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
    } else {
      if (partition.nonEmpty) {
        throw new AnalysisException(s"LOAD DATA target table $tableIdentwithDB is not " +
          s"partitioned, but a partition spec was provided.")
      }
    }
    val loadPath = {
      if (isLocal) {
        val localFS = FileContext.getLocalFSFileContext()
        LoadDataCommand.makeQualified(FsConstants.LOCAL_FS_URI, localFS.getWorkingDirectory(),
          new Path(path))
      } else {
        val loadPath = new Path(path)
        // Follow Hive's behavior:
        // If no schema or authority is provided with non-local inpath,
        // we will use hadoop configuration "fs.defaultFS".
        val defaultFSConf = sparkSession.sessionState.newHadoopConf().get("fs.defaultFS")
        val defaultFS = if (defaultFSConf == null) new URI("") else new URI(defaultFSConf)
        // Follow Hive's behavior:
        // If LOCAL is not specified, and the path is relative,
        // then the path is interpreted relative to "/user/<username>"
        val uriPath = new Path(s"/user/${System.getProperty("user.name")}/")
        // makeQualified() will ignore the query parameter part while creating a path, so the
        // entire  string will be considered while making a Path instance,this is mainly done
        // by considering the wild card scenario in mind.as per old logic query param  is
        // been considered while creating URI instance and if path contains wild card char '?'
        // the remaining charecters after '?' will be removed while forming URI instance
        LoadDataCommand.makeQualified(defaultFS, uriPath, loadPath)
      }
    }
    val fs = loadPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
    // This handling is because while resolving the invalid URLs starting with file:///
    // system throws IllegalArgumentException from globStatus API,so in order to handle
    // such scenarios this code is added in try catch block and after catching the
    // runtime exception a generic error will be displayed to the user.
    try {
      val fileStatus = fs.globStatus(loadPath)
      if (fileStatus == null || fileStatus.isEmpty) {
        throw new AnalysisException(s"LOAD DATA input path does not exist: $path")
      }
    } catch {
      case e: IllegalArgumentException =>
        log.warn(s"Exception while validating the load path $path ", e)
        throw new AnalysisException(s"LOAD DATA input path does not exist: $path")
    }
    if (partition.nonEmpty) {
      catalog.loadPartition(
        targetTable.identifier,
        loadPath.toString,
        normalizedSpec.get,
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

    CommandUtils.updateTableStats(sparkSession, targetTable)
    Seq.empty[Row]
  }
}

object LoadDataCommand {
  /**
   * Returns a qualified path object. Method ported from org.apache.hadoop.fs.Path class.
   *
   * @param defaultUri default uri corresponding to the filesystem provided.
   * @param workingDir the working directory for the particular child path wd-relative names.
   * @param path       Path instance based on the path string specified by the user.
   * @return qualified path object
   */
  private[sql] def makeQualified(defaultUri: URI, workingDir: Path, path: Path): Path = {
    val newPath = new Path(workingDir, path)
    val pathUri = if (path.isAbsolute()) path.toUri() else newPath.toUri()
    if (pathUri.getScheme == null || pathUri.getAuthority == null &&
        defaultUri.getAuthority != null) {
      val scheme = if (pathUri.getScheme == null) defaultUri.getScheme else pathUri.getScheme
      val authority = if (pathUri.getAuthority == null) {
        if (defaultUri.getAuthority == null) "" else defaultUri.getAuthority
      } else {
        pathUri.getAuthority
      }
      try {
        val newUri = new URI(scheme, authority, pathUri.getPath, null, pathUri.getFragment)
        new Path(newUri)
      } catch {
        case e: URISyntaxException =>
          throw new IllegalArgumentException(e)
      }
    } else {
      newPath
    }
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
    val ignorePermissionAcl = SQLConf.get.truncateTableIgnorePermissionAcl
    val isTrashEnabled = SQLConf.get.truncateTrashEnabled
    locations.foreach { location =>
      if (location.isDefined) {
        val path = new Path(location.get)
        try {
          val fs = path.getFileSystem(hadoopConf)

          // Not all fs impl. support these APIs.
          var optPermission: Option[FsPermission] = None
          var optAcls: Option[java.util.List[AclEntry]] = None
          if (!ignorePermissionAcl) {
            try {
              val fileStatus = fs.getFileStatus(path)
              optPermission = Some(fileStatus.getPermission())
            } catch {
              case NonFatal(_) => // do nothing
            }

            try {
              optAcls = Some(fs.getAclStatus(path).getEntries)
            } catch {
              case NonFatal(_) => // do nothing
            }
          }

          Utils.moveToTrashOrDelete(fs, path, isTrashEnabled, hadoopConf)

          // We should keep original permission/acl of the path.
          // For owner/group, only super-user can set it, for example on HDFS. Because
          // current user can delete the path, we assume the user/group is correct or not an issue.
          fs.mkdirs(path)
          if (!ignorePermissionAcl) {
            optPermission.foreach { permission =>
              try {
                fs.setPermission(path, permission)
              } catch {
                case NonFatal(e) =>
                  throw new SecurityException(
                    s"Failed to set original permission $permission back to " +
                      s"the created path: $path. Exception: ${e.getMessage}")
              }
            }
            optAcls.foreach { acls =>
              val aclEntries = acls.asScala.filter(_.getName != null).asJava

              // If the path doesn't have default ACLs, `setAcl` API will throw an error
              // as it expects user/group/other permissions must be in ACL entries.
              // So we need to add tradition user/group/other permission
              // in the form of ACL.
              optPermission.map { permission =>
                aclEntries.add(newAclEntry(AclEntryScope.ACCESS,
                  AclEntryType.USER, permission.getUserAction()))
                aclEntries.add(newAclEntry(AclEntryScope.ACCESS,
                  AclEntryType.GROUP, permission.getGroupAction()))
                aclEntries.add(newAclEntry(AclEntryScope.ACCESS,
                  AclEntryType.OTHER, permission.getOtherAction()))
              }

              try {
                fs.setAcl(path, aclEntries)
              } catch {
                case NonFatal(e) =>
                  throw new SecurityException(
                    s"Failed to set original ACL $aclEntries back to " +
                      s"the created path: $path. Exception: ${e.getMessage}")
              }
            }
          }
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
      spark.sharedState.cacheManager.uncacheQuery(spark.table(table.identifier), cascade = true)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table $tableIdentWithDB", e)
    }

    if (table.stats.nonEmpty) {
      // empty table after truncation
      val newStats = CatalogStatistics(sizeInBytes = 0, rowCount = Some(0))
      catalog.alterTableStats(tableName, Some(newStats))
    }
    Seq.empty[Row]
  }

  private def newAclEntry(
      scope: AclEntryScope,
      aclType: AclEntryType,
      permission: FsAction): AclEntry = {
    new AclEntry.Builder()
      .setScope(scope)
      .setType(aclType)
      .setPermission(permission).build()
  }
}

abstract class DescribeCommandBase extends RunnableCommand {
  override val output = DescribeCommandSchema.describeTableAttributes()

  protected def describeSchema(
      schema: StructType,
      buffer: ArrayBuffer[Row],
      header: Boolean): Unit = {
    if (header) {
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
    }
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  protected def append(
    buffer: ArrayBuffer[Row], column: String, dataType: String, comment: String): Unit = {
    buffer += Row(column, dataType, comment)
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
  extends DescribeCommandBase {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val catalog = sparkSession.sessionState.catalog

    if (catalog.isTemporaryTable(table)) {
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException(
          s"DESC PARTITION is not allowed on a temporary view: ${table.identifier}")
      }
      describeSchema(catalog.lookupRelation(table).schema, result, header = false)
    } else {
      val metadata = catalog.getTableMetadata(table)
      if (metadata.schema.isEmpty) {
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        describeSchema(sparkSession.table(metadata.identifier).schema, result, header = false)
      } else {
        describeSchema(metadata.schema, result, header = false)
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

    result.toSeq
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (table.partitionColumnNames.nonEmpty) {
      append(buffer, "# Partition Information", "", "")
      describeSchema(table.partitionSchema, buffer, header = true)
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
}

/**
 * Command that looks like
 * {{{
 *   DESCRIBE [QUERY] statement
 * }}}
 *
 * Parameter 'statement' can be one of the following types :
 * 1. SELECT statements
 * 2. SELECT statements inside set operators (UNION, INTERSECT etc)
 * 3. VALUES statement.
 * 4. TABLE statement. Example : TABLE table_name
 * 5. statements of the form 'FROM table SELECT *'
 * 6. Multi select statements of the following form:
 *    select * from (from a select * select *)
 * 7. Common table expressions (CTEs)
 */
case class DescribeQueryCommand(queryText: String, plan: LogicalPlan)
  extends DescribeCommandBase {

  override def simpleString(maxFields: Int): String = s"$nodeName $queryText".trim

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val queryExecution = sparkSession.sessionState.executePlan(plan)
    describeSchema(queryExecution.analyzed.schema, result, header = false)
    result.toSeq
  }
}

/**
 * A command to list the info for a column, including name, data type, comment and column stats.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   DESCRIBE [EXTENDED|FORMATTED] table_name column_name;
 * }}}
 */
case class DescribeColumnCommand(
    table: TableIdentifier,
    colNameParts: Seq[String],
    isExtended: Boolean)
  extends RunnableCommand {

  override val output: Seq[Attribute] = DescribeCommandSchema.describeColumnAttributes()

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val resolver = sparkSession.sessionState.conf.resolver
    val relation = sparkSession.table(table).queryExecution.analyzed

    val colName = UnresolvedAttribute(colNameParts).name
    val field = {
      relation.resolve(colNameParts, resolver).getOrElse {
        throw new AnalysisException(s"Column $colName does not exist")
      }
    }
    if (!field.isInstanceOf[Attribute]) {
      // If the field is not an attribute after `resolve`, then it's a nested field.
      throw new AnalysisException(
        s"DESC TABLE COLUMN command does not support nested data types: $colName")
    }

    val catalogTable = catalog.getTempViewOrPermanentTableMetadata(table)
    val colStatsMap = catalogTable.stats.map(_.colStats).getOrElse(Map.empty)
    val colStats = if (conf.caseSensitiveAnalysis) colStatsMap else CaseInsensitiveMap(colStatsMap)
    val cs = colStats.get(field.name)

    val comment = if (field.metadata.contains("comment")) {
      Option(field.metadata.getString("comment"))
    } else {
      None
    }

    val buffer = ArrayBuffer[Row](
      Row("col_name", field.name),
      Row("data_type", field.dataType.catalogString),
      Row("comment", comment.getOrElse("NULL"))
    )
    if (isExtended) {
      // Show column stats when EXTENDED or FORMATTED is specified.
      buffer += Row("min", cs.flatMap(_.min.map(_.toString)).getOrElse("NULL"))
      buffer += Row("max", cs.flatMap(_.max.map(_.toString)).getOrElse("NULL"))
      buffer += Row("num_nulls", cs.flatMap(_.nullCount.map(_.toString)).getOrElse("NULL"))
      buffer += Row("distinct_count",
        cs.flatMap(_.distinctCount.map(_.toString)).getOrElse("NULL"))
      buffer += Row("avg_col_len", cs.flatMap(_.avgLen.map(_.toString)).getOrElse("NULL"))
      buffer += Row("max_col_len", cs.flatMap(_.maxLen.map(_.toString)).getOrElse("NULL"))
      val histDesc = for {
        c <- cs
        hist <- c.histogram
      } yield histogramDescription(hist)
      buffer ++= histDesc.getOrElse(Seq(Row("histogram", "NULL")))
    }
    buffer.toSeq
  }

  private def histogramDescription(histogram: Histogram): Seq[Row] = {
    val header = Row("histogram",
      s"height: ${histogram.height}, num_of_bins: ${histogram.bins.length}")
    val bins = histogram.bins.zipWithIndex.map {
      case (bin, index) =>
        Row(s"bin_$index",
          s"lower_bound: ${bin.lo}, upper_bound: ${bin.hi}, distinct_count: ${bin.ndv}")
    }
    header +: bins
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
      val catalogTable = catalog.getTableMetadata(table)
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
 * A command to list the column names for a table.
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
    val lookupTable = databaseName match {
      case None => tableName
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

/**
 * Provides common utilities between `ShowCreateTableCommand` and `ShowCreateTableAsSparkCommand`.
 */
trait ShowCreateTableCommandBase {

  protected val table: TableIdentifier

  protected def showTableLocation(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.tableType == EXTERNAL) {
      metadata.storage.locationUri.foreach { location =>
        builder ++= s"LOCATION '${escapeSingleQuotedString(CatalogUtils.URIToString(location))}'\n"
      }
    }
  }

  protected def showTableComment(metadata: CatalogTable, builder: StringBuilder): Unit = {
    metadata
      .comment
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  protected def showTableProperties(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.properties.nonEmpty) {
      val props = metadata.properties.map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= "TBLPROPERTIES "
      builder ++= concatByMultiLines(props)
    }
  }


  protected def concatByMultiLines(iter: Iterable[String]): String = {
    iter.mkString("(\n  ", ",\n  ", ")\n")
  }

  protected def showCreateView(metadata: CatalogTable, builder: StringBuilder): Unit = {
    showViewDataColumns(metadata, builder)
    showTableComment(metadata, builder)
    showViewProperties(metadata, builder)
    showViewText(metadata, builder)
  }

  private def showViewDataColumns(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.schema.nonEmpty) {
      val viewColumns = metadata.schema.map { f =>
        val comment = f.getComment()
          .map(escapeSingleQuotedString)
          .map(" COMMENT '" + _ + "'")

        // view columns shouldn't have data type info
        s"${quoteIdentifier(f.name)}${comment.getOrElse("")}"
      }
      builder ++= concatByMultiLines(viewColumns)
    }
  }

  private def showViewProperties(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val viewProps = metadata.properties.filterKeys(!_.startsWith(CatalogTable.VIEW_PREFIX))
    if (viewProps.nonEmpty) {
      val props = viewProps.map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= s"TBLPROPERTIES ${concatByMultiLines(props)}"
    }
  }

  private def showViewText(metadata: CatalogTable, builder: StringBuilder): Unit = {
    builder ++= metadata.viewText.mkString("AS ", "", "\n")
  }
}

/**
 * A command that shows the Spark DDL syntax that can be used to create a given table.
 * For Hive serde table, this command will generate Spark DDL that can be used to
 * create corresponding Spark table.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW CREATE TABLE [db_name.]table_name
 * }}}
 */
case class ShowCreateTableCommand(table: TableIdentifier)
    extends RunnableCommand with ShowCreateTableCommandBase {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("createtab_stmt", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (catalog.isTemporaryTable(table)) {
      throw new AnalysisException(
        s"SHOW CREATE TABLE is not supported on a temporary view: ${table.identifier}")
    } else {
      val tableMetadata = catalog.getTableMetadata(table)

      // TODO: [SPARK-28692] unify this after we unify the
      //  CREATE TABLE syntax for hive serde and data source table.
      val metadata = if (DDLUtils.isDatasourceTable(tableMetadata)) {
        tableMetadata
      } else {
        // For a Hive serde table, we try to convert it to Spark DDL.
        if (tableMetadata.unsupportedFeatures.nonEmpty) {
          throw new AnalysisException(
            "Failed to execute SHOW CREATE TABLE against table " +
              s"${tableMetadata.identifier}, which is created by Hive and uses the " +
              "following unsupported feature(s)\n" +
              tableMetadata.unsupportedFeatures.map(" - " + _).mkString("\n") + ". " +
              s"Please use `SHOW CREATE TABLE ${tableMetadata.identifier} AS SERDE` " +
              "to show Hive DDL instead."
          )
        }

        if ("true".equalsIgnoreCase(tableMetadata.properties.getOrElse("transactional", "false"))) {
          throw new AnalysisException(
            "SHOW CREATE TABLE doesn't support transactional Hive table. " +
              s"Please use `SHOW CREATE TABLE ${tableMetadata.identifier} AS SERDE` " +
              "to show Hive DDL instead.")
        }

        if (tableMetadata.tableType == VIEW) {
          tableMetadata
        } else {
          convertTableMetadata(tableMetadata)
        }
      }

      val builder = StringBuilder.newBuilder

      val stmt = if (tableMetadata.tableType == VIEW) {
        builder ++= s"CREATE VIEW ${table.quotedString} "
        showCreateView(metadata, builder)

        builder.toString()
      } else {
        builder ++= s"CREATE TABLE ${table.quotedString} "

        showCreateDataSourceTable(metadata, builder)
        builder.toString()
      }

      Seq(Row(stmt))
    }
  }

  private def convertTableMetadata(tableMetadata: CatalogTable): CatalogTable = {
    val hiveSerde = HiveSerDe(
      serde = tableMetadata.storage.serde,
      inputFormat = tableMetadata.storage.inputFormat,
      outputFormat = tableMetadata.storage.outputFormat)

    // Looking for Spark data source that maps to to the Hive serde.
    // TODO: some Hive fileformat + row serde might be mapped to Spark data source, e.g. CSV.
    val source = HiveSerDe.serdeToSource(hiveSerde)
    if (source.isEmpty) {
      val builder = StringBuilder.newBuilder
      hiveSerde.serde.foreach { serde =>
        builder ++= s" SERDE: $serde"
      }
      hiveSerde.inputFormat.foreach { format =>
        builder ++= s" INPUTFORMAT: $format"
      }
      hiveSerde.outputFormat.foreach { format =>
        builder ++= s" OUTPUTFORMAT: $format"
      }
      throw new AnalysisException(
        "Failed to execute SHOW CREATE TABLE against table " +
          s"${tableMetadata.identifier}, which is created by Hive and uses the " +
          "following unsupported serde configuration\n" +
          builder.toString()
      )
    } else {
      // TODO: should we keep Hive serde properties?
      val newStorage = tableMetadata.storage.copy(properties = Map.empty)
      tableMetadata.copy(provider = source, storage = newStorage)
    }
  }

  private def showDataSourceTableDataColumns(
      metadata: CatalogTable, builder: StringBuilder): Unit = {
    val columns = metadata.schema.fields.map(_.toDDL)
    builder ++= concatByMultiLines(columns)
  }

  private def showDataSourceTableOptions(metadata: CatalogTable, builder: StringBuilder): Unit = {
    // For datasource table, there is a provider there in the metadata.
    // If it is a Hive table, we already convert its metadata and fill in a provider.
    builder ++= s"USING ${metadata.provider.get}\n"

    val dataSourceOptions = SQLConf.get.redactOptions(metadata.storage.properties).map {
      case (key, value) => s"${quoteIdentifier(key)} '${escapeSingleQuotedString(value)}'"
    }

    if (dataSourceOptions.nonEmpty) {
      builder ++= "OPTIONS "
      builder ++= concatByMultiLines(dataSourceOptions)
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

  private def showCreateDataSourceTable(metadata: CatalogTable, builder: StringBuilder): Unit = {
    showDataSourceTableDataColumns(metadata, builder)
    showDataSourceTableOptions(metadata, builder)
    showDataSourceTableNonDataColumns(metadata, builder)
    showTableComment(metadata, builder)
    showTableLocation(metadata, builder)
    showTableProperties(metadata, builder)
  }
}

/**
 * This commands generates the DDL for Hive serde table.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW CREATE TABLE table_identifier AS SERDE;
 * }}}
 */
case class ShowCreateTableAsSerdeCommand(table: TableIdentifier)
    extends RunnableCommand with ShowCreateTableCommandBase {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("createtab_stmt", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val tableMetadata = catalog.getTableMetadata(table)

    val stmt = if (DDLUtils.isDatasourceTable(tableMetadata)) {
      throw new AnalysisException(
        s"$table is a Spark data source table. Use `SHOW CREATE TABLE` without `AS SERDE` instead.")
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
      case t =>
        throw new IllegalArgumentException(
          s"Unknown table type is found at showCreateHiveTable: $t")
    }

    builder ++= s"CREATE$tableTypeString ${table.quotedString}"

    if (metadata.tableType == VIEW) {
      showCreateView(metadata, builder)
    } else {
      showHiveTableHeader(metadata, builder)
      showTableComment(metadata, builder)
      showHiveTableNonDataColumns(metadata, builder)
      showHiveTableStorageInfo(metadata, builder)
      showTableLocation(metadata, builder)
      showTableProperties(metadata, builder)
    }

    builder.toString()
  }

  private def showHiveTableHeader(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val columns = metadata.schema.filterNot { column =>
      metadata.partitionColumnNames.contains(column.name)
    }.map(_.toDDL)

    if (columns.nonEmpty) {
      builder ++= concatByMultiLines(columns)
    }
  }

  private def showHiveTableNonDataColumns(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.partitionColumnNames.nonEmpty) {
      val partCols = metadata.partitionSchema.map(_.toDDL)
      builder ++= partCols.mkString("PARTITIONED BY (", ", ", ")\n")
    }

    if (metadata.bucketSpec.isDefined) {
      val bucketSpec = metadata.bucketSpec.get
      builder ++= s"CLUSTERED BY (${bucketSpec.bucketColumnNames.mkString(", ")})\n"

      if (bucketSpec.sortColumnNames.nonEmpty) {
        builder ++= s"SORTED BY (${bucketSpec.sortColumnNames.map(_ + " ASC").mkString(", ")})\n"
      }
      builder ++= s"INTO ${bucketSpec.numBuckets} BUCKETS\n"
    }
  }

  private def showHiveTableStorageInfo(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val storage = metadata.storage

    storage.serde.foreach { serde =>
      builder ++= s"ROW FORMAT SERDE '$serde'\n"

      val serdeProps = SQLConf.get.redactOptions(metadata.storage.properties).map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= s"WITH SERDEPROPERTIES ${concatByMultiLines(serdeProps)}"
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
  }
}

/**
 * A command to refresh all cached entries associated with the table.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   REFRESH TABLE [db_name.]table_name
 * }}}
 */
case class RefreshTableCommand(tableIdent: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Refresh the given table's metadata. If this table is cached as an InMemoryRelation,
    // drop the original cached version and make the new version cached lazily.
    sparkSession.catalog.refreshTable(tableIdent.quotedString)
    Seq.empty[Row]
  }
}
