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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileContext, FsConstants, Path}
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope, AclEntryType, FsAction, FsPermission}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.DescribeCommandSchema
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIfNeeded, CaseInsensitiveMap, CharVarcharUtils, DateTimeUtils, ResolveDefaultColumns}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TableIdentifierHelper
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PartitioningUtils
import org.apache.spark.sql.util.SchemaUtils

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
    ifNotExists: Boolean) extends LeafRunnableCommand {

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
    } else if (fileFormat.inputFormat.isDefined) {
      Some(DDLUtils.HIVE_PROVIDER)
    } else if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
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

    val newTableSchema = CharVarcharUtils.getRawSchema(
      sourceTableDesc.schema, sparkSession.sessionState.conf)
    val newTableDesc =
      CatalogTable(
        identifier = targetTable,
        tableType = tblType,
        storage = newStorage,
        schema = newTableSchema,
        provider = newProvider,
        partitionColumnNames = sourceTableDesc.partitionColumnNames,
        bucketSpec = sourceTableDesc.bucketSpec,
        properties = properties,
        tracksPartitionsInCatalog = sourceTableDesc.tracksPartitionsInCatalog)

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
case class CreateTableCommand(
    table: CatalogTable,
    ignoreIfExists: Boolean) extends LeafRunnableCommand {

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
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    // If this is a temp view, just rename the view.
    // Otherwise, if this is a real table, we also need to uncache and invalidate the table.
    if (catalog.isTempView(oldName)) {
      catalog.renameTable(oldName, newName)
    } else {
      val table = catalog.getTableMetadata(oldName)
      DDLUtils.verifyAlterTableType(catalog, table, isView)
      // If `optStorageLevel` is defined, the old table was cached.
      val optCachedData = sparkSession.sharedState.cacheManager.lookupCachedData(
        sparkSession.table(oldName.unquotedString))
      val optStorageLevel = optCachedData.map(_.cachedRepresentation.cacheBuilder.storageLevel)
      if (optStorageLevel.isDefined) {
        CommandUtils.uncacheTableOrView(sparkSession, oldName.unquotedString)
      }
      // Invalidate the table last, otherwise uncaching the table would load the logical plan
      // back into the hive metastore cache
      catalog.refreshTable(oldName)
      catalog.renameTable(oldName, newName)
      optStorageLevel.foreach { storageLevel =>
        sparkSession.catalog.cacheTable(newName.unquotedString, storageLevel)
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
    colsToAdd: Seq[StructField]) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val catalogTable = verifyAlterTableAddColumn(sparkSession.sessionState.conf, catalog, table)
    val colsWithProcessedDefaults =
      constantFoldCurrentDefaultsToExistDefaults(sparkSession, catalogTable.provider)

    CommandUtils.uncacheTableOrView(sparkSession, table.quotedString)
    catalog.refreshTable(table)

    SchemaUtils.checkColumnNameDuplication(
      (colsWithProcessedDefaults ++ catalogTable.schema).map(_.name),
      conf.caseSensitiveAnalysis)
    DDLUtils.checkTableColumns(catalogTable, StructType(colsWithProcessedDefaults))

    val existingSchema = CharVarcharUtils.getRawSchema(catalogTable.dataSchema)
    catalog.alterTableDataSchema(table, StructType(existingSchema ++ colsWithProcessedDefaults))
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
      throw QueryCompilationErrors.alterAddColNotSupportViewError(table)
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
          throw QueryCompilationErrors.alterAddColNotSupportDatasourceTableError(s, table)
      }
    }
    catalogTable
  }

  /**
   * ALTER TABLE ADD COLUMNS commands may optionally specify a DEFAULT expression for any column.
   * In that case, this method evaluates its originally specified value and then stores the result
   * in a separate column metadata entry, then returns the updated column definitions.
   */
  private def constantFoldCurrentDefaultsToExistDefaults(
      sparkSession: SparkSession, tableProvider: Option[String]): Seq[StructField] = {
    colsToAdd.map { col: StructField =>
      if (col.metadata.contains(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
        val foldedStructType = ResolveDefaultColumns.constantFoldCurrentDefaultsToExistDefaults(
          StructType(Array(col)), tableProvider, "ALTER TABLE ADD COLUMNS", true)
        foldedStructType.fields(0)
      } else {
        col
      }
    }
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
    partition: Option[TablePartitionSpec]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val targetTable = catalog.getTableMetadata(table)
    val tableIdentWithDB = targetTable.identifier.quotedString
    val normalizedSpec = partition.map { spec =>
      PartitioningUtils.normalizePartitionSpec(
        spec,
        targetTable.partitionSchema,
        tableIdentWithDB,
        sparkSession.sessionState.conf.resolver)
    }

    if (DDLUtils.isDatasourceTable(targetTable)) {
      throw QueryCompilationErrors.loadDataNotSupportedForDatasourceTablesError(tableIdentWithDB)
    }
    if (targetTable.partitionColumnNames.nonEmpty) {
      if (partition.isEmpty) {
        throw QueryCompilationErrors.loadDataWithoutPartitionSpecProvidedError(tableIdentWithDB)
      }
      if (targetTable.partitionColumnNames.size != partition.get.size) {
        throw QueryCompilationErrors.loadDataPartitionSizeNotMatchNumPartitionColumnsError(
          tableIdentWithDB, partition.get.size, targetTable.partitionColumnNames.size)
      }
    } else {
      if (partition.nonEmpty) {
        throw QueryCompilationErrors
          .loadDataTargetTableNotPartitionedButPartitionSpecWasProvidedError(tableIdentWithDB)
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
        // the remaining characters after '?' will be removed while forming URI instance
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
        throw QueryCompilationErrors.loadDataInputPathNotExistError(path)
      }
    } catch {
      case e: IllegalArgumentException =>
        log.warn(s"Exception while validating the load path $path ", e)
        throw QueryCompilationErrors.loadDataInputPathNotExistError(path)
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

    // Refresh the data and metadata cache to ensure the data visible to the users
    sparkSession.catalog.refreshTable(tableIdentWithDB)

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
    partitionSpec: Option[TablePartitionSpec]) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString

    if (table.tableType == CatalogTableType.EXTERNAL) {
      throw QueryCompilationErrors.truncateTableOnExternalTablesError(tableIdentWithDB)
    }
    if (table.partitionColumnNames.isEmpty && partitionSpec.isDefined) {
      throw QueryCompilationErrors.truncateTablePartitionNotSupportedForNotPartitionedTablesError(
        tableIdentWithDB)
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
            table.partitionSchema,
            table.identifier.quotedString,
            spark.sessionState.conf.resolver)
        }
        val partLocations =
          catalog.listPartitions(table.identifier, normalizedSpec).map(_.storage.locationUri)

        // Fail if the partition spec is fully specified (not partial) and the partition does not
        // exist.
        for (spec <- partitionSpec if partLocations.isEmpty && spec.size == partCols.length) {
          throw QueryCompilationErrors.noSuchPartitionError(table.database,
            table.identifier.table, spec)
        }

        partLocations
      }
    val hadoopConf = spark.sessionState.newHadoopConf()
    val ignorePermissionAcl = conf.truncateTableIgnorePermissionAcl
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

          fs.delete(path, true)

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
                  throw QueryExecutionErrors.failToSetOriginalPermissionBackError(
                    permission, path, e)
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
                  throw QueryExecutionErrors.failToSetOriginalACLBackError(aclEntries.toString,
                    path, e)
              }
            }
          }
        } catch {
          case NonFatal(e) =>
            throw QueryCompilationErrors.failToTruncateTableWhenRemovingDataError(tableIdentWithDB,
              path, e)
        }
      }
    }
    // After deleting the data, refresh the table to make sure we don't keep around a stale
    // file relation in the metastore cache and cached table data in the cache manager.
    spark.catalog.refreshTable(tableIdentWithDB)

    CommandUtils.updateTableStats(spark, table)
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

abstract class DescribeCommandBase extends LeafRunnableCommand {
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
    isExtended: Boolean,
    override val output: Seq[Attribute])
  extends DescribeCommandBase {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val catalog = sparkSession.sessionState.catalog

    if (catalog.isTempView(table)) {
      if (partitionSpec.nonEmpty) {
        throw QueryCompilationErrors.descPartitionNotAllowedOnTempView(table.identifier)
      }
      val schema = catalog.getTempViewOrPermanentTableMetadata(table).schema
      describeSchema(schema, result, header = false)
    } else {
      val metadata = catalog.getTableRawMetadata(table)
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
      throw QueryCompilationErrors.descPartitionNotAllowedOnView(table.identifier)
    }
    DDLUtils.verifyPartitionProviderIsHive(spark, metadata, "DESC PARTITION")
    val normalizedPartSpec = PartitioningUtils.normalizePartitionSpec(
      partitionSpec,
      metadata.partitionSchema,
      table.quotedString,
      spark.sessionState.conf.resolver)
    val partition = catalog.getPartition(table, normalizedPartSpec)
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

  override val output = DescribeCommandSchema.describeTableAttributes()

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
    isExtended: Boolean,
    override val output: Seq[Attribute])
  extends LeafRunnableCommand {


  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val resolver = sparkSession.sessionState.conf.resolver
    val relation = sparkSession.table(table).queryExecution.analyzed

    val colName = UnresolvedAttribute(colNameParts).name
    val field = {
      relation.resolve(colNameParts, resolver).getOrElse {
        throw QueryCompilationErrors.columnNotFoundError(colName)
      }
    }
    if (!field.isInstanceOf[Attribute]) {
      // If the field is not an attribute after `resolve`, then it's a nested field.
      throw QueryCompilationErrors.commandNotSupportNestedColumnError(
        "DESC TABLE COLUMN", colName)
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

    val dataType = CharVarcharUtils.getRawType(field.metadata)
      .getOrElse(field.dataType).catalogString
    val buffer = ArrayBuffer[Row](
      Row("col_name", field.name),
      Row("data_type", dataType),
      Row("comment", comment.getOrElse("NULL"))
    )
    if (isExtended) {
      // Show column stats when EXTENDED or FORMATTED is specified.
      buffer += Row("min", cs.flatMap(_.min.map(
        toZoneAwareExternalString(_, field.name, field.dataType))).getOrElse("NULL"))
      buffer += Row("max", cs.flatMap(_.max.map(
        toZoneAwareExternalString(_, field.name, field.dataType))).getOrElse("NULL"))
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

  private def toZoneAwareExternalString(
      valueStr: String,
      name: String,
      dataType: DataType): String = {
    dataType match {
      case TimestampType =>
        // When writing to metastore, we always format timestamp value in the default UTC time zone.
        // So here we need to first convert to internal value, then format it using the current
        // time zone.
        val internalValue =
          CatalogColumnStat.fromExternalString(valueStr, name, dataType, CatalogColumnStat.VERSION)
        val curZoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
        CatalogColumnStat
          .getTimestampFormatter(
            isParsing = false, format = "yyyy-MM-dd HH:mm:ss.SSSSSS Z", zoneId = curZoneId)
          .format(internalValue.asInstanceOf[Long])
      case _ =>
        valueStr
    }
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
    override val output: Seq[Attribute],
    isExtended: Boolean = false,
    partitionSpec: Option[TablePartitionSpec] = None) extends LeafRunnableCommand {

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
        val isTemp = catalog.isTempView(tableIdent)
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
      val table = catalog.getTableMetadata(TableIdentifier(tableIdentifierPattern.get, Some(db)))

      DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "SHOW TABLE EXTENDED")

      val tableIdent = table.identifier
      val normalizedSpec = PartitioningUtils.normalizePartitionSpec(
        partitionSpec.get,
        table.partitionSchema,
        tableIdent.quotedString,
        sparkSession.sessionState.conf.resolver)
      val partition = catalog.getPartition(tableIdent, normalizedSpec)
      val database = tableIdent.database.getOrElse("")
      val tableName = tableIdent.table
      val isTemp = catalog.isTempView(tableIdent)
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
case class ShowTablePropertiesCommand(
    table: TableIdentifier,
    propertyKey: Option[String],
    override val output: Seq[Attribute]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (catalog.isTempView(table)) {
      Seq.empty[Row]
    } else {
      val catalogTable = catalog.getTableMetadata(table)
      val properties = conf.redactOptions(catalogTable.properties)
      propertyKey match {
        case Some(p) =>
          val propValue = properties
            .getOrElse(p, s"Table ${catalogTable.qualifiedName} does not have property: $p")
          if (output.length == 1) {
            Seq(Row(propValue))
          } else {
            Seq(Row(p, propValue))
          }
        case None =>
          properties.filterKeys(!_.startsWith(CatalogTable.VIEW_PREFIX))
            .toSeq.sortBy(_._1).map(p => Row(p._1, p._2))
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
    tableName: TableIdentifier,
    override val output: Seq[Attribute]) extends LeafRunnableCommand {

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
    override val output: Seq[Attribute],
    spec: Option[TablePartitionSpec]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString

    /**
     * Validate and throws an [[AnalysisException]] exception under the following conditions:
     * 1. If the table is not partitioned.
     * 2. If it is a datasource table.
     */

    if (table.partitionColumnNames.isEmpty) {
      throw QueryCompilationErrors.showPartitionNotAllowedOnTableNotPartitionedError(
        tableIdentWithDB)
    }

    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "SHOW PARTITIONS")

    /**
     * Normalizes the partition spec w.r.t the partition columns and case sensitivity settings,
     * and validates the spec by making sure all the referenced columns are
     * defined as partitioning columns in table definition. An AnalysisException exception is
     * thrown if the partitioning spec is invalid.
     */
    val normalizedSpec = spec.map(partitionSpec => PartitioningUtils.normalizePartitionSpec(
      partitionSpec,
      table.partitionSchema,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver))

    val partNames = catalog.listPartitionNames(tableName, normalizedSpec)
    partNames.map(Row(_))
  }
}

/**
 * Provides common utilities between `ShowCreateTableCommand` and `ShowCreateTableAsSparkCommand`.
 */
trait ShowCreateTableCommandBase extends SQLConfHelper {

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
      val props =
        conf.redactOptions(metadata.properties)
          .toSeq.sortBy(_._1).map { case (key, value) =>
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
        s"${quoteIfNeeded(f.name)}${comment.getOrElse("")}"
      }
      builder ++= concatByMultiLines(viewColumns)
    }
  }

  private def showViewProperties(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val viewProps = metadata.properties.filterKeys(!_.startsWith(CatalogTable.VIEW_PREFIX))
    if (viewProps.nonEmpty) {
      val props = viewProps.toSeq.sortBy(_._1).map { case (key, value) =>
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
case class ShowCreateTableCommand(
    table: TableIdentifier,
    override val output: Seq[Attribute])
    extends LeafRunnableCommand with ShowCreateTableCommandBase {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (catalog.isTempView(table)) {
      throw QueryCompilationErrors.showCreateTableNotSupportedOnTempView(table.identifier)
    } else {
      val tableMetadata = catalog.getTableRawMetadata(table)

      // TODO: [SPARK-28692] unify this after we unify the
      //  CREATE TABLE syntax for hive serde and data source table.
      val metadata = if (DDLUtils.isDatasourceTable(tableMetadata)) {
        tableMetadata
      } else {
        // For a Hive serde table, we try to convert it to Spark DDL.
        if (tableMetadata.unsupportedFeatures.nonEmpty) {
          throw QueryCompilationErrors.showCreateTableFailToExecuteUnsupportedFeatureError(
            tableMetadata)
        }

        if ("true".equalsIgnoreCase(tableMetadata.properties.getOrElse("transactional", "false"))) {
          throw QueryCompilationErrors.showCreateTableNotSupportTransactionalHiveTableError(
            tableMetadata)
        }

        if (tableMetadata.tableType == VIEW) {
          tableMetadata
        } else {
          convertTableMetadata(tableMetadata)
        }
      }

      val builder = new StringBuilder

      val stmt = if (tableMetadata.tableType == VIEW) {
        builder ++= s"CREATE VIEW ${table.quoted} "
        showCreateView(metadata, builder)

        builder.toString()
      } else {
        builder ++= s"CREATE TABLE ${table.quoted} "

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
      val builder = new StringBuilder
      hiveSerde.serde.foreach { serde =>
        builder ++= s" SERDE: $serde"
      }
      hiveSerde.inputFormat.foreach { format =>
        builder ++= s" INPUTFORMAT: $format"
      }
      hiveSerde.outputFormat.foreach { format =>
        builder ++= s" OUTPUTFORMAT: $format"
      }
      throw QueryCompilationErrors.showCreateTableFailToExecuteUnsupportedConfError(table, builder)
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

    val dataSourceOptions = conf.redactOptions(metadata.storage.properties).toSeq.sortBy(_._1).map {
      case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
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
case class ShowCreateTableAsSerdeCommand(
    table: TableIdentifier,
    override val output: Seq[Attribute])
    extends LeafRunnableCommand with ShowCreateTableCommandBase {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val tableMetadata = catalog.getTableRawMetadata(table)

    val stmt = if (DDLUtils.isDatasourceTable(tableMetadata)) {
      throw QueryCompilationErrors.showCreateTableAsSerdeNotAllowedOnSparkDataSourceTableError(
        table)
    } else {
      showCreateHiveTable(tableMetadata)
    }

    Seq(Row(stmt))
  }

  private def showCreateHiveTable(metadata: CatalogTable): String = {
    def reportUnsupportedError(features: Seq[String]): Unit = {
      throw QueryCompilationErrors.showCreateTableOrViewFailToExecuteUnsupportedFeatureError(
        metadata, features)
    }

    if (metadata.unsupportedFeatures.nonEmpty) {
      reportUnsupportedError(metadata.unsupportedFeatures)
    }

    val builder = new StringBuilder

    val tableTypeString = metadata.tableType match {
      case EXTERNAL => " EXTERNAL TABLE"
      case VIEW => " VIEW"
      case MANAGED => " TABLE"
      case t =>
        throw new IllegalArgumentException(
          s"Unknown table type is found at showCreateHiveTable: $t")
    }

    builder ++= s"CREATE$tableTypeString ${table.quoted} "

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

      val serdeProps = conf.redactOptions(metadata.storage.properties).toSeq.sortBy(_._1).map {
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
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Refresh the given table's metadata. If this table is cached as an InMemoryRelation,
    // drop the original cached version and make the new version cached lazily.
    sparkSession.catalog.refreshTable(tableIdent.quotedString)
    Seq.empty[Row]
  }
}
