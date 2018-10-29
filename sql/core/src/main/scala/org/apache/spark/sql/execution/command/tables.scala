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
import java.net.{URI, URISyntaxException}
import java.nio.file.FileSystems

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileContext, FsConstants, Path}

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.Histogram
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIdentifier}
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.Utils

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
    colsToAdd: Seq[StructField]) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val catalogTable = verifyAlterTableAddColumn(sparkSession.sessionState.conf, catalog, table)

    try {
      sparkSession.catalog.uncacheTable(table.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${table.quotedString}", e)
    }
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
   * For datasource table, it currently only supports parquet, json, csv.
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
      DataSource.lookupDataSource(catalogTable.provider.get, conf).newInstance() match {
        // For datasource table, this command can only support the following File format.
        // TextFileFormat only default to one column "value"
        // Hive type is already considered as hive serde table, so the logic will not
        // come in here.
        case _: JsonFileFormat | _: CSVFileFormat | _: ParquetFileFormat =>
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
    val pathUri = if (path.isAbsolute()) path.toUri() else new Path(workingDir, path).toUri()
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
      path
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

    result
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

  private def describeSchema(
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

  private def append(
      buffer: ArrayBuffer[Row], column: String, dataType: String, comment: String): Unit = {
    buffer += Row(column, dataType, comment)
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

  override val output: Seq[Attribute] = {
    Seq(
      AttributeReference("info_name", StringType, nullable = false,
        new MetadataBuilder().putString("comment", "name of the column info").build())(),
      AttributeReference("info_value", StringType, nullable = false,
        new MetadataBuilder().putString("comment", "value of the column info").build())()
    )
  }

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
    val colStats = catalogTable.stats.map(_.colStats).getOrElse(Map.empty)
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
    buffer
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
      case t =>
        throw new IllegalArgumentException(
          s"Unknown table type is found at showCreateHiveTable: $t")
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
    }.map(_.toDDL)

    if (columns.nonEmpty) {
      builder ++= columns.mkString("(", ", ", ")\n")
    }

    metadata
      .comment
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }


  private def showHiveTableNonDataColumns(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.partitionColumnNames.nonEmpty) {
      val partCols = metadata.partitionSchema.map(_.toDDL)
      builder ++= partCols.mkString("PARTITIONED BY (", ", ", ")\n")
    }

    if (metadata.bucketSpec.isDefined) {
      val bucketSpec = metadata.bucketSpec.get
      builder ++= s"CLUSTERED BY (${bucketSpec.bucketColumnNames.mkString(",")})\n"

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
    val columns = metadata.schema.fields.map(_.toDDL)
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
}
