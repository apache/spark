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

import java.util.Locale
import java.util.concurrent.TimeUnit._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.catalog.SupportsNamespaces._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.errors.QueryExecutionErrors.hiveTableWithAnsiIntervalsError
import org.apache.spark.sql.execution.datasources.{DataSource, DataSourceUtils, FileFormat, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PartitioningUtils
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}
import org.apache.spark.util.ArrayImplicits._

// Note: The definition of these commands are based on the ones described in
// https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL

/**
 * A command for users to create a new database.
 *
 * It will issue an error message when the database with the same name already exists,
 * unless 'ifNotExists' is true.
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
 *     [COMMENT database_comment]
 *     [LOCATION database_directory]
 *     [WITH DBPROPERTIES (property_name=property_value, ...)];
 * }}}
 */
case class CreateDatabaseCommand(
    databaseName: String,
    ifNotExists: Boolean,
    path: Option[String],
    comment: Option[String],
    props: Map[String, String])
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    catalog.createDatabase(
      CatalogDatabase(
        databaseName,
        comment.getOrElse(""),
        path.map(CatalogUtils.stringToURI).getOrElse(catalog.getDefaultDBPath(databaseName)),
        props),
      ifNotExists)
    Seq.empty[Row]
  }
}


/**
 * A command for users to remove a database from the system.
 *
 * 'ifExists':
 * - true, if database_name doesn't exist, no action
 * - false (default), if database_name doesn't exist, a warning message will be issued
 * 'cascade':
 * - true, the dependent objects are automatically dropped before dropping database.
 * - false (default), it is in the Restrict mode. The database cannot be dropped if
 * it is not empty. The inclusive tables must be dropped at first.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *    DROP DATABASE [IF EXISTS] database_name [RESTRICT|CASCADE];
 * }}}
 */
case class DropDatabaseCommand(
    databaseName: String,
    ifExists: Boolean,
    cascade: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.catalog.dropDatabase(databaseName, ifExists, cascade)
    Seq.empty[Row]
  }
}

/**
 * A command for users to add new (key, value) pairs into DBPROPERTIES
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is:
 * {{{
 *    ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...)
 * }}}
 */
case class AlterDatabasePropertiesCommand(
    databaseName: String,
    props: Map[String, String])
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val db: CatalogDatabase = catalog.getDatabaseMetadata(databaseName)
    catalog.alterDatabase(db.copy(properties = db.properties ++ props))

    Seq.empty[Row]
  }
}

/**
 * A command for users to set new location path for a database
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is:
 * {{{
 *    ALTER (DATABASE|SCHEMA) database_name SET LOCATION path
 * }}}
 */
case class AlterDatabaseSetLocationCommand(databaseName: String, location: String)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val oldDb = catalog.getDatabaseMetadata(databaseName)
    catalog.alterDatabase(oldDb.copy(locationUri = CatalogUtils.stringToURI(location)))

    Seq.empty[Row]
  }
}

/**
 * A command for users to show the name of the database, its comment (if one has been set), and its
 * root location on the filesystem. When extended is true, it also shows the database's properties
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is
 * {{{
 *    DESCRIBE DATABASE [EXTENDED] db_name
 * }}}
 */
case class DescribeDatabaseCommand(
    databaseName: String,
    extended: Boolean,
    override val output: Seq[Attribute])
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbMetadata: CatalogDatabase =
      sparkSession.sessionState.catalog.getDatabaseMetadata(databaseName)
    val allDbProperties = dbMetadata.properties
    val result =
      Row("Catalog Name", SESSION_CATALOG_NAME) ::
        Row("Database Name", dbMetadata.name) ::
        Row("Comment", dbMetadata.description) ::
        Row("Location", CatalogUtils.URIToString(dbMetadata.locationUri))::
        Row("Owner", allDbProperties.getOrElse(PROP_OWNER, "")) :: Nil

    if (extended) {
      val properties = allDbProperties -- CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES
      val propertiesStr =
        if (properties.isEmpty) {
          ""
        } else {
          conf.redactOptions(properties).toSeq.sortBy(_._1).mkString("(", ", ", ")")
        }
      result :+ Row("Properties", propertiesStr)
    } else {
      result
    }
  }
}

/**
 * Drops a table/view from the metastore and removes it if it is cached. This command does not drop
 * temp views, which should be handled by [[DropTempViewCommand]].
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
    purge: Boolean) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog

    if (catalog.tableExists(tableName)) {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      catalog.getTableMetadata(tableName).tableType match {
        case CatalogTableType.VIEW if !isView =>
          throw QueryCompilationErrors.wrongCommandForObjectTypeError(
            operation = "DROP TABLE",
            requiredType = s"${CatalogTableType.EXTERNAL.name} or ${CatalogTableType.MANAGED.name}",
            objectName = catalog.getTableMetadata(tableName).qualifiedName,
            foundType = catalog.getTableMetadata(tableName).tableType.name,
            alternative = "DROP VIEW"
          )
        case o if o != CatalogTableType.VIEW && isView =>
          throw QueryCompilationErrors.wrongCommandForObjectTypeError(
            operation = "DROP VIEW",
            requiredType = CatalogTableType.VIEW.name,
            objectName = catalog.getTableMetadata(tableName).qualifiedName,
            foundType = o.name,
            alternative = "DROP TABLE"
          )
        case _ =>
      }

      try {
        sparkSession.sharedState.cacheManager.uncacheQuery(
          sparkSession.table(tableName), cascade = true)
      } catch {
        case NonFatal(e) => log.warn(e.toString, e)
      }
      catalog.refreshTable(tableName)
      catalog.dropTable(tableName, ifExists, purge)
    } else if (ifExists) {
      // no-op
    } else {
      throw QueryCompilationErrors.noSuchTableError(
        tableName.catalog.toSeq ++ tableName.database :+ tableName.table)
    }
    Seq.empty[Row]
  }
}

case class DropTempViewCommand(ident: Identifier) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(ident.namespace().isEmpty || ident.namespace().length == 1)
    val nameParts = (ident.namespace() :+ ident.name()).toImmutableArraySeq
    val catalog = sparkSession.sessionState.catalog
    catalog.getRawLocalOrGlobalTempView(nameParts).foreach { view =>
      val hasViewText = view.tableMeta.viewText.isDefined
      sparkSession.sharedState.cacheManager.uncacheTableOrView(
        sparkSession, nameParts, cascade = hasViewText)
      view.refresh()
      if (ident.namespace().isEmpty) {
        catalog.dropTempView(ident.name())
      } else {
        catalog.dropGlobalTempView(ident.name())
      }
    }
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
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableRawMetadata(tableName)
    // This overrides old properties and update the comment parameter of CatalogTable
    // with the newly added/modified comment since CatalogTable also holds comment as its
    // direct property.
    val newTable = table.copy(
      properties = table.properties ++ properties,
      comment = properties.get(TableCatalog.PROP_COMMENT).orElse(table.comment))
    catalog.alterTable(newTable)
    catalog.invalidateCachedTable(tableName)
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
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableRawMetadata(tableName)
    // If comment is in the table property, we reset it to None
    val tableComment = if (propKeys.contains(TableCatalog.PROP_COMMENT)) None else table.comment
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties, comment = tableComment)
    catalog.alterTable(newTable)
    catalog.invalidateCachedTable(tableName)
    Seq.empty[Row]
  }

}


/**
 * A command to change the column for a table, only support changing the comment or collation of
 * the data type or nested types (recursively) of a non-partition column for now.
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
    newColumn: StructField) extends LeafRunnableCommand {

  // TODO: support change column name/dataType/metadata/position.
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    // This command may change column default values, so we need to refresh the table relation cache
    // here so that DML commands can resolve these default values correctly.
    catalog.refreshTable(tableName)
    val table = catalog.getTableRawMetadata(tableName)
    val resolver = sparkSession.sessionState.conf.resolver
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)

    // Check that the column is not a partition column
    if (table.partitionSchema.fieldNames.exists(resolver(columnName, _))) {
        throw QueryCompilationErrors.cannotAlterPartitionColumn(table.qualifiedName, columnName)
    }
    // Find the origin column from dataSchema by column name.
    val originColumn = findColumnByName(table.dataSchema, columnName, resolver)
    val validType = canEvolveType(originColumn, newColumn)
    // Throw an AnalysisException on attempt to change collation of bucket column.
    if (validType && originColumn.dataType != newColumn.dataType) {
      val isBucketColumn = table.bucketSpec match {
        case Some(bucketSpec) => bucketSpec.bucketColumnNames.exists(resolver(columnName, _))
        case _ => false
      }
      if (isBucketColumn) {
        throw QueryCompilationErrors.cannotAlterCollationBucketColumn(
          table.qualifiedName, columnName)
      }
    }
    // Throw an AnalysisException if the column name is changed or we cannot evolve the data type.
    // Only changes in collation of column data type or its nested types (recursively) are allowed.
    if (!validType || !namesEqual(originColumn, newColumn, resolver)) {
      throw QueryCompilationErrors.alterTableChangeColumnNotSupportedForColumnTypeError(
        toSQLId(table.identifier.nameParts), originColumn, newColumn, this.origin)
    }

    val newDataSchema = table.dataSchema.fields.map { field =>
      if (field.name == originColumn.name) {
        // Create a new column from the origin column with the new type and new comment.
        val withNewTypeAndComment: StructField =
          addComment(withNewType(field, newColumn.dataType), newColumn.getComment())
        // Create a new column from the origin column with the new current default value.
        if (newColumn.getCurrentDefaultValue().isDefined) {
          if (newColumn.getCurrentDefaultValue().get.nonEmpty) {
            val result: StructField =
              addCurrentDefaultValue(withNewTypeAndComment, newColumn.getCurrentDefaultValue())
            // Check that the proposed default value parses and analyzes correctly, and that the
            // type of the resulting expression is equivalent or coercible to the destination column
            // type.
            ResolveDefaultColumns.analyze(result, "ALTER TABLE ALTER COLUMN")
            result
          } else {
            withNewTypeAndComment.clearCurrentDefaultValue()
          }
        } else {
          withNewTypeAndComment
        }
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
    }.getOrElse(throw QueryCompilationErrors.cannotFindColumnError(name, schema.fieldNames))
  }

  // Change the dataType of the column.
  private def withNewType(column: StructField, dataType: DataType): StructField =
    column.copy(dataType = dataType)

  // Add the comment to a column, if comment is empty, return the original column.
  private def addComment(column: StructField, comment: Option[String]): StructField =
    comment.map(column.withComment).getOrElse(column)

  // Add the current default value to a column, if default value is empty, return the original
  // column.
  private def addCurrentDefaultValue(column: StructField, value: Option[String]): StructField =
    value.map(column.withCurrentDefaultValue).getOrElse(column)

  // Compare a [[StructField]] to another, return true if they have the same column
  // name(by resolver).
  private def namesEqual(
      field: StructField, other: StructField, resolver: Resolver): Boolean = {
    resolver(field.name, other.name)
  }

  // Compare dataType of [[StructField]] to another, return true if it is valid to evolve the type
  // when altering column. Only changes in collation of data type or its nested types (recursively)
  // are allowed.
  private def canEvolveType(from: StructField, to: StructField): Boolean = {
    DataType.equalsIgnoreCompatibleCollation(from.dataType, to.dataType)
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
  extends LeafRunnableCommand {

  // should never happen if we parsed things correctly
  require(serdeClassName.isDefined || serdeProperties.isDefined,
    "ALTER TABLE attempted to set neither serde class name nor serde properties")

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableRawMetadata(tableName)
    // For datasource tables, disallow setting serde or specifying partition
    if (partSpec.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw QueryCompilationErrors.alterTableSetSerdeForSpecificPartitionNotSupportedError()
    }
    if (serdeClassName.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw QueryCompilationErrors.alterTableSetSerdeNotSupportedError()
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
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE ADD PARTITION")
    val parts = partitionSpecsAndLocs.map { case (spec, location) =>
      val normalizedSpec = PartitioningUtils.normalizePartitionSpec(
        spec,
        table.partitionSchema,
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
    parts.iterator.grouped(batchSize).foreach { batch =>
      catalog.createPartitions(table.identifier, batch, ignoreIfExists = ifNotExists)
    }

    sparkSession.catalog.refreshTable(table.identifier.quotedString)
    if (table.stats.nonEmpty && sparkSession.sessionState.conf.autoSizeUpdateEnabled) {
      // Updating table stats only if new partition is not empty
      val addedSize = CommandUtils.calculateMultipleLocationSizes(sparkSession, table.identifier,
        parts.map(_.storage.locationUri)).sum
      if (addedSize > 0) {
        val newStats = CatalogStatistics(sizeInBytes = table.stats.get.sizeInBytes + addedSize)
        catalog.alterTableStats(table.identifier, Some(newStats))
        catalog.alterPartitions(table.identifier, parts)
      }
    } else {
      // Re-calculating of table size including all partitions
      CommandUtils.updateTableStats(sparkSession, table)
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
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE RENAME PARTITION")

    val normalizedOldPartition = PartitioningUtils.normalizePartitionSpec(
      oldPartition,
      table.partitionSchema,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver)

    val normalizedNewPartition = PartitioningUtils.normalizePartitionSpec(
      newPartition,
      table.partitionSchema,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver)

    catalog.renamePartitions(
      tableName, Seq(normalizedOldPartition), Seq(normalizedNewPartition))
    sparkSession.catalog.refreshTable(table.identifier.quotedString)
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
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE DROP PARTITION")

    val normalizedSpecs = specs.map { spec =>
      PartitioningUtils.normalizePartitionSpec(
        spec,
        table.partitionSchema,
        table.identifier.quotedString,
        sparkSession.sessionState.conf.resolver)
    }

    catalog.dropPartitions(
      table.identifier, normalizedSpecs, ignoreIfNotExists = ifExists, purge = purge,
      retainData = retainData)

    sparkSession.catalog.refreshTable(table.identifier.quotedString)
    CommandUtils.updateTableStats(sparkSession, table)

    Seq.empty[Row]
  }

}


case class PartitionStatistics(numFiles: Int, totalSize: Long)

/**
 * Repair a table by recovering all the partition in the directory of the table and
 * update the catalog.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table RECOVER PARTITIONS;
 *   MSCK REPAIR TABLE table [{ADD|DROP|SYNC} PARTITIONS];
 * }}}
 */
case class RepairTableCommand(
    tableName: TableIdentifier,
    enableAddPartitions: Boolean,
    enableDropPartitions: Boolean,
    cmd: String = "MSCK REPAIR TABLE") extends LeafRunnableCommand {

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
    val table = catalog.getTableRawMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString
    if (table.partitionColumnNames.isEmpty) {
      throw QueryCompilationErrors.cmdOnlyWorksOnPartitionedTablesError(cmd, tableIdentWithDB)
    }

    if (table.storage.locationUri.isEmpty) {
      throw QueryCompilationErrors.cmdOnlyWorksOnTableWithLocationError(cmd, tableIdentWithDB)
    }

    val root = new Path(table.location)
    logInfo(log"Recover all the partitions in ${MDC(LogKeys.PATH, root)}")
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fs = root.getFileSystem(hadoopConf)

    val droppedAmount = if (enableDropPartitions) {
      dropPartitions(catalog, fs)
    } else 0
    val addedAmount = if (enableAddPartitions) {
      val threshold = spark.sparkContext.conf.get(RDD_PARALLEL_LISTING_THRESHOLD)
      val pathFilter = getPathFilter(hadoopConf)

      val evalPool = ThreadUtils.newForkJoinPool("RepairTableCommand", 8)
      val partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)] =
        try {
          scanPartitions(spark, fs, pathFilter, root, Map(), table.partitionColumnNames, threshold,
            spark.sessionState.conf.resolver, new ForkJoinTaskSupport(evalPool))
        } finally {
          evalPool.shutdown()
        }
      val total = partitionSpecsAndLocs.length
      logInfo(log"Found ${MDC(LogKeys.NUM_PARTITIONS, total)} partitions " +
        log"in ${MDC(LogKeys.PATH, root)}")

      val partitionStats = if (spark.sessionState.conf.gatherFastStats) {
        gatherPartitionStats(spark, partitionSpecsAndLocs, fs, pathFilter, threshold)
      } else {
        Map.empty[Path, PartitionStatistics]
      }
      logInfo(log"Finished to gather the fast stats for all " +
        log"${MDC(LogKeys.NUM_PARTITIONS, total)} partitions.")

      addPartitions(spark, table, partitionSpecsAndLocs, partitionStats)
      total
    } else 0
    // Updates the table to indicate that its partition metadata is stored in the Hive metastore.
    // This is always the case for Hive format tables, but is not true for Datasource tables created
    // before Spark 2.1 unless they are converted via `msck repair table`.
    spark.sessionState.catalog.alterTable(table.copy(tracksPartitionsInCatalog = true))
    try {
      spark.catalog.refreshTable(tableIdentWithDB)
    } catch {
      case NonFatal(e) =>
        logError(log"Cannot refresh the table '${MDC(LogKeys.TABLE_NAME, tableIdentWithDB)}'. " +
          log"A query of the table might return wrong result if the table was cached. " +
          log"To avoid such issue, you should uncache the table manually via the UNCACHE TABLE " +
          log"command after table recovering will complete fully.", e)
    }
    logInfo(log"Recovered all partitions: " +
      log"added (${MDC(LogKeys.NUM_ADDED_PARTITIONS, addedAmount)}), " +
      log"dropped (${MDC(LogKeys.NUM_DROPPED_PARTITIONS, droppedAmount)}).")
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
      evalTaskSupport: ForkJoinTaskSupport): Seq[(TablePartitionSpec, Path)] = {
    if (partitionNames.isEmpty) {
      return Seq(spec -> path)
    }

    val statuses = fs.listStatus(path, filter)
    val statusPar: Seq[FileStatus] =
      if (partitionNames.length > 1 && statuses.length > threshold || partitionNames.length > 2) {
        // parallelize the list of partitions here, then we can have better parallelism later.
        // scalastyle:off parvector
        val parArray = new ParVector(statuses.toVector)
        parArray.tasksupport = evalTaskSupport
        // scalastyle:on parvector
        parArray.seq
      } else {
        statuses.toImmutableArraySeq
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
          logWarning(log"expected partition column " +
            log"${MDC(LogKeys.EXPECTED_PARTITION_COLUMN, partitionNames.head)}," +
            log" but got ${MDC(LogKeys.ACTUAL_PARTITION_COLUMN, ps(0))}, ignoring it")
          Seq.empty
        }
      } else {
        logWarning(log"ignore ${MDC(LogKeys.PATH, new Path(path, name))}")
        Seq.empty
      }
    }
  }

  private def gatherPartitionStats(
      spark: SparkSession,
      partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)],
      fs: FileSystem,
      pathFilter: PathFilter,
      threshold: Int): Map[Path, PartitionStatistics] = {
    val partitionNum = partitionSpecsAndLocs.length
    if (partitionNum > threshold) {
      val hadoopConf = spark.sessionState.newHadoopConf()
      val serializableConfiguration = new SerializableConfiguration(hadoopConf)
      val locations = partitionSpecsAndLocs.map(_._2)

      // Set the number of parallelism to prevent following file listing from generating many tasks
      // in case of large #defaultParallelism.
      val numParallelism = Math.min(partitionNum,
        Math.min(spark.sparkContext.defaultParallelism, 10000))
      // gather the fast stats for all the partitions otherwise Hive metastore will list all the
      // files for all the new partitions in sequential way, which is super slow.
      logInfo(log"Gather the fast stats in parallel using ${MDC(LogKeys.COUNT, numParallelism)} " +
        log"tasks.")
      spark.sparkContext.parallelize(locations, numParallelism)
        .mapPartitions { locationsEachPartition =>
          val pathFilter = getPathFilter(serializableConfiguration.value)
          locationsEachPartition.map { location =>
            val fs = location.getFileSystem(serializableConfiguration.value)
            val statuses = fs.listStatus(location, pathFilter)
            (location, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
          }
        }.collectAsMap().toMap
    } else {
      partitionSpecsAndLocs.map { case (_, location) =>
        val statuses = fs.listStatus(location, pathFilter)
        (location, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
      }.toMap
    }
  }

  private def addPartitions(
      spark: SparkSession,
      table: CatalogTable,
      partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)],
      partitionStats: Map[Path, PartitionStatistics]): Unit = {
    val total = partitionSpecsAndLocs.length
    var done = 0L
    // Hive metastore may not have enough memory to handle millions of partitions in single RPC,
    // we should split them into smaller batches. Since Hive client is not thread safe, we cannot
    // do this in parallel.
    val batchSize = spark.sessionState.conf.getConf(SQLConf.ADD_PARTITION_BATCH_SIZE)
    partitionSpecsAndLocs.iterator.grouped(batchSize).foreach { batch =>
      val now = MILLISECONDS.toSeconds(System.currentTimeMillis())
      val parts = batch.map { case (spec, location) =>
        val params = partitionStats.get(location).map {
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

  // Drops the partitions that do not exist in the file system
  private def dropPartitions(catalog: SessionCatalog, fs: FileSystem): Int = {
    val dropPartSpecs = ThreadUtils.parmap(
      catalog.listPartitions(tableName),
      "RepairTableCommand: non-existing partitions",
      maxThreads = 8) { partition =>
      partition.storage.locationUri.flatMap { uri =>
        if (fs.exists(new Path(uri))) None else Some(partition.spec)
      }
    }.flatten
    catalog.dropPartitions(
      tableName,
      dropPartSpecs,
      ignoreIfNotExists = true,
      purge = false,
      // Since we have already checked that partition directories do not exist, we can avoid
      // additional calls to the file system at the catalog side by setting this flag.
      retainData = true)
    dropPartSpecs.length
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
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val locUri = CatalogUtils.stringToURI(location)
    partitionSpec match {
      case Some(spec) =>
        DDLUtils.verifyPartitionProviderIsHive(
          sparkSession, table, "ALTER TABLE ... SET LOCATION")
        // Partition spec is specified, so we set the location only for this partition
        val normalizedSpec = PartitioningUtils.normalizePartitionSpec(
          spec,
          table.partitionSchema,
          table.identifier.quotedString,
          sparkSession.sessionState.conf.resolver)
        val part = catalog.getPartition(table.identifier, normalizedSpec)
        val newPart = part.copy(storage = part.storage.copy(locationUri = Some(locUri)))
        catalog.alterPartitions(table.identifier, Seq(newPart))
      case None =>
        // No partition spec is specified, so we set the location for the table itself
        catalog.alterTable(table.withNewStorage(locationUri = Some(locUri)))
    }
    sparkSession.catalog.refreshTable(table.identifier.quotedString)
    CommandUtils.updateTableStats(sparkSession, table)
    Seq.empty[Row]
  }
}


object DDLUtils extends Logging {
  val HIVE_PROVIDER = "hive"

  def isHiveTable(table: CatalogTable): Boolean = {
    isHiveTable(table.provider)
  }

  def isHiveTable(provider: Option[String]): Boolean = {
    provider.isDefined && provider.get.toLowerCase(Locale.ROOT) == HIVE_PROVIDER
  }

  def isDatasourceTable(table: CatalogTable): Boolean = {
    table.provider.isDefined && table.provider.get.toLowerCase(Locale.ROOT) != HIVE_PROVIDER
  }

  def readHiveTable(table: CatalogTable): HiveTableRelation = {
    HiveTableRelation(
      table,
      // Hive table columns are always nullable.
      toAttributes(table.dataSchema.asNullable),
      toAttributes(table.partitionSchema.asNullable))
  }

  /**
   * Throws a standard error for actions that require partitionProvider = hive.
   */
  def verifyPartitionProviderIsHive(
      spark: SparkSession, table: CatalogTable, action: String): Unit = {
    val tableName = table.identifier.table
    if (!spark.sessionState.conf.manageFilesourcePartitions && isDatasourceTable(table)) {
      throw QueryCompilationErrors
        .actionNotAllowedOnTableWithFilesourcePartitionManagementDisabledError(action, tableName)
    }
    if (!table.tracksPartitionsInCatalog && isDatasourceTable(table)) {
      throw QueryCompilationErrors.actionNotAllowedOnTableSincePartitionMetadataNotStoredError(
        action, tableName)
    }
  }

  /**
   * If the command ALTER VIEW is to alter a table or ALTER TABLE is to alter a view,
   * issue an exception [[AnalysisException]].
   *
   * Note: temporary views can be altered by both ALTER VIEW and ALTER TABLE commands,
   * since temporary views can be also created by CREATE TEMPORARY TABLE. In the future,
   * when we decided to drop the support, we should disallow users to alter temporary views
   * by ALTER TABLE.
   */
  def verifyAlterTableType(
      catalog: SessionCatalog,
      tableMetadata: CatalogTable,
      isView: Boolean): Unit = {
    if (!catalog.isTempView(tableMetadata.identifier)) {
      tableMetadata.tableType match {
        case CatalogTableType.VIEW if !isView =>
          throw QueryCompilationErrors.cannotAlterViewWithAlterTableError(
            viewName = tableMetadata.identifier.table
          )
        case o if o != CatalogTableType.VIEW && isView =>
          throw QueryCompilationErrors.cannotAlterTableWithAlterViewError(
            tableName = tableMetadata.identifier.table
          )
        case _ =>
      }
    }
  }

  private[sql] def checkTableColumns(table: CatalogTable): Unit = {
    checkTableColumns(table, table.dataSchema)
  }

  // Checks correctness of table's column names and types.
  private[sql] def checkTableColumns(table: CatalogTable, schema: StructType): Unit = {
    table.provider.foreach {
      _.toLowerCase(Locale.ROOT) match {
        case HIVE_PROVIDER =>
          val serde = table.storage.serde
          if (schema.exists(_.dataType.isInstanceOf[AnsiIntervalType])) {
            throw hiveTableWithAnsiIntervalsError(table.identifier)
          } else if (serde == HiveSerDe.sourceToSerDe("orc").get.serde) {
            checkDataColNames("orc", schema)
          } else if (serde == HiveSerDe.sourceToSerDe("parquet").get.serde ||
            serde == Some("parquet.hive.serde.ParquetHiveSerDe") ||
            serde == Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")) {
            checkDataColNames("parquet", schema)
          } else if (serde == HiveSerDe.sourceToSerDe("avro").get.serde) {
            checkDataColNames("avro", schema)
          }
        case "parquet" => checkDataColNames("parquet", schema)
        case "orc" => checkDataColNames("orc", schema)
        case "avro" => checkDataColNames("avro", schema)
        case _ =>
      }
    }
  }

  def checkDataColNames(provider: String, schema: StructType): Unit = {
    val source = try {
      DataSource.lookupDataSource(provider, SQLConf.get).getConstructor().newInstance()
    } catch {
      case e: Throwable =>
        logError(log"Failed to find data source: ${MDC(LogKeys.DATA_SOURCE, provider)} " +
          log"when check data column names.", e)
        return
    }
    source match {
      case f: FileFormat => DataSourceUtils.checkFieldNames(f, schema)
      case f: FileDataSourceV2 =>
        DataSourceUtils.checkFieldNames(
          f.fallbackFileFormat.getDeclaredConstructor().newInstance(), schema)
      case _ =>
    }
  }

  /**
   * Throws exception if outputPath tries to overwrite inputpath.
   */
  def verifyNotReadPath(
      query: LogicalPlan,
      outputPath: Path,
      table: Option[CatalogTable] = None) : Unit = {
    val inputPaths = query.collect {
      case LogicalRelation(r: HadoopFsRelation, _, _, _) =>
        r.location.rootPaths
    }.flatten

    if (inputPaths.contains(outputPath)) {
      table match {
        case Some(v) =>
          throw QueryCompilationErrors.cannotOverwriteTableThatIsBeingReadFromError(v.identifier)
        case _ =>
          throw QueryCompilationErrors.cannotOverwritePathBeingReadFromError(outputPath.toString)
      }
    }
  }
}
