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

import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.types._


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
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    catalog.createDatabase(
      CatalogDatabase(
        databaseName,
        comment.getOrElse(""),
        path.getOrElse(catalog.getDefaultDBPath(databaseName)),
        props),
      ifNotExists)
    Seq.empty[Row]
  }

  override val output: Seq[Attribute] = Seq.empty
}


/**
 * A command for users to remove a database from the system.
 *
 * 'ifExists':
 * - true, if database_name does't exist, no action
 * - false (default), if database_name does't exist, a warning message will be issued
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
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.catalog.dropDatabase(databaseName, ifExists, cascade)
    Seq.empty[Row]
  }

  override val output: Seq[Attribute] = Seq.empty
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
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val db: CatalogDatabase = catalog.getDatabaseMetadata(databaseName)
    catalog.alterDatabase(db.copy(properties = db.properties ++ props))

    Seq.empty[Row]
  }

  override val output: Seq[Attribute] = Seq.empty
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
    extended: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbMetadata: CatalogDatabase =
      sparkSession.sessionState.catalog.getDatabaseMetadata(databaseName)
    val result =
      Row("Database Name", dbMetadata.name) ::
        Row("Description", dbMetadata.description) ::
        Row("Location", dbMetadata.locationUri) :: Nil

    if (extended) {
      val properties =
        if (dbMetadata.properties.isEmpty) {
          ""
        } else {
          dbMetadata.properties.toSeq.mkString("(", ", ", ")")
        }
      result :+ Row("Properties", properties)
    } else {
      result
    }
  }

  override val output: Seq[Attribute] = {
    AttributeReference("database_description_item", StringType, nullable = false)() ::
      AttributeReference("database_description_value", StringType, nullable = false)() :: Nil
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
    isView: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (!catalog.tableExists(tableName)) {
      if (!ifExists) {
        val objectName = if (isView) "View" else "Table"
        logError(s"$objectName '${tableName.quotedString}' does not exist")
      }
    } else {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      catalog.getTableMetadataOption(tableName).map(_.tableType match {
        case CatalogTableType.VIEW if !isView =>
          throw new AnalysisException(
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
        case o if o != CatalogTableType.VIEW && isView =>
          throw new AnalysisException(
            s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead")
        case _ =>
      })
      try {
        sparkSession.cacheManager.tryUncacheQuery(sparkSession.table(tableName.quotedString))
      } catch {
        case NonFatal(e) => log.warn(s"${e.getMessage}", e)
      }
      catalog.invalidateTable(tableName)
      catalog.dropTable(tableName, ifExists)
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
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    DDLUtils.verifyAlterTableType(catalog, tableName, isView)
    val table = catalog.getTableMetadata(tableName)
    val newProperties = table.properties ++ properties
    if (DDLUtils.isDatasourceTable(newProperties)) {
      throw new AnalysisException("ALTER TABLE SET TBLPROPERTIES is not supported for " +
        "tables defined using the datasource API")
    }
    val newTable = table.copy(properties = newProperties)
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
    DDLUtils.verifyAlterTableType(catalog, tableName, isView)
    val table = catalog.getTableMetadata(tableName)
    if (DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException(
        "alter table properties is not supported for datasource tables")
    }
    if (!ifExists) {
      propKeys.foreach { k =>
        if (!table.properties.contains(k)) {
          throw new AnalysisException(
            s"attempted to unset non-existent property '$k' in table '$tableName'")
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
    partition: Option[Map[String, String]])
  extends RunnableCommand {

  // should never happen if we parsed things correctly
  require(serdeClassName.isDefined || serdeProperties.isDefined,
    "ALTER TABLE attempted to set neither serde class name nor serde properties")

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    // Do not support setting serde for datasource tables
    if (serdeClassName.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException("ALTER TABLE SET SERDE is not supported for datasource tables")
    }
    val newTable = table.withNewStorage(
      serde = serdeClassName.orElse(table.storage.serde),
      serdeProperties = table.storage.serdeProperties ++ serdeProperties.getOrElse(Map()))
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

/**
 * Add Partition in ALTER TABLE: add the table partitions.
 *
 * 'partitionSpecsAndLocs': the syntax of ALTER VIEW is identical to ALTER TABLE,
 * EXCEPT that it is ILLEGAL to specify a LOCATION clause.
 * An error message will be issued if the partition exists, unless 'ifNotExists' is true.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table ADD [IF NOT EXISTS] PARTITION spec [LOCATION 'loc1']
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
    if (DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException(
        "ALTER TABLE ADD PARTITION is not allowed for tables defined using the datasource API")
    }
    val parts = partitionSpecsAndLocs.map { case (spec, location) =>
      // inherit table storage format (possibly except for location)
      CatalogTablePartition(spec, table.storage.copy(locationUri = location))
    }
    catalog.createPartitions(tableName, parts, ignoreIfExists = ifNotExists)
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
    sparkSession.sessionState.catalog.renamePartitions(
      tableName, Seq(oldPartition), Seq(newPartition))
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
    ifExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    if (DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException(
        "ALTER TABLE DROP PARTITIONS is not allowed for tables defined using the datasource API")
    }
    catalog.dropPartitions(tableName, specs, ignoreIfNotExists = ifExists)
    Seq.empty[Row]
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
    partitionSpec match {
      case Some(spec) =>
        // Partition spec is specified, so we set the location only for this partition
        val part = catalog.getPartition(tableName, spec)
        val newPart =
          if (DDLUtils.isDatasourceTable(table)) {
            throw new AnalysisException(
              "ALTER TABLE SET LOCATION for partition is not allowed for tables defined " +
              "using the datasource API")
          } else {
            part.copy(storage = part.storage.copy(locationUri = Some(location)))
          }
        catalog.alterPartitions(tableName, Seq(newPart))
      case None =>
        // No partition spec is specified, so we set the location for the table itself
        val newTable =
          if (DDLUtils.isDatasourceTable(table)) {
            table.withNewStorage(
              locationUri = Some(location),
              serdeProperties = table.storage.serdeProperties ++ Map("path" -> location))
          } else {
            table.withNewStorage(locationUri = Some(location))
          }
        catalog.alterTable(newTable)
    }
    Seq.empty[Row]
  }
}


object DDLUtils {

  def isDatasourceTable(props: Map[String, String]): Boolean = {
    props.contains("spark.sql.sources.provider")
  }

  def isDatasourceTable(table: CatalogTable): Boolean = {
    isDatasourceTable(table.properties)
  }

  /**
   * If the command ALTER VIEW is to alter a table or ALTER TABLE is to alter a view,
   * issue an exception [[AnalysisException]].
   */
  def verifyAlterTableType(
      catalog: SessionCatalog,
      tableIdentifier: TableIdentifier,
      isView: Boolean): Unit = {
    catalog.getTableMetadataOption(tableIdentifier).map(_.tableType match {
      case CatalogTableType.VIEW if !isView =>
        throw new AnalysisException(
          "Cannot alter a view with ALTER TABLE. Please use ALTER VIEW instead")
      case o if o != CatalogTableType.VIEW && isView =>
        throw new AnalysisException(
          s"Cannot alter a table with ALTER VIEW. Please use ALTER TABLE instead")
      case _ =>
    })
  }

  def isTablePartitioned(table: CatalogTable): Boolean = {
    table.partitionColumns.nonEmpty ||
      table.properties.contains("spark.sql.sources.schema.numPartCols")
  }

  // A persisted data source table may not store its schema in the catalog. In this case, its schema
  // will be inferred at runtime when the table is referenced.
  def getSchemaFromTableProperties(metadata: CatalogTable): Option[StructType] = {
    require(isDatasourceTable(metadata))
    val props = metadata.properties
    if (props.isDefinedAt("spark.sql.sources.schema")) {
      // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
      // After SPARK-6024, we removed this flag.
      // Although we are not using spark.sql.sources.schema any more, we need to still support.
      props.get("spark.sql.sources.schema").map(DataType.fromJson(_).asInstanceOf[StructType])
    } else {
      metadata.properties.get("spark.sql.sources.schema.numParts").map { numParts =>
        val parts = (0 until numParts.toInt).map { index =>
          val part = metadata.properties.get(s"spark.sql.sources.schema.part.$index").orNull
          if (part == null) {
            throw new AnalysisException(
              "Could not read schema from the metastore because it is corrupted " +
                s"(missing part $index of the schema, $numParts parts are expected).")
          }

          part
        }
        // Stick all parts back to a single schema string.
        DataType.fromJson(parts.mkString).asInstanceOf[StructType]
      }
    }
  }

  private def getColumnNamesByType(
      props: Map[String, String], colType: String, typeName: String): Seq[String] = {
    require(isDatasourceTable(props))

    for {
      numCols <- props.get(s"spark.sql.sources.schema.num${colType.capitalize}Cols").toSeq
      index <- 0 until numCols.toInt
    } yield props.getOrElse(
      s"spark.sql.sources.schema.${colType}Col.$index",
      throw new AnalysisException(
        s"Corrupted $typeName in catalog: $numCols parts expected, but part $index is missing."
      )
    )
  }

  def getPartitionColumnsFromTableProperties(metadata: CatalogTable): Seq[String] = {
    getColumnNamesByType(metadata.properties, "part", "partitioning columns")
  }

  def getBucketSpecFromTableProperties(metadata: CatalogTable): Option[BucketSpec] = {
    if (isDatasourceTable(metadata)) {
      metadata.properties.get("spark.sql.sources.schema.numBuckets").map { numBuckets =>
        BucketSpec(
          numBuckets.toInt,
          getColumnNamesByType(metadata.properties, "bucket", "bucketing columns"),
          getColumnNamesByType(metadata.properties, "sort", "sorting columns"))
      }
    } else {
      None
    }
  }
}
