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

import org.apache.hadoop.fs._

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.connector.catalog.SupportsNamespaces._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.internal.HiveSerDe
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
  extends RunnableCommand {

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
    extended: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbMetadata: CatalogDatabase =
      sparkSession.sessionState.catalog.getDatabaseMetadata(databaseName)
    val allDbProperties = dbMetadata.properties
    val result =
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
          properties.toSeq.mkString("(", ", ", ")")
        }
      result :+ Row("Properties", propertiesStr)
    } else {
      result
    }
  }

  override val output: Seq[Attribute] = {
    AttributeReference("database_description_item", StringType, nullable = false)() ::
      AttributeReference("database_description_value", StringType, nullable = false)() :: Nil
  }
}

object DDLUtils {
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
      table.dataSchema.asNullable.toAttributes,
      table.partitionSchema.asNullable.toAttributes)
  }

  /**
   * Throws a standard error for actions that require partitionProvider = hive.
   */
  def verifyPartitionProviderIsHive(
      spark: SparkSession, table: CatalogTable, action: String): Unit = {
    val tableName = table.identifier.table
    if (!spark.sqlContext.conf.manageFilesourcePartitions && isDatasourceTable(table)) {
      throw new AnalysisException(
        s"$action is not allowed on $tableName since filesource partition management is " +
          "disabled (spark.sql.hive.manageFilesourcePartitions = false).")
    }
    if (!table.tracksPartitionsInCatalog && isDatasourceTable(table)) {
      throw new AnalysisException(
        s"$action is not allowed on $tableName since its partition metadata is not stored in " +
          "the Hive metastore. To import this information into the metastore, run " +
          s"`msck repair table $tableName`")
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
    if (!catalog.isTemporaryTable(tableMetadata.identifier)) {
      tableMetadata.tableType match {
        case CatalogTableType.VIEW if !isView =>
          throw new AnalysisException(
            "Cannot alter a view with ALTER TABLE. Please use ALTER VIEW instead")
        case o if o != CatalogTableType.VIEW && isView =>
          throw new AnalysisException(
            s"Cannot alter a table with ALTER VIEW. Please use ALTER TABLE instead")
        case _ =>
      }
    }
  }

  private[sql] def checkDataColNames(table: CatalogTable): Unit = {
    checkDataColNames(table, table.dataSchema.fieldNames)
  }

  private[sql] def checkDataColNames(table: CatalogTable, colNames: Seq[String]): Unit = {
    table.provider.foreach {
      _.toLowerCase(Locale.ROOT) match {
        case HIVE_PROVIDER =>
          val serde = table.storage.serde
          if (serde == HiveSerDe.sourceToSerDe("orc").get.serde) {
            OrcFileFormat.checkFieldNames(colNames)
          } else if (serde == HiveSerDe.sourceToSerDe("parquet").get.serde ||
            serde == Some("parquet.hive.serde.ParquetHiveSerDe") ||
            serde == Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")) {
            ParquetSchemaConverter.checkFieldNames(colNames)
          }
        case "parquet" => ParquetSchemaConverter.checkFieldNames(colNames)
        case "orc" => OrcFileFormat.checkFieldNames(colNames)
        case _ =>
      }
    }
  }

  /**
   * Throws exception if outputPath tries to overwrite inputpath.
   */
  def verifyNotReadPath(query: LogicalPlan, outputPath: Path) : Unit = {
    val inputPaths = query.collect {
      case LogicalRelation(r: HadoopFsRelation, _, _, _) =>
        r.location.rootPaths
    }.flatten

    if (inputPaths.contains(outputPath)) {
      throw new AnalysisException(
        "Cannot overwrite a path that is also being read from.")
    }
  }
}
