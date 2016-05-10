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

import java.util.regex.Pattern

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types._

/**
 * A command used to create a data source table.
 *
 * Note: This is different from [[CreateTable]]. Please check the syntax for difference.
 * This is not intended for temporary tables.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   [(col1 data_type [COMMENT col_comment], ...)]
 *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
 * }}}
 */
case class CreateDataSourceTableCommand(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String],
    ignoreIfExists: Boolean,
    managedIfNoPath: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    if (!CreateDataSourceTableUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined &&
      !CreateDataSourceTableUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

    val tableName = tableIdent.unquotedString
    val sessionState = sparkSession.sessionState

    if (sessionState.catalog.tableExists(tableIdent)) {
      if (ignoreIfExists) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table $tableName already exists.")
      }
    }

    var isExternal = true
    val optionsWithPath =
      if (!new CaseInsensitiveMap(options).contains("path") && managedIfNoPath) {
        isExternal = false
        options + ("path" -> sessionState.catalog.defaultTablePath(tableIdent))
      } else {
        options
      }

    // Create the relation to validate the arguments before writing the metadata to the metastore.
    DataSource(
      sparkSession = sparkSession,
      userSpecifiedSchema = userSpecifiedSchema,
      className = provider,
      bucketSpec = None,
      options = optionsWithPath).resolveRelation(checkPathExist = false)

    CreateDataSourceTableUtils.createDataSourceTable(
      sparkSession = sparkSession,
      tableIdent = tableIdent,
      userSpecifiedSchema = userSpecifiedSchema,
      partitionColumns = Array.empty[String],
      bucketSpec = None,
      provider = provider,
      options = optionsWithPath,
      isExternal = isExternal)

    Seq.empty[Row]
  }
}

/**
 * A command used to create a data source table using the result of a query.
 *
 * Note: This is different from [[CreateTableAsSelect]]. Please check the syntax for difference.
 * This is not intended for temporary tables.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
 *   AS SELECT ...
 * }}}
 */
case class CreateDataSourceTableAsSelectCommand(
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    bucketSpec: Option[BucketSpec],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    if (!CreateDataSourceTableUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined &&
      !CreateDataSourceTableUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

    val tableName = tableIdent.unquotedString
    val sessionState = sparkSession.sessionState
    var createMetastoreTable = false
    var isExternal = true
    val optionsWithPath =
      if (!new CaseInsensitiveMap(options).contains("path")) {
        isExternal = false
        options + ("path" -> sessionState.catalog.defaultTablePath(tableIdent))
      } else {
        options
      }

    var existingSchema = None: Option[StructType]
    if (sparkSession.sessionState.catalog.tableExists(tableIdent)) {
      // Check if we need to throw an exception or just return.
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableName already exists. " +
            s"If you are using saveAsTable, you can set SaveMode to SaveMode.Append to " +
            s"insert data into the table or set SaveMode to SaveMode.Overwrite to overwrite" +
            s"the existing data. " +
            s"Or, if you are using SQL CREATE TABLE, you need to drop $tableName first.")
        case SaveMode.Ignore =>
          // Since the table already exists and the save mode is Ignore, we will just return.
          return Seq.empty[Row]
        case SaveMode.Append =>
          // Check if the specified data source match the data source of the existing table.
          val dataSource = DataSource(
            sparkSession = sparkSession,
            userSpecifiedSchema = Some(query.schema.asNullable),
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            className = provider,
            options = optionsWithPath)
          // TODO: Check that options from the resolved relation match the relation that we are
          // inserting into (i.e. using the same compression).

          EliminateSubqueryAliases(
            sessionState.catalog.lookupRelation(tableIdent)) match {
            case l @ LogicalRelation(_: InsertableRelation | _: HadoopFsRelation, _, _) =>
              existingSchema = Some(l.schema)
            case o =>
              throw new AnalysisException(s"Saving data in ${o.toString} is not supported.")
          }
        case SaveMode.Overwrite =>
          sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
          // Need to create the table again.
          createMetastoreTable = true
      }
    } else {
      // The table does not exist. We need to create it in metastore.
      createMetastoreTable = true
    }

    val data = Dataset.ofRows(sparkSession, query)
    val df = existingSchema match {
      // If we are inserting into an existing table, just use the existing schema.
      case Some(s) => data.selectExpr(s.fieldNames: _*)
      case None => data
    }

    // Create the relation based on the data of df.
    val dataSource = DataSource(
      sparkSession,
      className = provider,
      partitionColumns = partitionColumns,
      bucketSpec = bucketSpec,
      options = optionsWithPath)

    val result = dataSource.write(mode, df)

    if (createMetastoreTable) {
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      CreateDataSourceTableUtils.createDataSourceTable(
        sparkSession = sparkSession,
        tableIdent = tableIdent,
        userSpecifiedSchema = Some(result.schema),
        partitionColumns = partitionColumns,
        bucketSpec = bucketSpec,
        provider = provider,
        options = optionsWithPath,
        isExternal = isExternal)
    }

    // Refresh the cache of the table in the catalog.
    sessionState.catalog.refreshTable(tableIdent)
    Seq.empty[Row]
  }
}

object CreateDataSourceTableUtils extends Logging {
  /**
   * Checks if the given name conforms the Hive standard ("[a-zA-z_0-9]+"),
   * i.e. if this name only contains characters, numbers, and _.
   *
   * This method is intended to have the same behavior of
   * org.apache.hadoop.hive.metastore.MetaStoreUtils.validateName.
   */
  def validateName(name: String): Boolean = {
    val tpat = Pattern.compile("[\\w_]+")
    val matcher = tpat.matcher(name)

    matcher.matches()
  }

  def createDataSourceTable(
      sparkSession: SparkSession,
      tableIdent: TableIdentifier,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      bucketSpec: Option[BucketSpec],
      provider: String,
      options: Map[String, String],
      isExternal: Boolean): Unit = {
    val tableProperties = new mutable.HashMap[String, String]
    tableProperties.put("spark.sql.sources.provider", provider)

    // Saves optional user specified schema.  Serialized JSON schema string may be too long to be
    // stored into a single metastore SerDe property.  In this case, we split the JSON string and
    // store each part as a separate SerDe property.
    userSpecifiedSchema.foreach { schema =>
      val threshold = sparkSession.sessionState.conf.schemaStringLengthThreshold
      val schemaJsonString = schema.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tableProperties.put("spark.sql.sources.schema.numParts", parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tableProperties.put(s"spark.sql.sources.schema.part.$index", part)
      }
    }

    if (userSpecifiedSchema.isDefined && partitionColumns.length > 0) {
      tableProperties.put("spark.sql.sources.schema.numPartCols", partitionColumns.length.toString)
      partitionColumns.zipWithIndex.foreach { case (partCol, index) =>
        tableProperties.put(s"spark.sql.sources.schema.partCol.$index", partCol)
      }
    }

    if (userSpecifiedSchema.isDefined && bucketSpec.isDefined) {
      val BucketSpec(numBuckets, bucketColumnNames, sortColumnNames) = bucketSpec.get

      tableProperties.put("spark.sql.sources.schema.numBuckets", numBuckets.toString)
      tableProperties.put("spark.sql.sources.schema.numBucketCols",
        bucketColumnNames.length.toString)
      bucketColumnNames.zipWithIndex.foreach { case (bucketCol, index) =>
        tableProperties.put(s"spark.sql.sources.schema.bucketCol.$index", bucketCol)
      }

      if (sortColumnNames.nonEmpty) {
        tableProperties.put("spark.sql.sources.schema.numSortCols",
          sortColumnNames.length.toString)
        sortColumnNames.zipWithIndex.foreach { case (sortCol, index) =>
          tableProperties.put(s"spark.sql.sources.schema.sortCol.$index", sortCol)
        }
      }
    }

    if (userSpecifiedSchema.isEmpty && partitionColumns.length > 0) {
      // The table does not have a specified schema, which means that the schema will be inferred
      // when we load the table. So, we are not expecting partition columns and we will discover
      // partitions when we load the table. However, if there are specified partition columns,
      // we simply ignore them and provide a warning message.
      logWarning(
        s"The schema and partitions of table $tableIdent will be inferred when it is loaded. " +
          s"Specified partition columns (${partitionColumns.mkString(",")}) will be ignored.")
    }

    val tableType = if (isExternal) {
      tableProperties.put("EXTERNAL", "TRUE")
      CatalogTableType.EXTERNAL
    } else {
      tableProperties.put("EXTERNAL", "FALSE")
      CatalogTableType.MANAGED
    }

    val maybeSerDe = HiveSerDe.sourceToSerDe(provider, sparkSession.sessionState.conf)
    val dataSource =
      DataSource(
        sparkSession,
        userSpecifiedSchema = userSpecifiedSchema,
        partitionColumns = partitionColumns,
        bucketSpec = bucketSpec,
        className = provider,
        options = options)

    def newSparkSQLSpecificMetastoreTable(): CatalogTable = {
      CatalogTable(
        identifier = tableIdent,
        tableType = tableType,
        schema = Nil,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          compressed = false,
          serdeProperties = options
        ),
        properties = tableProperties.toMap)
    }

    def newHiveCompatibleMetastoreTable(
        relation: HadoopFsRelation,
        serde: HiveSerDe): CatalogTable = {
      assert(partitionColumns.isEmpty)
      assert(relation.partitionSchema.isEmpty)

      CatalogTable(
        identifier = tableIdent,
        tableType = tableType,
        storage = CatalogStorageFormat(
          locationUri = Some(relation.location.paths.map(_.toUri.toString).head),
          inputFormat = serde.inputFormat,
          outputFormat = serde.outputFormat,
          serde = serde.serde,
          compressed = false,
          serdeProperties = options
        ),
        schema = relation.schema.map { f =>
          CatalogColumn(f.name, f.dataType.catalogString)
        },
        properties = tableProperties.toMap,
        viewText = None)
    }

    // TODO: Support persisting partitioned data source relations in Hive compatible format
    val qualifiedTableName = tableIdent.quotedString
    val skipHiveMetadata = options.getOrElse("skipHiveMetadata", "false").toBoolean
    val resolvedRelation = dataSource.resolveRelation(checkPathExist = false)
    val (hiveCompatibleTable, logMessage) = (maybeSerDe, resolvedRelation) match {
      case _ if skipHiveMetadata =>
        val message =
          s"Persisting partitioned data source relation $qualifiedTableName into " +
            "Hive metastore in Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)

      case (Some(serde), relation: HadoopFsRelation)
        if relation.location.paths.length == 1 && relation.partitionSchema.isEmpty =>
        val hiveTable = newHiveCompatibleMetastoreTable(relation, serde)
        val message =
          s"Persisting data source relation $qualifiedTableName with a single input path " +
            s"into Hive metastore in Hive compatible format. Input path: " +
            s"${relation.location.paths.head}."
        (Some(hiveTable), message)

      case (Some(serde), relation: HadoopFsRelation) if relation.partitionSchema.nonEmpty =>
        val message =
          s"Persisting partitioned data source relation $qualifiedTableName into " +
            "Hive metastore in Spark SQL specific format, which is NOT compatible with Hive. " +
            "Input path(s): " + relation.location.paths.mkString("\n", "\n", "")
        (None, message)

      case (Some(serde), relation: HadoopFsRelation) =>
        val message =
          s"Persisting data source relation $qualifiedTableName with multiple input paths into " +
            "Hive metastore in Spark SQL specific format, which is NOT compatible with Hive. " +
            s"Input paths: " + relation.location.paths.mkString("\n", "\n", "")
        (None, message)

      case (Some(serde), _) =>
        val message =
          s"Data source relation $qualifiedTableName is not a " +
            s"${classOf[HadoopFsRelation].getSimpleName}. Persisting it into Hive metastore " +
            "in Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)

      case _ =>
        val message =
          s"Couldn't find corresponding Hive SerDe for data source provider $provider. " +
            s"Persisting data source relation $qualifiedTableName into Hive metastore in " +
            s"Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)
    }

    (hiveCompatibleTable, logMessage) match {
      case (Some(table), message) =>
        // We first try to save the metadata of the table in a Hive compatible way.
        // If Hive throws an error, we fall back to save its metadata in the Spark SQL
        // specific way.
        try {
          logInfo(message)
          sparkSession.sessionState.catalog.createTable(table, ignoreIfExists = false)
        } catch {
          case NonFatal(e) =>
            val warningMessage =
              s"Could not persist $qualifiedTableName in a Hive compatible way. Persisting " +
                s"it into Hive metastore in Spark SQL specific format."
            logWarning(warningMessage, e)
            val table = newSparkSQLSpecificMetastoreTable()
            sparkSession.sessionState.catalog.createTable(table, ignoreIfExists = false)
        }

      case (None, message) =>
        logWarning(message)
        val table = newSparkSQLSpecificMetastoreTable()
        sparkSession.sessionState.catalog.createTable(table, ignoreIfExists = false)
    }
  }
}
