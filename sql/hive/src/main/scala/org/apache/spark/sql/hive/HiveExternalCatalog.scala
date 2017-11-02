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

package org.apache.spark.sql.hive

import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.util
import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.thrift.TException

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.types.{DataType, StructType}


/**
 * A persistent implementation of the system catalog using Hive.
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class HiveExternalCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends ExternalCatalog with Logging {

  import CatalogTypes.TablePartitionSpec
  import HiveExternalCatalog._
  import CatalogTableType._

  /**
   * A Hive client used to interact with the metastore.
   */
  lazy val client: HiveClient = {
    HiveUtils.newClientForMetadata(conf, hadoopConf)
  }

  // Exceptions thrown by the hive client that we would like to wrap
  private val clientExceptions = Set(
    classOf[HiveException].getCanonicalName,
    classOf[TException].getCanonicalName,
    classOf[InvocationTargetException].getCanonicalName)

  /**
   * Whether this is an exception thrown by the hive client that should be wrapped.
   *
   * Due to classloader isolation issues, pattern matching won't work here so we need
   * to compare the canonical names of the exceptions, which we assume to be stable.
   */
  private def isClientException(e: Throwable): Boolean = {
    var temp: Class[_] = e.getClass
    var found = false
    while (temp != null && !found) {
      found = clientExceptions.contains(temp.getCanonicalName)
      temp = temp.getSuperclass
    }
    found
  }

  /**
   * Run some code involving `client` in a [[synchronized]] block and wrap certain
   * exceptions thrown in the process in [[AnalysisException]].
   */
  private def withClient[T](body: => T): T = synchronized {
    try {
      body
    } catch {
      case NonFatal(exception) if isClientException(exception) =>
        val e = exception match {
          // Since we are using shim, the exceptions thrown by the underlying method of
          // Method.invoke() are wrapped by InvocationTargetException
          case i: InvocationTargetException => i.getCause
          case o => o
        }
        throw new AnalysisException(
          e.getClass.getCanonicalName + ": " + e.getMessage, cause = Some(e))
    }
  }

  /**
   * Get the raw table metadata from hive metastore directly. The raw table metadata may contains
   * special data source properties and should not be exposed outside of `HiveExternalCatalog`. We
   * should interpret these special data source properties and restore the original table metadata
   * before returning it.
   */
  private[hive] def getRawTable(db: String, table: String): CatalogTable = withClient {
    client.getTable(db, table)
  }

  /**
   * If the given table properties contains datasource properties, throw an exception. We will do
   * this check when create or alter a table, i.e. when we try to write table metadata to Hive
   * metastore.
   */
  private def verifyTableProperties(table: CatalogTable): Unit = {
    val invalidKeys = table.properties.keys.filter(_.startsWith(SPARK_SQL_PREFIX))
    if (invalidKeys.nonEmpty) {
      throw new AnalysisException(s"Cannot persistent ${table.qualifiedName} into hive metastore " +
        s"as table property keys may not start with '$SPARK_SQL_PREFIX': " +
        invalidKeys.mkString("[", ", ", "]"))
    }
    // External users are not allowed to set/switch the table type. In Hive metastore, the table
    // type can be switched by changing the value of a case-sensitive table property `EXTERNAL`.
    if (table.properties.contains("EXTERNAL")) {
      throw new AnalysisException("Cannot set or change the preserved property key: 'EXTERNAL'")
    }
  }

  /**
   * Checks the validity of data column names. Hive metastore disallows the table to use comma in
   * data column names. Partition columns do not have such a restriction. Views do not have such
   * a restriction.
   */
  private def verifyDataSchema(
      tableName: TableIdentifier, tableType: CatalogTableType, dataSchema: StructType): Unit = {
    if (tableType != VIEW) {
      dataSchema.map(_.name).foreach { colName =>
        if (colName.contains(",")) {
          throw new AnalysisException("Cannot create a table having a column whose name contains " +
            s"commas in Hive metastore. Table: $tableName; Column: $colName")
        }
      }
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override protected def doCreateDatabase(
      dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withClient {
    client.createDatabase(dbDefinition, ignoreIfExists)
  }

  override protected def doDropDatabase(
      db: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = withClient {
    client.dropDatabase(db, ignoreIfNotExists, cascade)
  }

  /**
   * Alter a database whose name matches the one specified in `dbDefinition`,
   * assuming the database exists.
   *
   * Note: As of now, this only supports altering database properties!
   */
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = withClient {
    val existingDb = getDatabase(dbDefinition.name)
    if (existingDb.properties == dbDefinition.properties) {
      logWarning(s"Request to alter database ${dbDefinition.name} is a no-op because " +
        s"the provided database properties are the same as the old ones. Hive does not " +
        s"currently support altering other database fields.")
    }
    client.alterDatabase(dbDefinition)
  }

  override def getDatabase(db: String): CatalogDatabase = withClient {
    client.getDatabase(db)
  }

  override def databaseExists(db: String): Boolean = withClient {
    client.databaseExists(db)
  }

  override def listDatabases(): Seq[String] = withClient {
    client.listDatabases("*")
  }

  override def listDatabases(pattern: String): Seq[String] = withClient {
    client.listDatabases(pattern)
  }

  override def setCurrentDatabase(db: String): Unit = withClient {
    client.setCurrentDatabase(db)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override protected def doCreateTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = withClient {
    assert(tableDefinition.identifier.database.isDefined)
    val db = tableDefinition.identifier.database.get
    val table = tableDefinition.identifier.table
    requireDbExists(db)
    verifyTableProperties(tableDefinition)
    verifyDataSchema(
      tableDefinition.identifier, tableDefinition.tableType, tableDefinition.dataSchema)

    if (tableExists(db, table) && !ignoreIfExists) {
      throw new TableAlreadyExistsException(db = db, table = table)
    }

    // Ideally we should not create a managed table with location, but Hive serde table can
    // specify location for managed table. And in [[CreateDataSourceTableAsSelectCommand]] we have
    // to create the table directory and write out data before we create this table, to avoid
    // exposing a partial written table.
    val needDefaultTableLocation = tableDefinition.tableType == MANAGED &&
      tableDefinition.storage.locationUri.isEmpty

    val tableLocation = if (needDefaultTableLocation) {
      Some(CatalogUtils.stringToURI(defaultTablePath(tableDefinition.identifier)))
    } else {
      tableDefinition.storage.locationUri
    }

    if (DDLUtils.isDatasourceTable(tableDefinition)) {
      createDataSourceTable(
        tableDefinition.withNewStorage(locationUri = tableLocation),
        ignoreIfExists)
    } else {
      val tableWithDataSourceProps = tableDefinition.copy(
        // We can't leave `locationUri` empty and count on Hive metastore to set a default table
        // location, because Hive metastore uses hive.metastore.warehouse.dir to generate default
        // table location for tables in default database, while we expect to use the location of
        // default database.
        storage = tableDefinition.storage.copy(locationUri = tableLocation),
        // Here we follow data source tables and put table metadata like table schema, partition
        // columns etc. in table properties, so that we can work around the Hive metastore issue
        // about not case preserving and make Hive serde table and view support mixed-case column
        // names.
        properties = tableDefinition.properties ++ tableMetaToTableProps(tableDefinition))
      client.createTable(tableWithDataSourceProps, ignoreIfExists)
    }
  }

  private def createDataSourceTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    // data source table always have a provider, it's guaranteed by `DDLUtils.isDatasourceTable`.
    val provider = table.provider.get

    // To work around some hive metastore issues, e.g. not case-preserving, bad decimal type
    // support, no column nullability, etc., we should do some extra works before saving table
    // metadata into Hive metastore:
    //  1. Put table metadata like table schema, partition columns, etc. in table properties.
    //  2. Check if this table is hive compatible.
    //    2.1  If it's not hive compatible, set location URI, schema, partition columns and bucket
    //         spec to empty and save table metadata to Hive.
    //    2.2  If it's hive compatible, set serde information in table metadata and try to save
    //         it to Hive. If it fails, treat it as not hive compatible and go back to 2.1
    val tableProperties = tableMetaToTableProps(table)

    // put table provider and partition provider in table properties.
    tableProperties.put(DATASOURCE_PROVIDER, provider)
    if (table.tracksPartitionsInCatalog) {
      tableProperties.put(TABLE_PARTITION_PROVIDER, TABLE_PARTITION_PROVIDER_CATALOG)
    }

    // Ideally we should also put `locationUri` in table properties like provider, schema, etc.
    // However, in older version of Spark we already store table location in storage properties
    // with key "path". Here we keep this behaviour for backward compatibility.
    val storagePropsWithLocation = table.storage.properties ++
      table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))

    // converts the table metadata to Spark SQL specific format, i.e. set data schema, names and
    // bucket specification to empty. Note that partition columns are retained, so that we can
    // call partition-related Hive API later.
    def newSparkSQLSpecificMetastoreTable(): CatalogTable = {
      table.copy(
        // Hive only allows directory paths as location URIs while Spark SQL data source tables
        // also allow file paths. For non-hive-compatible format, we should not set location URI
        // to avoid hive metastore to throw exception.
        storage = table.storage.copy(
          locationUri = None,
          properties = storagePropsWithLocation),
        schema = StructType(EMPTY_DATA_SCHEMA ++ table.partitionSchema),
        bucketSpec = None,
        properties = table.properties ++ tableProperties)
    }

    // converts the table metadata to Hive compatible format, i.e. set the serde information.
    def newHiveCompatibleMetastoreTable(serde: HiveSerDe): CatalogTable = {
      val location = if (table.tableType == EXTERNAL) {
        // When we hit this branch, we are saving an external data source table with hive
        // compatible format, which means the data source is file-based and must have a `path`.
        require(table.storage.locationUri.isDefined,
          "External file-based data source table must have a `path` entry in storage properties.")
        Some(table.location)
      } else {
        None
      }

      // TODO: empty data schema is not hive compatible, we only do it to keep behavior as it was
      // because previously we generate the special empty schema in `HiveClient`. Remove this in
      // Spark 2.3.
      val schema = if (table.dataSchema.isEmpty) {
        StructType(EMPTY_DATA_SCHEMA ++ table.partitionSchema)
      } else {
        table.schema
      }

      table.copy(
        storage = table.storage.copy(
          locationUri = location,
          inputFormat = serde.inputFormat,
          outputFormat = serde.outputFormat,
          serde = serde.serde,
          properties = storagePropsWithLocation
        ),
        schema = schema,
        properties = table.properties ++ tableProperties)
    }

    val qualifiedTableName = table.identifier.quotedString
    val maybeSerde = HiveSerDe.sourceToSerDe(provider)
    val skipHiveMetadata = table.storage.properties
      .getOrElse("skipHiveMetadata", "false").toBoolean

    val (hiveCompatibleTable, logMessage) = maybeSerde match {
      case _ if skipHiveMetadata =>
        val message =
          s"Persisting data source table $qualifiedTableName into Hive metastore in" +
            "Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)

      // our bucketing is un-compatible with hive(different hash function)
      case _ if table.bucketSpec.nonEmpty =>
        val message =
          s"Persisting bucketed data source table $qualifiedTableName into " +
            "Hive metastore in Spark SQL specific format, which is NOT compatible with Hive. "
        (None, message)

      case Some(serde) =>
        val message =
          s"Persisting file based data source table $qualifiedTableName into " +
            s"Hive metastore in Hive compatible format."
        (Some(newHiveCompatibleMetastoreTable(serde)), message)

      case _ =>
        val message =
          s"Couldn't find corresponding Hive SerDe for data source provider $provider. " +
            s"Persisting data source table $qualifiedTableName into Hive metastore in " +
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
          saveTableIntoHive(table, ignoreIfExists)
        } catch {
          case NonFatal(e) =>
            val warningMessage =
              s"Could not persist ${table.identifier.quotedString} in a Hive " +
                "compatible way. Persisting it into Hive metastore in Spark SQL specific format."
            logWarning(warningMessage, e)
            saveTableIntoHive(newSparkSQLSpecificMetastoreTable(), ignoreIfExists)
        }

      case (None, message) =>
        logWarning(message)
        saveTableIntoHive(newSparkSQLSpecificMetastoreTable(), ignoreIfExists)
    }
  }

  /**
   * Data source tables may be non Hive compatible and we need to store table metadata in table
   * properties to workaround some Hive metastore limitations.
   * This method puts table schema, partition column names, bucket specification into a map, which
   * can be used as table properties later.
   */
  private def tableMetaToTableProps(table: CatalogTable): mutable.Map[String, String] = {
    tableMetaToTableProps(table, table.schema)
  }

  private def tableMetaToTableProps(
      table: CatalogTable,
      schema: StructType): mutable.Map[String, String] = {
    val partitionColumns = table.partitionColumnNames
    val bucketSpec = table.bucketSpec

    val properties = new mutable.HashMap[String, String]
    // Serialized JSON schema string may be too long to be stored into a single metastore table
    // property. In this case, we split the JSON string and store each part as a separate table
    // property.
    val threshold = conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
    val schemaJsonString = schema.json
    // Split the JSON string.
    val parts = schemaJsonString.grouped(threshold).toSeq
    properties.put(DATASOURCE_SCHEMA_NUMPARTS, parts.size.toString)
    parts.zipWithIndex.foreach { case (part, index) =>
      properties.put(s"$DATASOURCE_SCHEMA_PART_PREFIX$index", part)
    }

    if (partitionColumns.nonEmpty) {
      properties.put(DATASOURCE_SCHEMA_NUMPARTCOLS, partitionColumns.length.toString)
      partitionColumns.zipWithIndex.foreach { case (partCol, index) =>
        properties.put(s"$DATASOURCE_SCHEMA_PARTCOL_PREFIX$index", partCol)
      }
    }

    if (bucketSpec.isDefined) {
      val BucketSpec(numBuckets, bucketColumnNames, sortColumnNames) = bucketSpec.get

      properties.put(DATASOURCE_SCHEMA_NUMBUCKETS, numBuckets.toString)
      properties.put(DATASOURCE_SCHEMA_NUMBUCKETCOLS, bucketColumnNames.length.toString)
      bucketColumnNames.zipWithIndex.foreach { case (bucketCol, index) =>
        properties.put(s"$DATASOURCE_SCHEMA_BUCKETCOL_PREFIX$index", bucketCol)
      }

      if (sortColumnNames.nonEmpty) {
        properties.put(DATASOURCE_SCHEMA_NUMSORTCOLS, sortColumnNames.length.toString)
        sortColumnNames.zipWithIndex.foreach { case (sortCol, index) =>
          properties.put(s"$DATASOURCE_SCHEMA_SORTCOL_PREFIX$index", sortCol)
        }
      }
    }

    properties
  }

  private def defaultTablePath(tableIdent: TableIdentifier): String = {
    val dbLocation = getDatabase(tableIdent.database.get).locationUri
    new Path(new Path(dbLocation), tableIdent.table).toString
  }

  private def saveTableIntoHive(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    assert(DDLUtils.isDatasourceTable(tableDefinition),
      "saveTableIntoHive only takes data source table.")
    // If this is an external data source table...
    if (tableDefinition.tableType == EXTERNAL &&
      // ... that is not persisted as Hive compatible format (external tables in Hive compatible
      // format always set `locationUri` to the actual data location and should NOT be hacked as
      // following.)
      tableDefinition.storage.locationUri.isEmpty) {
      // !! HACK ALERT !!
      //
      // Due to a restriction of Hive metastore, here we have to set `locationUri` to a temporary
      // directory that doesn't exist yet but can definitely be successfully created, and then
      // delete it right after creating the external data source table. This location will be
      // persisted to Hive metastore as standard Hive table location URI, but Spark SQL doesn't
      // really use it. Also, since we only do this workaround for external tables, deleting the
      // directory after the fact doesn't do any harm.
      //
      // Please refer to https://issues.apache.org/jira/browse/SPARK-15269 for more details.
      val tempPath = {
        val dbLocation = new Path(getDatabase(tableDefinition.database).locationUri)
        new Path(dbLocation, tableDefinition.identifier.table + "-__PLACEHOLDER__")
      }

      try {
        client.createTable(
          tableDefinition.withNewStorage(locationUri = Some(tempPath.toUri)),
          ignoreIfExists)
      } finally {
        FileSystem.get(tempPath.toUri, hadoopConf).delete(tempPath, true)
      }
    } else {
      client.createTable(tableDefinition, ignoreIfExists)
    }
  }

  override protected def doDropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withClient {
    requireDbExists(db)
    client.dropTable(db, table, ignoreIfNotExists, purge)
  }

  override protected def doRenameTable(
      db: String,
      oldName: String,
      newName: String): Unit = withClient {
    val rawTable = getRawTable(db, oldName)

    // Note that Hive serde tables don't use path option in storage properties to store the value
    // of table location, but use `locationUri` field to store it directly. And `locationUri` field
    // will be updated automatically in Hive metastore by the `alterTable` call at the end of this
    // method. Here we only update the path option if the path option already exists in storage
    // properties, to avoid adding a unnecessary path option for Hive serde tables.
    val hasPathOption = CaseInsensitiveMap(rawTable.storage.properties).contains("path")
    val storageWithNewPath = if (rawTable.tableType == MANAGED && hasPathOption) {
      // If it's a managed table with path option and we are renaming it, then the path option
      // becomes inaccurate and we need to update it according to the new table name.
      val newTablePath = defaultTablePath(TableIdentifier(newName, Some(db)))
      updateLocationInStorageProps(rawTable, Some(newTablePath))
    } else {
      rawTable.storage
    }

    val newTable = rawTable.copy(
      identifier = TableIdentifier(newName, Some(db)),
      storage = storageWithNewPath)

    client.alterTable(oldName, newTable)
  }

  private def getLocationFromStorageProps(table: CatalogTable): Option[String] = {
    CaseInsensitiveMap(table.storage.properties).get("path")
  }

  private def updateLocationInStorageProps(
      table: CatalogTable,
      newPath: Option[String]): CatalogStorageFormat = {
    // We can't use `filterKeys` here, as the map returned by `filterKeys` is not serializable,
    // while `CatalogTable` should be serializable.
    val propsWithoutPath = table.storage.properties.filter {
      case (k, v) => k.toLowerCase(Locale.ROOT) != "path"
    }
    table.storage.copy(properties = propsWithoutPath ++ newPath.map("path" -> _))
  }

  /**
   * Alter a table whose name that matches the one specified in `tableDefinition`,
   * assuming the table exists.
   *
   * Note: As of now, this doesn't support altering table schema, partition column names and bucket
   * specification. We will ignore them even if users do specify different values for these fields.
   */
  override def alterTable(tableDefinition: CatalogTable): Unit = withClient {
    assert(tableDefinition.identifier.database.isDefined)
    val db = tableDefinition.identifier.database.get
    requireTableExists(db, tableDefinition.identifier.table)
    verifyTableProperties(tableDefinition)

    // convert table statistics to properties so that we can persist them through hive api
    val withStatsProps = if (tableDefinition.stats.isDefined) {
      val stats = tableDefinition.stats.get
      var statsProperties: Map[String, String] =
        Map(STATISTICS_TOTAL_SIZE -> stats.sizeInBytes.toString())
      if (stats.rowCount.isDefined) {
        statsProperties += STATISTICS_NUM_ROWS -> stats.rowCount.get.toString()
      }
      val colNameTypeMap: Map[String, DataType] =
        tableDefinition.schema.fields.map(f => (f.name, f.dataType)).toMap
      stats.colStats.foreach { case (colName, colStat) =>
        colStat.toMap(colName, colNameTypeMap(colName)).foreach { case (k, v) =>
          statsProperties += (columnStatKeyPropName(colName, k) -> v)
        }
      }
      tableDefinition.copy(properties = tableDefinition.properties ++ statsProperties)
    } else {
      tableDefinition
    }

    if (tableDefinition.tableType == VIEW) {
      client.alterTable(withStatsProps)
    } else {
      val oldTableDef = getRawTable(db, withStatsProps.identifier.table)

      val newStorage = if (DDLUtils.isHiveTable(tableDefinition)) {
        tableDefinition.storage
      } else {
        // We can't alter the table storage of data source table directly for 2 reasons:
        //   1. internally we use path option in storage properties to store the value of table
        //      location, but the given `tableDefinition` is from outside and doesn't have the path
        //      option, we need to add it manually.
        //   2. this data source table may be created on a file, not a directory, then we can't set
        //      the `locationUri` field and save it to Hive metastore, because Hive only allows
        //      directory as table location.
        //
        // For example, an external data source table is created with a single file '/path/to/file'.
        // Internally, we will add a path option with value '/path/to/file' to storage properties,
        // and set the `locationUri` to a special value due to SPARK-15269(please see
        // `saveTableIntoHive` for more details). When users try to get the table metadata back, we
        // will restore the `locationUri` field from the path option and remove the path option from
        // storage properties. When users try to alter the table storage, the given
        // `tableDefinition` will have `locationUri` field with value `/path/to/file` and the path
        // option is not set.
        //
        // Here we need 2 extra steps:
        //   1. add path option to storage properties, to match the internal format, i.e. using path
        //      option to store the value of table location.
        //   2. set the `locationUri` field back to the old one from the existing table metadata,
        //      if users don't want to alter the table location. This step is necessary as the
        //      `locationUri` is not always same with the path option, e.g. in the above example
        //      `locationUri` is a special value and we should respect it. Note that, if users
        //       want to alter the table location to a file path, we will fail. This should be fixed
        //       in the future.

        val newLocation = tableDefinition.storage.locationUri.map(CatalogUtils.URIToString(_))
        val storageWithPathOption = tableDefinition.storage.copy(
          properties = tableDefinition.storage.properties ++ newLocation.map("path" -> _))

        val oldLocation = getLocationFromStorageProps(oldTableDef)
        if (oldLocation == newLocation) {
          storageWithPathOption.copy(locationUri = oldTableDef.storage.locationUri)
        } else {
          storageWithPathOption
        }
      }

      val partitionProviderProp = if (tableDefinition.tracksPartitionsInCatalog) {
        TABLE_PARTITION_PROVIDER -> TABLE_PARTITION_PROVIDER_CATALOG
      } else {
        TABLE_PARTITION_PROVIDER -> TABLE_PARTITION_PROVIDER_FILESYSTEM
      }

      // Sets the `schema`, `partitionColumnNames` and `bucketSpec` from the old table definition,
      // to retain the spark specific format if it is. Also add old data source properties to table
      // properties, to retain the data source table format.
      val oldDataSourceProps = oldTableDef.properties.filter(_._1.startsWith(DATASOURCE_PREFIX))
      val newTableProps = oldDataSourceProps ++ withStatsProps.properties + partitionProviderProp
      val newDef = withStatsProps.copy(
        storage = newStorage,
        schema = oldTableDef.schema,
        partitionColumnNames = oldTableDef.partitionColumnNames,
        bucketSpec = oldTableDef.bucketSpec,
        properties = newTableProps)

      client.alterTable(newDef)
    }
  }

  override def alterTableDataSchema(
      db: String, table: String, newDataSchema: StructType): Unit = withClient {
    requireTableExists(db, table)
    val oldTable = getTable(db, table)
    verifyDataSchema(oldTable.identifier, oldTable.tableType, newDataSchema)
    val schemaProps =
      tableMetaToTableProps(oldTable, StructType(newDataSchema ++ oldTable.partitionSchema)).toMap

    if (isDatasourceTable(oldTable)) {
      // For data source tables, first try to write it with the schema set; if that does not work,
      // try again with updated properties and the partition schema. This is a simplified version of
      // what createDataSourceTable() does, and may leave the table in a state unreadable by Hive
      // (for example, the schema does not match the data source schema, or does not match the
      // storage descriptor).
      try {
        client.alterTableDataSchema(db, table, newDataSchema, schemaProps)
      } catch {
        case NonFatal(e) =>
          val warningMessage =
            s"Could not alter schema of table ${oldTable.identifier.quotedString} in a Hive " +
              "compatible way. Updating Hive metastore in Spark SQL specific format."
          logWarning(warningMessage, e)
          client.alterTableDataSchema(db, table, EMPTY_DATA_SCHEMA, schemaProps)
      }
    } else {
      client.alterTableDataSchema(db, table, newDataSchema, schemaProps)
    }
  }

  override def getTable(db: String, table: String): CatalogTable = withClient {
    restoreTableMetadata(getRawTable(db, table))
  }

  /**
   * Restores table metadata from the table properties. This method is kind of a opposite version
   * of [[createTable]].
   *
   * It reads table schema, provider, partition column names and bucket specification from table
   * properties, and filter out these special entries from table properties.
   */
  private def restoreTableMetadata(inputTable: CatalogTable): CatalogTable = {
    if (conf.get(DEBUG_MODE)) {
      return inputTable
    }

    var table = inputTable

    table.properties.get(DATASOURCE_PROVIDER) match {
      case None if table.tableType == VIEW =>
        // If this is a view created by Spark 2.2 or higher versions, we should restore its schema
        // from table properties.
        if (table.properties.contains(DATASOURCE_SCHEMA_NUMPARTS)) {
          table = table.copy(schema = getSchemaFromTableProperties(table))
        }

      // No provider in table properties, which means this is a Hive serde table.
      case None =>
        table = restoreHiveSerdeTable(table)

      // This is a regular data source table.
      case Some(provider) =>
        table = restoreDataSourceTable(table, provider)
    }

    // construct Spark's statistics from information in Hive metastore
    val statsProps = table.properties.filterKeys(_.startsWith(STATISTICS_PREFIX))

    if (statsProps.nonEmpty) {
      val colStats = new mutable.HashMap[String, ColumnStat]

      // For each column, recover its column stats. Note that this is currently a O(n^2) operation,
      // but given the number of columns it usually not enormous, this is probably OK as a start.
      // If we want to map this a linear operation, we'd need a stronger contract between the
      // naming convention used for serialization.
      table.schema.foreach { field =>
        if (statsProps.contains(columnStatKeyPropName(field.name, ColumnStat.KEY_VERSION))) {
          // If "version" field is defined, then the column stat is defined.
          val keyPrefix = columnStatKeyPropName(field.name, "")
          val colStatMap = statsProps.filterKeys(_.startsWith(keyPrefix)).map { case (k, v) =>
            (k.drop(keyPrefix.length), v)
          }

          ColumnStat.fromMap(table.identifier.table, field, colStatMap).foreach {
            colStat => colStats += field.name -> colStat
          }
        }
      }

      table = table.copy(
        stats = Some(CatalogStatistics(
          sizeInBytes = BigInt(table.properties(STATISTICS_TOTAL_SIZE)),
          rowCount = table.properties.get(STATISTICS_NUM_ROWS).map(BigInt(_)),
          colStats = colStats.toMap)))
    }

    // Get the original table properties as defined by the user.
    table.copy(
      properties = table.properties.filterNot { case (key, _) => key.startsWith(SPARK_SQL_PREFIX) })
  }

  // Reorder table schema to put partition columns at the end. Before Spark 2.2, the partition
  // columns are not put at the end of schema. We need to reorder it when reading the schema
  // from the table properties.
  private def reorderSchema(schema: StructType, partColumnNames: Seq[String]): StructType = {
    val partitionFields = partColumnNames.map { partCol =>
      schema.find(_.name == partCol).getOrElse {
        throw new AnalysisException("The metadata is corrupted. Unable to find the " +
          s"partition column names from the schema. schema: ${schema.catalogString}. " +
          s"Partition columns: ${partColumnNames.mkString("[", ", ", "]")}")
      }
    }
    StructType(schema.filterNot(partitionFields.contains) ++ partitionFields)
  }

  private def restoreHiveSerdeTable(table: CatalogTable): CatalogTable = {
    val hiveTable = table.copy(
      provider = Some(DDLUtils.HIVE_PROVIDER),
      tracksPartitionsInCatalog = true)

    // If this is a Hive serde table created by Spark 2.1 or higher versions, we should restore its
    // schema from table properties.
    if (table.properties.contains(DATASOURCE_SCHEMA_NUMPARTS)) {
      val schemaFromTableProps = getSchemaFromTableProperties(table)
      val partColumnNames = getPartitionColumnsFromTableProperties(table)
      val reorderedSchema = reorderSchema(schema = schemaFromTableProps, partColumnNames)

      if (DataType.equalsIgnoreCaseAndNullability(reorderedSchema, table.schema)) {
        hiveTable.copy(
          schema = reorderedSchema,
          partitionColumnNames = partColumnNames,
          bucketSpec = getBucketSpecFromTableProperties(table))
      } else {
        // Hive metastore may change the table schema, e.g. schema inference. If the table
        // schema we read back is different(ignore case and nullability) from the one in table
        // properties which was written when creating table, we should respect the table schema
        // from hive.
        logWarning(s"The table schema given by Hive metastore(${table.schema.simpleString}) is " +
          "different from the schema when this table was created by Spark SQL" +
          s"(${schemaFromTableProps.simpleString}). We have to fall back to the table schema " +
          "from Hive metastore which is not case preserving.")
        hiveTable.copy(schemaPreservesCase = false)
      }
    } else {
      hiveTable.copy(schemaPreservesCase = false)
    }
  }

  private def restoreDataSourceTable(table: CatalogTable, provider: String): CatalogTable = {
    // Internally we store the table location in storage properties with key "path" for data
    // source tables. Here we set the table location to `locationUri` field and filter out the
    // path option in storage properties, to avoid exposing this concept externally.
    val storageWithLocation = {
      val tableLocation = getLocationFromStorageProps(table)
      // We pass None as `newPath` here, to remove the path option in storage properties.
      updateLocationInStorageProps(table, newPath = None).copy(
        locationUri = tableLocation.map(CatalogUtils.stringToURI(_)))
    }
    val partitionProvider = table.properties.get(TABLE_PARTITION_PROVIDER)

    val schemaFromTableProps = getSchemaFromTableProperties(table)
    val partColumnNames = getPartitionColumnsFromTableProperties(table)
    val reorderedSchema = reorderSchema(schema = schemaFromTableProps, partColumnNames)

    table.copy(
      provider = Some(provider),
      storage = storageWithLocation,
      schema = reorderedSchema,
      partitionColumnNames = partColumnNames,
      bucketSpec = getBucketSpecFromTableProperties(table),
      tracksPartitionsInCatalog = partitionProvider == Some(TABLE_PARTITION_PROVIDER_CATALOG))
  }

  override def tableExists(db: String, table: String): Boolean = withClient {
    client.tableExists(db, table)
  }

  override def listTables(db: String): Seq[String] = withClient {
    requireDbExists(db)
    client.listTables(db)
  }

  override def listTables(db: String, pattern: String): Seq[String] = withClient {
    requireDbExists(db)
    client.listTables(db, pattern)
  }

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = withClient {
    requireTableExists(db, table)
    client.loadTable(
      loadPath,
      s"$db.$table",
      isOverwrite,
      isSrcLocal)
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = withClient {
    requireTableExists(db, table)

    val orderedPartitionSpec = new util.LinkedHashMap[String, String]()
    getTable(db, table).partitionColumnNames.foreach { colName =>
      // Hive metastore is not case preserving and keeps partition columns with lower cased names,
      // and Hive will validate the column names in partition spec to make sure they are partition
      // columns. Here we Lowercase the column names before passing the partition spec to Hive
      // client, to satisfy Hive.
      orderedPartitionSpec.put(colName.toLowerCase, partition(colName))
    }

    client.loadPartition(
      loadPath,
      db,
      table,
      orderedPartitionSpec,
      isOverwrite,
      inheritTableSpecs,
      isSrcLocal)
  }

  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit = withClient {
    requireTableExists(db, table)

    val orderedPartitionSpec = new util.LinkedHashMap[String, String]()
    getTable(db, table).partitionColumnNames.foreach { colName =>
      // Hive metastore is not case preserving and keeps partition columns with lower cased names,
      // and Hive will validate the column names in partition spec to make sure they are partition
      // columns. Here we Lowercase the column names before passing the partition spec to Hive
      // client, to satisfy Hive.
      orderedPartitionSpec.put(colName.toLowerCase, partition(colName))
    }

    client.loadDynamicPartitions(
      loadPath,
      db,
      table,
      orderedPartitionSpec,
      replace,
      numDP)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  // Hive metastore is not case preserving and the partition columns are always lower cased. We need
  // to lower case the column names in partition specification before calling partition related Hive
  // APIs, to match this behaviour.
  private def lowerCasePartitionSpec(spec: TablePartitionSpec): TablePartitionSpec = {
    spec.map { case (k, v) => k.toLowerCase -> v }
  }

  // Build a map from lower-cased partition column names to exact column names for a given table
  private def buildLowerCasePartColNameMap(table: CatalogTable): Map[String, String] = {
    val actualPartColNames = table.partitionColumnNames
    actualPartColNames.map(colName => (colName.toLowerCase, colName)).toMap
  }

  // Hive metastore is not case preserving and the column names of the partition specification we
  // get from the metastore are always lower cased. We should restore them w.r.t. the actual table
  // partition columns.
  private def restorePartitionSpec(
      spec: TablePartitionSpec,
      partColMap: Map[String, String]): TablePartitionSpec = {
    spec.map { case (k, v) => partColMap(k.toLowerCase) -> v }
  }

  private def restorePartitionSpec(
      spec: TablePartitionSpec,
      partCols: Seq[String]): TablePartitionSpec = {
    spec.map { case (k, v) => partCols.find(_.equalsIgnoreCase(k)).get -> v }
  }

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = withClient {
    requireTableExists(db, table)

    val tableMeta = getTable(db, table)
    val partitionColumnNames = tableMeta.partitionColumnNames
    val tablePath = new Path(tableMeta.location)
    val partsWithLocation = parts.map { p =>
      // Ideally we can leave the partition location empty and let Hive metastore to set it.
      // However, Hive metastore is not case preserving and will generate wrong partition location
      // with lower cased partition column names. Here we set the default partition location
      // manually to avoid this problem.
      val partitionPath = p.storage.locationUri.map(uri => new Path(uri)).getOrElse {
        ExternalCatalogUtils.generatePartitionPath(p.spec, partitionColumnNames, tablePath)
      }
      p.copy(storage = p.storage.copy(locationUri = Some(partitionPath.toUri)))
    }
    val lowerCasedParts = partsWithLocation.map(p => p.copy(spec = lowerCasePartitionSpec(p.spec)))
    client.createPartitions(db, table, lowerCasedParts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = withClient {
    requireTableExists(db, table)
    client.dropPartitions(
      db, table, parts.map(lowerCasePartitionSpec), ignoreIfNotExists, purge, retainData)
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = withClient {
    client.renamePartitions(
      db, table, specs.map(lowerCasePartitionSpec), newSpecs.map(lowerCasePartitionSpec))

    val tableMeta = getTable(db, table)
    val partitionColumnNames = tableMeta.partitionColumnNames
    // Hive metastore is not case preserving and keeps partition columns with lower cased names.
    // When Hive rename partition for managed tables, it will create the partition location with
    // a default path generate by the new spec with lower cased partition column names. This is
    // unexpected and we need to rename them manually and alter the partition location.
    val hasUpperCasePartitionColumn = partitionColumnNames.exists(col => col.toLowerCase != col)
    if (tableMeta.tableType == MANAGED && hasUpperCasePartitionColumn) {
      val tablePath = new Path(tableMeta.location)
      val fs = tablePath.getFileSystem(hadoopConf)
      val newParts = newSpecs.map { spec =>
        val rightPath = renamePartitionDirectory(fs, tablePath, partitionColumnNames, spec)
        val partition = client.getPartition(db, table, lowerCasePartitionSpec(spec))
        partition.copy(storage = partition.storage.copy(locationUri = Some(rightPath.toUri)))
      }
      alterPartitions(db, table, newParts)
    }
  }

  /**
   * Rename the partition directory w.r.t. the actual partition columns.
   *
   * It will recursively rename the partition directory from the first partition column, to be most
   * compatible with different file systems. e.g. in some file systems, renaming `a=1/b=2` to
   * `A=1/B=2` will result to `a=1/B=2`, while in some other file systems, the renaming works, but
   * will leave an empty directory `a=1`.
   */
  private def renamePartitionDirectory(
      fs: FileSystem,
      tablePath: Path,
      partCols: Seq[String],
      newSpec: TablePartitionSpec): Path = {
    import ExternalCatalogUtils.getPartitionPathString

    var currentFullPath = tablePath
    partCols.foreach { col =>
      val partValue = newSpec(col)
      val expectedPartitionString = getPartitionPathString(col, partValue)
      val expectedPartitionPath = new Path(currentFullPath, expectedPartitionString)

      if (fs.exists(expectedPartitionPath)) {
        // It is possible that some parental partition directories already exist or doesn't need to
        // be renamed. e.g. the partition columns are `a` and `B`, then we don't need to rename
        // `/table_path/a=1`. Or we already have a partition directory `A=1/B=2`, and we rename
        // another partition to `A=1/B=3`, then we will have `A=1/B=2` and `a=1/b=3`, and we should
        // just move `a=1/b=3` into `A=1` with new name `B=3`.
      } else {
        val actualPartitionString = getPartitionPathString(col.toLowerCase, partValue)
        val actualPartitionPath = new Path(currentFullPath, actualPartitionString)
        try {
          fs.rename(actualPartitionPath, expectedPartitionPath)
        } catch {
          case e: IOException =>
            throw new SparkException("Unable to rename partition path from " +
              s"$actualPartitionPath to $expectedPartitionPath", e)
        }
      }
      currentFullPath = expectedPartitionPath
    }

    currentFullPath
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withClient {
    val lowerCasedParts = newParts.map(p => p.copy(spec = lowerCasePartitionSpec(p.spec)))
    // Note: Before altering table partitions in Hive, you *must* set the current database
    // to the one that contains the table of interest. Otherwise you will end up with the
    // most helpful error message ever: "Unable to alter partition. alter is not possible."
    // See HIVE-2742 for more detail.
    client.setCurrentDatabase(db)
    client.alterPartitions(db, table, lowerCasedParts)
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = withClient {
    val part = client.getPartition(db, table, lowerCasePartitionSpec(spec))
    part.copy(spec = restorePartitionSpec(part.spec, getTable(db, table).partitionColumnNames))
  }

  /**
   * Returns the specified partition or None if it does not exist.
   */
  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = withClient {
    client.getPartitionOption(db, table, lowerCasePartitionSpec(spec)).map { part =>
      part.copy(spec = restorePartitionSpec(part.spec, getTable(db, table).partitionColumnNames))
    }
  }

  /**
   * Returns the partition names from hive metastore for a given table in a database.
   */
  override def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = withClient {
    val catalogTable = getTable(db, table)
    val partColNameMap = buildLowerCasePartColNameMap(catalogTable).mapValues(escapePathName)
    val clientPartitionNames =
      client.getPartitionNames(catalogTable, partialSpec.map(lowerCasePartitionSpec))
    clientPartitionNames.map { partitionPath =>
      val partSpec = PartitioningUtils.parsePathFragmentAsSeq(partitionPath)
      partSpec.map { case (partName, partValue) =>
        partColNameMap(partName.toLowerCase) + "=" + escapePathName(partValue)
      }.mkString("/")
    }
  }

  /**
   * Returns the partitions from hive metastore for a given table in a database.
   */
  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = withClient {
    val partColNameMap = buildLowerCasePartColNameMap(getTable(db, table))
    val res = client.getPartitions(db, table, partialSpec.map(lowerCasePartitionSpec)).map { part =>
      part.copy(spec = restorePartitionSpec(part.spec, partColNameMap))
    }

    partialSpec match {
      // This might be a bug of Hive: When the partition value inside the partial partition spec
      // contains dot, and we ask Hive to list partitions w.r.t. the partial partition spec, Hive
      // treats dot as matching any single character and may return more partitions than we
      // expected. Here we do an extra filter to drop unexpected partitions.
      case Some(spec) if spec.exists(_._2.contains(".")) =>
        res.filter(p => isPartialPartitionSpec(spec, p.spec))
      case _ => res
    }
  }

  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = withClient {
    val rawTable = getRawTable(db, table)
    val catalogTable = restoreTableMetadata(rawTable)

    val partColNameMap = buildLowerCasePartColNameMap(catalogTable)

    val clientPrunedPartitions =
      client.getPartitionsByFilter(rawTable, predicates).map { part =>
        part.copy(spec = restorePartitionSpec(part.spec, partColNameMap))
      }
    prunePartitionsByFilter(catalogTable, clientPrunedPartitions, predicates, defaultTimeZoneId)
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override protected def doCreateFunction(
      db: String,
      funcDefinition: CatalogFunction): Unit = withClient {
    requireDbExists(db)
    // Hive's metastore is case insensitive. However, Hive's createFunction does
    // not normalize the function name (unlike the getFunction part). So,
    // we are normalizing the function name.
    val functionName = funcDefinition.identifier.funcName.toLowerCase(Locale.ROOT)
    requireFunctionNotExists(db, functionName)
    val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
    client.createFunction(db, funcDefinition.copy(identifier = functionIdentifier))
  }

  override protected def doDropFunction(db: String, name: String): Unit = withClient {
    requireFunctionExists(db, name)
    client.dropFunction(db, name)
  }

  override protected def doRenameFunction(
      db: String,
      oldName: String,
      newName: String): Unit = withClient {
    requireFunctionExists(db, oldName)
    requireFunctionNotExists(db, newName)
    client.renameFunction(db, oldName, newName)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = withClient {
    requireFunctionExists(db, funcName)
    client.getFunction(db, funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = withClient {
    requireDbExists(db)
    client.functionExists(db, funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = withClient {
    requireDbExists(db)
    client.listFunctions(db, pattern)
  }

}

object HiveExternalCatalog {
  val SPARK_SQL_PREFIX = "spark.sql."

  val DATASOURCE_PREFIX = SPARK_SQL_PREFIX + "sources."
  val DATASOURCE_PROVIDER = DATASOURCE_PREFIX + "provider"
  val DATASOURCE_SCHEMA = DATASOURCE_PREFIX + "schema"
  val DATASOURCE_SCHEMA_PREFIX = DATASOURCE_SCHEMA + "."
  val DATASOURCE_SCHEMA_NUMPARTS = DATASOURCE_SCHEMA_PREFIX + "numParts"
  val DATASOURCE_SCHEMA_NUMPARTCOLS = DATASOURCE_SCHEMA_PREFIX + "numPartCols"
  val DATASOURCE_SCHEMA_NUMSORTCOLS = DATASOURCE_SCHEMA_PREFIX + "numSortCols"
  val DATASOURCE_SCHEMA_NUMBUCKETS = DATASOURCE_SCHEMA_PREFIX + "numBuckets"
  val DATASOURCE_SCHEMA_NUMBUCKETCOLS = DATASOURCE_SCHEMA_PREFIX + "numBucketCols"
  val DATASOURCE_SCHEMA_PART_PREFIX = DATASOURCE_SCHEMA_PREFIX + "part."
  val DATASOURCE_SCHEMA_PARTCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "partCol."
  val DATASOURCE_SCHEMA_BUCKETCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "bucketCol."
  val DATASOURCE_SCHEMA_SORTCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "sortCol."

  val STATISTICS_PREFIX = SPARK_SQL_PREFIX + "statistics."
  val STATISTICS_TOTAL_SIZE = STATISTICS_PREFIX + "totalSize"
  val STATISTICS_NUM_ROWS = STATISTICS_PREFIX + "numRows"
  val STATISTICS_COL_STATS_PREFIX = STATISTICS_PREFIX + "colStats."

  val TABLE_PARTITION_PROVIDER = SPARK_SQL_PREFIX + "partitionProvider"
  val TABLE_PARTITION_PROVIDER_CATALOG = "catalog"
  val TABLE_PARTITION_PROVIDER_FILESYSTEM = "filesystem"

  // When storing data source tables in hive metastore, we need to set data schema to empty if the
  // schema is hive-incompatible. However we need a hack to preserve existing behavior. Before
  // Spark 2.0, we do not set a default serde here (this was done in Hive), and so if the user
  // provides an empty schema Hive would automatically populate the schema with a single field
  // "col". However, after SPARK-14388, we set the default serde to LazySimpleSerde so this
  // implicit behavior no longer happens. Therefore, we need to do it in Spark ourselves.
  val EMPTY_DATA_SCHEMA = new StructType()
    .add("col", "array<string>", nullable = true, comment = "from deserializer")

  /**
   * Returns the fully qualified name used in table properties for a particular column stat.
   * For example, for column "mycol", and "min" stat, this should return
   * "spark.sql.statistics.colStats.mycol.min".
   */
  private def columnStatKeyPropName(columnName: String, statKey: String): String = {
    STATISTICS_COL_STATS_PREFIX + columnName + "." + statKey
  }

  // A persisted data source table always store its schema in the catalog.
  private def getSchemaFromTableProperties(metadata: CatalogTable): StructType = {
    val errorMessage = "Could not read schema from the hive metastore because it is corrupted."
    val props = metadata.properties
    val schema = props.get(DATASOURCE_SCHEMA)
    if (schema.isDefined) {
      // Originally, we used `spark.sql.sources.schema` to store the schema of a data source table.
      // After SPARK-6024, we removed this flag.
      // Although we are not using `spark.sql.sources.schema` any more, we need to still support.
      DataType.fromJson(schema.get).asInstanceOf[StructType]
    } else if (props.filterKeys(_.startsWith(DATASOURCE_SCHEMA_PREFIX)).isEmpty) {
      // If there is no schema information in table properties, it means the schema of this table
      // was empty when saving into metastore, which is possible in older version(prior to 2.1) of
      // Spark. We should respect it.
      new StructType()
    } else {
      val numSchemaParts = props.get(DATASOURCE_SCHEMA_NUMPARTS)
      if (numSchemaParts.isDefined) {
        val parts = (0 until numSchemaParts.get.toInt).map { index =>
          val part = metadata.properties.get(s"$DATASOURCE_SCHEMA_PART_PREFIX$index").orNull
          if (part == null) {
            throw new AnalysisException(errorMessage +
              s" (missing part $index of the schema, ${numSchemaParts.get} parts are expected).")
          }
          part
        }
        // Stick all parts back to a single schema string.
        DataType.fromJson(parts.mkString).asInstanceOf[StructType]
      } else {
        throw new AnalysisException(errorMessage)
      }
    }
  }

  private def getColumnNamesByType(
      props: Map[String, String],
      colType: String,
      typeName: String): Seq[String] = {
    for {
      numCols <- props.get(s"spark.sql.sources.schema.num${colType.capitalize}Cols").toSeq
      index <- 0 until numCols.toInt
    } yield props.getOrElse(
      s"$DATASOURCE_SCHEMA_PREFIX${colType}Col.$index",
      throw new AnalysisException(
        s"Corrupted $typeName in catalog: $numCols parts expected, but part $index is missing."
      )
    )
  }

  private def getPartitionColumnsFromTableProperties(metadata: CatalogTable): Seq[String] = {
    getColumnNamesByType(metadata.properties, "part", "partitioning columns")
  }

  private def getBucketSpecFromTableProperties(metadata: CatalogTable): Option[BucketSpec] = {
    metadata.properties.get(DATASOURCE_SCHEMA_NUMBUCKETS).map { numBuckets =>
      BucketSpec(
        numBuckets.toInt,
        getColumnNamesByType(metadata.properties, "bucket", "bucketing columns"),
        getColumnNamesByType(metadata.properties, "sort", "sorting columns"))
    }
  }

  /**
   * Detects a data source table. This checks both the table provider and the table properties,
   * unlike DDLUtils which just checks the former.
   */
  private[spark] def isDatasourceTable(table: CatalogTable): Boolean = {
    val provider = table.provider.orElse(table.properties.get(DATASOURCE_PROVIDER))
    provider.isDefined && provider != Some(DDLUtils.HIVE_PROVIDER)
  }

}
