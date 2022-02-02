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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DDL_TIME
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT
import org.apache.thrift.TException

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{PartitioningUtils, SourceOptions}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.types.{AnsiIntervalType, ArrayType, DataType, MapType, StructType}

/**
 * A persistent implementation of the system catalog using Hive.
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class HiveExternalCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends ExternalCatalog with Logging {

  import CatalogTypes.TablePartitionSpec
  import HiveExternalCatalog._
  import CatalogTableType._

  // SPARK-32256: Make sure `VersionInfo` is initialized before touching the isolated classloader.
  // This is to ensure Hive can get the Hadoop version when using the isolated classloader.
  org.apache.hadoop.util.VersionInfo.getVersion()

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
   * Get the raw table metadata from hive metastore directly. The raw table metadata may contain
   * special data source properties that should not be exposed outside of `HiveExternalCatalog`. We
   * should interpret these special data source properties and restore the original table metadata
   * before returning it.
   */
  private[hive] def getRawTable(db: String, table: String): CatalogTable = {
    client.getTable(db, table)
  }

  private[hive] def getRawTablesByNames(db: String, tables: Seq[String]): Seq[CatalogTable] = {
    client.getTablesByName(db, tables)
  }

  /**
   * If the given table properties contains datasource properties, throw an exception. We will do
   * this check when create or alter a table, i.e. when we try to write table metadata to Hive
   * metastore.
   */
  private def verifyTableProperties(table: CatalogTable): Unit = {
    val invalidKeys = table.properties.keys.filter(_.startsWith(SPARK_SQL_PREFIX))
    if (invalidKeys.nonEmpty) {
      throw new AnalysisException(s"Cannot persist ${table.qualifiedName} into Hive metastore " +
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
   * Checks the validity of data column names. Hive metastore disallows the table to use some
   * special characters (',', ':', and ';') in data column names, including nested column names.
   * Partition columns do not have such a restriction. Views do not have such a restriction.
   */
  private def verifyDataSchema(
      tableName: TableIdentifier, tableType: CatalogTableType, dataSchema: StructType): Unit = {
    if (tableType != VIEW) {
      val invalidChars = Seq(",", ":", ";")
      def verifyNestedColumnNames(schema: StructType): Unit = schema.foreach { f =>
        f.dataType match {
          case st: StructType => verifyNestedColumnNames(st)
          case _ if invalidChars.exists(f.name.contains) =>
            val invalidCharsString = invalidChars.map(c => s"'$c'").mkString(", ")
            val errMsg = "Cannot create a table having a nested column whose name contains " +
              s"invalid characters ($invalidCharsString) in Hive metastore. Table: $tableName; " +
              s"Column: ${f.name}"
            throw new AnalysisException(errMsg)
          case _ =>
        }
      }

      dataSchema.foreach { f =>
        f.dataType match {
          // Checks top-level column names
          case _ if f.name.contains(",") =>
            throw new AnalysisException("Cannot create a table having a column whose name " +
              s"contains commas in Hive metastore. Table: $tableName; Column: ${f.name}")
          // Checks nested column names
          case st: StructType =>
            verifyNestedColumnNames(st)
          case _ =>
        }
      }
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(
      dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withClient {
    client.createDatabase(dbDefinition, ignoreIfExists)
  }

  override def dropDatabase(
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

  override def createTable(
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
    val options = new SourceOptions(table.storage.properties)

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

      table.copy(
        storage = table.storage.copy(
          locationUri = location,
          inputFormat = serde.inputFormat,
          outputFormat = serde.outputFormat,
          serde = serde.serde,
          properties = storagePropsWithLocation
        ),
        properties = table.properties ++ tableProperties)
    }

    val qualifiedTableName = table.identifier.quotedString
    val maybeSerde = HiveSerDe.sourceToSerDe(provider)
    val incompatibleTypes =
      table.schema.filter(f => !isHiveCompatibleDataType(f.dataType)).map(_.dataType.simpleString)

    val (hiveCompatibleTable, logMessage) = maybeSerde match {
      case _ if options.skipHiveMetadata =>
        val message =
          s"Persisting data source table $qualifiedTableName into Hive metastore in" +
            "Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)

      case _ if incompatibleTypes.nonEmpty =>
        val message =
          s"Hive incompatible types found: ${incompatibleTypes.mkString(", ")}. " +
            s"Persisting data source table $qualifiedTableName into Hive metastore in " +
            "Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)
      // our bucketing is un-compatible with hive(different hash function)
      case Some(serde) if table.bucketSpec.nonEmpty =>
        val message =
          s"Persisting bucketed data source table $qualifiedTableName into " +
            "Hive metastore in Spark SQL specific format, which is NOT compatible with " +
            "Hive bucketed table. But Hive can read this table as a non-bucketed table."
        (Some(newHiveCompatibleMetastoreTable(serde)), message)

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

    properties.put(CREATED_SPARK_VERSION, table.createVersion)
    // This is for backward compatibility to Spark 2 to read tables with char/varchar created by
    // Spark 3.1. At read side, we will restore a table schema from its properties. So, we need to
    // clear the `varchar(n)` and `char(n)` and replace them with `string` as Spark 2 does not have
    // a type mapping for them in `DataType.nameToType`.
    // See `restoreHiveSerdeTable` for example.
    val newSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
    CatalogTable.splitLargeTableProp(
      DATASOURCE_SCHEMA,
      newSchema.json,
      properties.put,
      conf.get(SCHEMA_STRING_LENGTH_THRESHOLD))

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

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withClient {
    requireDbExists(db)
    client.dropTable(db, table, ignoreIfNotExists, purge)
  }

  override def renameTable(
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

    client.alterTable(db, oldName, newTable)
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
   * assuming the table exists. This method does not change the properties for data source and
   * statistics.
   *
   * Note: As of now, this doesn't support altering table schema, partition column names and bucket
   * specification. We will ignore them even if users do specify different values for these fields.
   */
  override def alterTable(tableDefinition: CatalogTable): Unit = withClient {
    assert(tableDefinition.identifier.database.isDefined)
    val db = tableDefinition.identifier.database.get
    requireTableExists(db, tableDefinition.identifier.table)
    verifyTableProperties(tableDefinition)

    if (tableDefinition.tableType == VIEW) {
      val newTableProps = tableDefinition.properties ++ tableMetaToTableProps(tableDefinition).toMap
      client.alterTable(tableDefinition.copy(properties = newTableProps))
    } else {
      val oldTableDef = getRawTable(db, tableDefinition.identifier.table)

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

      // Add old data source properties to table properties, to retain the data source table format.
      // Add old stats properties to table properties, to retain spark's stats.
      // Set the `schema`, `partitionColumnNames` and `bucketSpec` from the old table definition,
      // to retain the spark specific format if it is.
      val propsFromOldTable = oldTableDef.properties.filter { case (k, v) =>
        k.startsWith(DATASOURCE_PREFIX) || k.startsWith(STATISTICS_PREFIX) ||
          k.startsWith(CREATED_SPARK_VERSION)
      }
      val newFormatIfExists = tableDefinition.provider.flatMap { p =>
        if (DDLUtils.isDatasourceTable(tableDefinition)) {
          Some(DATASOURCE_PROVIDER -> p)
        } else {
          None
        }
      }
      val newTableProps =
        propsFromOldTable ++ tableDefinition.properties + partitionProviderProp ++ newFormatIfExists

      // // Add old table's owner if we need to restore
      val owner = Option(tableDefinition.owner).filter(_.nonEmpty).getOrElse(oldTableDef.owner)
      val newDef = tableDefinition.copy(
        storage = newStorage,
        schema = oldTableDef.schema,
        partitionColumnNames = oldTableDef.partitionColumnNames,
        bucketSpec = oldTableDef.bucketSpec,
        properties = newTableProps,
        owner = owner)

      client.alterTable(newDef)
    }
  }

  /**
   * Alter the data schema of a table identified by the provided database and table name. The new
   * data schema should not have conflict column names with the existing partition columns, and
   * should still contain all the existing data columns.
   */
  override def alterTableDataSchema(
      db: String,
      table: String,
      newDataSchema: StructType): Unit = withClient {
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

  /** Alter the statistics of a table. If `stats` is None, then remove all existing statistics. */
  override def alterTableStats(
      db: String,
      table: String,
      stats: Option[CatalogStatistics]): Unit = withClient {
    requireTableExists(db, table)
    val rawTable = getRawTable(db, table)

    // convert table statistics to properties so that we can persist them through hive client
    val statsProperties =
      if (stats.isDefined) {
        statsToProperties(stats.get)
      } else {
        new mutable.HashMap[String, String]()
      }

    val oldTableNonStatsProps = rawTable.properties.filterNot(_._1.startsWith(STATISTICS_PREFIX))
    val updatedTable = rawTable.copy(properties = oldTableNonStatsProps ++ statsProperties)
    client.alterTable(updatedTable)
  }

  override def getTable(db: String, table: String): CatalogTable = withClient {
    restoreTableMetadata(getRawTable(db, table))
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = withClient {
    getRawTablesByNames(db, tables).map(restoreTableMetadata)
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
        getSchemaFromTableProperties(table.properties).foreach { schemaFromTableProps =>
          table = table.copy(schema = schemaFromTableProps)
        }

      // No provider in table properties, which means this is a Hive serde table.
      case None =>
        table = restoreHiveSerdeTable(table)

      // This is a regular data source table.
      case Some(provider) =>
        table = restoreDataSourceTable(table, provider)
    }

    // Restore version info
    val version: String = table.properties.getOrElse(CREATED_SPARK_VERSION, "2.2 or prior")

    // Restore Spark's statistics from information in Metastore.
    val restoredStats =
      statsFromProperties(table.properties, table.identifier.table, table.schema)
    if (restoredStats.isDefined) {
      table = table.copy(stats = restoredStats)
    }

    // Get the original table properties as defined by the user.
    table.copy(
      createVersion = version,
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
    val options = new SourceOptions(table.storage.properties)
    val hiveTable = table.copy(
      provider = Some(DDLUtils.HIVE_PROVIDER),
      tracksPartitionsInCatalog = true)

    // If this is a Hive serde table created by Spark 2.1 or higher versions, we should restore its
    // schema from table properties.
    val maybeSchemaFromTableProps = getSchemaFromTableProperties(table.properties)
    if (maybeSchemaFromTableProps.isDefined) {
      val schemaFromTableProps = maybeSchemaFromTableProps.get
      val partColumnNames = getPartitionColumnsFromTableProperties(table)
      val reorderedSchema = reorderSchema(schema = schemaFromTableProps, partColumnNames)

      if (DataType.equalsIgnoreCaseAndNullability(reorderedSchema, table.schema) ||
          options.respectSparkSchema) {
        hiveTable.copy(
          schema = reorderedSchema,
          partitionColumnNames = partColumnNames,
          bucketSpec = getBucketSpecFromTableProperties(table))
      } else {
        // Hive metastore may change the table schema, e.g. schema inference. If the table
        // schema we read back is different(ignore case and nullability) from the one in table
        // properties which was written when creating table, we should respect the table schema
        // from hive.
        logWarning(s"The table schema given by Hive metastore(${table.schema.catalogString}) is " +
          "different from the schema when this table was created by Spark SQL" +
          s"(${schemaFromTableProps.catalogString}). We have to fall back to the table schema " +
          "from Hive metastore which is not case preserving.")
        hiveTable.copy(schemaPreservesCase = false)
      }
    } else {
      hiveTable.copy(schemaPreservesCase = false)
    }
  }

  private def getSchemaFromTableProperties(
      tableProperties: Map[String, String]): Option[StructType] = {
    CatalogTable.readLargeTableProp(tableProperties, DATASOURCE_SCHEMA).map { schemaJson =>
      val parsed = DataType.fromJson(schemaJson).asInstanceOf[StructType]
      CharVarcharUtils.getRawSchema(parsed)
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
    val storageWithoutHiveGeneratedProperties = storageWithLocation.copy(properties =
      storageWithLocation.properties.filterKeys(!HIVE_GENERATED_STORAGE_PROPERTIES(_)).toMap)
    val partitionProvider = table.properties.get(TABLE_PARTITION_PROVIDER)

    val schemaFromTableProps =
      getSchemaFromTableProperties(table.properties).getOrElse(new StructType())
    val partColumnNames = getPartitionColumnsFromTableProperties(table)
    val reorderedSchema = reorderSchema(schema = schemaFromTableProps, partColumnNames)

    table.copy(
      provider = Some(provider),
      storage = storageWithoutHiveGeneratedProperties,
      schema = reorderedSchema,
      partitionColumnNames = partColumnNames,
      bucketSpec = getBucketSpecFromTableProperties(table),
      tracksPartitionsInCatalog = partitionProvider == Some(TABLE_PARTITION_PROVIDER_CATALOG),
      properties = table.properties.filterKeys(!HIVE_GENERATED_TABLE_PROPERTIES(_)).toMap)
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

  override def listViews(db: String, pattern: String): Seq[String] = withClient {
    requireDbExists(db)
    client.listTablesByType(db, pattern, CatalogTableType.VIEW)
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
      // scalastyle:off caselocale
      orderedPartitionSpec.put(colName.toLowerCase, partition(colName))
      // scalastyle:on caselocale
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
      // scalastyle:off caselocale
      orderedPartitionSpec.put(colName.toLowerCase, partition(colName))
      // scalastyle:on caselocale
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
  private def toMetaStorePartitionSpec(spec: TablePartitionSpec): TablePartitionSpec = {
    // scalastyle:off caselocale
    val lowNames = spec.map { case (k, v) => k.toLowerCase -> v }
    ExternalCatalogUtils.convertNullPartitionValues(lowNames)
    // scalastyle:on caselocale
  }

  // Build a map from lower-cased partition column names to exact column names for a given table
  private def buildLowerCasePartColNameMap(table: CatalogTable): Map[String, String] = {
    val actualPartColNames = table.partitionColumnNames
    // scalastyle:off caselocale
    actualPartColNames.map(colName => (colName.toLowerCase, colName)).toMap
    // scalastyle:on caselocale
  }

  // Hive metastore is not case preserving and the column names of the partition specification we
  // get from the metastore are always lower cased. We should restore them w.r.t. the actual table
  // partition columns.
  private def restorePartitionSpec(
      spec: TablePartitionSpec,
      partColMap: Map[String, String]): TablePartitionSpec = {
    // scalastyle:off caselocale
    spec.map { case (k, v) => partColMap(k.toLowerCase) -> v }
    // scalastyle:on caselocale
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
    val metaStoreParts = partsWithLocation
      .map(p => p.copy(spec = toMetaStorePartitionSpec(p.spec)))
    client.createPartitions(db, table, metaStoreParts, ignoreIfExists)
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
      db, table, parts.map(toMetaStorePartitionSpec), ignoreIfNotExists, purge, retainData)
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = withClient {
    client.renamePartitions(
      db, table, specs.map(toMetaStorePartitionSpec), newSpecs.map(toMetaStorePartitionSpec))

    val tableMeta = getTable(db, table)
    val partitionColumnNames = tableMeta.partitionColumnNames
    // Hive metastore is not case preserving and keeps partition columns with lower cased names.
    // When Hive rename partition for managed tables, it will create the partition location with
    // a default path generate by the new spec with lower cased partition column names. This is
    // unexpected and we need to rename them manually and alter the partition location.
    // scalastyle:off caselocale
    val hasUpperCasePartitionColumn = partitionColumnNames.exists(col => col.toLowerCase != col)
    // scalastyle:on caselocale
    if (tableMeta.tableType == MANAGED && hasUpperCasePartitionColumn) {
      val tablePath = new Path(tableMeta.location)
      val fs = tablePath.getFileSystem(hadoopConf)
      val newParts = newSpecs.map { spec =>
        val rightPath = renamePartitionDirectory(fs, tablePath, partitionColumnNames, spec)
        val partition = client.getPartition(db, table, toMetaStorePartitionSpec(spec))
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
        // scalastyle:off caselocale
        val actualPartitionString = getPartitionPathString(col.toLowerCase, partValue)
        // scalastyle:on caselocale
        val actualPartitionPath = new Path(currentFullPath, actualPartitionString)
        try {
          fs.mkdirs(expectedPartitionPath)
          if(!fs.rename(actualPartitionPath, expectedPartitionPath)) {
            throw new IOException(s"Renaming partition path from $actualPartitionPath to " +
              s"$expectedPartitionPath returned false")
          }
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

  private def statsToProperties(stats: CatalogStatistics): Map[String, String] = {

    val statsProperties = new mutable.HashMap[String, String]()
    statsProperties += STATISTICS_TOTAL_SIZE -> stats.sizeInBytes.toString()
    if (stats.rowCount.isDefined) {
      statsProperties += STATISTICS_NUM_ROWS -> stats.rowCount.get.toString()
    }

    stats.colStats.foreach { case (colName, colStat) =>
      colStat.toMap(colName).foreach { case (k, v) =>
        // Fully qualified name used in table properties for a particular column stat.
        // For example, for column "mycol", and "min" stat, this should return
        // "spark.sql.statistics.colStats.mycol.min".
        statsProperties += (STATISTICS_COL_STATS_PREFIX + k -> v)
      }
    }

    statsProperties.toMap
  }

  private def statsFromProperties(
      properties: Map[String, String],
      table: String,
      schema: StructType): Option[CatalogStatistics] = {

    val statsProps = properties.filterKeys(_.startsWith(STATISTICS_PREFIX))
    if (statsProps.isEmpty) {
      None
    } else {
      val colStats = new mutable.HashMap[String, CatalogColumnStat]
      val colStatsProps = properties.filterKeys(_.startsWith(STATISTICS_COL_STATS_PREFIX)).map {
        case (k, v) => k.drop(STATISTICS_COL_STATS_PREFIX.length) -> v
      }.toMap

      // Find all the column names by matching the KEY_VERSION properties for them.
      colStatsProps.keys.filter {
        k => k.endsWith(CatalogColumnStat.KEY_VERSION)
      }.map { k =>
        k.dropRight(CatalogColumnStat.KEY_VERSION.length + 1)
      }.foreach { fieldName =>
        // and for each, create a column stat.
        CatalogColumnStat.fromMap(table, fieldName, colStatsProps).foreach { cs =>
          colStats += fieldName -> cs
        }
      }

      Some(CatalogStatistics(
        sizeInBytes = BigInt(statsProps(STATISTICS_TOTAL_SIZE)),
        rowCount = statsProps.get(STATISTICS_NUM_ROWS).map(BigInt(_)),
        colStats = colStats.toMap))
    }
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withClient {
    val metaStoreParts = newParts.map(p => p.copy(spec = toMetaStorePartitionSpec(p.spec)))
    // convert partition statistics to properties so that we can persist them through hive api
    val withStatsProps = metaStoreParts.map { p =>
      if (p.stats.isDefined) {
        val statsProperties = statsToProperties(p.stats.get)
        p.copy(parameters = p.parameters ++ statsProperties)
      } else {
        p
      }
    }

    client.alterPartitions(db, table, withStatsProps)
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = withClient {
    val part = client.getPartition(db, table, toMetaStorePartitionSpec(spec))
    restorePartitionMetadata(part, getTable(db, table))
  }

  /**
   * Restores partition metadata from the partition properties.
   *
   * Reads partition-level statistics from partition properties, puts these
   * into [[CatalogTablePartition#stats]] and removes these special entries
   * from the partition properties.
   */
  private def restorePartitionMetadata(
      partition: CatalogTablePartition,
      table: CatalogTable): CatalogTablePartition = {
    val restoredSpec = restorePartitionSpec(partition.spec, table.partitionColumnNames)

    // Restore Spark's statistics from information in Metastore.
    // Note: partition-level statistics were introduced in 2.3.
    val restoredStats =
      statsFromProperties(partition.parameters, table.identifier.table, table.schema)
    if (restoredStats.isDefined) {
      partition.copy(
        spec = restoredSpec,
        stats = restoredStats,
        parameters = partition.parameters.filterNot {
          case (key, _) => key.startsWith(SPARK_SQL_PREFIX) })
    } else {
      partition.copy(spec = restoredSpec)
    }
  }

  /**
   * Returns the specified partition or None if it does not exist.
   */
  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = withClient {
    client.getPartitionOption(db, table, toMetaStorePartitionSpec(spec)).map { part =>
      restorePartitionMetadata(part, getTable(db, table))
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
      client.getPartitionNames(catalogTable, partialSpec.map(toMetaStorePartitionSpec))
    clientPartitionNames.map { partitionPath =>
      val partSpec = PartitioningUtils.parsePathFragmentAsSeq(partitionPath)
      partSpec.map { case (partName, partValue) =>
        // scalastyle:off caselocale
        partColNameMap(partName.toLowerCase) + "=" + escapePathName(partValue)
        // scalastyle:on caselocale
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
    val metaStoreSpec = partialSpec.map(toMetaStorePartitionSpec)
    val res = client.getPartitions(db, table, metaStoreSpec)
      .map { part => part.copy(spec = restorePartitionSpec(part.spec, partColNameMap))
    }

    metaStoreSpec match {
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

  override def createFunction(
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

  override def dropFunction(db: String, name: String): Unit = withClient {
    requireFunctionExists(db, name)
    client.dropFunction(db, name)
  }

  override def alterFunction(
      db: String, funcDefinition: CatalogFunction): Unit = withClient {
    requireDbExists(db)
    val functionName = funcDefinition.identifier.funcName.toLowerCase(Locale.ROOT)
    requireFunctionExists(db, functionName)
    val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
    client.alterFunction(db, funcDefinition.copy(identifier = functionIdentifier))
  }

  override def renameFunction(
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

  val CREATED_SPARK_VERSION = SPARK_SQL_PREFIX + "create.version"

  val HIVE_GENERATED_TABLE_PROPERTIES = Set(DDL_TIME)
  val HIVE_GENERATED_STORAGE_PROPERTIES = Set(SERIALIZATION_FORMAT)

  // When storing data source tables in hive metastore, we need to set data schema to empty if the
  // schema is hive-incompatible. However we need a hack to preserve existing behavior. Before
  // Spark 2.0, we do not set a default serde here (this was done in Hive), and so if the user
  // provides an empty schema Hive would automatically populate the schema with a single field
  // "col". However, after SPARK-14388, we set the default serde to LazySimpleSerde so this
  // implicit behavior no longer happens. Therefore, we need to do it in Spark ourselves.
  val EMPTY_DATA_SCHEMA = new StructType()
    .add("col", "array<string>", nullable = true, comment = "from deserializer")

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

  private[spark] def isHiveCompatibleDataType(dt: DataType): Boolean = dt match {
    case _: AnsiIntervalType => false
    case s: StructType => s.forall(f => isHiveCompatibleDataType(f.dataType))
    case a: ArrayType => isHiveCompatibleDataType(a.elementType)
    case m: MapType =>
      isHiveCompatibleDataType(m.keyType) && isHiveCompatibleDataType(m.valueType)
    case _ => true
  }
}
