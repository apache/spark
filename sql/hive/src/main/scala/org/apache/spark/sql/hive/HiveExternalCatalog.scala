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

import java.util

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.thrift.TException

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics}
import org.apache.spark.sql.execution.command.{ColumnStatStruct, DDLUtils}
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
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
  val client: HiveClient = {
    HiveUtils.newClientForMetadata(conf, hadoopConf)
  }

  // Exceptions thrown by the hive client that we would like to wrap
  private val clientExceptions = Set(
    classOf[HiveException].getCanonicalName,
    classOf[TException].getCanonicalName)

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
      case NonFatal(e) if isClientException(e) =>
        throw new AnalysisException(
          e.getClass.getCanonicalName + ": " + e.getMessage, cause = Some(e))
    }
  }

  private def requireTableExists(db: String, table: String): Unit = {
    withClient { getTable(db, table) }
  }

  /**
   * If the given table properties contains datasource properties, throw an exception. We will do
   * this check when create or alter a table, i.e. when we try to write table metadata to Hive
   * metastore.
   */
  private def verifyTableProperties(table: CatalogTable): Unit = {
    val invalidKeys = table.properties.keys.filter { key =>
      key.startsWith(DATASOURCE_PREFIX) || key.startsWith(STATISTICS_PREFIX)
    }
    if (invalidKeys.nonEmpty) {
      throw new AnalysisException(s"Cannot persistent ${table.qualifiedName} into hive metastore " +
        s"as table property keys may not start with '$DATASOURCE_PREFIX' or '$STATISTICS_PREFIX':" +
        s" ${invalidKeys.mkString("[", ", ", "]")}")
    }
    // External users are not allowed to set/switch the table type. In Hive metastore, the table
    // type can be switched by changing the value of a case-sensitive table property `EXTERNAL`.
    if (table.properties.contains("EXTERNAL")) {
      throw new AnalysisException("Cannot set or change the preserved property key: 'EXTERNAL'")
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
    client.getDatabaseOption(db).isDefined
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

    if (tableExists(db, table) && !ignoreIfExists) {
      throw new TableAlreadyExistsException(db = db, table = table)
    }
    // Before saving data source table metadata into Hive metastore, we should:
    //  1. Put table schema, partition column names and bucket specification in table properties.
    //  2. Check if this table is hive compatible
    //    2.1  If it's not hive compatible, set schema, partition columns and bucket spec to empty
    //         and save table metadata to Hive.
    //    2.1  If it's hive compatible, set serde information in table metadata and try to save
    //         it to Hive. If it fails, treat it as not hive compatible and go back to 2.1
    if (DDLUtils.isDatasourceTable(tableDefinition)) {
      // data source table always have a provider, it's guaranteed by `DDLUtils.isDatasourceTable`.
      val provider = tableDefinition.provider.get
      val partitionColumns = tableDefinition.partitionColumnNames
      val bucketSpec = tableDefinition.bucketSpec

      val tableProperties = new scala.collection.mutable.HashMap[String, String]
      tableProperties.put(DATASOURCE_PROVIDER, provider)

      // Serialized JSON schema string may be too long to be stored into a single metastore table
      // property. In this case, we split the JSON string and store each part as a separate table
      // property.
      val threshold = conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
      val schemaJsonString = tableDefinition.schema.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tableProperties.put(DATASOURCE_SCHEMA_NUMPARTS, parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tableProperties.put(s"$DATASOURCE_SCHEMA_PART_PREFIX$index", part)
      }

      if (partitionColumns.nonEmpty) {
        tableProperties.put(DATASOURCE_SCHEMA_NUMPARTCOLS, partitionColumns.length.toString)
        partitionColumns.zipWithIndex.foreach { case (partCol, index) =>
          tableProperties.put(s"$DATASOURCE_SCHEMA_PARTCOL_PREFIX$index", partCol)
        }
      }

      if (bucketSpec.isDefined) {
        val BucketSpec(numBuckets, bucketColumnNames, sortColumnNames) = bucketSpec.get

        tableProperties.put(DATASOURCE_SCHEMA_NUMBUCKETS, numBuckets.toString)
        tableProperties.put(DATASOURCE_SCHEMA_NUMBUCKETCOLS, bucketColumnNames.length.toString)
        bucketColumnNames.zipWithIndex.foreach { case (bucketCol, index) =>
          tableProperties.put(s"$DATASOURCE_SCHEMA_BUCKETCOL_PREFIX$index", bucketCol)
        }

        if (sortColumnNames.nonEmpty) {
          tableProperties.put(DATASOURCE_SCHEMA_NUMSORTCOLS, sortColumnNames.length.toString)
          sortColumnNames.zipWithIndex.foreach { case (sortCol, index) =>
            tableProperties.put(s"$DATASOURCE_SCHEMA_SORTCOL_PREFIX$index", sortCol)
          }
        }
      }

      // converts the table metadata to Spark SQL specific format, i.e. set schema, partition column
      // names and bucket specification to empty.
      def newSparkSQLSpecificMetastoreTable(): CatalogTable = {
        tableDefinition.copy(
          schema = new StructType,
          partitionColumnNames = Nil,
          bucketSpec = None,
          properties = tableDefinition.properties ++ tableProperties)
      }

      // converts the table metadata to Hive compatible format, i.e. set the serde information.
      def newHiveCompatibleMetastoreTable(serde: HiveSerDe): CatalogTable = {
        val location = if (tableDefinition.tableType == EXTERNAL) {
          // When we hit this branch, we are saving an external data source table with hive
          // compatible format, which means the data source is file-based and must have a `path`.
          val map = new CaseInsensitiveMap(tableDefinition.storage.properties)
          require(map.contains("path"),
            "External file-based data source table must have a `path` entry in storage properties.")
          Some(new Path(map("path")).toUri.toString)
        } else {
          None
        }

        tableDefinition.copy(
          storage = tableDefinition.storage.copy(
            locationUri = location,
            inputFormat = serde.inputFormat,
            outputFormat = serde.outputFormat,
            serde = serde.serde
          ),
          properties = tableDefinition.properties ++ tableProperties)
      }

      val qualifiedTableName = tableDefinition.identifier.quotedString
      val maybeSerde = HiveSerDe.sourceToSerDe(tableDefinition.provider.get)
      val skipHiveMetadata = tableDefinition.storage.properties
        .getOrElse("skipHiveMetadata", "false").toBoolean

      val (hiveCompatibleTable, logMessage) = maybeSerde match {
        case _ if skipHiveMetadata =>
          val message =
            s"Persisting data source table $qualifiedTableName into Hive metastore in" +
              "Spark SQL specific format, which is NOT compatible with Hive."
          (None, message)

        // our bucketing is un-compatible with hive(different hash function)
        case _ if tableDefinition.bucketSpec.nonEmpty =>
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
          val provider = tableDefinition.provider.get
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
                s"Could not persist ${tableDefinition.identifier.quotedString} in a Hive " +
                  "compatible way. Persisting it into Hive metastore in Spark SQL specific format."
              logWarning(warningMessage, e)
              saveTableIntoHive(newSparkSQLSpecificMetastoreTable(), ignoreIfExists)
          }

        case (None, message) =>
          logWarning(message)
          saveTableIntoHive(newSparkSQLSpecificMetastoreTable(), ignoreIfExists)
      }
    } else {
      client.createTable(tableDefinition, ignoreIfExists)
    }
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
        val dbLocation = getDatabase(tableDefinition.database).locationUri
        new Path(dbLocation, tableDefinition.identifier.table + "-__PLACEHOLDER__")
      }

      try {
        client.createTable(
          tableDefinition.withNewStorage(locationUri = Some(tempPath.toString)),
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

  override def renameTable(db: String, oldName: String, newName: String): Unit = withClient {
    val newTable = client.getTable(db, oldName)
      .copy(identifier = TableIdentifier(newName, Some(db)))
    client.alterTable(oldName, newTable)
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
      stats.colStats.foreach { case (colName, colStat) =>
        statsProperties += (STATISTICS_COL_STATS_PREFIX + colName) -> colStat.toString
      }
      tableDefinition.copy(properties = tableDefinition.properties ++ statsProperties)
    } else {
      tableDefinition
    }

    if (DDLUtils.isDatasourceTable(withStatsProps)) {
      val oldDef = client.getTable(db, withStatsProps.identifier.table)
      // Sets the `schema`, `partitionColumnNames` and `bucketSpec` from the old table definition,
      // to retain the spark specific format if it is. Also add old data source properties to table
      // properties, to retain the data source table format.
      val oldDataSourceProps = oldDef.properties.filter(_._1.startsWith(DATASOURCE_PREFIX))
      val newDef = withStatsProps.copy(
        schema = oldDef.schema,
        partitionColumnNames = oldDef.partitionColumnNames,
        bucketSpec = oldDef.bucketSpec,
        properties = oldDataSourceProps ++ withStatsProps.properties)

      client.alterTable(newDef)
    } else {
      client.alterTable(withStatsProps)
    }
  }

  override def getTable(db: String, table: String): CatalogTable = withClient {
    restoreTableMetadata(client.getTable(db, table))
  }

  override def getTableOption(db: String, table: String): Option[CatalogTable] = withClient {
    client.getTableOption(db, table).map(restoreTableMetadata)
  }

  /**
   * Restores table metadata from the table properties if it's a datasouce table. This method is
   * kind of a opposite version of [[createTable]].
   *
   * It reads table schema, provider, partition column names and bucket specification from table
   * properties, and filter out these special entries from table properties.
   */
  private def restoreTableMetadata(table: CatalogTable): CatalogTable = {
    val catalogTable = if (table.tableType == VIEW) {
      table
    } else {
      getProviderFromTableProperties(table).map { provider =>
        assert(provider != "hive", "Hive serde table should not save provider in table properties.")
        // SPARK-15269: Persisted data source tables always store the location URI as a storage
        // property named "path" instead of standard Hive `dataLocation`, because Hive only
        // allows directory paths as location URIs while Spark SQL data source tables also
        // allows file paths. So the standard Hive `dataLocation` is meaningless for Spark SQL
        // data source tables.
        // Spark SQL may also save external data source in Hive compatible format when
        // possible, so that these tables can be directly accessed by Hive. For these tables,
        // `dataLocation` is still necessary. Here we also check for input format because only
        // these Hive compatible tables set this field.
        val storage = if (table.tableType == EXTERNAL && table.storage.inputFormat.isEmpty) {
          table.storage.copy(locationUri = None)
        } else {
          table.storage
        }
        val tableProps = if (conf.get(DEBUG_MODE)) {
          table.properties
        } else {
          getOriginalTableProperties(table)
        }
        table.copy(
          storage = storage,
          schema = getSchemaFromTableProperties(table),
          provider = Some(provider),
          partitionColumnNames = getPartitionColumnsFromTableProperties(table),
          bucketSpec = getBucketSpecFromTableProperties(table),
          properties = tableProps)
      } getOrElse {
        table.copy(provider = Some("hive"))
      }
    }
    // construct Spark's statistics from information in Hive metastore
    val statsProps = catalogTable.properties.filterKeys(_.startsWith(STATISTICS_PREFIX))
    if (statsProps.nonEmpty) {
      val colStatsProps = statsProps.filterKeys(_.startsWith(STATISTICS_COL_STATS_PREFIX))
        .map { case (k, v) => (k.drop(STATISTICS_COL_STATS_PREFIX.length), v) }
      val colStats: Map[String, ColumnStat] = catalogTable.schema.collect {
        case f if colStatsProps.contains(f.name) =>
          val numFields = ColumnStatStruct.numStatFields(f.dataType)
          (f.name, ColumnStat(numFields, colStatsProps(f.name)))
      }.toMap
      catalogTable.copy(
        properties = removeStatsProperties(catalogTable),
        stats = Some(Statistics(
          sizeInBytes = BigInt(catalogTable.properties(STATISTICS_TOTAL_SIZE)),
          rowCount = catalogTable.properties.get(STATISTICS_NUM_ROWS).map(BigInt(_)),
          colStats = colStats)))
    } else {
      catalogTable
    }
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
      holdDDLTime: Boolean): Unit = withClient {
    requireTableExists(db, table)
    client.loadTable(
      loadPath,
      s"$db.$table",
      isOverwrite,
      holdDDLTime)
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean): Unit = withClient {
    requireTableExists(db, table)

    val orderedPartitionSpec = new util.LinkedHashMap[String, String]()
    getTable(db, table).partitionColumnNames.foreach { colName =>
      orderedPartitionSpec.put(colName, partition(colName))
    }

    client.loadPartition(
      loadPath,
      db,
      table,
      orderedPartitionSpec,
      isOverwrite,
      holdDDLTime,
      inheritTableSpecs)
  }

  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean): Unit = withClient {
    requireTableExists(db, table)

    val orderedPartitionSpec = new util.LinkedHashMap[String, String]()
    getTable(db, table).partitionColumnNames.foreach { colName =>
      orderedPartitionSpec.put(colName, partition(colName))
    }

    client.loadDynamicPartitions(
      loadPath,
      db,
      table,
      orderedPartitionSpec,
      replace,
      numDP,
      holdDDLTime)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = withClient {
    requireTableExists(db, table)
    client.createPartitions(db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withClient {
    requireTableExists(db, table)
    client.dropPartitions(db, table, parts, ignoreIfNotExists, purge)
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = withClient {
    client.renamePartitions(db, table, specs, newSpecs)
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withClient {
    client.alterPartitions(db, table, newParts)
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = withClient {
    client.getPartition(db, table, spec)
  }

  /**
   * Returns the specified partition or None if it does not exist.
   */
  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = withClient {
    client.getPartitionOption(db, table, spec)
  }

  /**
   * Returns the partition names from hive metastore for a given table in a database.
   */
  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = withClient {
    client.getPartitions(db, table, partialSpec)
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
    val functionName = funcDefinition.identifier.funcName.toLowerCase
    requireFunctionNotExists(db, functionName)
    val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
    client.createFunction(db, funcDefinition.copy(identifier = functionIdentifier))
  }

  override def dropFunction(db: String, name: String): Unit = withClient {
    requireFunctionExists(db, name)
    client.dropFunction(db, name)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = withClient {
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
  val DATASOURCE_PREFIX = "spark.sql.sources."
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

  val STATISTICS_PREFIX = "spark.sql.statistics."
  val STATISTICS_TOTAL_SIZE = STATISTICS_PREFIX + "totalSize"
  val STATISTICS_NUM_ROWS = STATISTICS_PREFIX + "numRows"
  val STATISTICS_COL_STATS_PREFIX = STATISTICS_PREFIX + "colStats."

  def removeStatsProperties(metadata: CatalogTable): Map[String, String] = {
    metadata.properties.filterNot { case (key, _) => key.startsWith(STATISTICS_PREFIX) }
  }

  def getProviderFromTableProperties(metadata: CatalogTable): Option[String] = {
    metadata.properties.get(DATASOURCE_PROVIDER)
  }

  def getOriginalTableProperties(metadata: CatalogTable): Map[String, String] = {
    metadata.properties.filterNot { case (key, _) => key.startsWith(DATASOURCE_PREFIX) }
  }

  // A persisted data source table always store its schema in the catalog.
  def getSchemaFromTableProperties(metadata: CatalogTable): StructType = {
    val errorMessage = "Could not read schema from the hive metastore because it is corrupted."
    val props = metadata.properties
    val schema = props.get(DATASOURCE_SCHEMA)
    if (schema.isDefined) {
      // Originally, we used `spark.sql.sources.schema` to store the schema of a data source table.
      // After SPARK-6024, we removed this flag.
      // Although we are not using `spark.sql.sources.schema` any more, we need to still support.
      DataType.fromJson(schema.get).asInstanceOf[StructType]
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

  def getPartitionColumnsFromTableProperties(metadata: CatalogTable): Seq[String] = {
    getColumnNamesByType(metadata.properties, "part", "partitioning columns")
  }

  def getBucketSpecFromTableProperties(metadata: CatalogTable): Option[BucketSpec] = {
    metadata.properties.get(DATASOURCE_SCHEMA_NUMBUCKETS).map { numBuckets =>
      BucketSpec(
        numBuckets.toInt,
        getColumnNamesByType(metadata.properties, "bucket", "bucketing columns"),
        getColumnNamesByType(metadata.properties, "sort", "sorting columns"))
    }
  }
}
