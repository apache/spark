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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableUtils, DDLUtils}
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.types.StructType


/**
 * A persistent implementation of the system catalog using Hive.
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class HiveExternalCatalog(client: HiveClient, hadoopConf: Configuration)
  extends ExternalCatalog with Logging {

  import CatalogTypes.TablePartitionSpec

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

  private def requireDbMatches(db: String, table: CatalogTable): Unit = {
    if (table.identifier.database != Some(db)) {
      throw new AnalysisException(
        s"Provided database '$db' does not match the one specified in the " +
        s"table definition (${table.identifier.database.getOrElse("n/a")})")
    }
  }

  private def requireTableExists(db: String, table: String): Unit = {
    withClient { getTable(db, table) }
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
      db: String,
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = withClient {
    requireDbExists(db)
    requireDbMatches(db, tableDefinition)

    if (tableDefinition.provider == Some("hive") ||
      tableDefinition.tableType == CatalogTableType.VIEW) {
      client.createTable(tableDefinition, ignoreIfExists)
    } else {
      import CreateDataSourceTableUtils._

      val provider = tableDefinition.provider.get
      val partitionColumns = tableDefinition.partitionColumnNames
      val bucketSpec = tableDefinition.bucketSpec

      val tableProperties = new scala.collection.mutable.HashMap[String, String]
      tableProperties.put(DATASOURCE_PROVIDER, provider)

      // Serialized JSON schema string may be too long to be stored into a single metastore table
      // property. In this case, we split the JSON string and store each part as a separate table
      // property.
      val threshold = 4000
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

      def newSparkSQLSpecificMetastoreTable(): CatalogTable = {
        CatalogTable(
          identifier = tableDefinition.identifier,
          tableType = tableDefinition.tableType,
          storage = tableDefinition.storage,
          schema = new StructType(),
          properties = tableProperties.toMap)
      }

      def newHiveCompatibleMetastoreTable(serde: HiveSerDe, path: String): CatalogTable = {
        tableDefinition.copy(properties = tableProperties.toMap).withNewStorage(
          locationUri = Some(new Path(path).toUri.toString),
          inputFormat = serde.inputFormat,
          outputFormat = serde.outputFormat,
          serde = serde.serde)
      }

      val maybePath = new CaseInsensitiveMap(tableDefinition.storage.properties).get("path")
      val skipHiveMetadata = tableDefinition.storage.properties
        .getOrElse("skipHiveMetadata", "false").toBoolean

      (HiveSerDe.sourceToSerDe(provider), maybePath) match {
        case (Some(serde), Some(path)) if !skipHiveMetadata =>
          try {
            saveTableIntoHive(newHiveCompatibleMetastoreTable(serde, path), ignoreIfExists)
          } catch {
            case NonFatal(e) =>
              val warningMessage =
                s"Could not persist ${tableDefinition.identifier.quotedString} in a Hive " +
                  "compatible way. Persisting it into Hive metastore in Spark SQL specific format."
              logWarning(warningMessage, e)
              saveTableIntoHive(newSparkSQLSpecificMetastoreTable(), ignoreIfExists)
          }
        case _ =>
          saveTableIntoHive(newSparkSQLSpecificMetastoreTable(), ignoreIfExists)

      }
    }
  }

  private def saveTableIntoHive(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    // If this is an external data source table...
    if (tableDefinition.tableType == CatalogTableType.EXTERNAL &&
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
   * Note: As of now, this only supports altering table properties, serde properties,
   * and num buckets!
   */
  override def alterTable(db: String, tableDefinition: CatalogTable): Unit = withClient {
    requireDbMatches(db, tableDefinition)
    requireTableExists(db, tableDefinition.identifier.table)
    client.alterTable(tableDefinition)
  }

  override def getTable(db: String, table: String): CatalogTable = withClient {
    restoreTableMetadata(client.getTable(db, table))
  }

  override def getTableOption(db: String, table: String): Option[CatalogTable] = withClient {
    client.getTableOption(db, table).map(restoreTableMetadata)
  }

  private def restoreTableMetadata(table: CatalogTable): CatalogTable = {
    if (table.tableType == CatalogTableType.VIEW) {
      table
    } else if (DDLUtils.isDatasourceTable(table.properties)) {
      table.copy(
        schema = DDLUtils.getSchemaFromTableProperties(table),
        provider = table.properties.get(CreateDataSourceTableUtils.DATASOURCE_PROVIDER),
        partitionColumnNames = DDLUtils.getPartitionColumnsFromTableProperties(table),
        bucketSpec = DDLUtils.getBucketSpecFromTableProperties(table))
    } else {
      table.copy(provider = Some("hive"))
    }
  }

  override def tableExists(db: String, table: String): Boolean = withClient {
    client.getTableOption(db, table).isDefined
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
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit = withClient {
    requireTableExists(db, table)

    val orderedPartitionSpec = new util.LinkedHashMap[String, String]()
    getTable(db, table).partitionColumnNames.foreach { colName =>
      orderedPartitionSpec.put(colName, partition(colName))
    }

    client.loadPartition(
      loadPath,
      s"$db.$table",
      orderedPartitionSpec,
      isOverwrite,
      holdDDLTime,
      inheritTableSpecs,
      isSkewedStoreAsSubdir)
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
    // Hive's metastore is case insensitive. However, Hive's createFunction does
    // not normalize the function name (unlike the getFunction part). So,
    // we are normalizing the function name.
    val functionName = funcDefinition.identifier.funcName.toLowerCase
    val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
    client.createFunction(db, funcDefinition.copy(identifier = functionIdentifier))
  }

  override def dropFunction(db: String, name: String): Unit = withClient {
    client.dropFunction(db, name)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = withClient {
    client.renameFunction(db, oldName, newName)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = withClient {
    client.getFunction(db, funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = withClient {
    client.functionExists(db, funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = withClient {
    client.listFunctions(db, pattern)
  }

}
