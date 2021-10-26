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

package org.apache.spark.sql.catalyst.catalog

import java.net.URI
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, StringUtils}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.GLOBAL_TEMP_DATABASE
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, PartitioningUtils}
import org.apache.spark.util.Utils

object SessionCatalog {
  val DEFAULT_DATABASE = "default"
}

/**
 * An internal catalog that is used by a Spark Session. This internal catalog serves as a
 * proxy to the underlying metastore (e.g. Hive Metastore) and it also manages temporary
 * views and functions of the Spark Session that it belongs to.
 *
 * This class must be thread-safe.
 */
class SessionCatalog(
    externalCatalogBuilder: () => ExternalCatalog,
    globalTempViewManagerBuilder: () => GlobalTempViewManager,
    functionRegistry: FunctionRegistry,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader,
    cacheSize: Int = SQLConf.get.tableRelationCacheSize,
    cacheTTL: Long = SQLConf.get.metadataCacheTTL) extends SQLConfHelper with Logging {
  import SessionCatalog._
  import CatalogTypes.TablePartitionSpec

  // For testing only.
  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      conf: SQLConf) = {
    this(
      () => externalCatalog,
      () => new GlobalTempViewManager(conf.getConf(GLOBAL_TEMP_DATABASE)),
      functionRegistry,
      new Configuration(),
      new CatalystSqlParser(),
      DummyFunctionResourceLoader,
      conf.tableRelationCacheSize,
      conf.metadataCacheTTL)
  }

  // For testing only.
  def this(externalCatalog: ExternalCatalog, functionRegistry: FunctionRegistry) = {
    this(externalCatalog, functionRegistry, SQLConf.get)
  }

  // For testing only.
  def this(externalCatalog: ExternalCatalog) = {
    this(externalCatalog, new SimpleFunctionRegistry)
  }

  lazy val externalCatalog = externalCatalogBuilder()
  lazy val globalTempViewManager = globalTempViewManagerBuilder()

  /** List of temporary views, mapping from table name to their logical plan. */
  @GuardedBy("this")
  protected val tempViews = new mutable.HashMap[String, LogicalPlan]

  // Note: we track current database here because certain operations do not explicitly
  // specify the database (e.g. DROP TABLE my_table). In these cases we must first
  // check whether the temporary view or function exists, then, if not, operate on
  // the corresponding item in the current database.
  @GuardedBy("this")
  protected var currentDb: String = formatDatabaseName(DEFAULT_DATABASE)

  private val validNameFormat = "([\\w_]+)".r

  /**
   * Checks if the given name conforms the Hive standard ("[a-zA-Z_0-9]+"),
   * i.e. if this name only contains characters, numbers, and _.
   *
   * This method is intended to have the same behavior of
   * org.apache.hadoop.hive.metastore.MetaStoreUtils.validateName.
   */
  private def validateName(name: String): Unit = {
    if (!validNameFormat.pattern.matcher(name).matches()) {
      throw new AnalysisException(s"`$name` is not a valid name for tables/databases. " +
        "Valid names only contain alphabet characters, numbers and _.")
    }
  }

  /**
   * Format table name, taking into account case sensitivity.
   */
  protected[this] def formatTableName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  /**
   * Format database name, taking into account case sensitivity.
   */
  protected[this] def formatDatabaseName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  private val tableRelationCache: Cache[QualifiedTableName, LogicalPlan] = {
    var builder = CacheBuilder.newBuilder()
      .maximumSize(cacheSize)

    if (cacheTTL > 0) {
      builder = builder.expireAfterWrite(cacheTTL, TimeUnit.SECONDS)
    }

    builder.build[QualifiedTableName, LogicalPlan]()
  }

  /** This method provides a way to get a cached plan. */
  def getCachedPlan(t: QualifiedTableName, c: Callable[LogicalPlan]): LogicalPlan = {
    tableRelationCache.get(t, c)
  }

  /** This method provides a way to get a cached plan if the key exists. */
  def getCachedTable(key: QualifiedTableName): LogicalPlan = {
    tableRelationCache.getIfPresent(key)
  }

  /** This method provides a way to cache a plan. */
  def cacheTable(t: QualifiedTableName, l: LogicalPlan): Unit = {
    tableRelationCache.put(t, l)
  }

  /** This method provides a way to invalidate a cached plan. */
  def invalidateCachedTable(key: QualifiedTableName): Unit = {
    tableRelationCache.invalidate(key)
  }

  /** This method discards any cached table relation plans for the given table identifier. */
  def invalidateCachedTable(name: TableIdentifier): Unit = {
    val dbName = formatDatabaseName(name.database.getOrElse(currentDb))
    val tableName = formatTableName(name.table)
    invalidateCachedTable(QualifiedTableName(dbName, tableName))
  }

  /** This method provides a way to invalidate all the cached plans. */
  def invalidateAllCachedTables(): Unit = {
    tableRelationCache.invalidateAll()
  }

  /**
   * This method is used to make the given path qualified before we
   * store this path in the underlying external catalog. So, when a path
   * does not contain a scheme, this path will not be changed after the default
   * FileSystem is changed.
   */
  private def makeQualifiedPath(path: URI): URI = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath).toUri
  }

  private def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }

  private def requireTableExists(name: TableIdentifier): Unit = {
    if (!tableExists(name)) {
      val db = name.database.getOrElse(currentDb)
      throw new NoSuchTableException(db = db, table = name.table)
    }
  }

  private def requireTableNotExists(name: TableIdentifier): Unit = {
    if (tableExists(name)) {
      val db = name.database.getOrElse(currentDb)
      throw new TableAlreadyExistsException(db = db, table = name.table)
    }
  }

  // ----------------------------------------------------------------------------
  // Databases
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // ----------------------------------------------------------------------------

  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val dbName = formatDatabaseName(dbDefinition.name)
    if (dbName == globalTempViewManager.database) {
      throw new AnalysisException(
        s"${globalTempViewManager.database} is a system preserved database, " +
          "you cannot create a database with this name.")
    }
    validateName(dbName)
    externalCatalog.createDatabase(
      dbDefinition.copy(name = dbName, locationUri = makeQualifiedDBPath(dbDefinition.locationUri)),
      ignoreIfExists)
  }

  private def makeQualifiedDBPath(locationUri: URI): URI = {
    if (locationUri.isAbsolute) {
      locationUri
    } else {
      val fullPath = new Path(conf.warehousePath, CatalogUtils.URIToString(locationUri))
      makeQualifiedPath(fullPath.toUri)
    }
  }

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    val dbName = formatDatabaseName(db)
    if (dbName == DEFAULT_DATABASE) {
      throw new AnalysisException(s"Can not drop default database")
    }
    if (cascade && databaseExists(dbName)) {
      listTables(dbName).foreach { t =>
        invalidateCachedTable(QualifiedTableName(dbName, t.table))
      }
    }
    externalCatalog.dropDatabase(dbName, ignoreIfNotExists, cascade)
  }

  def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val dbName = formatDatabaseName(dbDefinition.name)
    requireDbExists(dbName)
    externalCatalog.alterDatabase(dbDefinition.copy(
      name = dbName, locationUri = makeQualifiedDBPath(dbDefinition.locationUri)))
  }

  def getDatabaseMetadata(db: String): CatalogDatabase = {
    val dbName = formatDatabaseName(db)
    requireDbExists(dbName)
    externalCatalog.getDatabase(dbName)
  }

  def databaseExists(db: String): Boolean = {
    val dbName = formatDatabaseName(db)
    externalCatalog.databaseExists(dbName)
  }

  def listDatabases(): Seq[String] = {
    externalCatalog.listDatabases()
  }

  def listDatabases(pattern: String): Seq[String] = {
    externalCatalog.listDatabases(pattern)
  }

  def getCurrentDatabase: String = synchronized { currentDb }

  def setCurrentDatabase(db: String): Unit = {
    val dbName = formatDatabaseName(db)
    if (dbName == globalTempViewManager.database) {
      throw new AnalysisException(
        s"${globalTempViewManager.database} is a system preserved database, " +
          "you cannot use it as current database. To access global temporary views, you should " +
          "use qualified name with the GLOBAL_TEMP_DATABASE, e.g. SELECT * FROM " +
          s"${globalTempViewManager.database}.viewName.")
    }
    requireDbExists(dbName)
    synchronized { currentDb = dbName }
  }

  /**
   * Get the path for creating a non-default database when database location is not provided
   * by users.
   */
  def getDefaultDBPath(db: String): URI = {
    CatalogUtils.stringToURI(formatDatabaseName(db) + ".db")
  }

  // ----------------------------------------------------------------------------
  // Tables
  // ----------------------------------------------------------------------------
  // There are two kinds of tables, temporary views and metastore tables.
  // Temporary views are isolated across sessions and do not belong to any
  // particular database. Metastore tables can be used across multiple
  // sessions as their metadata is persisted in the underlying catalog.
  // ----------------------------------------------------------------------------

  // ----------------------------------------------------
  // | Methods that interact with metastore tables only |
  // ----------------------------------------------------

  /**
   * Create a metastore table in the database specified in `tableDefinition`.
   * If no such database is specified, create it in the current database.
   */
  def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true): Unit = {
    val db = formatDatabaseName(tableDefinition.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDefinition.identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    validateName(table)

    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // make the location of the table qualified.
      val qualifiedTableLocation =
        makeQualifiedTablePath(tableDefinition.storage.locationUri.get, db)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = tableIdentifier)
    } else {
      tableDefinition.copy(identifier = tableIdentifier)
    }

    requireDbExists(db)
    if (tableExists(newTableDefinition.identifier)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistsException(db = db, table = table)
      }
    } else if (validateLocation) {
      validateTableLocation(newTableDefinition)
    }
    externalCatalog.createTable(newTableDefinition, ignoreIfExists)
  }

  def validateTableLocation(table: CatalogTable): Unit = {
    // SPARK-19724: the default location of a managed table should be non-existent or empty.
    if (table.tableType == CatalogTableType.MANAGED) {
      val tableLocation =
        new Path(table.storage.locationUri.getOrElse(defaultTablePath(table.identifier)))
      val fs = tableLocation.getFileSystem(hadoopConf)

      if (fs.exists(tableLocation) && fs.listStatus(tableLocation).nonEmpty) {
        throw new AnalysisException(s"Can not create the managed table('${table.identifier}')" +
          s". The associated location('${tableLocation.toString}') already exists.")
      }
    }
  }

  private def makeQualifiedTablePath(locationUri: URI, database: String): URI = {
    if (locationUri.isAbsolute) {
      locationUri
    } else {
      val dbName = formatDatabaseName(database)
      val dbLocation = makeQualifiedDBPath(getDatabaseMetadata(dbName).locationUri)
      new Path(new Path(dbLocation), CatalogUtils.URIToString(locationUri)).toUri
    }
  }

  /**
   * Alter the metadata of an existing metastore table identified by `tableDefinition`.
   *
   * If no database is specified in `tableDefinition`, assume the table is in the
   * current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterTable(tableDefinition: CatalogTable): Unit = {
    val db = formatDatabaseName(tableDefinition.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDefinition.identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)
    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // make the location of the table qualified.
      val qualifiedTableLocation =
        makeQualifiedTablePath(tableDefinition.storage.locationUri.get, db)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = tableIdentifier)
    } else {
      tableDefinition.copy(identifier = tableIdentifier)
    }

    externalCatalog.alterTable(newTableDefinition)
  }

  /**
   * Alter the data schema of a table identified by the provided table identifier. The new data
   * schema should not have conflict column names with the existing partition columns, and should
   * still contain all the existing data columns.
   *
   * @param identifier TableIdentifier
   * @param newDataSchema Updated data schema to be used for the table
   */
  def alterTableDataSchema(
      identifier: TableIdentifier,
      newDataSchema: StructType): Unit = {
    val db = formatDatabaseName(identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)

    val catalogTable = externalCatalog.getTable(db, table)
    val oldDataSchema = catalogTable.dataSchema
    // not supporting dropping columns yet
    val nonExistentColumnNames =
      oldDataSchema.map(_.name).filterNot(columnNameResolved(newDataSchema, _))
    if (nonExistentColumnNames.nonEmpty) {
      throw new AnalysisException(
        s"""
           |Some existing schema fields (${nonExistentColumnNames.mkString("[", ",", "]")}) are
           |not present in the new schema. We don't support dropping columns yet.
         """.stripMargin)
    }

    externalCatalog.alterTableDataSchema(db, table, newDataSchema)
  }

  private def columnNameResolved(schema: StructType, colName: String): Boolean = {
    schema.fields.map(_.name).exists(conf.resolver(_, colName))
  }

  /**
   * Alter Spark's statistics of an existing metastore table identified by the provided table
   * identifier.
   */
  def alterTableStats(identifier: TableIdentifier, newStats: Option[CatalogStatistics]): Unit = {
    val db = formatDatabaseName(identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)
    externalCatalog.alterTableStats(db, table, newStats)
    // Invalidate the table relation cache
    refreshTable(identifier)
  }

  /**
   * Return whether a table/view with the specified name exists. If no database is specified, check
   * with current database.
   */
  def tableExists(name: TableIdentifier): Boolean = synchronized {
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    externalCatalog.tableExists(db, table)
  }

  /**
   * Retrieve the metadata of an existing permanent table/view. If no database is specified,
   * assume the table/view is in the current database.
   * We replace char/varchar with "annotated" string type in the table schema, as the query
   * engine doesn't support char/varchar yet.
   */
  @throws[NoSuchDatabaseException]
  @throws[NoSuchTableException]
  def getTableMetadata(name: TableIdentifier): CatalogTable = {
    val t = getTableRawMetadata(name)
    t.copy(schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(t.schema))
  }

  /**
   * Retrieve the metadata of an existing permanent table/view. If no database is specified,
   * assume the table/view is in the current database.
   */
  @throws[NoSuchDatabaseException]
  @throws[NoSuchTableException]
  def getTableRawMetadata(name: TableIdentifier): CatalogTable = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    externalCatalog.getTable(db, table)
  }

  /**
   * Retrieve all metadata of existing permanent tables/views. If no database is specified,
   * assume the table/view is in the current database.
   * Only the tables/views belong to the same database that can be retrieved are returned.
   * For example, if none of the requested tables could be retrieved, an empty list is returned.
   * There is no guarantee of ordering of the returned tables.
   */
  @throws[NoSuchDatabaseException]
  def getTablesByName(names: Seq[TableIdentifier]): Seq[CatalogTable] = {
    if (names.nonEmpty) {
      val dbs = names.map(_.database.getOrElse(getCurrentDatabase))
      if (dbs.distinct.size != 1) {
        val tables = names.map(name => formatTableName(name.table))
        val qualifiedTableNames = dbs.zip(tables).map { case (d, t) => QualifiedTableName(d, t)}
        throw new AnalysisException(
          s"Only the tables/views belong to the same database can be retrieved. Querying " +
          s"tables/views are $qualifiedTableNames"
        )
      }
      val db = formatDatabaseName(dbs.head)
      requireDbExists(db)
      val tables = names.map(name => formatTableName(name.table))
      externalCatalog.getTablesByName(db, tables)
    } else {
      Seq.empty
    }
  }

  /**
   * Load files stored in given path into an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then a [[NoSuchTableException]] is thrown.
   */
  def loadTable(
      name: TableIdentifier,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    externalCatalog.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
  }

  /**
   * Load files stored in given path into the partition of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then a [[NoSuchTableException]] is thrown.
   */
  def loadPartition(
      name: TableIdentifier,
      loadPath: String,
      spec: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    requireNonEmptyValueInPartitionSpec(Seq(spec))
    externalCatalog.loadPartition(
      db, table, loadPath, spec, isOverwrite, inheritTableSpecs, isSrcLocal)
  }

  def defaultTablePath(tableIdent: TableIdentifier): URI = {
    val dbName = formatDatabaseName(tableIdent.database.getOrElse(getCurrentDatabase))
    val dbLocation = getDatabaseMetadata(dbName).locationUri

    new Path(new Path(dbLocation), formatTableName(tableIdent.table)).toUri
  }

  // ----------------------------------------------
  // | Methods that interact with temp views only |
  // ----------------------------------------------

  /**
   * Create a local temporary view.
   */
  def createTempView(
      name: String,
      tableDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = synchronized {
    val table = formatTableName(name)
    if (tempViews.contains(table) && !overrideIfExists) {
      throw new TempTableAlreadyExistsException(name)
    }
    tempViews.put(table, tableDefinition)
  }

  /**
   * Create a global temporary view.
   */
  def createGlobalTempView(
      name: String,
      viewDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = {
    globalTempViewManager.create(formatTableName(name), viewDefinition, overrideIfExists)
  }

  /**
   * Alter the definition of a local/global temp view matching the given name, returns true if a
   * temp view is matched and altered, false otherwise.
   */
  def alterTempViewDefinition(
      name: TableIdentifier,
      viewDefinition: LogicalPlan): Boolean = synchronized {
    val viewName = formatTableName(name.table)
    if (name.database.isEmpty) {
      if (tempViews.contains(viewName)) {
        createTempView(viewName, viewDefinition, overrideIfExists = true)
        true
      } else {
        false
      }
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.update(viewName, viewDefinition)
    } else {
      false
    }
  }

  /**
   * Return a local temporary view exactly as it was stored.
   */
  def getRawTempView(name: String): Option[LogicalPlan] = synchronized {
    tempViews.get(formatTableName(name))
  }

  /**
   * Generate a [[View]] operator from the view description if the view stores sql text,
   * otherwise, it is same to `getRawTempView`
   */
  def getTempView(name: String): Option[LogicalPlan] = synchronized {
    getRawTempView(name).map(getTempViewPlan)
  }

  def getTempViewNames(): Seq[String] = synchronized {
    tempViews.keySet.toSeq
  }

  /**
   * Return a global temporary view exactly as it was stored.
   */
  def getRawGlobalTempView(name: String): Option[LogicalPlan] = {
    globalTempViewManager.get(formatTableName(name))
  }

  /**
   * Generate a [[View]] operator from the view description if the view stores sql text,
   * otherwise, it is same to `getRawGlobalTempView`
   */
  def getGlobalTempView(name: String): Option[LogicalPlan] = {
    getRawGlobalTempView(name).map(getTempViewPlan)
  }

  /**
   * Drop a local temporary view.
   *
   * Returns true if this view is dropped successfully, false otherwise.
   */
  def dropTempView(name: String): Boolean = synchronized {
    tempViews.remove(formatTableName(name)).isDefined
  }

  /**
   * Drop a global temporary view.
   *
   * Returns true if this view is dropped successfully, false otherwise.
   */
  def dropGlobalTempView(name: String): Boolean = {
    globalTempViewManager.remove(formatTableName(name))
  }

  // -------------------------------------------------------------
  // | Methods that interact with temporary and metastore tables |
  // -------------------------------------------------------------

  /**
   * Retrieve the metadata of an existing temporary view or permanent table/view.
   *
   * If a database is specified in `name`, this will return the metadata of table/view in that
   * database.
   * If no database is specified, this will first attempt to get the metadata of a temporary view
   * with the same name, then, if that does not exist, return the metadata of table/view in the
   * current database.
   */
  def getTempViewOrPermanentTableMetadata(name: TableIdentifier): CatalogTable = synchronized {
    val table = formatTableName(name.table)
    if (name.database.isEmpty) {
      tempViews.get(table).map {
        case TemporaryViewRelation(metadata) => metadata
        case plan =>
          CatalogTable(
            identifier = TableIdentifier(table),
            tableType = CatalogTableType.VIEW,
            storage = CatalogStorageFormat.empty,
            schema = plan.output.toStructType)
      }.getOrElse(getTableMetadata(name))
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.get(table).map {
        case TemporaryViewRelation(metadata) => metadata
        case plan =>
          CatalogTable(
            identifier = TableIdentifier(table, Some(globalTempViewManager.database)),
            tableType = CatalogTableType.VIEW,
            storage = CatalogStorageFormat.empty,
            schema = plan.output.toStructType)
      }.getOrElse(throw new NoSuchTableException(globalTempViewManager.database, table))
    } else {
      getTableMetadata(name)
    }
  }

  /**
   * Rename a table.
   *
   * If a database is specified in `oldName`, this will rename the table in that database.
   * If no database is specified, this will first attempt to rename a temporary view with
   * the same name, then, if that does not exist, rename the table in the current database.
   *
   * This assumes the database specified in `newName` matches the one in `oldName`.
   */
  def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit = synchronized {
    val db = formatDatabaseName(oldName.database.getOrElse(currentDb))
    newName.database.map(formatDatabaseName).foreach { newDb =>
      if (db != newDb) {
        throw new AnalysisException(
          s"RENAME TABLE source and destination databases do not match: '$db' != '$newDb'")
      }
    }

    val oldTableName = formatTableName(oldName.table)
    val newTableName = formatTableName(newName.table)
    if (db == globalTempViewManager.database) {
      globalTempViewManager.rename(oldTableName, newTableName)
    } else {
      requireDbExists(db)
      if (oldName.database.isDefined || !tempViews.contains(oldTableName)) {
        requireTableExists(TableIdentifier(oldTableName, Some(db)))
        requireTableNotExists(TableIdentifier(newTableName, Some(db)))
        validateName(newTableName)
        validateNewLocationOfRename(oldName, newName)
        externalCatalog.renameTable(db, oldTableName, newTableName)
      } else {
        if (newName.database.isDefined) {
          throw new AnalysisException(
            s"RENAME TEMPORARY VIEW from '$oldName' to '$newName': cannot specify database " +
              s"name '${newName.database.get}' in the destination table")
        }
        if (tempViews.contains(newTableName)) {
          throw new AnalysisException(s"RENAME TEMPORARY VIEW from '$oldName' to '$newName': " +
            "destination table already exists")
        }
        val table = tempViews(oldTableName)
        tempViews.remove(oldTableName)
        tempViews.put(newTableName, table)
      }
    }
  }

  /**
   * Drop a table.
   *
   * If a database is specified in `name`, this will drop the table from that database.
   * If no database is specified, this will first attempt to drop a temporary view with
   * the same name, then, if that does not exist, drop the table from the current database.
   */
  def dropTable(
      name: TableIdentifier,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = synchronized {
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    if (db == globalTempViewManager.database) {
      val viewExists = globalTempViewManager.remove(table)
      if (!viewExists && !ignoreIfNotExists) {
        throw new NoSuchTableException(globalTempViewManager.database, table)
      }
    } else {
      if (name.database.isDefined || !tempViews.contains(table)) {
        requireDbExists(db)
        // When ignoreIfNotExists is false, no exception is issued when the table does not exist.
        // Instead, log it as an error message.
        if (tableExists(TableIdentifier(table, Option(db)))) {
          externalCatalog.dropTable(db, table, ignoreIfNotExists = true, purge = purge)
        } else if (!ignoreIfNotExists) {
          throw new NoSuchTableException(db = db, table = table)
        }
      } else {
        tempViews.remove(table)
      }
    }
  }

  /**
   * Return a [[LogicalPlan]] that represents the given table or view.
   *
   * If a database is specified in `name`, this will return the table/view from that database.
   * If no database is specified, this will first attempt to return a temporary view with
   * the same name, then, if that does not exist, return the table/view from the current database.
   *
   * Note that, the global temp view database is also valid here, this will return the global temp
   * view matching the given name.
   *
   * If the relation is a view, we generate a [[View]] operator from the view description, and
   * wrap the logical plan in a [[SubqueryAlias]] which will track the name of the view.
   * [[SubqueryAlias]] will also keep track of the name and database(optional) of the table/view
   *
   * @param name The name of the table/view that we look up.
   */
  def lookupRelation(name: TableIdentifier): LogicalPlan = {
    synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      val table = formatTableName(name.table)
      if (db == globalTempViewManager.database) {
        globalTempViewManager.get(table).map { viewDef =>
          SubqueryAlias(table, db, getTempViewPlan(viewDef))
        }.getOrElse(throw new NoSuchTableException(db, table))
      } else if (name.database.isDefined || !tempViews.contains(table)) {
        val metadata = externalCatalog.getTable(db, table)
        getRelation(metadata)
      } else {
        SubqueryAlias(table, getTempViewPlan(tempViews(table)))
      }
    }
  }

  def getRelation(
      metadata: CatalogTable,
      options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()): LogicalPlan = {
    val name = metadata.identifier
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    val multiParts = Seq(CatalogManager.SESSION_CATALOG_NAME, db, table)

    if (metadata.tableType == CatalogTableType.VIEW) {
      // The relation is a view, so we wrap the relation by:
      // 1. Add a [[View]] operator over the relation to keep track of the view desc;
      // 2. Wrap the logical plan in a [[SubqueryAlias]] which tracks the name of the view.
      SubqueryAlias(multiParts, fromCatalogTable(metadata, isTempView = false))
    } else {
      SubqueryAlias(multiParts, UnresolvedCatalogRelation(metadata, options))
    }
  }

  private def getTempViewPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case viewInfo: TemporaryViewRelation =>
        fromCatalogTable(viewInfo.tableMeta, isTempView = true)
      case v => v
    }
  }

  private def fromCatalogTable(metadata: CatalogTable, isTempView: Boolean): View = {
    val viewText = metadata.viewText.getOrElse(sys.error("Invalid view without text."))
    val viewConfigs = metadata.viewSQLConfigs
    val viewPlan =
      SQLConf.withExistingConf(View.effectiveSQLConf(viewConfigs, isTempView = isTempView)) {
        parser.parsePlan(viewText)
      }
    View(
      desc = metadata,
      isTempView = isTempView,
      output = metadata.schema.toAttributes,
      child = viewPlan)
  }

  def lookupTempView(table: String): Option[SubqueryAlias] = {
    val formattedTable = formatTableName(table)
    getTempView(formattedTable).map { view =>
      SubqueryAlias(formattedTable, view)
    }
  }

  def lookupGlobalTempView(db: String, table: String): Option[SubqueryAlias] = {
    val formattedDB = formatDatabaseName(db)
    if (formattedDB == globalTempViewManager.database) {
      val formattedTable = formatTableName(table)
      getGlobalTempView(formattedTable).map { view =>
        SubqueryAlias(formattedTable, formattedDB, view)
      }
    } else {
      None
    }
  }

  // TODO: merge it with `isTemporaryTable`.
  def isTempView(nameParts: Seq[String]): Boolean = {
    if (nameParts.length > 2) return false
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    isTemporaryTable(nameParts.asTableIdentifier)
  }

  /**
   * Return whether a table with the specified name is a temporary view.
   *
   * Note: The temporary view cache is checked only when database is not
   * explicitly specified.
   */
  def isTemporaryTable(name: TableIdentifier): Boolean = synchronized {
    val table = formatTableName(name.table)
    if (name.database.isEmpty) {
      tempViews.contains(table)
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.get(table).isDefined
    } else {
      false
    }
  }

  def isView(nameParts: Seq[String]): Boolean = {
    nameParts.length <= 2 && {
      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
      val ident = nameParts.asTableIdentifier
      try {
        getTempViewOrPermanentTableMetadata(ident).tableType == CatalogTableType.VIEW
      } catch {
        case _: NoSuchTableException => false
        case _: NoSuchDatabaseException => false
        case _: NoSuchNamespaceException => false
      }
    }
  }

  /**
   * List all tables in the specified database, including local temporary views.
   *
   * Note that, if the specified database is global temporary view database, we will list global
   * temporary views.
   */
  def listTables(db: String): Seq[TableIdentifier] = listTables(db, "*")

  /**
   * List all matching tables in the specified database, including local temporary views.
   *
   * Note that, if the specified database is global temporary view database, we will list global
   * temporary views.
   */
  def listTables(db: String, pattern: String): Seq[TableIdentifier] = listTables(db, pattern, true)

  /**
   * List all matching tables in the specified database, including local temporary views
   * if includeLocalTempViews is enabled.
   *
   * Note that, if the specified database is global temporary view database, we will list global
   * temporary views.
   */
  def listTables(
      db: String,
      pattern: String,
      includeLocalTempViews: Boolean): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    val dbTables = if (dbName == globalTempViewManager.database) {
      globalTempViewManager.listViewNames(pattern).map { name =>
        TableIdentifier(name, Some(globalTempViewManager.database))
      }
    } else {
      requireDbExists(dbName)
      externalCatalog.listTables(dbName, pattern).map { name =>
        TableIdentifier(name, Some(dbName))
      }
    }

    if (includeLocalTempViews) {
      dbTables ++ listLocalTempViews(pattern)
    } else {
      dbTables
    }
  }

  /**
   * List all matching views in the specified database, including local temporary views.
   */
  def listViews(db: String, pattern: String): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    val dbViews = if (dbName == globalTempViewManager.database) {
      globalTempViewManager.listViewNames(pattern).map { name =>
        TableIdentifier(name, Some(globalTempViewManager.database))
      }
    } else {
      requireDbExists(dbName)
      externalCatalog.listViews(dbName, pattern).map { name =>
        TableIdentifier(name, Some(dbName))
      }
    }

    dbViews ++ listLocalTempViews(pattern)
  }

  /**
   * List all matching local temporary views.
   */
  def listLocalTempViews(pattern: String): Seq[TableIdentifier] = {
    synchronized {
      StringUtils.filterPattern(tempViews.keys.toSeq, pattern).map { name =>
        TableIdentifier(name)
      }
    }
  }

  /**
   * Refresh the cache entry for a metastore table, if any.
   */
  def refreshTable(name: TableIdentifier): Unit = synchronized {
    val dbName = formatDatabaseName(name.database.getOrElse(currentDb))
    val tableName = formatTableName(name.table)

    // Go through temporary views and invalidate them.
    // If the database is defined, this may be a global temporary view.
    // If the database is not defined, there is a good chance this is a temp view.
    if (name.database.isEmpty) {
      tempViews.get(tableName).foreach(_.refresh())
    } else if (dbName == globalTempViewManager.database) {
      globalTempViewManager.get(tableName).foreach(_.refresh())
    }

    // Also invalidate the table relation cache.
    val qualifiedTableName = QualifiedTableName(dbName, tableName)
    tableRelationCache.invalidate(qualifiedTableName)
  }

  /**
   * Drop all existing temporary views.
   * For testing only.
   */
  def clearTempTables(): Unit = synchronized {
    tempViews.clear()
  }

  // ----------------------------------------------------------------------------
  // Partitions
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // These methods are concerned with only metastore tables.
  // ----------------------------------------------------------------------------

  // TODO: We need to figure out how these methods interact with our data source
  // tables. For such tables, we do not store values of partitioning columns in
  // the metastore. For now, partition values of a data source table will be
  // automatically discovered when we load the table.

  /**
   * Create partitions in an existing table, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(parts.map(_.spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(parts.map(_.spec))
    externalCatalog.createPartitions(
      db, table, partitionWithQualifiedPath(tableName, parts), ignoreIfExists)
  }

  /**
   * Drop partitions from a table, assuming they exist.
   * If no database is specified, assume the table is in the current database.
   */
  def dropPartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requirePartialMatchedPartitionSpec(specs, getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(specs)
    externalCatalog.dropPartitions(db, table, specs, ignoreIfNotExists, purge, retainData)
  }

  /**
   * Override the specs of one or many existing table partitions, assuming they exist.
   *
   * This assumes index i of `specs` corresponds to index i of `newSpecs`.
   * If no database is specified, assume the table is in the current database.
   */
  def renamePartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    val tableMetadata = getTableMetadata(tableName)
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(specs, tableMetadata)
    requireExactMatchedPartitionSpec(newSpecs, tableMetadata)
    requireNonEmptyValueInPartitionSpec(specs)
    requireNonEmptyValueInPartitionSpec(newSpecs)
    externalCatalog.renamePartitions(db, table, specs, newSpecs)
  }

  /**
   * Alter one or many table partitions whose specs that match those specified in `parts`,
   * assuming the partitions exist.
   *
   * If no database is specified, assume the table is in the current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterPartitions(tableName: TableIdentifier, parts: Seq[CatalogTablePartition]): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(parts.map(_.spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(parts.map(_.spec))
    externalCatalog.alterPartitions(db, table, partitionWithQualifiedPath(tableName, parts))
  }

  /**
   * Retrieve the metadata of a table partition, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def getPartition(tableName: TableIdentifier, spec: TablePartitionSpec): CatalogTablePartition = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(Seq(spec))
    externalCatalog.getPartition(db, table, spec)
  }

  /**
   * List the names of all partitions that belong to the specified table, assuming it exists.
   *
   * A partial partition spec may optionally be provided to filter the partitions returned.
   * For instance, if there exist partitions (a='1', b='2'), (a='1', b='3') and (a='2', b='4'),
   * then a partial spec of (a='1') will return the first two only.
   */
  def listPartitionNames(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    partialSpec.foreach { spec =>
      requirePartialMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
      requireNonEmptyValueInPartitionSpec(Seq(spec))
    }
    externalCatalog.listPartitionNames(db, table, partialSpec)
  }

  /**
   * List the metadata of all partitions that belong to the specified table, assuming it exists.
   *
   * A partial partition spec may optionally be provided to filter the partitions returned.
   * For instance, if there exist partitions (a='1', b='2'), (a='1', b='3') and (a='2', b='4'),
   * then a partial spec of (a='1') will return the first two only.
   */
  def listPartitions(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    partialSpec.foreach { spec =>
      requirePartialMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
      requireNonEmptyValueInPartitionSpec(Seq(spec))
    }
    externalCatalog.listPartitions(db, table, partialSpec)
  }

  /**
   * List the metadata of partitions that belong to the specified table, assuming it exists, that
   * satisfy the given partition-pruning predicate expressions.
   */
  def listPartitionsByFilter(
      tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    externalCatalog.listPartitionsByFilter(db, table, predicates, conf.sessionLocalTimeZone)
  }

  /**
   * Verify if the input partition spec has any empty value.
   */
  private def requireNonEmptyValueInPartitionSpec(specs: Seq[TablePartitionSpec]): Unit = {
    specs.foreach { s =>
      if (s.values.exists(v => v != null && v.isEmpty)) {
        val spec = s.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
        throw new AnalysisException(
          s"Partition spec is invalid. The spec ($spec) contains an empty partition column value")
      }
    }
  }

  /**
   * Verify if the input partition spec exactly matches the existing defined partition spec
   * The columns must be the same but the orders could be different.
   */
  private def requireExactMatchedPartitionSpec(
      specs: Seq[TablePartitionSpec],
      table: CatalogTable): Unit = {
    specs.foreach { spec =>
      PartitioningUtils.requireExactMatchedPartitionSpec(
        table.identifier.toString,
        spec,
        table.partitionColumnNames)
    }
  }

  /**
   * Verify if the input partition spec partially matches the existing defined partition spec
   * That is, the columns of partition spec should be part of the defined partition spec.
   */
  private def requirePartialMatchedPartitionSpec(
      specs: Seq[TablePartitionSpec],
      table: CatalogTable): Unit = {
    val defined = table.partitionColumnNames
    specs.foreach { s =>
      if (!s.keys.forall(defined.contains)) {
        throw new AnalysisException(
          s"Partition spec is invalid. The spec (${s.keys.mkString(", ")}) must be contained " +
            s"within the partition spec (${table.partitionColumnNames.mkString(", ")}) defined " +
            s"in table '${table.identifier}'")
      }
    }
  }

  /**
   * Make the partition path qualified.
   * If the partition path is relative, e.g. 'paris', it will be qualified with
   * parent path using table location, e.g. 'file:/warehouse/table/paris'
   */
  private def partitionWithQualifiedPath(
      tableIdentifier: TableIdentifier,
      parts: Seq[CatalogTablePartition]): Seq[CatalogTablePartition] = {
    lazy val tbl = getTableMetadata(tableIdentifier)
    parts.map { part =>
      if (part.storage.locationUri.isDefined && !part.storage.locationUri.get.isAbsolute) {
        val partPath = new Path(new Path(tbl.location), new Path(part.storage.locationUri.get))
        val qualifiedPartPath = makeQualifiedPath(CatalogUtils.stringToURI(partPath.toString))
        part.copy(storage = part.storage.copy(locationUri = Some(qualifiedPartPath)))
      } else part
    }
  }
  // ----------------------------------------------------------------------------
  // Functions
  // ----------------------------------------------------------------------------
  // There are two kinds of functions, temporary functions and metastore
  // functions (permanent UDFs). Temporary functions are isolated across
  // sessions. Metastore functions can be used across multiple sessions as
  // their metadata is persisted in the underlying catalog.
  // ----------------------------------------------------------------------------

  // -------------------------------------------------------
  // | Methods that interact with metastore functions only |
  // -------------------------------------------------------

  /**
   * Create a function in the database specified in `funcDefinition`.
   * If no such database is specified, create it in the current database.
   *
   * @param ignoreIfExists: When true, ignore if the function with the specified name exists
   *                        in the specified database.
   */
  def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(funcDefinition.identifier.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = FunctionIdentifier(funcDefinition.identifier.funcName, Some(db))
    val newFuncDefinition = funcDefinition.copy(identifier = identifier)
    if (!functionExists(identifier)) {
      externalCatalog.createFunction(db, newFuncDefinition)
    } else if (!ignoreIfExists) {
      throw new FunctionAlreadyExistsException(db = db, func = identifier.toString)
    }
  }

  /**
   * Drop a metastore function.
   * If no database is specified, assume the function is in the current database.
   */
  def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = name.copy(database = Some(db))
    if (functionExists(identifier)) {
      if (functionRegistry.functionExists(identifier)) {
        // If we have loaded this function into the FunctionRegistry,
        // also drop it from there.
        // For a permanent function, because we loaded it to the FunctionRegistry
        // when it's first used, we also need to drop it from the FunctionRegistry.
        functionRegistry.dropFunction(identifier)
      }
      externalCatalog.dropFunction(db, name.funcName)
    } else if (!ignoreIfNotExists) {
      throw new NoSuchPermanentFunctionException(db = db, func = identifier.toString)
    }
  }

  /**
   * overwrite a metastore function in the database specified in `funcDefinition`..
   * If no database is specified, assume the function is in the current database.
   */
  def alterFunction(funcDefinition: CatalogFunction): Unit = {
    val db = formatDatabaseName(funcDefinition.identifier.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = FunctionIdentifier(funcDefinition.identifier.funcName, Some(db))
    val newFuncDefinition = funcDefinition.copy(identifier = identifier)
    if (functionExists(identifier)) {
      if (functionRegistry.functionExists(identifier)) {
        // If we have loaded this function into the FunctionRegistry,
        // also drop it from there.
        // For a permanent function, because we loaded it to the FunctionRegistry
        // when it's first used, we also need to drop it from the FunctionRegistry.
        functionRegistry.dropFunction(identifier)
      }
      externalCatalog.alterFunction(db, newFuncDefinition)
    } else {
      throw new NoSuchPermanentFunctionException(db = db, func = identifier.toString)
    }
  }

  /**
   * Retrieve the metadata of a metastore function.
   *
   * If a database is specified in `name`, this will return the function in that database.
   * If no database is specified, this will return the function in the current database.
   */
  def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    externalCatalog.getFunction(db, name.funcName)
  }

  /**
   * Check if the function with the specified name exists
   */
  def functionExists(name: FunctionIdentifier): Boolean = {
    functionRegistry.functionExists(name) || {
      val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
      requireDbExists(db)
      externalCatalog.functionExists(db, name.funcName)
    }
  }

  // ----------------------------------------------------------------
  // | Methods that interact with temporary and metastore functions |
  // ----------------------------------------------------------------

  /**
   * Constructs a [[FunctionBuilder]] based on the provided class that represents a function.
   */
  private def makeFunctionBuilder(name: String, functionClassName: String): FunctionBuilder = {
    val clazz = Utils.classForName(functionClassName)
    (input: Seq[Expression]) => makeFunctionExpression(name, clazz, input)
  }

  /**
   * Constructs a [[Expression]] based on the provided class that represents a function.
   *
   * This performs reflection to decide what type of [[Expression]] to return in the builder.
   */
  protected def makeFunctionExpression(
      name: String,
      clazz: Class[_],
      input: Seq[Expression]): Expression = {
    // Unfortunately we need to use reflection here because UserDefinedAggregateFunction
    // and ScalaUDAF are defined in sql/core module.
    val clsForUDAF =
      Utils.classForName("org.apache.spark.sql.expressions.UserDefinedAggregateFunction")
    if (clsForUDAF.isAssignableFrom(clazz)) {
      val cls = Utils.classForName("org.apache.spark.sql.execution.aggregate.ScalaUDAF")
      val e = cls.getConstructor(classOf[Seq[Expression]], clsForUDAF, classOf[Int], classOf[Int])
        .newInstance(input,
          clazz.getConstructor().newInstance().asInstanceOf[Object], Int.box(1), Int.box(1))
        .asInstanceOf[ImplicitCastInputTypes]

      // Check input argument size
      if (e.inputTypes.size != input.size) {
        throw new AnalysisException(s"Invalid number of arguments for function $name. " +
          s"Expected: ${e.inputTypes.size}; Found: ${input.size}")
      }
      e
    } else {
      throw new InvalidUDFClassException(s"No handler for UDAF '${clazz.getCanonicalName}'. " +
        s"Use sparkSession.udf.register(...) instead.")
    }
  }

  /**
   * Loads resources such as JARs and Files for a function. Every resource is represented
   * by a tuple (resource type, resource uri).
   */
  def loadFunctionResources(resources: Seq[FunctionResource]): Unit = {
    resources.foreach(functionResourceLoader.loadResource)
  }

  /**
   * Registers a temporary or permanent function into a session-specific [[FunctionRegistry]]
   */
  def registerFunction(
      funcDefinition: CatalogFunction,
      overrideIfExists: Boolean,
      functionBuilder: Option[FunctionBuilder] = None): Unit = {
    val func = funcDefinition.identifier
    if (functionRegistry.functionExists(func) && !overrideIfExists) {
      throw new AnalysisException(s"Function $func already exists")
    }
    val info = new ExpressionInfo(funcDefinition.className, func.database.orNull, func.funcName)
    val builder =
      functionBuilder.getOrElse {
        val className = funcDefinition.className
        if (!Utils.classIsLoadable(className)) {
          throw new AnalysisException(s"Can not load class '$className' when registering " +
            s"the function '$func', please make sure it is on the classpath")
        }
        makeFunctionBuilder(func.unquotedString, className)
      }
    functionRegistry.registerFunction(func, info, builder)
  }

  /**
   * Unregister a temporary or permanent function from a session-specific [[FunctionRegistry]]
   * Return true if function exists.
   */
  def unregisterFunction(name: FunctionIdentifier): Boolean = {
    functionRegistry.dropFunction(name)
  }

  /**
   * Drop a temporary function.
   */
  def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit = {
    if (!functionRegistry.dropFunction(FunctionIdentifier(name)) && !ignoreIfNotExists) {
      throw new NoSuchTempFunctionException(name)
    }
  }

  /**
   * Returns whether it is a temporary function. If not existed, returns false.
   */
  def isTemporaryFunction(name: FunctionIdentifier): Boolean = {
    // copied from HiveSessionCatalog
    val hiveFunctions = Seq("histogram_numeric")

    // A temporary function is a function that has been registered in functionRegistry
    // without a database name, and is neither a built-in function nor a Hive function
    name.database.isEmpty &&
      functionRegistry.functionExists(name) &&
      !FunctionRegistry.builtin.functionExists(name) &&
      !hiveFunctions.contains(name.funcName.toLowerCase(Locale.ROOT))
  }

  def isTempFunction(name: String): Boolean = {
    isTemporaryFunction(FunctionIdentifier(name))
  }

  /**
   * Return whether this function has been registered in the function registry of the current
   * session. If not existed, return false.
   */
  def isRegisteredFunction(name: FunctionIdentifier): Boolean = {
    functionRegistry.functionExists(name)
  }

  /**
   * Returns whether it is a persistent function. If not existed, returns false.
   */
  def isPersistentFunction(name: FunctionIdentifier): Boolean = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    databaseExists(db) && externalCatalog.functionExists(db, name.funcName)
  }

  protected[sql] def failFunctionLookup(
      name: FunctionIdentifier, cause: Option[Throwable] = None): Nothing = {
    throw new NoSuchFunctionException(
      db = name.database.getOrElse(getCurrentDatabase), func = name.funcName, cause)
  }

  /**
   * Look up the [[ExpressionInfo]] associated with the specified function, assuming it exists.
   */
  def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = synchronized {
    // TODO: just make function registry take in FunctionIdentifier instead of duplicating this
    val database = name.database.orElse(Some(currentDb)).map(formatDatabaseName)
    val qualifiedName = name.copy(database = database)
    functionRegistry.lookupFunction(name)
      .orElse(functionRegistry.lookupFunction(qualifiedName))
      .getOrElse {
        val db = qualifiedName.database.get
        requireDbExists(db)
        if (externalCatalog.functionExists(db, name.funcName)) {
          val metadata = externalCatalog.getFunction(db, name.funcName)
          new ExpressionInfo(
            metadata.className,
            qualifiedName.database.orNull,
            qualifiedName.identifier)
        } else {
          failFunctionLookup(name)
        }
      }
  }

  /**
   * Return an [[Expression]] that represents the specified function, assuming it exists.
   *
   * For a temporary function or a permanent function that has been loaded,
   * this method will simply lookup the function through the
   * FunctionRegistry and create an expression based on the builder.
   *
   * For a permanent function that has not been loaded, we will first fetch its metadata
   * from the underlying external catalog. Then, we will load all resources associated
   * with this function (i.e. jars and files). Finally, we create a function builder
   * based on the function class and put the builder into the FunctionRegistry.
   * The name of this function in the FunctionRegistry will be `databaseName.functionName`.
   */
  def lookupFunction(
      name: FunctionIdentifier,
      children: Seq[Expression]): Expression = synchronized {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    // Note: the implementation of this function is a little bit convoluted.
    // We probably shouldn't use a single FunctionRegistry to register all three kinds of functions
    // (built-in, temp, and external).
    if (name.database.isEmpty && functionRegistry.functionExists(name)) {
      val referredTempFunctionNames = AnalysisContext.get.referredTempFunctionNames
      val isResolvingView = AnalysisContext.get.catalogAndNamespace.nonEmpty
      // Lookup the function as a temporary or a built-in function (i.e. without database) and
      // 1. if we are not resolving view, we don't care about the function type and just return it.
      // 2. if we are resolving view, only return a temp function if it's referred by this view.
      if (!isResolvingView ||
          !isTemporaryFunction(name) ||
          referredTempFunctionNames.contains(name.funcName)) {
        // This function has been already loaded into the function registry.
        return functionRegistry.lookupFunction(name, children)
      }
    }

    // Get the database from AnalysisContext if it's defined, otherwise, use current database
    val currentDatabase = AnalysisContext.get.catalogAndNamespace match {
      case Seq() => getCurrentDatabase
      case Seq(_, db) => db
      case Seq(catalog, namespace @ _*) =>
        throw new AnalysisException(
          s"V2 catalog does not support functions yet. " +
            s"catalog: ${catalog}, namespace: '${namespace.quoted}'")
    }

    // If the name itself is not qualified, add the current database to it.
    val database = formatDatabaseName(name.database.getOrElse(currentDatabase))
    val qualifiedName = name.copy(database = Some(database))

    if (functionRegistry.functionExists(qualifiedName)) {
      // This function has been already loaded into the function registry.
      // Unlike the above block, we find this function by using the qualified name.
      return functionRegistry.lookupFunction(qualifiedName, children)
    }

    // The function has not been loaded to the function registry, which means
    // that the function is a permanent function (if it actually has been registered
    // in the metastore). We need to first put the function in the FunctionRegistry.
    // TODO: why not just check whether the function exists first?
    val catalogFunction = try {
      externalCatalog.getFunction(database, name.funcName)
    } catch {
      case _: AnalysisException => failFunctionLookup(name)
      case _: NoSuchPermanentFunctionException => failFunctionLookup(name)
    }
    loadFunctionResources(catalogFunction.resources)
    // Please note that qualifiedName is provided by the user. However,
    // catalogFunction.identifier.unquotedString is returned by the underlying
    // catalog. So, it is possible that qualifiedName is not exactly the same as
    // catalogFunction.identifier.unquotedString (difference is on case-sensitivity).
    // At here, we preserve the input from the user.
    registerFunction(catalogFunction.copy(identifier = qualifiedName), overrideIfExists = false)
    // Now, we need to create the Expression.
    functionRegistry.lookupFunction(qualifiedName, children)
  }

  /**
   * List all functions in the specified database, including temporary functions. This
   * returns the function identifier and the scope in which it was defined (system or user
   * defined).
   */
  def listFunctions(db: String): Seq[(FunctionIdentifier, String)] = listFunctions(db, "*")

  /**
   * List all matching functions in the specified database, including temporary functions. This
   * returns the function identifier and the scope in which it was defined (system or user
   * defined).
   */
  def listFunctions(db: String, pattern: String): Seq[(FunctionIdentifier, String)] = {
    val dbName = formatDatabaseName(db)
    requireDbExists(dbName)
    val dbFunctions = externalCatalog.listFunctions(dbName, pattern).map { f =>
      FunctionIdentifier(f, Some(dbName)) }
    val loadedFunctions = StringUtils
      .filterPattern(functionRegistry.listFunction().map(_.unquotedString), pattern).map { f =>
        // In functionRegistry, function names are stored as an unquoted format.
        Try(parser.parseFunctionIdentifier(f)) match {
          case Success(e) => e
          case Failure(_) =>
            // The names of some built-in functions are not parsable by our parser, e.g., %
            FunctionIdentifier(f)
        }
      }
    val functions = dbFunctions ++ loadedFunctions
    // The session catalog caches some persistent functions in the FunctionRegistry
    // so there can be duplicates.
    functions.map {
      case f if FunctionRegistry.functionSet.contains(f) => (f, "SYSTEM")
      case f => (f, "USER")
    }.distinct
  }


  // -----------------
  // | Other methods |
  // -----------------

  /**
   * Drop all existing databases (except "default"), tables, partitions and functions,
   * and set the current database to "default".
   *
   * This is mainly used for tests.
   */
  def reset(): Unit = synchronized {
    setCurrentDatabase(DEFAULT_DATABASE)
    externalCatalog.setCurrentDatabase(DEFAULT_DATABASE)
    listDatabases().filter(_ != DEFAULT_DATABASE).foreach { db =>
      dropDatabase(db, ignoreIfNotExists = false, cascade = true)
    }
    listTables(DEFAULT_DATABASE).foreach { table =>
      dropTable(table, ignoreIfNotExists = false, purge = false)
    }
    listFunctions(DEFAULT_DATABASE).map(_._1).foreach { func =>
      if (func.database.isDefined) {
        dropFunction(func, ignoreIfNotExists = false)
      } else {
        dropTempFunction(func.funcName, ignoreIfNotExists = false)
      }
    }
    clearTempTables()
    globalTempViewManager.clear()
    functionRegistry.clear()
    tableRelationCache.invalidateAll()
    // restore built-in functions
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
  }

  /**
   * Copy the current state of the catalog to another catalog.
   *
   * This function is synchronized on this [[SessionCatalog]] (the source) to make sure the copied
   * state is consistent. The target [[SessionCatalog]] is not synchronized, and should not be
   * because the target [[SessionCatalog]] should not be published at this point. The caller must
   * synchronize on the target if this assumption does not hold.
   */
  private[sql] def copyStateTo(target: SessionCatalog): Unit = synchronized {
    target.currentDb = currentDb
    // copy over temporary views
    tempViews.foreach(kv => target.tempViews.put(kv._1, kv._2))
  }

  /**
   * Validate the new locatoin before renaming a managed table, which should be non-existent.
   */
  private def validateNewLocationOfRename(
      oldName: TableIdentifier,
      newName: TableIdentifier): Unit = {
    val oldTable = getTableMetadata(oldName)
    if (oldTable.tableType == CatalogTableType.MANAGED) {
      val databaseLocation =
        externalCatalog.getDatabase(oldName.database.getOrElse(currentDb)).locationUri
      val newTableLocation = new Path(new Path(databaseLocation), formatTableName(newName.table))
      val fs = newTableLocation.getFileSystem(hadoopConf)
      if (fs.exists(newTableLocation)) {
        throw new AnalysisException(s"Can not rename the managed table('$oldName')" +
          s". The associated location('$newTableLocation') already exists.")
      }
    }
  }
}
