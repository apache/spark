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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, ExpressionInfo, NamedExpression, UpCast}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, StringUtils}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.GLOBAL_TEMP_DATABASE
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, PartitioningUtils}
import org.apache.spark.util.ArrayImplicits._
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
    tableFunctionRegistry: TableFunctionRegistry,
    hadoopConf: Configuration,
    val parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader,
    functionExpressionBuilder: FunctionExpressionBuilder,
    cacheSize: Int = SQLConf.get.tableRelationCacheSize,
    cacheTTL: Long = SQLConf.get.metadataCacheTTL,
    defaultDatabase: String = SQLConf.get.defaultDatabase) extends SQLConfHelper with Logging {
  import SessionCatalog._
  import CatalogTypes.TablePartitionSpec

  // For testing only.
  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      tableFunctionRegistry: TableFunctionRegistry,
      conf: SQLConf) = {
    this(
      () => externalCatalog,
      () => new GlobalTempViewManager(conf.getConf(GLOBAL_TEMP_DATABASE)),
      functionRegistry,
      tableFunctionRegistry,
      new Configuration(),
      new CatalystSqlParser(),
      DummyFunctionResourceLoader,
      DummyFunctionExpressionBuilder,
      conf.tableRelationCacheSize,
      conf.metadataCacheTTL,
      conf.defaultDatabase)
  }

  // For testing only.
  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      conf: SQLConf) = {
    this(externalCatalog, functionRegistry, new SimpleTableFunctionRegistry, conf)
  }

  // For testing only.
  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      tableFunctionRegistry: TableFunctionRegistry) = {
    this(externalCatalog, functionRegistry, tableFunctionRegistry, SQLConf.get)
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
  val globalTempDatabase: String = SQLConf.get.globalTempDatabase

  /** List of temporary views, mapping from table name to their logical plan. */
  @GuardedBy("this")
  protected val tempViews = new mutable.HashMap[String, TemporaryViewRelation]

  // Note: we track current database here because certain operations do not explicitly
  // specify the database (e.g. DROP TABLE my_table). In these cases we must first
  // check whether the temporary view or function exists, then, if not, operate on
  // the corresponding item in the current database.
  @GuardedBy("this")
  protected var currentDb: String = format(defaultDatabase)

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
      throw QueryCompilationErrors.invalidNameForTableOrDatabaseError(name)
    }
  }

  /**
   * Formats object names, taking into account case sensitivity.
   */
  protected def format(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  /**
   * Qualifies the table identifier with the current database if not specified, and normalize all
   * the names.
   */
  def qualifyIdentifier(ident: TableIdentifier): TableIdentifier = {
    TableIdentifier(
      table = format(ident.table),
      database = getDatabase(ident),
      catalog = getCatalog(ident))
  }

  /**
   * Qualifies the function identifier with the current database if not specified, and normalize all
   * the names.
   */
  def qualifyIdentifier(ident: FunctionIdentifier): FunctionIdentifier = {
    FunctionIdentifier(
      funcName = format(ident.funcName),
      database = getDatabase(ident),
      catalog = getCatalog(ident))
  }

  private def attachCatalogName(table: CatalogTable): CatalogTable = {
    table.copy(identifier = table.identifier.copy(catalog = getCatalog(table.identifier)))
  }

  private def getDatabase(ident: CatalystIdentifier): Option[String] = {
    Some(format(ident.database.getOrElse(getCurrentDatabase)))
  }

  private def getCatalog(ident: CatalystIdentifier): Option[String] = {
    if (conf.getConf(SQLConf.LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME)) {
      ident.catalog
    } else {
      Some(format(ident.catalog.getOrElse(CatalogManager.SESSION_CATALOG_NAME)))
    }
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
    val qualified = qualifyIdentifier(name)
    invalidateCachedTable(QualifiedTableName(
      qualified.catalog.get, qualified.database.get, qualified.table))
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
    CatalogUtils.makeQualifiedPath(path, hadoopConf)
  }

  private def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new NoSuchNamespaceException(Seq(CatalogManager.SESSION_CATALOG_NAME, db))
    }
  }

  private def requireTableExists(name: TableIdentifier): Unit = {
    if (!tableExists(name)) {
      throw new NoSuchTableException(db = name.database.get, table = name.table)
    }
  }

  private def requireTableNotExists(name: TableIdentifier): Unit = {
    if (tableExists(name)) {
      throw new TableAlreadyExistsException(db = name.database.get, table = name.table)
    }
  }

  // ----------------------------------------------------------------------------
  // Databases
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // ----------------------------------------------------------------------------

  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val dbName = format(dbDefinition.name)
    if (dbName == globalTempDatabase) {
      throw QueryCompilationErrors.cannotCreateDatabaseWithSameNameAsPreservedDatabaseError(
        globalTempDatabase)
    }
    validateName(dbName)
    externalCatalog.createDatabase(
      dbDefinition.copy(name = dbName, locationUri = makeQualifiedDBPath(dbDefinition.locationUri)),
      ignoreIfExists)
  }

  private def makeQualifiedDBPath(locationUri: URI): URI = {
    CatalogUtils.makeQualifiedDBObjectPath(locationUri, conf.warehousePath, hadoopConf)
  }

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    val dbName = format(db)
    if (dbName == DEFAULT_DATABASE) {
      throw QueryCompilationErrors.cannotDropDefaultDatabaseError(
        Seq(CatalogManager.SESSION_CATALOG_NAME, dbName))
    }
    if (!ignoreIfNotExists) {
      requireDbExists(dbName)
    }
    if (cascade && databaseExists(dbName)) {
      listTables(dbName).foreach { t =>
        invalidateCachedTable(QualifiedTableName(SESSION_CATALOG_NAME, dbName, t.table))
      }
    }
    externalCatalog.dropDatabase(dbName, ignoreIfNotExists, cascade)
  }

  def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val dbName = format(dbDefinition.name)
    requireDbExists(dbName)
    externalCatalog.alterDatabase(dbDefinition.copy(
      name = dbName, locationUri = makeQualifiedDBPath(dbDefinition.locationUri)))
  }

  def getDatabaseMetadata(db: String): CatalogDatabase = {
    val dbName = format(db)
    requireDbExists(dbName)
    externalCatalog.getDatabase(dbName)
  }

  def databaseExists(db: String): Boolean = {
    val dbName = format(db)
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
    setCurrentDatabaseWithNameCheck(db, requireDbExists)
  }

  def setCurrentDatabaseWithNameCheck(db: String, nameCheck: String => Unit): Unit = {
    val dbName = format(db)
    if (dbName == globalTempDatabase) {
      throw QueryCompilationErrors.cannotUsePreservedDatabaseAsCurrentDatabaseError(
        globalTempDatabase)
    }
    nameCheck(dbName)
    synchronized { currentDb = dbName }
  }

  /**
   * Get the path for creating a non-default database when database location is not provided
   * by users.
   */
  def getDefaultDBPath(db: String): URI = {
    CatalogUtils.stringToURI(format(db) + ".db")
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
    val isExternal = tableDefinition.tableType == CatalogTableType.EXTERNAL
    if (isExternal && tableDefinition.storage.locationUri.isEmpty) {
      throw QueryCompilationErrors.createExternalTableWithoutLocationError()
    }

    val qualifiedIdent = qualifyIdentifier(tableDefinition.identifier)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
    validateName(table)

    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // make the location of the table qualified.
      val qualifiedTableLocation =
        makeQualifiedTablePath(tableDefinition.storage.locationUri.get, db)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = qualifiedIdent)
    } else {
      tableDefinition.copy(identifier = qualifiedIdent)
    }

    requireDbExists(db)
    if (tableExists(newTableDefinition.identifier)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistsException(db = db, table = table)
      }
    } else {
      if (validateLocation) {
        validateTableLocation(newTableDefinition)
      }
      externalCatalog.createTable(newTableDefinition, ignoreIfExists)
    }
  }

  def validateTableLocation(table: CatalogTable): Unit = {
    // SPARK-19724: the default location of a managed table should be non-existent or empty.
    if (table.tableType == CatalogTableType.MANAGED) {
      val tableLocation =
        new Path(table.storage.locationUri.getOrElse(defaultTablePath(table.identifier)))
      val fs = tableLocation.getFileSystem(hadoopConf)

      if (fs.exists(tableLocation) && fs.listStatus(tableLocation).nonEmpty) {
        throw QueryExecutionErrors.locationAlreadyExists(table.identifier, tableLocation)
      }
    }
  }

  def makeQualifiedTablePath(locationUri: URI, database: String): URI = {
    if (locationUri.isAbsolute) {
      locationUri
    } else if (new Path(locationUri).isAbsolute) {
      makeQualifiedPath(locationUri)
    } else {
      val dbName = format(database)
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
    val qualifiedIdent = qualifyIdentifier(tableDefinition.identifier)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // make the location of the table qualified.
      val qualifiedTableLocation =
        makeQualifiedTablePath(tableDefinition.storage.locationUri.get, db)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = qualifiedIdent)
    } else {
      tableDefinition.copy(identifier = qualifiedIdent)
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
    val qualifiedIdent = qualifyIdentifier(identifier)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
    requireDbExists(db)
    requireTableExists(qualifiedIdent)

    val catalogTable = externalCatalog.getTable(db, table)
    val oldDataSchema = catalogTable.dataSchema
    // not supporting dropping columns yet
    val resolver = conf.resolver
    val nonExistentColumnNames =
      oldDataSchema.map(_.name).filterNot(columnNameResolved(resolver, newDataSchema, _))
    if (nonExistentColumnNames.nonEmpty) {
      throw QueryCompilationErrors.dropNonExistentColumnsNotSupportedError(nonExistentColumnNames)
    }

    externalCatalog.alterTableDataSchema(db, table, newDataSchema)
  }

  private def columnNameResolved(
      resolver: Resolver,
      schema: StructType,
      colName: String): Boolean = {
    schema.fields.exists(f => resolver(f.name, colName))
  }

  /**
   * Alter Spark's statistics of an existing metastore table identified by the provided table
   * identifier.
   */
  def alterTableStats(identifier: TableIdentifier, newStats: Option[CatalogStatistics]): Unit = {
    val qualifiedIdent = qualifyIdentifier(identifier)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    externalCatalog.alterTableStats(db, table, newStats)
    // Invalidate the table relation cache
    refreshTable(qualifiedIdent)
  }

  /**
   * Return whether a table/view with the specified name exists. If no database is specified, check
   * with current database.
   */
  def tableExists(name: TableIdentifier): Boolean = {
    val qualifiedIdent = qualifyIdentifier(name)
    externalCatalog.tableExists(qualifiedIdent.database.get, qualifiedIdent.table)
  }

  /**
   * Retrieve the metadata of an existing permanent table/view. If no database is specified,
   * assume the table/view is in the current database.
   * We replace char/varchar with "annotated" string type in the table schema, as the query
   * engine doesn't support char/varchar yet.
   */
  @throws[NoSuchNamespaceException]
  @throws[NoSuchTableException]
  def getTableMetadata(name: TableIdentifier): CatalogTable = {
    val t = getTableRawMetadata(name)
    t.copy(schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(t.schema))
  }

  /**
   * Retrieve the metadata of an existing permanent table/view. If no database is specified,
   * assume the table/view is in the current database.
   */
  @throws[NoSuchNamespaceException]
  @throws[NoSuchTableException]
  def getTableRawMetadata(name: TableIdentifier): CatalogTable = {
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    attachCatalogName(externalCatalog.getTable(db, table))
  }

  /**
   * Retrieve all metadata of existing permanent tables/views. If no database is specified,
   * assume the table/view is in the current database.
   * Only the tables/views belong to the same database that can be retrieved are returned.
   * For example, if none of the requested tables could be retrieved, an empty list is returned.
   * There is no guarantee of ordering of the returned tables.
   */
  @throws[NoSuchNamespaceException]
  def getTablesByName(names: Seq[TableIdentifier]): Seq[CatalogTable] = {
    if (names.nonEmpty) {
      val qualifiedIdents = names.map(qualifyIdentifier)
      val dbs = qualifiedIdents.map(_.database.get)
      val tables = qualifiedIdents.map(_.table)
      if (dbs.distinct.size != 1) {
        val qualifiedTableNames = dbs.zip(tables).map { case (d, t) => QualifiedTableName(d, t)}
        throw QueryCompilationErrors.cannotRetrieveTableOrViewNotInSameDatabaseError(
          qualifiedTableNames)
      }
      val db = dbs.head
      requireDbExists(db)
      externalCatalog.getTablesByName(db, tables).map(attachCatalogName)
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
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
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
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    requireNonEmptyValueInPartitionSpec(Seq(spec))
    externalCatalog.loadPartition(
      db, table, loadPath, spec, isOverwrite, inheritTableSpecs, isSrcLocal)
  }

  def defaultTablePath(tableIdent: TableIdentifier): URI = {
    val qualifiedIdent = qualifyIdentifier(tableIdent)
    val dbLocation = getDatabaseMetadata(qualifiedIdent.database.get).locationUri
    new Path(new Path(dbLocation), qualifiedIdent.table).toUri
  }

  // ----------------------------------------------
  // | Methods that interact with temp views only |
  // ----------------------------------------------

  /**
   * Create a local temporary view.
   */
  def createTempView(
      name: String,
      viewDefinition: TemporaryViewRelation,
      overrideIfExists: Boolean): Unit = synchronized {
    val normalized = format(name)
    if (tempViews.contains(normalized) && !overrideIfExists) {
      throw new TempTableAlreadyExistsException(name)
    }
    tempViews.put(normalized, viewDefinition)
  }

  /**
   * Create a global temporary view.
   */
  def createGlobalTempView(
      name: String,
      viewDefinition: TemporaryViewRelation,
      overrideIfExists: Boolean): Unit = {
    globalTempViewManager.create(format(name), viewDefinition, overrideIfExists)
  }

  /**
   * Alter the definition of a local/global temp view matching the given name, returns true if a
   * temp view is matched and altered, false otherwise.
   */
  def alterTempViewDefinition(
      name: TableIdentifier,
      viewDefinition: TemporaryViewRelation): Boolean = synchronized {
    val viewName = format(name.table)
    if (name.database.isEmpty) {
      if (tempViews.contains(viewName)) {
        createTempView(viewName, viewDefinition, overrideIfExists = true)
        true
      } else {
        false
      }
    } else if (format(name.database.get) == globalTempDatabase) {
      globalTempViewManager.update(viewName, viewDefinition)
    } else {
      false
    }
  }

  /**
   * Return a local temporary view exactly as it was stored.
   */
  def getRawTempView(name: String): Option[TemporaryViewRelation] = synchronized {
    tempViews.get(format(name))
  }

  /**
   * Generate a [[View]] operator from the temporary view stored.
   */
  def getTempView(name: String): Option[View] = synchronized {
    getRawTempView(name).map(getTempViewPlan)
  }

  def getTempViewNames(): Seq[String] = synchronized {
    tempViews.keySet.toSeq
  }

  /**
   * Return a global temporary view exactly as it was stored.
   */
  def getRawGlobalTempView(name: String): Option[TemporaryViewRelation] = {
    globalTempViewManager.get(format(name))
  }

  /**
   * Generate a [[View]] operator from the global temporary view stored.
   */
  def getGlobalTempView(name: String): Option[View] = {
    getRawGlobalTempView(name).map(getTempViewPlan)
  }

  /**
   * Generate a [[View]] operator from the local or global temporary view stored.
   */
  def getLocalOrGlobalTempView(name: TableIdentifier): Option[View] = {
    getRawLocalOrGlobalTempView(toNameParts(name)).map(getTempViewPlan)
  }

  /**
   * Return the raw logical plan of a temporary local or global view for the given name.
   */
  def getRawLocalOrGlobalTempView(name: Seq[String]): Option[TemporaryViewRelation] = {
    name match {
      case Seq(v) => getRawTempView(v)
      case Seq(db, v) if isGlobalTempViewDB(db) => getRawGlobalTempView(v)
      case _ => None
    }
  }

  /**
   * Drop a local temporary view.
   *
   * Returns true if this view is dropped successfully, false otherwise.
   */
  def dropTempView(name: String): Boolean = synchronized {
    tempViews.remove(format(name)).isDefined
  }

  /**
   * Drop a global temporary view.
   *
   * Returns true if this view is dropped successfully, false otherwise.
   */
  def dropGlobalTempView(name: String): Boolean = {
    globalTempViewManager.remove(format(name))
  }

  private def toNameParts(ident: TableIdentifier): Seq[String] = {
    ident.database.toSeq :+ ident.table
  }

  private def getTempViewPlan(viewInfo: TemporaryViewRelation): View = viewInfo.plan match {
    case Some(p) => View(desc = viewInfo.tableMeta, isTempView = true, child = p)
    case None => fromCatalogTable(viewInfo.tableMeta, isTempView = true)
  }

  /**
   * Generates a [[SubqueryAlias]] operator from the stored temporary view.
   */
  def getTempViewRelation(viewInfo: TemporaryViewRelation): SubqueryAlias = {
    SubqueryAlias(toNameParts(viewInfo.tableMeta.identifier).map(format), getTempViewPlan(viewInfo))
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
    val table = format(name.table)
    if (name.database.isEmpty) {
      tempViews.get(table).map(_.tableMeta).getOrElse(getTableMetadata(name))
    } else if (format(name.database.get) == globalTempDatabase) {
      globalTempViewManager.get(table).map(_.tableMeta)
        .getOrElse(throw new NoSuchTableException(globalTempDatabase, table))
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
    val qualifiedIdent = qualifyIdentifier(oldName)
    val db = qualifiedIdent.database.get
    newName.database.map(format).foreach { newDb =>
      if (db != newDb) {
        throw QueryCompilationErrors.renameTableSourceAndDestinationMismatchError(db, newDb)
      }
    }

    val oldTableName = qualifiedIdent.table
    val newTableName = format(newName.table)
    if (db == globalTempDatabase) {
      globalTempViewManager.rename(oldTableName, newTableName)
    } else {
      requireDbExists(db)
      if (oldName.database.isDefined || !tempViews.contains(oldTableName)) {
        validateName(newTableName)
        validateNewLocationOfRename(qualifiedIdent, qualifyIdentifier(newName))
        externalCatalog.renameTable(db, oldTableName, newTableName)
      } else {
        if (newName.database.isDefined) {
          throw QueryCompilationErrors.cannotRenameTempViewWithDatabaseSpecifiedError(
            oldName, newName)
        }
        if (tempViews.contains(newTableName)) {
          throw QueryCompilationErrors.cannotRenameTempViewToExistingTableError(newName)
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
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
    if (db == globalTempDatabase) {
      val viewExists = globalTempViewManager.remove(table)
      if (!viewExists && !ignoreIfNotExists) {
        throw new NoSuchTableException(globalTempDatabase, table)
      }
    } else {
      if (name.database.isDefined || !tempViews.contains(table)) {
        requireDbExists(db)
        // When ignoreIfNotExists is false, no exception is issued when the table does not exist.
        // Instead, log it as an error message.
        if (tableExists(qualifiedIdent)) {
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
  def lookupRelation(name: TableIdentifier): LogicalPlan = synchronized {
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
    if (db == globalTempDatabase) {
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

  def getRelation(
      metadata: CatalogTable,
      options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()): LogicalPlan = {
    val qualifiedIdent = qualifyIdentifier(metadata.identifier)
    val db = qualifiedIdent.database.get
    val table = qualifiedIdent.table
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

  private def buildViewDDL(metadata: CatalogTable, isTempView: Boolean): Option[String] = {
    if (isTempView) {
      None
    } else {
      val viewName = metadata.identifier.unquotedString
      val viewText = metadata.viewText.get
      val userSpecifiedColumns =
        if (metadata.schema.fieldNames.toImmutableArraySeq == metadata.viewQueryColumnNames) {
          " "
        } else {
          s" (${metadata.schema.fieldNames.mkString(", ")}) "
        }
      Some(s"CREATE OR REPLACE VIEW $viewName${userSpecifiedColumns}AS $viewText")
    }
  }

  private def isHiveCreatedView(metadata: CatalogTable): Boolean = {
    // For views created by hive without explicit column names, there will be auto-generated
    // column names like "_c0", "_c1", "_c2"...
    metadata.viewQueryColumnNames.isEmpty &&
      metadata.schema.fieldNames.exists(_.matches("_c[0-9]+"))
  }


  private def castColToType(
    col: Expression,
    toField: StructField,
    schemaMode: ViewSchemaMode): NamedExpression = {
    val cast = schemaMode match {
      /*
      ** For schema binding, we cast the column to the expected type using safe cast only.
      ** For legacy behavior, we cast the column to the expected type using safe cast only.
      ** For schema compensation, we cast the column to the expected type using any cast
      *  in ansi mode.
      ** For schema (type) evolution, we take the column as is.
      */
      case SchemaBinding => UpCast(col, toField.dataType)
      case SchemaUnsupported => if (conf.viewSchemaCompensation) {
        Cast(col, toField.dataType, ansiEnabled = true)
      } else {
        UpCast(col, toField.dataType)
      }
      case SchemaCompensation => Cast(col, toField.dataType, ansiEnabled = true)
      case SchemaTypeEvolution => col
      case other => throw SparkException.internalError("Unexpected ViewSchemaMode")
    }
    Alias(cast, toField.name)(explicitMetadata = Some(toField.metadata))
  }
  private def fromCatalogTable(metadata: CatalogTable, isTempView: Boolean): View = {
    val viewText = metadata.viewText.getOrElse {
      throw SparkException.internalError("Invalid view without text.")
    }
    val viewConfigs = metadata.viewSQLConfigs
    val origin = CurrentOrigin.get.copy(
      objectType = Some("VIEW"),
      objectName = Some(metadata.qualifiedName)
    )
    val parsedPlan = SQLConf.withExistingConf(View.effectiveSQLConf(viewConfigs, isTempView)) {
        CurrentOrigin.withOrigin(origin) {
          parser.parseQuery(viewText)
        }
    }
    val schemaMode = metadata.viewSchemaMode
    if (schemaMode == SchemaEvolution) {
      View(desc = metadata, isTempView = isTempView, child = parsedPlan)
    } else {
      val projectList = if (!isHiveCreatedView(metadata)) {
        val viewColumnNames = if (metadata.viewQueryColumnNames.isEmpty) {
          // For view created before Spark 2.2.0, the view text is already fully qualified, the plan
          // output is the same with the view output.
          metadata.schema.fieldNames.toImmutableArraySeq
        } else {
          assert(metadata.viewQueryColumnNames.length == metadata.schema.length)
          metadata.viewQueryColumnNames
        }

        // For view queries like `SELECT * FROM t`, the schema of the referenced table/view may
        // change after the view has been created. We need to add an extra SELECT to pick the
        // columns according to the recorded column names (to get the correct view column ordering
        // and omit the extra columns that we don't require), with UpCast (to make sure the type
        // change is safe) and Alias (to respect user-specified view column names) according to the
        // view schema in the catalog.
        // Note that, the column names may have duplication, e.g. `CREATE VIEW v(x, y) AS
        // SELECT 1 col, 2 col`. We need to make sure that the matching attributes have the same
        // number of duplications, and pick the corresponding attribute by ordinal.
        val viewConf = View.effectiveSQLConf(metadata.viewSQLConfigs, isTempView)
        val normalizeColName: String => String = if (viewConf.caseSensitiveAnalysis) {
          identity
        } else {
          _.toLowerCase(Locale.ROOT)
        }
        val nameToCounts = viewColumnNames.groupBy(normalizeColName).transform((_, v) => v.length)
        val nameToCurrentOrdinal = scala.collection.mutable.HashMap.empty[String, Int]
        val viewDDL = buildViewDDL(metadata, isTempView)

        viewColumnNames.zip(metadata.schema).map { case (name, field) =>
          val normalizedName = normalizeColName(name)
          val count = nameToCounts(normalizedName)
          val ordinal = nameToCurrentOrdinal.getOrElse(normalizedName, 0)
          nameToCurrentOrdinal(normalizedName) = ordinal + 1
          val col = GetViewColumnByNameAndOrdinal(
            metadata.identifier.toString, name, ordinal, count, viewDDL)
          castColToType(col, field, schemaMode)
        }
      } else {
        // For view created by hive, the parsed view plan may have different output columns with
        // the schema stored in metadata. For example: `CREATE VIEW v AS SELECT 1 FROM t`
        // the schema in metadata will be `_c0` while the parsed view plan has column named `1`
        metadata.schema.zipWithIndex.map { case (field, index) =>
          val col = GetColumnByOrdinal(index, field.dataType)
          castColToType(col, field, schemaMode)
        }
      }
      View(desc = metadata, isTempView = isTempView, child = Project(projectList, parsedPlan))
    }
  }

  def isGlobalTempViewDB(dbName: String): Boolean = {
    globalTempDatabase.equalsIgnoreCase(dbName)
  }

  /**
   * Return whether the given name parts belong to a temporary or global temporary view.
   */
  def isTempView(nameParts: Seq[String]): Boolean = {
    getRawLocalOrGlobalTempView(nameParts).isDefined
  }

  /**
   * Return whether a table with the specified name is a temporary view.
   */
  def isTempView(name: TableIdentifier): Boolean = synchronized {
    isTempView(toNameParts(name))
  }

  def isView(nameParts: Seq[String]): Boolean = {
    nameParts.length <= 2 && {
      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
      val ident = nameParts.asTableIdentifier
      try {
        getTempViewOrPermanentTableMetadata(ident).tableType == CatalogTableType.VIEW
      } catch {
        case _: NoSuchTableException => false
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
    val dbName = format(db)
    val dbTables = if (dbName == globalTempDatabase) {
      globalTempViewManager.listViewNames(pattern).map { name =>
        TableIdentifier(name, Some(globalTempDatabase))
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
    val dbName = format(db)
    val dbViews = if (dbName == globalTempDatabase) {
      globalTempViewManager.listViewNames(pattern).map { name =>
        TableIdentifier(name, Some(globalTempDatabase))
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
   * List all matching temp views in the specified database, including global/local temporary views.
   */
  def listTempViews(db: String, pattern: String): Seq[CatalogTable] = {
    val globalTempViews = if (format(db) == globalTempDatabase) {
      globalTempViewManager.listViewNames(pattern).flatMap { viewName =>
        globalTempViewManager.get(viewName).map(_.tableMeta)
      }
    } else {
      Seq.empty
    }

    val localTempViews = listLocalTempViews(pattern).flatMap { viewIndent =>
      tempViews.get(viewIndent.table).map(_.tableMeta)
    }

    globalTempViews ++ localTempViews
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
   * Refresh table entries in structures maintained by the session catalog such as:
   *   - The map of temporary or global temporary view names to their logical plans
   *   - The relation cache which maps table identifiers to their logical plans
   *
   * For temp views, it refreshes their logical plans, and as a consequence of that it can refresh
   * the file indexes of the base relations (`HadoopFsRelation` for instance) used in the views.
   * The method still keeps the views in the internal lists of session catalog.
   *
   * For tables/views, it removes their entries from the relation cache.
   *
   * The method is supposed to use in the following situations:
   *   1. The logical plan of a table/view was changed, and cached table/view data is cleared
   *      explicitly. For example, like in `AlterTableRenameCommand` which re-caches the table
   *      itself. Otherwise if you need to refresh cached data, consider using of
   *      `CatalogImpl.refreshTable()`.
   *   2. A table/view doesn't exist, and need to only remove its entry in the relation cache since
   *      the cached data is invalidated explicitly like in `DropTableCommand` which uncaches
   *      table/view data itself.
   *   3. Meta-data (such as file indexes) of any relation used in a temporary view should be
   *      updated.
   */
  def refreshTable(name: TableIdentifier): Unit = synchronized {
    getLocalOrGlobalTempView(name).map(_.refresh()).getOrElse {
      val qualifiedIdent = qualifyIdentifier(name)
      val qualifiedTableName = QualifiedTableName(
        qualifiedIdent.catalog.get, qualifiedIdent.database.get, qualifiedIdent.table)
      tableRelationCache.invalidate(qualifiedTableName)
    }
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
    val qualifiedIdent = qualifyIdentifier(tableName)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    requireExactMatchedPartitionSpec(parts.map(_.spec), getTableMetadata(qualifiedIdent))
    requireNonEmptyValueInPartitionSpec(parts.map(_.spec))
    externalCatalog.createPartitions(
      db, qualifiedIdent.table, partitionWithQualifiedPath(qualifiedIdent, parts), ignoreIfExists)
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
    val qualifiedIdent = qualifyIdentifier(tableName)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    requirePartialMatchedPartitionSpec(specs, getTableMetadata(qualifiedIdent))
    requireNonEmptyValueInPartitionSpec(specs)
    externalCatalog.dropPartitions(
      db, qualifiedIdent.table, specs, ignoreIfNotExists, purge, retainData)
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
    val qualifiedIdent = qualifyIdentifier(tableName)
    val db = qualifiedIdent.database.get
    val tableMetadata = getTableMetadata(qualifiedIdent)
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    requireExactMatchedPartitionSpec(specs, tableMetadata)
    requireExactMatchedPartitionSpec(newSpecs, tableMetadata)
    requireNonEmptyValueInPartitionSpec(specs)
    requireNonEmptyValueInPartitionSpec(newSpecs)
    externalCatalog.renamePartitions(db, qualifiedIdent.table, specs, newSpecs)
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
    val qualifiedIdent = qualifyIdentifier(tableName)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    requireExactMatchedPartitionSpec(parts.map(_.spec), getTableMetadata(qualifiedIdent))
    requireNonEmptyValueInPartitionSpec(parts.map(_.spec))
    externalCatalog.alterPartitions(
      db, qualifiedIdent.table, partitionWithQualifiedPath(qualifiedIdent, parts))
  }

  /**
   * Retrieve the metadata of a table partition, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def getPartition(tableName: TableIdentifier, spec: TablePartitionSpec): CatalogTablePartition = {
    val qualifiedIdent = qualifyIdentifier(tableName)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    requireExactMatchedPartitionSpec(Seq(spec), getTableMetadata(qualifiedIdent))
    requireNonEmptyValueInPartitionSpec(Seq(spec))
    externalCatalog.getPartition(db, qualifiedIdent.table, spec)
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
    val qualifiedIdent = qualifyIdentifier(tableName)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    partialSpec.foreach { spec =>
      requirePartialMatchedPartitionSpec(Seq(spec), getTableMetadata(qualifiedIdent))
      requireNonEmptyValueInPartitionSpec(Seq(spec))
    }
    externalCatalog.listPartitionNames(db, qualifiedIdent.table, partialSpec)
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
    val qualifiedIdent = qualifyIdentifier(tableName)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    partialSpec.foreach { spec =>
      requirePartialMatchedPartitionSpec(Seq(spec), getTableMetadata(qualifiedIdent))
      requireNonEmptyValueInPartitionSpec(Seq(spec))
    }
    externalCatalog.listPartitions(db, qualifiedIdent.table, partialSpec)
  }

  /**
   * List the metadata of partitions that belong to the specified table, assuming it exists, that
   * satisfy the given partition-pruning predicate expressions.
   */
  def listPartitionsByFilter(
      tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    val qualifiedIdent = qualifyIdentifier(tableName)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    requireTableExists(qualifiedIdent)
    externalCatalog.listPartitionsByFilter(
      db, qualifiedIdent.table, predicates, conf.sessionLocalTimeZone)
  }

  /**
   * Verify if the input partition spec has any empty value.
   */
  private def requireNonEmptyValueInPartitionSpec(specs: Seq[TablePartitionSpec]): Unit = {
    specs.foreach { s =>
      if (s.values.exists(v => v != null && v.isEmpty)) {
        val spec = s.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
        throw QueryCompilationErrors.invalidPartitionSpecError(
          s"The spec ($spec) contains an empty partition column value")
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
        throw QueryCompilationErrors.invalidPartitionSpecError(
          s"The spec (${s.keys.mkString(", ")}) must be contained " +
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
    val qualifiedIdent = qualifyIdentifier(funcDefinition.identifier)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    val newFuncDefinition = funcDefinition.copy(identifier = qualifiedIdent)
    if (!functionExists(qualifiedIdent)) {
      externalCatalog.createFunction(db, newFuncDefinition)
    } else if (!ignoreIfExists) {
      throw new FunctionAlreadyExistsException(Seq(db, qualifiedIdent.funcName))
    }
  }

  /**
   * Drop a metastore function.
   * If no database is specified, assume the function is in the current database.
   */
  def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    val funcName = qualifiedIdent.funcName
    requireDbExists(db)
    if (functionExists(qualifiedIdent)) {
      if (functionRegistry.functionExists(qualifiedIdent)) {
        // If we have loaded this function into the FunctionRegistry,
        // also drop it from there.
        // For a permanent function, because we loaded it to the FunctionRegistry
        // when it's first used, we also need to drop it from the FunctionRegistry.
        functionRegistry.dropFunction(qualifiedIdent)
      }
      externalCatalog.dropFunction(db, funcName)
    } else if (!ignoreIfNotExists) {
      throw new NoSuchPermanentFunctionException(db = db, func = funcName)
    }
  }

  /**
   * overwrite a metastore function in the database specified in `funcDefinition`..
   * If no database is specified, assume the function is in the current database.
   */
  def alterFunction(funcDefinition: CatalogFunction): Unit = {
    val qualifiedIdent = qualifyIdentifier(funcDefinition.identifier)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    val newFuncDefinition = funcDefinition.copy(identifier = qualifiedIdent)
    if (functionExists(qualifiedIdent)) {
      if (functionRegistry.functionExists(qualifiedIdent)) {
        // If we have loaded this function into the FunctionRegistry,
        // also drop it from there.
        // For a permanent function, because we loaded it to the FunctionRegistry
        // when it's first used, we also need to drop it from the FunctionRegistry.
        functionRegistry.dropFunction(qualifiedIdent)
      }
      externalCatalog.alterFunction(db, newFuncDefinition)
    } else {
      throw new NoSuchPermanentFunctionException(db = db, func = qualifiedIdent.funcName)
    }
  }

  /**
   * Retrieve the metadata of a metastore function.
   *
   * If a database is specified in `name`, this will return the function in that database.
   * If no database is specified, this will return the function in the current database.
   */
  def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction = {
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    requireDbExists(db)
    externalCatalog.getFunction(db, qualifiedIdent.funcName).copy(identifier = qualifiedIdent)
  }

  /**
   * Check if the function with the specified name exists
   */
  def functionExists(name: FunctionIdentifier): Boolean = {
    isRegisteredFunction(name) || {
      val qualifiedIdent = qualifyIdentifier(name)
      val db = qualifiedIdent.database.get
      requireDbExists(db)
      externalCatalog.functionExists(db, qualifiedIdent.funcName)
    }
  }

  // ----------------------------------------------------------------
  // | Methods that interact with temporary and metastore functions |
  // ----------------------------------------------------------------

  /**
   * Constructs a [[FunctionBuilder]] based on the provided function metadata.
   */
  private def makeFunctionBuilder(func: CatalogFunction): FunctionBuilder = {
    val className = func.className
    if (!Utils.classIsLoadable(className)) {
      throw QueryCompilationErrors.cannotLoadClassWhenRegisteringFunctionError(
        className, func.identifier)
    }
    val clazz = Utils.classForName(className)
    val name = func.identifier.unquotedString
    (input: Seq[Expression]) => functionExpressionBuilder.makeExpression(name, clazz, input)
  }

  /**
   * Loads resources such as JARs and Files for a function. Every resource is represented
   * by a tuple (resource type, resource uri).
   */
  def loadFunctionResources(resources: Seq[FunctionResource]): Unit = {
    resources.foreach(functionResourceLoader.loadResource)
  }

  /**
   * Registers a temporary or permanent scalar function into a session-specific [[FunctionRegistry]]
   */
  def registerFunction(
      funcDefinition: CatalogFunction,
      overrideIfExists: Boolean,
      functionBuilder: Option[FunctionBuilder] = None): Unit = {
    val builder = functionBuilder.getOrElse(makeFunctionBuilder(funcDefinition))
    registerFunction(funcDefinition, overrideIfExists, functionRegistry, builder)
  }

  private def registerFunction[T](
      funcDefinition: CatalogFunction,
      overrideIfExists: Boolean,
      registry: FunctionRegistryBase[T],
      functionBuilder: FunctionRegistryBase[T]#FunctionBuilder): Unit = {
    val func = funcDefinition.identifier
    if (registry.functionExists(func) && !overrideIfExists) {
      throw QueryCompilationErrors.functionAlreadyExistsError(func)
    }
    val info = makeExprInfoForHiveFunction(funcDefinition)
    registry.registerFunction(func, info, functionBuilder)
  }

  private def makeExprInfoForHiveFunction(func: CatalogFunction): ExpressionInfo = {
    new ExpressionInfo(
      func.className,
      func.identifier.database.orNull,
      func.identifier.funcName,
      null,
      "",
      "",
      "",
      "",
      "",
      "",
      "hive")
  }

  /**
   * Unregister a temporary or permanent function from a session-specific [[FunctionRegistry]]
   * or [[TableFunctionRegistry]]. Return true if function exists.
   */
  def unregisterFunction(name: FunctionIdentifier): Boolean = {
    functionRegistry.dropFunction(name) || tableFunctionRegistry.dropFunction(name)
  }

  /**
   * Drop a temporary function.
   */
  def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit = {
    if (!functionRegistry.dropFunction(FunctionIdentifier(name)) &&
        !tableFunctionRegistry.dropFunction(FunctionIdentifier(name)) &&
        !ignoreIfNotExists) {
      throw new NoSuchTempFunctionException(name)
    }
  }

  /**
   * Returns whether it is a temporary function. If not existed, returns false.
   */
  def isTemporaryFunction(name: FunctionIdentifier): Boolean = {
    // A temporary function is a function that has been registered in functionRegistry
    // without a database name, and is neither a built-in function nor a Hive function
    name.database.isEmpty && isRegisteredFunction(name) && !isBuiltinFunction(name)
  }

  /**
   * Return whether this function has been registered in the function registry of the current
   * session. If not existed, return false.
   */
  def isRegisteredFunction(name: FunctionIdentifier): Boolean = {
    functionRegistry.functionExists(name) || tableFunctionRegistry.functionExists(name)
  }

  /**
   * Returns whether it is a persistent function. If not existed, returns false.
   */
  def isPersistentFunction(name: FunctionIdentifier): Boolean = {
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    val funcName = qualifiedIdent.funcName
    databaseExists(db) && externalCatalog.functionExists(db, funcName)
  }

  /**
   * Returns whether it is a built-in function.
   */
  def isBuiltinFunction(name: FunctionIdentifier): Boolean = {
    FunctionRegistry.builtin.functionExists(name) ||
      TableFunctionRegistry.builtin.functionExists(name)
  }

  protected[sql] def failFunctionLookup(name: FunctionIdentifier): Nothing = {
    throw new NoSuchFunctionException(
      db = name.database.getOrElse(getCurrentDatabase), func = name.funcName)
  }

  /**
   * Look up the `ExpressionInfo` of the given function by name if it's a built-in or temp function.
   * This only supports scalar functions.
   */
  def lookupBuiltinOrTempFunction(name: String): Option[ExpressionInfo] = {
    FunctionRegistry.builtinOperators.get(name.toLowerCase(Locale.ROOT)).orElse {
      synchronized(lookupTempFuncWithViewContext(
        name, FunctionRegistry.builtin.functionExists, functionRegistry.lookupFunction))
    }
  }

  /**
   * Look up the `ExpressionInfo` of the given function by name if it's a built-in or
   * temp table function.
   */
  def lookupBuiltinOrTempTableFunction(name: String): Option[ExpressionInfo] = synchronized {
    lookupTempFuncWithViewContext(
      name, TableFunctionRegistry.builtin.functionExists, tableFunctionRegistry.lookupFunction)
  }

  /**
   * Look up a built-in or temp scalar function by name and resolves it to an Expression if such
   * a function exists.
   */
  def resolveBuiltinOrTempFunction(name: String, arguments: Seq[Expression]): Option[Expression] = {
    resolveBuiltinOrTempFunctionInternal(
      name, arguments, FunctionRegistry.builtin.functionExists, functionRegistry)
  }

  /**
   * Look up a built-in or temp table function by name and resolves it to a LogicalPlan if such
   * a function exists.
   */
  def resolveBuiltinOrTempTableFunction(
      name: String, arguments: Seq[Expression]): Option[LogicalPlan] = {
    resolveBuiltinOrTempFunctionInternal(
      name, arguments, TableFunctionRegistry.builtin.functionExists, tableFunctionRegistry)
  }

  private def resolveBuiltinOrTempFunctionInternal[T](
      name: String,
      arguments: Seq[Expression],
      isBuiltin: FunctionIdentifier => Boolean,
      registry: FunctionRegistryBase[T]): Option[T] = synchronized {
    val funcIdent = FunctionIdentifier(name)
    if (!registry.functionExists(funcIdent)) {
      None
    } else {
      lookupTempFuncWithViewContext(
        name, isBuiltin, ident => Option(registry.lookupFunction(ident, arguments)))
    }
  }

  private def lookupTempFuncWithViewContext[T](
      name: String,
      isBuiltin: FunctionIdentifier => Boolean,
      lookupFunc: FunctionIdentifier => Option[T]): Option[T] = {
    val funcIdent = FunctionIdentifier(name)
    if (isBuiltin(funcIdent)) {
      lookupFunc(funcIdent)
    } else {
      val isResolvingView = AnalysisContext.get.catalogAndNamespace.nonEmpty
      val referredTempFunctionNames = AnalysisContext.get.referredTempFunctionNames
      if (isResolvingView) {
        // When resolving a view, only return a temp function if it's referred by this view.
        if (referredTempFunctionNames.contains(name)) {
          lookupFunc(funcIdent)
        } else {
          None
        }
      } else {
        val result = lookupFunc(funcIdent)
        if (result.isDefined) {
          // We are not resolving a view and the function is a temp one, add it to
          // `AnalysisContext`, so during the view creation, we can save all referred temp
          // functions to view metadata.
          AnalysisContext.get.referredTempFunctionNames.add(name)
        }
        result
      }
    }
  }

  /**
   * Look up the `ExpressionInfo` of the given function by name if it's a persistent function.
   * This supports both scalar and table functions.
   */
  def lookupPersistentFunction(name: FunctionIdentifier): ExpressionInfo = {
    val qualifiedIdent = qualifyIdentifier(name)
    val db = qualifiedIdent.database.get
    val funcName = qualifiedIdent.funcName
    functionRegistry.lookupFunction(qualifiedIdent)
      .orElse(tableFunctionRegistry.lookupFunction(qualifiedIdent))
      .getOrElse {
        requireDbExists(db)
        if (externalCatalog.functionExists(db, funcName)) {
          val metadata = externalCatalog.getFunction(db, funcName)
          makeExprInfoForHiveFunction(metadata.copy(identifier = qualifiedIdent))
        } else {
          failFunctionLookup(name)
        }
      }
  }

  /**
   * Look up a persistent scalar function by name and resolves it to an Expression.
   */
  def resolvePersistentFunction(
      name: FunctionIdentifier, arguments: Seq[Expression]): Expression = {
    resolvePersistentFunctionInternal(name, arguments, functionRegistry, makeFunctionBuilder)
  }

  /**
   * Look up a persistent table function by name and resolves it to a LogicalPlan.
   */
  def resolvePersistentTableFunction(
      name: FunctionIdentifier,
      arguments: Seq[Expression]): LogicalPlan = {
    // We don't support persistent table functions yet.
    val builder = (func: CatalogFunction) => failFunctionLookup(name)
    resolvePersistentFunctionInternal(name, arguments, tableFunctionRegistry, builder)
  }

  private def resolvePersistentFunctionInternal[T](
      name: FunctionIdentifier,
      arguments: Seq[Expression],
      registry: FunctionRegistryBase[T],
      createFunctionBuilder: CatalogFunction => FunctionRegistryBase[T]#FunctionBuilder): T = {
    // `synchronized` is used to prevent multiple threads from concurrently resolving the
    // same function that has not yet been loaded into the function registry. This is needed
    // because calling `registerFunction` twice with `overrideIfExists = false` can lead to
    // a FunctionAlreadyExistsException.
    synchronized {
      val qualifiedIdent = qualifyIdentifier(name)
      val db = qualifiedIdent.database.get
      val funcName = qualifiedIdent.funcName
      if (registry.functionExists(qualifiedIdent)) {
        // This function has been already loaded into the function registry.
        registry.lookupFunction(qualifiedIdent, arguments)
      } else {
        // The function has not been loaded to the function registry, which means
        // that the function is a persistent function (if it actually has been registered
        // in the metastore). We need to first put the function in the function registry.
        val catalogFunction = externalCatalog.getFunction(db, funcName)
        loadFunctionResources(catalogFunction.resources)
        // Please note that qualifiedName is provided by the user. However,
        // catalogFunction.identifier.unquotedString is returned by the underlying
        // catalog. So, it is possible that qualifiedName is not exactly the same as
        // catalogFunction.identifier.unquotedString (difference is on case-sensitivity).
        // At here, we preserve the input from the user.
        val funcMetadata = catalogFunction.copy(identifier = qualifiedIdent)
        registerFunction(
          funcMetadata,
          overrideIfExists = false,
          registry = registry,
          functionBuilder = createFunctionBuilder(funcMetadata))
        // Now, we need to create the Expression.
        registry.lookupFunction(qualifiedIdent, arguments)
      }
    }
  }

  /**
   * Look up the [[ExpressionInfo]] associated with the specified function, assuming it exists.
   */
  def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = synchronized {
    if (name.database.isEmpty) {
      lookupBuiltinOrTempFunction(name.funcName)
        .orElse(lookupBuiltinOrTempTableFunction(name.funcName))
        .getOrElse(lookupPersistentFunction(name))
    } else {
      lookupPersistentFunction(name)
    }
  }

  // The actual function lookup logic looks up temp/built-in function first, then persistent
  // function from either v1 or v2 catalog. This method only look up v1 catalog.
  def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    if (name.database.isEmpty) {
      resolveBuiltinOrTempFunction(name.funcName, children)
        .getOrElse(resolvePersistentFunction(name, children))
    } else {
      resolvePersistentFunction(name, children)
    }
  }

  def lookupTableFunction(name: FunctionIdentifier, children: Seq[Expression]): LogicalPlan = {
    if (name.database.isEmpty) {
      resolveBuiltinOrTempTableFunction(name.funcName, children)
        .getOrElse(resolvePersistentTableFunction(name, children))
    } else {
      resolvePersistentTableFunction(name, children)
    }
  }

  /**
   * List all built-in and temporary functions with the given pattern.
   */
  private def listBuiltinAndTempFunctions(pattern: String): Seq[FunctionIdentifier] = {
    val functions = (functionRegistry.listFunction() ++ tableFunctionRegistry.listFunction())
      .filter(_.database.isEmpty)
    StringUtils.filterPattern(functions.map(_.unquotedString), pattern).map { f =>
      // In functionRegistry, function names are stored as an unquoted format.
      Try(parser.parseFunctionIdentifier(f)) match {
        case Success(e) => e
        case Failure(_) =>
          // The names of some built-in functions are not parsable by our parser, e.g., %
          FunctionIdentifier(f)
      }
    }
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
    val dbName = format(db)
    requireDbExists(dbName)
    val dbFunctions = externalCatalog.listFunctions(dbName, pattern).map { f =>
      FunctionIdentifier(f, Some(dbName)) }
    val loadedFunctions = listBuiltinAndTempFunctions(pattern)
    val functions = dbFunctions ++ loadedFunctions
    // The session catalog caches some persistent functions in the FunctionRegistry
    // so there can be duplicates.
    functions.map {
      case f if FunctionRegistry.functionSet.contains(f) => (f, "SYSTEM")
      case f if TableFunctionRegistry.functionSet.contains(f) => (f, "SYSTEM")
      case f if f.database.isDefined => (qualifyIdentifier(f), "USER")
      case f => (f, "USER")
    }.distinct
  }

  /**
   * List all temporary functions.
   */
  def listTemporaryFunctions(): Seq[FunctionIdentifier] = {
    (functionRegistry.listFunction() ++ tableFunctionRegistry.listFunction())
      .filter(isTemporaryFunction)
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
    // Temp functions are dropped below, we only need to drop permanent functions here.
    externalCatalog.listFunctions(DEFAULT_DATABASE, "*").map { f =>
      FunctionIdentifier(f, Some(DEFAULT_DATABASE))
    }.foreach(dropFunction(_, ignoreIfNotExists = false))
    clearTempTables()
    globalTempViewManager.clear()
    functionRegistry.clear()
    tableFunctionRegistry.clear()
    tableRelationCache.invalidateAll()
    // restore built-in functions
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
    // restore built-in table functions
    TableFunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = TableFunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = TableFunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      tableFunctionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
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
   * Validate the new location before renaming a managed table, which should be non-existent.
   */
  private def validateNewLocationOfRename(
      oldName: TableIdentifier,
      newName: TableIdentifier): Unit = {
    requireTableExists(oldName)
    requireTableNotExists(newName)
    val oldTable = getTableMetadata(oldName)
    if (oldTable.tableType == CatalogTableType.MANAGED) {
      assert(oldName.database.nonEmpty)
      val databaseLocation =
        externalCatalog.getDatabase(oldName.database.get).locationUri
      val newTableLocation = new Path(new Path(databaseLocation), format(newName.table))
      val fs = newTableLocation.getFileSystem(hadoopConf)
      if (fs.exists(newTableLocation)) {
        throw QueryExecutionErrors.locationAlreadyExists(newName, newTableLocation)
      }
    }
  }
}
