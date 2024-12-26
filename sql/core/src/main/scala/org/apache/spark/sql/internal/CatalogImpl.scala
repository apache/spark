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

package org.apache.spark.sql.internal

import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.sql._
import org.apache.spark.sql.catalog.{Catalog, CatalogMetadata, Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.DefinedByConstructorParams
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{ColumnDefinition, CreateTable, LocalRelation, LogicalPlan, OptionList, RecoverPartitions, ShowFunctions, ShowNamespaces, ShowTables, UnresolvedTableSpec, View}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogManager, SupportsNamespaces, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, MultipartIdentifierHelper, NamespaceHelper, TransformHelper}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.ShowTablesCommand
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._


/**
 * Internal implementation of the user-facing `Catalog`.
 */
class CatalogImpl(sparkSession: SparkSession) extends Catalog {

  private def sessionCatalog: SessionCatalog = sparkSession.sessionState.catalog

  /**
   * Helper function for parsing identifiers.
   * @param fallbackOnException if true, when parsing fails, return the original name.
   */
  private def parseIdent(name: String, fallbackOnException: Boolean = false): Seq[String] = {
    try {
      sparkSession.sessionState.sqlParser.parseMultipartIdentifier(name)
    } catch {
      case _: ParseException if fallbackOnException => Seq(name)
    }
  }

  private def qualifyV1Ident(nameParts: Seq[String]): Seq[String] = {
    assert(nameParts.length == 1 || nameParts.length == 2)
    if (nameParts.length == 1) {
      Seq(CatalogManager.SESSION_CATALOG_NAME, sessionCatalog.getCurrentDatabase) ++ nameParts
    } else {
      CatalogManager.SESSION_CATALOG_NAME +: nameParts
    }
  }

  /**
   * Returns the current default database in this session.
   */
  override def currentDatabase: String =
    sparkSession.sessionState.catalogManager.currentNamespace.quoted

  /**
   * Sets the current default database in this session.
   */
  @throws[AnalysisException]("database does not exist")
  override def setCurrentDatabase(dbName: String): Unit = {
    // we assume `dbName` will not include the catalog name. e.g. if you call
    // `setCurrentDatabase("catalog.db")`, it will search for a database 'catalog.db' in the current
    // catalog.
    sparkSession.sessionState.catalogManager.setCurrentNamespace(parseIdent(dbName).toArray)
  }

  /**
   * Returns a list of databases available across all sessions.
   */
  override def listDatabases(): Dataset[Database] = listDatabasesInternal(None)

  /**
   * Returns a list of databases (namespaces) which name match the specify pattern and
   * available within the current catalog.
   *
   * @since 3.5.0
   */
  override def listDatabases(pattern: String): Dataset[Database] =
    listDatabasesInternal(Some(pattern))

  private def listDatabasesInternal(patternOpt: Option[String]): Dataset[Database] = {
    val plan = ShowNamespaces(UnresolvedNamespace(Nil), patternOpt)
    val qe = sparkSession.sessionState.executePlan(plan)
    val catalog = qe.analyzed.collectFirst {
      case ShowNamespaces(r: ResolvedNamespace, _, _) => r.catalog
    }.get
    val databases = qe.toRdd.collect().map { row =>
      // dbName can either be a quoted identifier (single or multi part) or an unquoted single part
      val dbName = row.getString(0)
      makeDatabase(Some(catalog.name()), dbName)
    }
    CatalogImpl.makeDataset(databases.toImmutableArraySeq, sparkSession)
  }

  /**
   * Returns a list of tables in the current database.
   * This includes all temporary tables.
   */
  override def listTables(): Dataset[Table] = {
    listTables(currentDatabase)
  }

  /**
   * Returns a list of tables in the specified database.
   * This includes all temporary tables.
   */
  @throws[AnalysisException]("database does not exist")
  override def listTables(dbName: String): Dataset[Table] = {
    listTablesInternal(dbName, None)
  }

  /**
   * Returns a list of tables/views in the specified database (namespace)
   * which name match the specify pattern (the name can be qualified with catalog).
   * This includes all temporary views.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  override def listTables(dbName: String, pattern: String): Dataset[Table] = {
    listTablesInternal(dbName, Some(pattern))
  }

  private def listTablesInternal(dbName: String, pattern: Option[String]): Dataset[Table] = {
    val namespace = resolveNamespace(dbName)
    val plan = ShowTables(UnresolvedNamespace(namespace), pattern)
    makeTablesDataset(plan)
  }

  private def makeTablesDataset(plan: ShowTables): Dataset[Table] = {
    val qe = sparkSession.sessionState.executePlan(plan)
    val catalog = qe.analyzed.collectFirst {
      case ShowTables(r: ResolvedNamespace, _, _) => r.catalog
      case _: ShowTablesCommand =>
        sparkSession.sessionState.catalogManager.v2SessionCatalog
    }.get
    val tables = qe.toRdd.collect().flatMap { row => resolveTable(row, catalog.name()) }
    CatalogImpl.makeDataset(tables.toImmutableArraySeq, sparkSession)
  }

  private[sql] def resolveTable(row: InternalRow, catalogName: String): Option[Table] = {
    val tableName = row.getString(1)
    val namespaceName = row.getString(0)
    val isTemp = row.getBoolean(2)
    try {
      if (isTemp) {
        // Temp views do not belong to any catalog. We shouldn't prepend the catalog name here.
        val ns = if (namespaceName.isEmpty) Nil else Seq(namespaceName)
        Some(makeTable(ns :+ tableName))
      } else {
        val ns = parseIdent(namespaceName)
        try {
          Some(makeTable(catalogName +: ns :+ tableName))
        } catch {
          case e: AnalysisException if e.getCondition == "UNSUPPORTED_FEATURE.HIVE_TABLE_TYPE" =>
            Some(new Table(
              name = tableName,
              catalog = catalogName,
              namespace = ns.toArray,
              description = null,
              tableType = null,
              isTemporary = false
            ))
        }
      }
    } catch {
      case e: AnalysisException if e.getCondition == "TABLE_OR_VIEW_NOT_FOUND" => None
    }
  }

  private def tableExists(nameParts: Seq[String]): Boolean = {
    val plan = UnresolvedTableOrView(nameParts, "Catalog.tableExists", true)
    try {
      sparkSession.sessionState.executePlan(plan).analyzed match {
        case _: ResolvedTable => true
        case _: ResolvedPersistentView => true
        case _: ResolvedTempView => true
        case _ => false
      }
    } catch {
      case e: AnalysisException if e.getCondition == "TABLE_OR_VIEW_NOT_FOUND" => false
    }
  }

  private def makeTable(nameParts: Seq[String]): Table = {
    val plan = UnresolvedTableOrView(nameParts, "Catalog.makeTable", true)
    sparkSession.sessionState.executePlan(plan).analyzed match {
      case ResolvedTable(catalog, ident, table, _) =>
        val isExternal = table.properties().getOrDefault(
          TableCatalog.PROP_EXTERNAL, "false").equals("true")
        new Table(
          name = ident.name(),
          catalog = catalog.name(),
          namespace = ident.namespace(),
          description = table.properties().get("comment"),
          tableType =
            if (isExternal) CatalogTableType.EXTERNAL.name
            else CatalogTableType.MANAGED.name,
          isTemporary = false)
      case ResolvedPersistentView(catalog, identifier, metadata) =>
        new Table(
          name = identifier.name(),
          catalog = catalog.name(),
          namespace = identifier.namespace(),
          description = metadata.comment.orNull,
          tableType = "VIEW",
          isTemporary = false
        )
      case ResolvedTempView(identifier, _) =>
        new Table(
          name = identifier.name(),
          catalog = null,
          namespace = identifier.namespace(),
          description = null,
          tableType = "TEMPORARY",
          isTemporary = true
        )
      case _ => throw QueryCompilationErrors.tableOrViewNotFound(nameParts)
    }
  }

  /**
   * Returns a list of functions registered in the current database.
   * This includes all temporary functions
   */
  override def listFunctions(): Dataset[Function] = {
    listFunctions(currentDatabase)
  }

  /**
   * Returns a list of functions registered in the specified database.
   * This includes all temporary functions
   */
  @throws[AnalysisException]("database does not exist")
  override def listFunctions(dbName: String): Dataset[Function] = {
    listFunctionsInternal(dbName, None)
  }

  /**
   * Returns a list of functions registered in the specified database (namespace)
   * which name match the specify pattern (the name can be qualified with catalog).
   * This includes all built-in and temporary functions.
   *
   * @since 3.5.0
   */
  @throws[AnalysisException]("database does not exist")
  def listFunctions(dbName: String, pattern: String): Dataset[Function] = {
    listFunctionsInternal(dbName, Some(pattern))
  }

  private def listFunctionsInternal(dbName: String, pattern: Option[String]): Dataset[Function] = {
    val namespace = resolveNamespace(dbName)
    val functions = collection.mutable.ArrayBuilder.make[Function]

    // TODO: The SHOW FUNCTIONS should tell us the function type (built-in, temp, persistent) and
    //       we can simply the code below quite a bit. For now we need to list built-in functions
    //       separately as several built-in function names are not parsable, such as `!=`.

    // List built-in functions. We don't need to specify the namespace here as SHOW FUNCTIONS with
    // only system scope does not need to know the catalog and namespace.
    val plan0 = ShowFunctions(CurrentNamespace, false, true, pattern)
    sparkSession.sessionState.executePlan(plan0).toRdd.collect().foreach { row =>
      // Built-in functions do not belong to any catalog or namespace. We can only look it up with
      // a single part name.
      val name = row.getString(0)
      functions += makeFunction(Seq(name))
    }

    // List user functions.
    val plan1 = ShowFunctions(UnresolvedNamespace(namespace), true, false, pattern)
    sparkSession.sessionState.executePlan(plan1).toRdd.collect().foreach { row =>
      functions += makeFunction(parseIdent(row.getString(0)))
    }

    CatalogImpl.makeDataset(functions.result().toImmutableArraySeq, sparkSession)
  }

  private def toFunctionIdent(functionName: String): Seq[String] = {
    val parsed = parseIdent(functionName)
    // For backward compatibility (Spark 3.3 and prior), we should check if the function exists in
    // the Hive Metastore first.
    if (parsed.length <= 2 &&
      !sessionCatalog.isTemporaryFunction(parsed.asFunctionIdentifier) &&
      sessionCatalog.isPersistentFunction(parsed.asFunctionIdentifier)) {
      qualifyV1Ident(parsed)
    } else {
      parsed
    }
  }

  private def functionExists(ident: Seq[String]): Boolean = {
    val plan =
      UnresolvedFunctionName(ident, CatalogImpl.FUNCTION_EXISTS_COMMAND_NAME, false, None)
    try {
      sparkSession.sessionState.executePlan(plan).analyzed match {
        case _: ResolvedPersistentFunc => true
        case _: ResolvedNonPersistentFunc => true
        case _ => false
      }
    } catch {
      case e: AnalysisException if e.getCondition == "UNRESOLVED_ROUTINE" => false
    }
  }

  private def makeFunction(ident: Seq[String]): Function = {
    val plan = UnresolvedFunctionName(ident, "Catalog.makeFunction", false, None)
    sparkSession.sessionState.executePlan(plan).analyzed match {
      case f: ResolvedPersistentFunc =>
        val className = f.func match {
          case f: V1Function => f.info.getClassName
          case f => f.getClass.getName
        }
        new Function(
          name = f.identifier.name(),
          catalog = f.catalog.name(),
          namespace = f.identifier.namespace(),
          description = f.func.description(),
          className = className,
          isTemporary = false)

      case f: ResolvedNonPersistentFunc =>
        val className = f.func match {
          case f: V1Function => f.info.getClassName
          case f => f.getClass.getName
        }
        new Function(
          name = f.name,
          catalog = null,
          namespace = null,
          description = f.func.description(),
          className = className,
          isTemporary = true)

      case _ =>
        val catalogPath = (currentCatalog() +:
          sparkSession.sessionState.catalogManager.currentNamespace).mkString(".")
        throw QueryCompilationErrors.unresolvedRoutineError(ident, Seq(catalogPath), plan.origin)
    }
  }

  /**
   * Returns a list of columns for the given table/view or temporary view.
   */
  @throws[AnalysisException]("table does not exist")
  override def listColumns(tableName: String): Dataset[Column] = {
    val parsed = parseIdent(tableName)
    // For backward compatibility (Spark 3.3 and prior), we should check if the table exists in
    // the Hive Metastore first.
    val nameParts = if (parsed.length <= 2 && !sessionCatalog.isTempView(parsed) &&
      sessionCatalog.tableExists(parsed.asTableIdentifier)) {
      qualifyV1Ident(parsed)
    } else {
      parsed
    }
    listColumns(nameParts)
  }

  /**
   * Returns a list of columns for the given table/view or temporary view in the specified database.
   */
  @throws[AnalysisException]("database or table does not exist")
  override def listColumns(dbName: String, tableName: String): Dataset[Column] = {
    // For backward compatibility (Spark 3.3 and prior), here we always look up the table from the
    // Hive Metastore.
    listColumns(Seq(CatalogManager.SESSION_CATALOG_NAME, dbName, tableName))
  }

  private def listColumns(ident: Seq[String]): Dataset[Column] = {
    val plan = UnresolvedTableOrView(ident, "Catalog.listColumns", true)

    val columns = sparkSession.sessionState.executePlan(plan).analyzed match {
      case ResolvedTable(_, _, table, _) =>
        val (partitionColumnNames, bucketSpecOpt, clusterBySpecOpt) =
          table.partitioning.toImmutableArraySeq.convertTransforms
        val bucketColumnNames = bucketSpecOpt.map(_.bucketColumnNames).getOrElse(Nil)
        val clusteringColumnNames = clusterBySpecOpt.map { clusterBySpec =>
          clusterBySpec.columnNames.map(_.toString)
        }.getOrElse(Nil).toSet
        schemaToColumns(table.schema(), partitionColumnNames.contains, bucketColumnNames.contains,
          clusteringColumnNames.contains)

      case ResolvedPersistentView(_, _, metadata) =>
        schemaToColumns(metadata.schema)

      case ResolvedTempView(_, metadata) =>
        schemaToColumns(metadata.schema)

      case _ => throw QueryCompilationErrors.tableOrViewNotFound(ident)
    }

    CatalogImpl.makeDataset(columns, sparkSession)
  }

  private def schemaToColumns(
      schema: StructType,
      isPartCol: String => Boolean = _ => false,
      isBucketCol: String => Boolean = _ => false,
      isClusteringCol: String => Boolean = _ => false): Seq[Column] = {
    schema.map { field =>
      new Column(
        name = field.name,
        description = field.getComment().orNull,
        dataType = field.dataType.simpleString,
        nullable = field.nullable,
        isPartition = isPartCol(field.name),
        isBucket = isBucketCol(field.name),
        isCluster = isClusteringCol(field.name))
    }
  }

  /**
   * Gets the database with the specified name. This throws an `AnalysisException` when no
   * `Database` can be found.
   */
  override def getDatabase(dbName: String): Database = {
    makeDatabase(None, dbName)
  }

  // when catalogName is specified, dbName should be a valid quoted multi-part identifier, or a
  // valid unquoted single part identifier.
  private def makeDatabase(catalogNameOpt: Option[String], dbName: String): Database = {
    val idents = catalogNameOpt match {
      case Some(catalogName) => catalogName +: parseIdent(dbName, fallbackOnException = true)
      case None => resolveNamespace(dbName)
    }
    val plan = UnresolvedNamespace(idents, fetchMetadata = true)
    sparkSession.sessionState.executePlan(plan).analyzed match {
      case ResolvedNamespace(catalog, namespace, metadata) =>
        new Database(
          name = namespace.quoted,
          catalog = catalog.name,
          description = metadata.get(SupportsNamespaces.PROP_COMMENT).orNull,
          locationUri = metadata.get(SupportsNamespaces.PROP_LOCATION).orNull
        )
      case _ => new Database(name = dbName, description = null, locationUri = null)
    }
  }

  private def resolveNamespace(dbName: String): Seq[String] = {
    // `dbName` could be either a single database name (behavior in Spark 3.3 and prior) or
    // a qualified namespace with catalog name. We assume it's a single database name
    // and check if we can find it in the sessionCatalog. If so we list functions under
    // that database. Otherwise we will resolve the catalog/namespace and list functions there.
    if (sessionCatalog.databaseExists(dbName)) {
      Seq(CatalogManager.SESSION_CATALOG_NAME, dbName)
    } else {
      parseIdent(dbName)
    }
  }

  private def toTableIdent(tableName: String): Seq[String] = {
    val parsed = parseIdent(tableName)
    // For backward compatibility (Spark 3.3 and prior), we should check if the table exists in
    // the Hive Metastore first.
    if (parsed.length <= 2 && !sessionCatalog.isTempView(parsed) &&
      sessionCatalog.tableExists(parsed.asTableIdentifier)) {
      qualifyV1Ident(parsed)
    } else {
      parsed
    }
  }

  /**
   * Gets the table or view with the specified name. This table can be a temporary view or a
   * table/view. This throws an `AnalysisException` when no `Table` can be found.
   */
  override def getTable(tableName: String): Table = {
    makeTable(toTableIdent(tableName))
  }

  /**
   * Gets the table or view with the specified name in the specified database. This throws an
   * `AnalysisException` when no `Table` can be found.
   */
  override def getTable(dbName: String, tableName: String): Table = {
    if (sessionCatalog.isGlobalTempViewDB(dbName)) {
      makeTable(Seq(dbName, tableName))
    } else {
      // For backward compatibility (Spark 3.3 and prior), here we always look up the table from the
      // Hive Metastore.
      makeTable(Seq(CatalogManager.SESSION_CATALOG_NAME, dbName, tableName))
    }
  }

  /**
   * Gets the function with the specified name. This function can be a temporary function or a
   * function. This throws an `AnalysisException` when no `Function` can be found.
   */
  override def getFunction(functionName: String): Function = {
    makeFunction(toFunctionIdent(functionName))
  }

  /**
   * Gets the function with the specified name. This returns `None` when no `Function` can be
   * found.
   */
  override def getFunction(dbName: String, functionName: String): Function = {
    // For backward compatibility (Spark 3.3 and prior), here we always look up the function from
    // the Hive Metastore.
    makeFunction(Seq(CatalogManager.SESSION_CATALOG_NAME, dbName, functionName))
  }

  /**
   * Checks if the database with the specified name exists.
   */
  override def databaseExists(dbName: String): Boolean = {
    try {
      getDatabase(dbName)
      true
    } catch {
      case _: NoSuchNamespaceException => false
    }
  }

  /**
   * Checks if the table or view with the specified name exists. This can either be a temporary
   * view or a table/view.
   */
  override def tableExists(tableName: String): Boolean = {
    tableExists(toTableIdent(tableName))
  }

  /**
   * Checks if the table or view with the specified name exists in the specified database.
   */
  override def tableExists(dbName: String, tableName: String): Boolean = {
    if (sessionCatalog.isGlobalTempViewDB(dbName)) {
      tableExists(Seq(dbName, tableName))
    } else {
      // For backward compatibility (Spark 3.3 and prior), here we always look up the table from the
      // Hive Metastore.
      tableExists(Seq(CatalogManager.SESSION_CATALOG_NAME, dbName, tableName))
    }
  }

  /**
   * Checks if the function with the specified name exists. This can either be a temporary function
   * or a function.
   */
  override def functionExists(functionName: String): Boolean = {
    functionExists(toFunctionIdent(functionName))
  }

  /**
   * Checks if the function with the specified name exists in the specified database.
   */
  override def functionExists(dbName: String, functionName: String): Boolean = {
    // For backward compatibility (Spark 3.3 and prior), here we always look up the function from
    // the Hive Metastore.
    functionExists(Seq(CatalogManager.SESSION_CATALOG_NAME, dbName, functionName))
  }

  /**
   * Creates a table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  override def createTable(tableName: String, path: String): DataFrame = {
    val dataSourceName = sparkSession.sessionState.conf.defaultDataSourceName
    createTable(tableName, path, dataSourceName)
  }

  /**
   * Creates a table from the given path and returns the corresponding
   * DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  override def createTable(tableName: String, path: String, source: String): DataFrame = {
    createTable(tableName, source, Map("path" -> path))
  }

  /**
   * (Scala-specific)
   * Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  override def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    createTable(tableName, source, new StructType, options)
  }

  /**
   * (Scala-specific)
   * Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 3.1.0
   */
  override def createTable(
      tableName: String,
      source: String,
      description: String,
      options: Map[String, String]): DataFrame = {
    createTable(tableName, source, new StructType, description, options)
  }

  /**
   * (Scala-specific)
   * Creates a table based on the dataset in a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    createTable(
      tableName = tableName,
      source = source,
      schema = schema,
      description = "",
      options = options
    )
  }

  /**
   * (Scala-specific)
   * Creates a table based on the dataset in a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 3.1.0
   */
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: Map[String, String]): DataFrame = {
    val ident = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val storage = DataSource.buildStorageFormatFromOptions(options)
    val tableType = if (storage.locationUri.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    // The location in UnresolvedTableSpec should be the original user-provided path string.
    val location = CaseInsensitiveMap(options).get("path")

    val newOptions = OptionList(options.map { case (key, value) =>
      (key, Literal(value).asInstanceOf[Expression])
    }.toSeq)
    val tableSpec = UnresolvedTableSpec(
      properties = Map(),
      provider = Some(source),
      optionExpression = newOptions,
      location = location,
      comment = { if (description.isEmpty) None else Some(description) },
      serde = None,
      external = tableType == CatalogTableType.EXTERNAL)

    val plan = CreateTable(
      name = UnresolvedIdentifier(ident),
      columns = schema.map(ColumnDefinition.fromV1Column(_, sparkSession.sessionState.sqlParser)),
      partitioning = Seq(),
      tableSpec = tableSpec,
      ignoreIfExists = false)

    sparkSession.sessionState.executePlan(plan).toRdd
    sparkSession.table(tableName)
  }

  /**
   * Drops the local temporary view with the given view name in the catalog.
   * If the view has been cached/persisted before, it's also unpersisted.
   *
   * @param viewName the identifier of the temporary view to be dropped.
   * @group ddl_ops
   * @since 2.0.0
   */
  override def dropTempView(viewName: String): Boolean = {
    sparkSession.sessionState.catalog.getTempView(viewName).exists { viewDef =>
      uncacheView(viewDef)
      sessionCatalog.dropTempView(viewName)
    }
  }

  /**
   * Drops the global temporary view with the given view name in the catalog.
   * If the view has been cached/persisted before, it's also unpersisted.
   *
   * @param viewName the identifier of the global temporary view to be dropped.
   * @group ddl_ops
   * @since 2.1.0
   */
  override def dropGlobalTempView(viewName: String): Boolean = {
    sparkSession.sessionState.catalog.getGlobalTempView(viewName).exists { viewDef =>
      uncacheView(viewDef)
      sessionCatalog.dropGlobalTempView(viewName)
    }
  }

  private def uncacheView(viewDef: View): Unit = {
    try {
      // If view text is defined, it means we are not storing analyzed logical plan for the view
      // and instead its behavior follows that of a permanent view (see SPARK-33142 for more
      // details). Therefore, when uncaching the view we should also do in a cascade fashion, the
      // same way as how a permanent view is handled. This also avoids a potential issue where a
      // dependent view becomes invalid because of the above while its data is still cached.
      val viewText = viewDef.desc.viewText
      val df = Dataset.ofRows(sparkSession, viewDef)
      sparkSession.sharedState.cacheManager.uncacheQuery(df, cascade = viewText.isDefined)
    } catch {
      case NonFatal(_) => // ignore
    }
  }

  /**
   * Recovers all the partitions in the directory of a table and update the catalog.
   * Only works with a partitioned table, and not a temporary view.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in the
   *                  current database.
   * @group ddl_ops
   * @since 2.1.1
   */
  override def recoverPartitions(tableName: String): Unit = {
    val multiPartIdent = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    sparkSession.sessionState.executePlan(
      RecoverPartitions(
        UnresolvedTable(multiPartIdent, "recoverPartitions()"))).toRdd
  }

  /**
   * Returns true if the table or view is currently cached in-memory.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def isCached(tableName: String): Boolean = {
    sparkSession.sharedState.cacheManager.lookupCachedData(sparkSession.table(tableName)).nonEmpty
  }

  /**
   * Persist the specified table or view with the default storage level,
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def cacheTable(tableName: String): Unit = {
    cacheTable(tableName, sparkSession.sessionState.conf.defaultCacheStorageLevel)
  }

  /**
   * Caches the specified table or view with the given storage level.
   *
   * @group cachemgmt
   * @since 2.3.0
   */
  override def cacheTable(tableName: String, storageLevel: StorageLevel): Unit = {
    sparkSession.sharedState.cacheManager.cacheQuery(
      sparkSession.table(tableName), Some(tableName), storageLevel)
  }

  /**
   * Removes the specified table or view from the in-memory cache.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def uncacheTable(tableName: String): Unit = {
    // We first try to parse `tableName` to see if it is 2 part name. If so, then in HMS we check
    // if it is a temp view and uncache the temp view from HMS, otherwise we uncache it from the
    // cache manager.
    // if `tableName` is not 2 part name, then we directly uncache it from the cache manager.
    try {
      val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
      sessionCatalog.getLocalOrGlobalTempView(tableIdent).map(uncacheView).getOrElse {
        sparkSession.sharedState.cacheManager.uncacheQuery(sparkSession.table(tableName),
          cascade = true)
      }
    } catch {
      case e: org.apache.spark.sql.catalyst.parser.ParseException =>
        sparkSession.sharedState.cacheManager.uncacheQuery(sparkSession.table(tableName),
          cascade = true)
    }
  }

  /**
   * Removes all cached tables or views from the in-memory cache.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def clearCache(): Unit = {
    sparkSession.sharedState.cacheManager.clearCache()
  }

  /**
   * Returns true if the [[Dataset]] is currently cached in-memory.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  protected[sql] def isCached(qName: Dataset[_]): Boolean = {
    sparkSession.sharedState.cacheManager.lookupCachedData(qName).nonEmpty
  }

  /**
   * The method fully refreshes a table or view with the given name including:
   *   1. The relation cache in the session catalog. The method removes table entry from the cache.
   *   2. The file indexes of all relations used by the given view.
   *   3. Table/View schema in the Hive Metastore if the SQL config
   *      `spark.sql.hive.caseSensitiveInferenceMode` is set to `INFER_AND_SAVE`.
   *   4. Cached data of the given table or view, and all its dependents that refer to it.
   *      Existing cached data will be cleared and the cache will be lazily filled when
   *      the next time the table/view or the dependents are accessed.
   *
   * The method does not do:
   *   - schema inference for file source tables
   *   - statistics update
   *
   * The method is supposed to be used in all cases when need to refresh table/view data
   * and meta-data.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def refreshTable(tableName: String): Unit = {
    val relation = sparkSession.table(tableName).queryExecution.analyzed

    relation.refresh()

    // Temporary and global temporary views are not supposed to be put into the relation cache
    // since they are tracked separately. V1 and V2 plans are cache invalidated accordingly.
    def invalidateCache(plan: LogicalPlan): Unit = plan match {
      case v: View =>
        if (!v.isTempView) sessionCatalog.invalidateCachedTable(v.desc.identifier)
      case r: LogicalRelation =>
        sessionCatalog.invalidateCachedTable(r.catalogTable.get.identifier)
      case h: HiveTableRelation =>
        sessionCatalog.invalidateCachedTable(h.tableMeta.identifier)
      case r: DataSourceV2Relation =>
        r.catalog.get.asTableCatalog.invalidateTable(r.identifier.get)
      case _ => plan.children.foreach(invalidateCache)
    }
    invalidateCache(relation)

    // Re-caches the logical plan of the relation.
    // Note this is a no-op for the relation itself if it's not cached, but will clear all
    // caches referencing this relation. If this relation is cached as an InMemoryRelation,
    // this will clear the relation cache and caches of all its dependents.
    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, relation)
  }

  /**
   * Refreshes the cache entry and the associated metadata for all Dataset (if any), that contain
   * the given data source path. Path matching is by prefix, i.e. "/" would invalidate
   * everything that is cached.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def refreshByPath(resourcePath: String): Unit = {
    sparkSession.sharedState.cacheManager.recacheByPath(sparkSession, resourcePath)
  }

  /**
   * Returns the current default catalog in this session.
   *
   * @since 3.4.0
   */
  override def currentCatalog(): String = {
    sparkSession.sessionState.catalogManager.currentCatalog.name()
  }

  /**
   * Sets the current default catalog in this session.
   *
   * @since 3.4.0
   */
  override def setCurrentCatalog(catalogName: String): Unit = {
    sparkSession.sessionState.catalogManager.setCurrentCatalog(catalogName)
  }

  /**
   * Returns a list of catalogs in this session.
   *
   * @since 3.4.0
   */
  override def listCatalogs(): Dataset[CatalogMetadata] = {
    val catalogs = sparkSession.sessionState.catalogManager.listCatalogs(None)
    CatalogImpl.makeDataset(catalogs.map(name => makeCatalog(name)), sparkSession)
  }

  /**
   * Returns a list of catalogs which name match the specify pattern and available in this session.
   *
   * @since 3.5.0
   */
  override def listCatalogs(pattern: String): Dataset[CatalogMetadata] = {
    val catalogs = sparkSession.sessionState.catalogManager.listCatalogs(Some(pattern))
    CatalogImpl.makeDataset(catalogs.map(name => makeCatalog(name)), sparkSession)
  }

  private def makeCatalog(name: String): CatalogMetadata = {
    new CatalogMetadata(
      name = name,
      description = null)
  }
}


private[sql] object CatalogImpl {

  def makeDataset[T <: DefinedByConstructorParams: TypeTag](
      data: Seq[T],
      sparkSession: SparkSession): Dataset[T] = {
    val enc = ExpressionEncoder[T]()
    val toRow = enc.createSerializer()
    val encoded = data.map(d => toRow(d).copy())
    val plan = new LocalRelation(DataTypeUtils.toAttributes(enc.schema), encoded)
    val queryExecution = sparkSession.sessionState.executePlan(plan)
    new Dataset[T](queryExecution, enc)
  }

  private val FUNCTION_EXISTS_COMMAND_NAME = "Catalog.functionExists"
}
