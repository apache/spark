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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkException
import org.apache.spark.internal.LogKeys.CONFIG
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils, ClusterBySpec}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, toPrettySQL, ResolveDefaultColumns => DefaultCols}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, CatalogV2Util, DelegatingCatalogExtension, LookupCatalog, SupportsNamespaces, TableCatalog, V1Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable => CreateTableV1}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * Converts resolved v2 commands to v1 if the catalog is the session catalog. Since the v2 commands
 * are resolved, the referred tables/views/functions are resolved as well. This rule uses qualified
 * identifiers to construct the v1 commands, so that v1 commands do not need to qualify identifiers
 * again, which may lead to inconsistent behavior if the current database is changed in the middle.
 */
class ResolveSessionCatalog(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util._
  import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case AddColumns(ResolvedV1TableIdentifier(ident), cols) =>
      cols.foreach { c =>
        if (c.name.length > 1) {
          throw QueryCompilationErrors.unsupportedTableOperationError(
            ident, "ADD COLUMN with qualified column")
        }
        if (!c.nullable) {
          throw QueryCompilationErrors.addColumnWithV1TableCannotSpecifyNotNullError()
        }
      }
      AlterTableAddColumnsCommand(ident, cols.map(convertToStructField))

    case ReplaceColumns(ResolvedV1TableIdentifier(ident), _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(ident, "REPLACE COLUMNS")

    case a @ AlterColumn(ResolvedTable(catalog, ident, table: V1Table, _), _, _, _, _, _, _)
        if supportsV1Command(catalog) =>
      if (a.column.name.length > 1) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, "ALTER COLUMN with qualified column")
      }
      if (a.nullable.isDefined) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, "ALTER COLUMN ... SET NOT NULL")
      }
      if (a.position.isDefined) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, "ALTER COLUMN ... FIRST | AFTER")
      }
      val builder = new MetadataBuilder
      // Add comment to metadata
      a.comment.map(c => builder.putString("comment", c))
      val colName = a.column.name(0)
      val dataType = a.dataType.getOrElse {
        table.schema.findNestedField(Seq(colName), resolver = conf.resolver)
          .map(_._2.dataType)
          .getOrElse {
            throw QueryCompilationErrors.unresolvedColumnError(
              toSQLId(a.column.name), table.schema.fieldNames)
          }
      }
      // Add the current default column value string (if any) to the column metadata.
      a.setDefaultExpression.map { c => builder.putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, c) }
      val newColumn = StructField(
        colName,
        dataType,
        nullable = true,
        builder.build())
      AlterTableChangeColumnCommand(table.catalogTable.identifier, colName, newColumn)

    case AlterTableClusterBy(ResolvedTable(catalog, _, table: V1Table, _), clusterBySpecOpt)
        if supportsV1Command(catalog) =>
      val prop = Map(ClusterBySpec.toProperty(table.schema,
        clusterBySpecOpt.getOrElse(ClusterBySpec(Nil)), conf.resolver))
      AlterTableSetPropertiesCommand(table.catalogTable.identifier, prop, isView = false)

    case SetTableCollation(ResolvedTable(catalog, _, table: V1Table, _), collation)
      if supportsV1Command(catalog) =>
      AlterTableSetPropertiesCommand(
        table.catalogTable.identifier,
        Map(TableCatalog.PROP_COLLATION -> collation),
        isView = false)

    case RenameColumn(ResolvedV1TableIdentifier(ident), _, _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(ident, "RENAME COLUMN")

    case DropColumns(ResolvedV1TableIdentifier(ident), _, _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(ident, "DROP COLUMN")

    case SetTableProperties(ResolvedV1TableIdentifier(ident), props) =>
      AlterTableSetPropertiesCommand(ident, props, isView = false)

    case UnsetTableProperties(ResolvedV1TableIdentifier(ident), keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(ident, keys, ifExists, isView = false)

    case SetViewProperties(ResolvedViewIdentifier(ident), props) =>
      AlterTableSetPropertiesCommand(ident, props, isView = true)

    case UnsetViewProperties(ResolvedViewIdentifier(ident), keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(ident, keys, ifExists, isView = true)

    case DescribeNamespace(ResolvedV1Database(db), extended, output) if conf.useV1Command =>
      DescribeDatabaseCommand(db, extended, output)

    case SetNamespaceProperties(ResolvedV1Database(db), properties) if conf.useV1Command =>
      AlterDatabasePropertiesCommand(db, properties)

    case SetNamespaceLocation(ResolvedV1Database(db), location) if conf.useV1Command =>
      AlterDatabaseSetLocationCommand(db, location)

    case SetNamespaceCollation(ResolvedV1Database(db), collation) if conf.useV1Command =>
      SetDatabaseCollationCommand(db, collation)

    case RenameTable(ResolvedV1TableOrViewIdentifier(oldIdent), newName, isView) =>
      AlterTableRenameCommand(oldIdent, newName.asTableIdentifier, isView)

    // Use v1 command to describe (temp) view, as v2 catalog doesn't support view yet.
    case DescribeRelation(
         ResolvedV1TableOrViewIdentifier(ident), partitionSpec, isExtended, output) =>
      DescribeTableCommand(ident, partitionSpec, isExtended, output)

    case DescribeColumn(
         ResolvedViewIdentifier(ident), column: UnresolvedAttribute, isExtended, output) =>
      // For views, the column will not be resolved by `ResolveReferences` because
      // `ResolvedView` stores only the identifier.
      DescribeColumnCommand(ident, column.nameParts, isExtended, output)

    case DescribeColumn(ResolvedV1TableIdentifier(ident), column, isExtended, output) =>
      column match {
        case u: UnresolvedAttribute =>
          throw QueryCompilationErrors.columnNotFoundError(u.name)
        case a: Attribute =>
          DescribeColumnCommand(ident, a.qualifier :+ a.name, isExtended, output)
        case Alias(child, _) =>
          throw QueryCompilationErrors.commandNotSupportNestedColumnError(
            "DESC TABLE COLUMN", toPrettySQL(child))
        case _ =>
          throw SparkException.internalError(s"[BUG] unexpected column expression: $column")
      }

    // For CREATE TABLE [AS SELECT], we should use the v1 command if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c @ CreateTable(ResolvedV1Identifier(ident), _, _, tableSpec: TableSpec, _)
        if c.resolved =>
      val (storageFormat, provider) = getStorageFormatAndProvider(
        c.tableSpec.provider, tableSpec.options, c.tableSpec.location, c.tableSpec.serde,
        ctas = false)
      if (!isV2Provider(provider)) {
        constructV1TableCmd(None, c.tableSpec, ident, c.tableSchema, c.partitioning,
          c.ignoreIfExists, storageFormat, provider)
      } else {
        c
      }

    case c @ CreateTableAsSelect(
        ResolvedV1Identifier(ident), _, _, tableSpec: TableSpec, writeOptions, _, _) =>
      val (storageFormat, provider) = getStorageFormatAndProvider(
        c.tableSpec.provider,
        tableSpec.options ++ writeOptions,
        c.tableSpec.location,
        c.tableSpec.serde,
        ctas = true)

      if (!isV2Provider(provider)) {
        constructV1TableCmd(Some(c.query), c.tableSpec, ident, new StructType, c.partitioning,
          c.ignoreIfExists, storageFormat, provider)
      } else {
        c
      }

    case RefreshTable(ResolvedV1TableOrViewIdentifier(ident)) =>
      RefreshTableCommand(ident)

    // For REPLACE TABLE [AS SELECT], we should fail if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c @ ReplaceTable(ResolvedV1Identifier(ident), _, _, _, _) if c.resolved =>
      val provider = c.tableSpec.provider.getOrElse(conf.defaultDataSourceName)
      if (!isV2Provider(provider)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          ident, "REPLACE TABLE")
      } else {
        c
      }

    case c @ ReplaceTableAsSelect(ResolvedV1Identifier(ident), _, _, _, _, _, _) =>
      val provider = c.tableSpec.provider.getOrElse(conf.defaultDataSourceName)
      if (!isV2Provider(provider)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          ident, "REPLACE TABLE AS SELECT")
      } else {
        c
      }

    case DropTable(ResolvedV1Identifier(ident), ifExists, purge) if conf.useV1Command =>
      DropTableCommand(ident, ifExists, isView = false, purge = purge)

    // v1 DROP TABLE supports temp view.
    case DropTable(ResolvedIdentifier(FakeSystemCatalog, ident), _, _) =>
      DropTempViewCommand(ident)

    case DropView(ResolvedIdentifierInSessionCatalog(ident), ifExists) =>
      DropTableCommand(ident, ifExists, isView = true, purge = false)

    case DropView(r @ ResolvedIdentifier(catalog, ident), _) =>
      if (catalog == FakeSystemCatalog) {
        DropTempViewCommand(ident)
      } else {
        throw QueryCompilationErrors.catalogOperationNotSupported(catalog, "views")
      }

    case c @ CreateNamespace(DatabaseNameInSessionCatalog(name), _, _) if conf.useV1Command =>
      val comment = c.properties.get(SupportsNamespaces.PROP_COMMENT)
      val collation = c.properties.get(SupportsNamespaces.PROP_COLLATION)
      val location = c.properties.get(SupportsNamespaces.PROP_LOCATION)
      val newProperties = c.properties -- CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES
      CreateDatabaseCommand(name, c.ifNotExists, location, comment, collation, newProperties)

    case d @ DropNamespace(ResolvedV1Database(db), _, _) if conf.useV1Command =>
      DropDatabaseCommand(db, d.ifExists, d.cascade)

    case ShowTables(ResolvedV1Database(db), pattern, output) if conf.useV1Command =>
      ShowTablesCommand(Some(db), pattern, output)

    case ShowTablesExtended(
        ResolvedV1Database(db),
        pattern,
        output) =>
      val newOutput = if (conf.getConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA)) {
        output.head.withName("database") +: output.tail
      } else {
        output
      }
      ShowTablesCommand(Some(db), Some(pattern), newOutput, isExtended = true)

    case ShowTablePartition(
        ResolvedTable(catalog, _, table: V1Table, _),
        partitionSpec,
        output) if supportsV1Command(catalog) =>
      val newOutput = if (conf.getConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA)) {
        output.head.withName("database") +: output.tail
      } else {
        output
      }
      val tablePartitionSpec = Option(partitionSpec).map(
        _.asInstanceOf[UnresolvedPartitionSpec].spec)
      ShowTablesCommand(table.catalogTable.identifier.database,
        Some(table.catalogTable.identifier.table), newOutput,
        isExtended = true, tablePartitionSpec)

    // ANALYZE TABLE works on permanent views if the views are cached.
    case AnalyzeTable(ResolvedV1TableOrViewIdentifier(ident), partitionSpec, noScan) =>
      if (partitionSpec.isEmpty) {
        AnalyzeTableCommand(ident, noScan)
      } else {
        AnalyzePartitionCommand(ident, partitionSpec, noScan)
      }

    case AnalyzeTables(ResolvedV1Database(db), noScan) =>
      AnalyzeTablesCommand(Some(db), noScan)

    case AnalyzeColumn(ResolvedV1TableOrViewIdentifier(ident), columnNames, allColumns) =>
      AnalyzeColumnCommand(ident, columnNames, allColumns)

    // V2 catalog doesn't support REPAIR TABLE yet, we must use v1 command here.
    case RepairTable(
        ResolvedV1TableIdentifierInSessionCatalog(ident),
        addPartitions,
        dropPartitions) =>
      RepairTableCommand(ident, addPartitions, dropPartitions)

    // V2 catalog doesn't support LOAD DATA yet, we must use v1 command here.
    case LoadData(
        ResolvedV1TableIdentifierInSessionCatalog(ident),
        path,
        isLocal,
        isOverwrite,
        partition) =>
      LoadDataCommand(
        ident,
        path,
        isLocal,
        isOverwrite,
        partition)

    case ShowCreateTable(ResolvedV1TableOrViewIdentifier(ident), asSerde, output) if asSerde =>
      ShowCreateTableAsSerdeCommand(ident, output)

    // If target is view, force use v1 command
    case ShowCreateTable(ResolvedViewIdentifier(ident), _, output) =>
      ShowCreateTableCommand(ident, output)

    case ShowCreateTable(ResolvedV1TableIdentifier(ident), _, output)
      if conf.useV1Command => ShowCreateTableCommand(ident, output)

    case ShowCreateTable(ResolvedTable(catalog, _, table: V1Table, _), _, output)
        if supportsV1Command(catalog) && DDLUtils.isHiveTable(table.catalogTable) =>
      ShowCreateTableCommand(table.catalogTable.identifier, output)

    case TruncateTable(ResolvedV1TableIdentifier(ident)) =>
      TruncateTableCommand(ident, None)

    case TruncatePartition(ResolvedV1TableIdentifier(ident), partitionSpec) =>
      TruncateTableCommand(
        ident,
        Seq(partitionSpec).asUnresolvedPartitionSpecs.map(_.spec).headOption)

    case ShowPartitions(
        ResolvedV1TableOrViewIdentifier(ident),
        pattern @ (None | Some(UnresolvedPartitionSpec(_, _))), output) =>
      ShowPartitionsCommand(
        ident,
        output,
        pattern.map(_.asInstanceOf[UnresolvedPartitionSpec].spec))

    case ShowColumns(ResolvedV1TableOrViewIdentifier(ident), ns, output) =>
      val v1TableName = ident
      val resolver = conf.resolver
      val db = ns match {
        case Some(db) if v1TableName.database.exists(!resolver(_, db.head)) =>
          throw QueryCompilationErrors.showColumnsWithConflictNamespacesError(
            Seq(db.head), Seq(v1TableName.database.get))
        case _ => ns.map(_.head)
      }
      ShowColumnsCommand(db, v1TableName, output)

    // V2 catalog doesn't support RECOVER PARTITIONS yet, we must use v1 command here.
    case RecoverPartitions(ResolvedV1TableIdentifierInSessionCatalog(ident)) =>
      RepairTableCommand(
        ident,
        enableAddPartitions = true,
        enableDropPartitions = false,
        "ALTER TABLE RECOVER PARTITIONS")

    case AddPartitions(ResolvedV1TableIdentifier(ident), partSpecsAndLocs, ifNotExists) =>
      AlterTableAddPartitionCommand(
        ident,
        partSpecsAndLocs.asUnresolvedPartitionSpecs.map(spec => (spec.spec, spec.location)),
        ifNotExists)

    case RenamePartitions(
        ResolvedV1TableIdentifier(ident),
        UnresolvedPartitionSpec(from, _),
        UnresolvedPartitionSpec(to, _)) =>
      AlterTableRenamePartitionCommand(ident, from, to)

    case DropPartitions(
        ResolvedV1TableIdentifier(ident), specs, ifExists, purge) =>
      AlterTableDropPartitionCommand(
        ident,
        specs.asUnresolvedPartitionSpecs.map(_.spec),
        ifExists,
        purge,
        retainData = false)

    // V2 catalog doesn't support setting serde properties yet, we must use v1 command here.
    case SetTableSerDeProperties(
        ResolvedV1TableIdentifierInSessionCatalog(ident),
        serdeClassName,
        serdeProperties,
        partitionSpec) =>
      AlterTableSerDePropertiesCommand(
        ident,
        serdeClassName,
        serdeProperties,
        partitionSpec)

    case SetTableLocation(ResolvedV1TableIdentifier(ident), None, location) =>
      AlterTableSetLocationCommand(ident, None, location)

    // V2 catalog doesn't support setting partition location yet, we must use v1 command here.
    case SetTableLocation(
        ResolvedV1TableIdentifierInSessionCatalog(ident),
        Some(partitionSpec),
        location) =>
      AlterTableSetLocationCommand(ident, Some(partitionSpec), location)

    case AlterViewAs(ResolvedViewIdentifier(ident), originalText, query) =>
      AlterViewAsCommand(ident, originalText, query)

    case AlterViewSchemaBinding(ResolvedViewIdentifier(ident), viewSchemaMode) =>
      AlterViewSchemaBindingCommand(ident, viewSchemaMode)

    case CreateView(ResolvedIdentifierInSessionCatalog(ident), userSpecifiedColumns, comment,
        properties, originalText, child, allowExisting, replace, viewSchemaMode) =>
      CreateViewCommand(
        name = ident,
        userSpecifiedColumns = userSpecifiedColumns,
        comment = comment,
        properties = properties,
        originalText = originalText,
        plan = child,
        allowExisting = allowExisting,
        replace = replace,
        viewType = PersistedView,
        viewSchemaMode = viewSchemaMode)

    case CreateView(ResolvedIdentifier(catalog, _), _, _, _, _, _, _, _, _) =>
      throw QueryCompilationErrors.missingCatalogAbilityError(catalog, "views")

    case ShowViews(ns: ResolvedNamespace, pattern, output) =>
      ns match {
        case ResolvedDatabaseInSessionCatalog(db) => ShowViewsCommand(db, pattern, output)
        case _ =>
          throw QueryCompilationErrors.missingCatalogAbilityError(ns.catalog, "views")
      }

    // If target is view, force use v1 command
    case ShowTableProperties(ResolvedViewIdentifier(ident), propertyKey, output) =>
      ShowTablePropertiesCommand(ident, propertyKey, output)

    case ShowTableProperties(ResolvedV1TableIdentifier(ident), propertyKey, output)
        if conf.useV1Command =>
      ShowTablePropertiesCommand(ident, propertyKey, output)

    case DescribeFunction(ResolvedNonPersistentFunc(_, V1Function(info)), extended) =>
      DescribeFunctionCommand(info, extended)

    case DescribeFunction(ResolvedPersistentFunc(catalog, _, func), extended) =>
      if (isSessionCatalog(catalog)) {
        DescribeFunctionCommand(func.asInstanceOf[V1Function].info, extended)
      } else {
        throw QueryCompilationErrors.missingCatalogAbilityError(catalog, "functions")
      }

    case ShowFunctions(
        ResolvedDatabaseInSessionCatalog(db), userScope, systemScope, pattern, output) =>
      ShowFunctionsCommand(db, pattern, userScope, systemScope, output)

    case DropFunction(ResolvedPersistentFunc(catalog, identifier, _), ifExists) =>
      if (isSessionCatalog(catalog)) {
        val funcIdentifier = catalogManager.v1SessionCatalog.qualifyIdentifier(
          identifier.asFunctionIdentifier)
        DropFunctionCommand(funcIdentifier, ifExists, false)
      } else {
        throw QueryCompilationErrors.missingCatalogAbilityError(catalog, "DROP FUNCTION")
      }

    case RefreshFunction(ResolvedPersistentFunc(catalog, identifier, _)) =>
      if (isSessionCatalog(catalog)) {
        val funcIdentifier = catalogManager.v1SessionCatalog.qualifyIdentifier(
          identifier.asFunctionIdentifier)
        RefreshFunctionCommand(funcIdentifier.database, funcIdentifier.funcName)
      } else {
        throw QueryCompilationErrors.missingCatalogAbilityError(catalog, "REFRESH FUNCTION")
      }

    case CreateFunction(
        ResolvedIdentifierInSessionCatalog(ident), className, resources, ifExists, replace) =>
      CreateFunctionCommand(
        FunctionIdentifier(ident.table, ident.database, ident.catalog),
        className,
        resources,
        false,
        ifExists,
        replace)

    case CreateFunction(ResolvedIdentifier(catalog, _), _, _, _, _) =>
      throw QueryCompilationErrors.missingCatalogAbilityError(catalog, "CREATE FUNCTION")
  }

  private def constructV1TableCmd(
      query: Option[LogicalPlan],
      tableSpec: TableSpecBase,
      ident: TableIdentifier,
      tableSchema: StructType,
      partitioning: Seq[Transform],
      ignoreIfExists: Boolean,
      storageFormat: CatalogStorageFormat,
      provider: String): CreateTableV1 = {
    val tableDesc = buildCatalogTable(
      ident, tableSchema, partitioning, tableSpec.properties, provider, tableSpec.location,
      tableSpec.comment, tableSpec.collation, storageFormat, tableSpec.external)
    val mode = if (ignoreIfExists) SaveMode.Ignore else SaveMode.ErrorIfExists
    CreateTableV1(tableDesc, mode, query)
  }

  private def getStorageFormatAndProvider(
      provider: Option[String],
      options: Map[String, String],
      location: Option[String],
      maybeSerdeInfo: Option[SerdeInfo],
      ctas: Boolean): (CatalogStorageFormat, String) = {
    val nonHiveStorageFormat = CatalogStorageFormat.empty.copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = options)
    val defaultHiveStorage = HiveSerDe.getDefaultStorage(conf).copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = options)

    if (provider.isDefined) {
      // The parser guarantees that USING and STORED AS/ROW FORMAT won't co-exist.
      if (maybeSerdeInfo.isDefined) {
        throw QueryCompilationErrors.cannotCreateTableWithBothProviderAndSerdeError(
          provider, maybeSerdeInfo)
      }
      (nonHiveStorageFormat, provider.get)
    } else if (maybeSerdeInfo.isDefined) {
      val serdeInfo = maybeSerdeInfo.get
      SerdeInfo.checkSerdePropMerging(serdeInfo.serdeProperties, defaultHiveStorage.properties)
      val storageFormat = if (serdeInfo.storedAs.isDefined) {
        // If `STORED AS fileFormat` is used, infer inputFormat, outputFormat and serde from it.
        HiveSerDe.sourceToSerDe(serdeInfo.storedAs.get) match {
          case Some(hiveSerde) =>
            defaultHiveStorage.copy(
              inputFormat = hiveSerde.inputFormat.orElse(defaultHiveStorage.inputFormat),
              outputFormat = hiveSerde.outputFormat.orElse(defaultHiveStorage.outputFormat),
              // User specified serde takes precedence over the one inferred from file format.
              serde = serdeInfo.serde.orElse(hiveSerde.serde).orElse(defaultHiveStorage.serde),
              properties = serdeInfo.serdeProperties ++ defaultHiveStorage.properties)
          case _ => throw QueryCompilationErrors.invalidFileFormatForStoredAsError(serdeInfo)
        }
      } else {
        defaultHiveStorage.copy(
          inputFormat =
            serdeInfo.formatClasses.map(_.input).orElse(defaultHiveStorage.inputFormat),
          outputFormat =
            serdeInfo.formatClasses.map(_.output).orElse(defaultHiveStorage.outputFormat),
          serde = serdeInfo.serde.orElse(defaultHiveStorage.serde),
          properties = serdeInfo.serdeProperties ++ defaultHiveStorage.properties)
      }
      (storageFormat, DDLUtils.HIVE_PROVIDER)
    } else {
      // If neither USING nor STORED AS/ROW FORMAT is specified, we create native data source
      // tables if:
      //   1. `LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT` is false, or
      //   2. It's a CTAS and `conf.convertCTAS` is true.
      val createHiveTableByDefault = conf.getConf(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT)
      if (!createHiveTableByDefault || (ctas && conf.convertCTAS)) {
        (nonHiveStorageFormat, conf.defaultDataSourceName)
      } else {
        logWarning(log"A Hive serde table will be created as there is no table provider " +
          log"specified. You can set " +
          log"${MDC(CONFIG, SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key)} to false so that " +
          log"native data source table will be created instead.")
        (defaultHiveStorage, DDLUtils.HIVE_PROVIDER)
      }
    }
  }

  private def buildCatalogTable(
      table: TableIdentifier,
      schema: StructType,
      partitioning: Seq[Transform],
      properties: Map[String, String],
      provider: String,
      location: Option[String],
      comment: Option[String],
      collation: Option[String],
      storageFormat: CatalogStorageFormat,
      external: Boolean): CatalogTable = {
    val tableType = if (external || location.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }
    val (partitionColumns, maybeBucketSpec, maybeClusterBySpec) = partitioning.convertTransforms

    CatalogTable(
      identifier = table,
      tableType = tableType,
      storage = storageFormat,
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = properties ++
        maybeClusterBySpec.map(
          clusterBySpec => ClusterBySpec.toProperty(schema, clusterBySpec, conf.resolver)),
      comment = comment,
      collation = collation
    )
  }

  object ResolvedViewIdentifier {
    def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
      case ResolvedPersistentView(catalog, ident, _) =>
        assert(isSessionCatalog(catalog))
        assert(ident.namespace().length == 1)
        Some(TableIdentifier(ident.name, Some(ident.namespace.head), Some(catalog.name)))

      case ResolvedTempView(ident, _) => Some(ident.asTableIdentifier)

      case _ => None
    }
  }

  object ResolvedV1TableIdentifier {
    def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
      case ResolvedTable(catalog, _, t: V1Table, _) if supportsV1Command(catalog) =>
        Some(t.catalogTable.identifier)
      case _ => None
    }
  }

  object ResolvedV1TableIdentifierInSessionCatalog {
    def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
      case ResolvedTable(catalog, _, t: V1Table, _) if isSessionCatalog(catalog) =>
        Some(t.catalogTable.identifier)
      case _ => None
    }
  }

  object ResolvedV1TableOrViewIdentifier {
    def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
      case ResolvedV1TableIdentifier(ident) => Some(ident)
      case ResolvedViewIdentifier(ident) => Some(ident)
      case _ => None
    }
  }

  object ResolvedV1Identifier {
    def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
      case ResolvedIdentifier(catalog, ident) if supportsV1Command(catalog) =>
        if (ident.namespace().length != 1) {
          throw QueryCompilationErrors
            .requiresSinglePartNamespaceError(ident.namespace().toImmutableArraySeq)
        }
        Some(TableIdentifier(ident.name, Some(ident.namespace.head), Some(catalog.name)))
      case _ => None
    }
  }

  // Use this object to help match commands that do not have a v2 implementation.
  object ResolvedIdentifierInSessionCatalog{
    def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
      case ResolvedIdentifier(catalog, ident) if isSessionCatalog(catalog) =>
        if (ident.namespace().length != 1) {
          throw QueryCompilationErrors
            .requiresSinglePartNamespaceError(ident.namespace().toImmutableArraySeq)
        }
        Some(TableIdentifier(ident.name, Some(ident.namespace.head), Some(catalog.name)))
      case _ => None
    }
  }

  private def convertToStructField(col: QualifiedColType): StructField = {
    val builder = new MetadataBuilder
    col.comment.foreach(builder.putString("comment", _))
    col.default.map {
      value: String => builder.putString(DefaultCols.CURRENT_DEFAULT_COLUMN_METADATA_KEY, value)
    }
    StructField(col.name.head, col.dataType, nullable = true, builder.build())
  }

  private def isV2Provider(provider: String): Boolean = {
    DataSourceV2Utils.getTableProvider(provider, conf).isDefined
  }

  private object ResolvedV1Database {
    def unapply(resolved: ResolvedNamespace): Option[String] = resolved match {
      case ResolvedNamespace(catalog, _, _) if !supportsV1Command(catalog) => None
      case ResolvedNamespace(_, Seq(), _) =>
        throw QueryCompilationErrors.databaseFromV1SessionCatalogNotSpecifiedError()
      case ResolvedNamespace(_, Seq(dbName), _) => Some(dbName)
      case _ =>
        assert(resolved.namespace.length > 1)
        throw QueryCompilationErrors.nestedDatabaseUnsupportedByV1SessionCatalogError(
          resolved.namespace.map(quoteIfNeeded).mkString("."))
    }
  }

  // Use this object to help match commands that do not have a v2 implementation.
  private object ResolvedDatabaseInSessionCatalog {
    def unapply(resolved: ResolvedNamespace): Option[String] = resolved match {
      case ResolvedNamespace(catalog, _, _) if !isSessionCatalog(catalog) => None
      case ResolvedNamespace(_, Seq(), _) =>
        throw QueryCompilationErrors.databaseFromV1SessionCatalogNotSpecifiedError()
      case ResolvedNamespace(_, Seq(dbName), _) => Some(dbName)
      case _ =>
        assert(resolved.namespace.length > 1)
        throw QueryCompilationErrors.nestedDatabaseUnsupportedByV1SessionCatalogError(
          resolved.namespace.map(quoteIfNeeded).mkString("."))
    }
  }

  private object DatabaseNameInSessionCatalog {
    def unapply(resolved: ResolvedNamespace): Option[String] = resolved match {
      case ResolvedNamespace(catalog, _, _) if !supportsV1Command(catalog) => None
      case ResolvedNamespace(_, Seq(dbName), _) => Some(dbName)
      case _ =>
        assert(resolved.namespace.length > 1)
        throw QueryCompilationErrors.requiresSinglePartNamespaceError(resolved.namespace)
    }
  }

  private def supportsV1Command(catalog: CatalogPlugin): Boolean = {
    isSessionCatalog(catalog) && (
      SQLConf.get.getConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION).isEmpty ||
        catalog.isInstanceOf[DelegatingCatalogExtension])
  }
}
