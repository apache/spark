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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, Identifier, LookupCatalog, SupportsNamespaces, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, RefreshTable}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, HiveStringType, MetadataBuilder, StructField, StructType}

/**
 * Resolves catalogs from the multi-part identifiers in SQL statements, and convert the statements
 * to the corresponding v1 or v2 commands if the resolved catalog is the session catalog.
 *
 * We can remove this rule once we implement all the catalog functionality in `V2SessionCatalog`.
 */
class ResolveSessionCatalog(
    val catalogManager: CatalogManager,
    conf: SQLConf,
    isView: Seq[String] => Boolean)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case AlterTableAddColumnsStatement(
         nameParts @ SessionCatalogAndTable(catalog, tbl), cols) =>
      loadTable(catalog, tbl.asIdentifier).collect {
        case v1Table: V1Table =>
          cols.foreach { c =>
            assertTopLevelColumn(c.name, "AlterTableAddColumnsCommand")
            if (!c.nullable) {
              throw new AnalysisException(
                "ADD COLUMN with v1 tables cannot specify NOT NULL.")
            }
          }
          AlterTableAddColumnsCommand(tbl.asTableIdentifier, cols.map(convertToStructField))
      }.getOrElse {
        val changes = cols.map { col =>
          TableChange.addColumn(
            col.name.toArray,
            col.dataType,
            col.nullable,
            col.comment.orNull,
            col.position.orNull)
        }
        createAlterTable(nameParts, catalog, tbl, changes)
      }

    case a @ AlterTableAlterColumnStatement(
         nameParts @ SessionCatalogAndTable(catalog, tbl), _, _, _, _, _) =>
      loadTable(catalog, tbl.asIdentifier).collect {
        case v1Table: V1Table =>
          if (a.column.length > 1) {
            throw new AnalysisException(
              "ALTER COLUMN with qualified column is only supported with v2 tables.")
          }
          if (a.dataType.isEmpty) {
            throw new AnalysisException(
              "ALTER COLUMN with v1 tables must specify new data type.")
          }
          if (a.nullable.isDefined) {
            throw new AnalysisException(
              "ALTER COLUMN with v1 tables cannot specify NOT NULL.")
          }
          if (a.position.isDefined) {
            throw new AnalysisException("" +
              "ALTER COLUMN ... FIRST | ALTER is only supported with v2 tables.")
          }
          val builder = new MetadataBuilder
          // Add comment to metadata
          a.comment.map(c => builder.putString("comment", c))
          // Add Hive type string to metadata.
          val cleanedDataType = HiveStringType.replaceCharType(a.dataType.get)
          if (a.dataType.get != cleanedDataType) {
            builder.putString(HIVE_TYPE_STRING, a.dataType.get.catalogString)
          }
          val newColumn = StructField(
            a.column(0),
            cleanedDataType,
            nullable = true,
            builder.build())
          AlterTableChangeColumnCommand(tbl.asTableIdentifier, a.column(0), newColumn)
      }.getOrElse {
        val colName = a.column.toArray
        val typeChange = a.dataType.map { newDataType =>
          TableChange.updateColumnType(colName, newDataType)
        }
        val nullabilityChange = a.nullable.map { nullable =>
          TableChange.updateColumnNullability(colName, nullable)
        }
        val commentChange = a.comment.map { newComment =>
          TableChange.updateColumnComment(colName, newComment)
        }
        val positionChange = a.position.map { newPosition =>
          TableChange.updateColumnPosition(colName, newPosition)
        }
        createAlterTable(
          nameParts,
          catalog,
          tbl,
          typeChange.toSeq ++ nullabilityChange ++ commentChange ++ positionChange)
      }

    case AlterTableRenameColumnStatement(
         nameParts @ SessionCatalogAndTable(catalog, tbl), col, newName) =>
      loadTable(catalog, tbl.asIdentifier).collect {
        case v1Table: V1Table =>
          throw new AnalysisException("RENAME COLUMN is only supported with v2 tables.")
      }.getOrElse {
        val changes = Seq(TableChange.renameColumn(col.toArray, newName))
        createAlterTable(nameParts, catalog, tbl, changes)
      }

    case AlterTableDropColumnsStatement(
         nameParts @ SessionCatalogAndTable(catalog, tbl), cols) =>
      loadTable(catalog, tbl.asIdentifier).collect {
        case v1Table: V1Table =>
          throw new AnalysisException("DROP COLUMN is only supported with v2 tables.")
      }.getOrElse {
        val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
        createAlterTable(nameParts, catalog, tbl, changes)
      }

    case AlterTableSetPropertiesStatement(
         nameParts @ SessionCatalogAndTable(catalog, tbl), props) =>
      loadTable(catalog, tbl.asIdentifier).collect {
        case v1Table: V1Table =>
          AlterTableSetPropertiesCommand(tbl.asTableIdentifier, props, isView = false)
      }.getOrElse {
        val changes = props.map { case (key, value) =>
          TableChange.setProperty(key, value)
        }.toSeq
        createAlterTable(nameParts, catalog, tbl, changes)
      }

    case AlterTableUnsetPropertiesStatement(
         nameParts @ SessionCatalogAndTable(catalog, tbl), keys, ifExists) =>
      loadTable(catalog, tbl.asIdentifier).collect {
        case v1Table: V1Table =>
          AlterTableUnsetPropertiesCommand(
            tbl.asTableIdentifier, keys, ifExists, isView = false)
      }.getOrElse {
        val changes = keys.map(key => TableChange.removeProperty(key))
        createAlterTable(nameParts, catalog, tbl, changes)
      }

    case AlterTableSetLocationStatement(
         nameParts @ SessionCatalogAndTable(catalog, tbl), partitionSpec, newLoc) =>
      loadTable(catalog, tbl.asIdentifier).collect {
        case v1Table: V1Table =>
          AlterTableSetLocationCommand(tbl.asTableIdentifier, partitionSpec, newLoc)
      }.getOrElse {
        if (partitionSpec.nonEmpty) {
          throw new AnalysisException(
            "ALTER TABLE SET LOCATION does not support partition for v2 tables.")
        }
        val changes = Seq(TableChange.setProperty(TableCatalog.PROP_LOCATION, newLoc))
        createAlterTable(nameParts, catalog, tbl, changes)
      }

    // ALTER VIEW should always use v1 command if the resolved catalog is session catalog.
    case AlterViewSetPropertiesStatement(SessionCatalogAndTable(_, tbl), props) =>
      AlterTableSetPropertiesCommand(tbl.asTableIdentifier, props, isView = true)

    case AlterViewUnsetPropertiesStatement(SessionCatalogAndTable(_, tbl), keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(tbl.asTableIdentifier, keys, ifExists, isView = true)

    case d @ DescribeNamespace(SessionCatalogAndNamespace(_, ns), _) =>
      if (ns.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${ns.quoted}")
      }
      DescribeDatabaseCommand(ns.head, d.extended)

    case AlterNamespaceSetProperties(SessionCatalogAndNamespace(_, ns), properties) =>
      if (ns.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${ns.quoted}")
      }
      AlterDatabasePropertiesCommand(ns.head, properties)

    case AlterNamespaceSetLocation(SessionCatalogAndNamespace(_, ns), location) =>
      if (ns.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${ns.quoted}")
      }
      AlterDatabaseSetLocationCommand(ns.head, location)

    case RenameTableStatement(SessionCatalogAndTable(_, oldName), newNameParts, isView) =>
      AlterTableRenameCommand(oldName.asTableIdentifier, newNameParts.asTableIdentifier, isView)

    case DescribeRelation(ResolvedTable(_, ident, _: V1Table), partitionSpec, isExtended) =>
      DescribeTableCommand(ident.asTableIdentifier, partitionSpec, isExtended)

    // Use v1 command to describe (temp) view, as v2 catalog doesn't support view yet.
    case DescribeRelation(ResolvedView(ident), partitionSpec, isExtended) =>
      DescribeTableCommand(ident.asTableIdentifier, partitionSpec, isExtended)

    case DescribeColumnStatement(
        SessionCatalogAndTable(catalog, tbl), colNameParts, isExtended) =>
      loadTable(catalog, tbl.asIdentifier).collect {
        case v1Table: V1Table =>
          DescribeColumnCommand(tbl.asTableIdentifier, colNameParts, isExtended)
      }.getOrElse {
        if (isView(tbl)) {
          DescribeColumnCommand(tbl.asTableIdentifier, colNameParts, isExtended)
        } else {
          throw new AnalysisException("Describing columns is not supported for v2 tables.")
        }
      }

    // For CREATE TABLE [AS SELECT], we should use the v1 command if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c @ CreateTableStatement(
         SessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _) =>
      if (!isV2Provider(c.provider)) {
        val tableDesc = buildCatalogTable(tbl.asTableIdentifier, c.tableSchema,
          c.partitioning, c.bucketSpec, c.properties, c.provider, c.options, c.location,
          c.comment, c.ifNotExists)
        val mode = if (c.ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists
        CreateTable(tableDesc, mode, None)
      } else {
        CreateV2Table(
          catalog.asTableCatalog,
          tbl.asIdentifier,
          c.tableSchema,
          // convert the bucket spec and add it as a transform
          c.partitioning ++ c.bucketSpec.map(_.asTransform),
          convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
          ignoreIfExists = c.ifNotExists)
      }

    case c @ CreateTableAsSelectStatement(
         SessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _) =>
      if (!isV2Provider(c.provider)) {
        val tableDesc = buildCatalogTable(tbl.asTableIdentifier, new StructType,
          c.partitioning, c.bucketSpec, c.properties, c.provider, c.options, c.location,
          c.comment, c.ifNotExists)
        val mode = if (c.ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists
        CreateTable(tableDesc, mode, Some(c.asSelect))
      } else {
        CreateTableAsSelect(
          catalog.asTableCatalog,
          tbl.asIdentifier,
          // convert the bucket spec and add it as a transform
          c.partitioning ++ c.bucketSpec.map(_.asTransform),
          c.asSelect,
          convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
          writeOptions = c.options,
          ignoreIfExists = c.ifNotExists)
      }

    case RefreshTableStatement(SessionCatalogAndTable(_, tbl)) =>
      RefreshTable(tbl.asTableIdentifier)

    // For REPLACE TABLE [AS SELECT], we should fail if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c @ ReplaceTableStatement(
         SessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _) =>
      if (!isV2Provider(c.provider)) {
        throw new AnalysisException("REPLACE TABLE is only supported with v2 tables.")
      } else {
        ReplaceTable(
          catalog.asTableCatalog,
          tbl.asIdentifier,
          c.tableSchema,
          // convert the bucket spec and add it as a transform
          c.partitioning ++ c.bucketSpec.map(_.asTransform),
          convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
          orCreate = c.orCreate)
      }

    case c @ ReplaceTableAsSelectStatement(
         SessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _) =>
      if (!isV2Provider(c.provider)) {
        throw new AnalysisException("REPLACE TABLE AS SELECT is only supported with v2 tables.")
      } else {
        ReplaceTableAsSelect(
          catalog.asTableCatalog,
          tbl.asIdentifier,
          // convert the bucket spec and add it as a transform
          c.partitioning ++ c.bucketSpec.map(_.asTransform),
          c.asSelect,
          convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
          writeOptions = c.options,
          orCreate = c.orCreate)
      }

    case DropTableStatement(SessionCatalogAndTable(catalog, tbl), ifExists, purge) =>
      DropTableCommand(tbl.asTableIdentifier, ifExists, isView = false, purge = purge)

    case DropViewStatement(SessionCatalogAndTable(catalog, viewName), ifExists) =>
      DropTableCommand(viewName.asTableIdentifier, ifExists, isView = true, purge = false)

    case c @ CreateNamespaceStatement(CatalogAndNamespace(catalog, ns), _, _)
        if isSessionCatalog(catalog) =>
      if (ns.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${ns.quoted}")
      }

      val comment = c.properties.get(SupportsNamespaces.PROP_COMMENT)
      val location = c.properties.get(SupportsNamespaces.PROP_LOCATION)
      val newProperties = c.properties -- SupportsNamespaces.RESERVED_PROPERTIES.asScala
      CreateDatabaseCommand(ns.head, c.ifNotExists, location, comment, newProperties)

    case d @ DropNamespace(SessionCatalogAndNamespace(_, ns), _, _) =>
      if (ns.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${ns.quoted}")
      }
      DropDatabaseCommand(ns.head, d.ifExists, d.cascade)

    case ShowTables(SessionCatalogAndNamespace(_, ns), pattern) =>
      assert(ns.nonEmpty)
      if (ns.length != 1) {
          throw new AnalysisException(
            s"The database name is not valid: ${ns.quoted}")
      }
      ShowTablesCommand(Some(ns.head), pattern)

    case ShowTableStatement(ns, pattern, partitionsSpec) =>
      val db = ns match {
        case Some(ns) if ns.length != 1 =>
          throw new AnalysisException(
            s"The database name is not valid: ${ns.quoted}")
        case _ => ns.map(_.head)
      }
      ShowTablesCommand(db, Some(pattern), true, partitionsSpec)

    case AnalyzeTableStatement(tbl, partitionSpec, noScan) =>
      val v1TableName = parseV1Table(tbl, "ANALYZE TABLE")
      if (partitionSpec.isEmpty) {
        AnalyzeTableCommand(v1TableName.asTableIdentifier, noScan)
      } else {
        AnalyzePartitionCommand(v1TableName.asTableIdentifier, partitionSpec, noScan)
      }

    case AnalyzeColumnStatement(tbl, columnNames, allColumns) =>
      val v1TableName = parseV1Table(tbl, "ANALYZE TABLE")
      AnalyzeColumnCommand(v1TableName.asTableIdentifier, columnNames, allColumns)

    case RepairTableStatement(tbl) =>
      val v1TableName = parseV1Table(tbl, "MSCK REPAIR TABLE")
      AlterTableRecoverPartitionsCommand(
        v1TableName.asTableIdentifier,
        "MSCK REPAIR TABLE")

    case LoadDataStatement(tbl, path, isLocal, isOverwrite, partition) =>
      val v1TableName = parseV1Table(tbl, "LOAD DATA")
      LoadDataCommand(
        v1TableName.asTableIdentifier,
        path,
        isLocal,
        isOverwrite,
        partition)

    case ShowCreateTableStatement(tbl) =>
      val v1TableName = parseV1Table(tbl, "SHOW CREATE TABLE")
      ShowCreateTableCommand(v1TableName.asTableIdentifier)

    case CacheTableStatement(tbl, plan, isLazy, options) =>
      val v1TableName = parseV1Table(tbl, "CACHE TABLE")
      CacheTableCommand(v1TableName.asTableIdentifier, plan, isLazy, options)

    case UncacheTableStatement(tbl, ifExists) =>
      val v1TableName = parseV1Table(tbl, "UNCACHE TABLE")
      UncacheTableCommand(v1TableName.asTableIdentifier, ifExists)

    case TruncateTableStatement(tbl, partitionSpec) =>
      val v1TableName = parseV1Table(tbl, "TRUNCATE TABLE")
      TruncateTableCommand(
        v1TableName.asTableIdentifier,
        partitionSpec)

    case ShowPartitionsStatement(tbl, partitionSpec) =>
      val v1TableName = parseV1Table(tbl, "SHOW PARTITIONS")
      ShowPartitionsCommand(
        v1TableName.asTableIdentifier,
        partitionSpec)

    case ShowColumnsStatement(tbl, ns) =>
      val sql = "SHOW COLUMNS"
      val v1TableName = parseV1Table(tbl, sql).asTableIdentifier
      val resolver = conf.resolver
      val db = ns match {
        case Some(db) if (v1TableName.database.exists(!resolver(_, db.head))) =>
          throw new AnalysisException(
            s"SHOW COLUMNS with conflicting databases: " +
              s"'${db.head}' != '${v1TableName.database.get}'")
        case _ => ns.map(_.head)
      }
      if (ns.isDefined && ns.get.length > 1) {
        throw new AnalysisException(
          s"Namespace name should have only one part if specified: ${ns.get.quoted}")
      }
      if (tbl.length > 2) {
        throw new AnalysisException(
          s"Table name should have at most two parts: ${tbl.quoted}")
      }
      ShowColumnsCommand(db, v1TableName)

    case AlterTableRecoverPartitionsStatement(tbl) =>
      val v1TableName = parseV1Table(tbl, "ALTER TABLE RECOVER PARTITIONS")
      AlterTableRecoverPartitionsCommand(
        v1TableName.asTableIdentifier,
        "ALTER TABLE RECOVER PARTITIONS")

    case AlterTableAddPartitionStatement(tbl, partitionSpecsAndLocs, ifNotExists) =>
      val v1TableName = parseV1Table(tbl, "ALTER TABLE ADD PARTITION")
      AlterTableAddPartitionCommand(
        v1TableName.asTableIdentifier,
        partitionSpecsAndLocs,
        ifNotExists)

    case AlterTableRenamePartitionStatement(tbl, from, to) =>
      val v1TableName = parseV1Table(tbl, "ALTER TABLE RENAME PARTITION")
      AlterTableRenamePartitionCommand(
        v1TableName.asTableIdentifier,
        from,
        to)

    case AlterTableDropPartitionStatement(tbl, specs, ifExists, purge, retainData) =>
      val v1TableName = parseV1Table(tbl, "ALTER TABLE DROP PARTITION")
      AlterTableDropPartitionCommand(
        v1TableName.asTableIdentifier,
        specs,
        ifExists,
        purge,
        retainData)

    case AlterTableSerDePropertiesStatement(tbl, serdeClassName, serdeProperties, partitionSpec) =>
      val v1TableName = parseV1Table(tbl, "ALTER TABLE SerDe Properties")
      AlterTableSerDePropertiesCommand(
        v1TableName.asTableIdentifier,
        serdeClassName,
        serdeProperties,
        partitionSpec)

    case AlterViewAsStatement(tbl, originalText, query) =>
      val v1TableName = parseV1Table(tbl, "ALTER VIEW QUERY")
      AlterViewAsCommand(
        v1TableName.asTableIdentifier,
        originalText,
        query)

    case CreateViewStatement(
      tbl, userSpecifiedColumns, comment, properties,
      originalText, child, allowExisting, replace, viewType) =>

      val v1TableName = parseV1Table(tbl, "CREATE VIEW")
      CreateViewCommand(
        v1TableName.asTableIdentifier,
        userSpecifiedColumns,
        comment,
        properties,
        originalText,
        child,
        allowExisting,
        replace,
        viewType)

    case ShowTableProperties(r: ResolvedTable, propertyKey) if isSessionCatalog(r.catalog) =>
      ShowTablePropertiesCommand(r.identifier.asTableIdentifier, propertyKey)

    case DescribeFunctionStatement(CatalogAndIdentifier(catalog, ident), extended) =>
      val functionIdent =
        parseSessionCatalogFunctionIdentifier("DESCRIBE FUNCTION", catalog, ident)
      DescribeFunctionCommand(functionIdent, extended)

    case ShowFunctionsStatement(userScope, systemScope, pattern, fun) =>
      val (database, function) = fun match {
        case Some(CatalogAndIdentifier(catalog, ident)) =>
          val FunctionIdentifier(fn, db) =
            parseSessionCatalogFunctionIdentifier("SHOW FUNCTIONS", catalog, ident)
          (db, Some(fn))
        case None => (None, pattern)
      }
      ShowFunctionsCommand(database, function, userScope, systemScope)

    case DropFunctionStatement(CatalogAndIdentifier(catalog, ident), ifExists, isTemp) =>
      val FunctionIdentifier(function, database) =
        parseSessionCatalogFunctionIdentifier("DROP FUNCTION", catalog, ident)
      DropFunctionCommand(database, function, ifExists, isTemp)

    case CreateFunctionStatement(CatalogAndIdentifier(catalog, ident),
      className, resources, isTemp, ignoreIfExists, replace) =>
      val FunctionIdentifier(function, database) =
        parseSessionCatalogFunctionIdentifier("CREATE FUNCTION", catalog, ident)
      CreateFunctionCommand(database, function, className, resources, isTemp, ignoreIfExists,
        replace)
  }

  private def parseSessionCatalogFunctionIdentifier(
      sql: String,
      catalog: CatalogPlugin,
      functionIdent: Identifier): FunctionIdentifier = {
    if (isSessionCatalog(catalog)) {
      functionIdent.asMultipartIdentifier match {
        case Seq(db, fn) => FunctionIdentifier(fn, Some(db))
        case Seq(fn) => FunctionIdentifier(fn, None)
        case _ =>
          throw new AnalysisException(s"Unsupported function name '${functionIdent.quoted}'")
      }
    } else {
      throw new AnalysisException(s"$sql is only supported in v1 catalog")
    }
  }

  private def parseV1Table(tableName: Seq[String], sql: String): Seq[String] = {
    val CatalogAndIdentifier(catalog, ident) = tableName
    if (!isSessionCatalog(catalog)) {
      throw new AnalysisException(s"$sql is only supported with v1 tables.")
    }
    ident.asMultipartIdentifier
  }

  private def buildCatalogTable(
      table: TableIdentifier,
      schema: StructType,
      partitioning: Seq[Transform],
      bucketSpec: Option[BucketSpec],
      properties: Map[String, String],
      provider: String,
      options: Map[String, String],
      location: Option[String],
      comment: Option[String],
      ifNotExists: Boolean): CatalogTable = {
    val storage = CatalogStorageFormat.empty.copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = options)

    val tableType = if (location.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    CatalogTable(
      identifier = table,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitioning.asPartitionColumns,
      bucketSpec = bucketSpec,
      properties = properties,
      comment = comment)
  }

  object SessionCatalogAndTable {
    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Seq[String])] = nameParts match {
      case SessionCatalogAndIdentifier(catalog, ident) =>
        Some(catalog -> ident.asMultipartIdentifier)
      case _ => None
    }
  }

  object SessionCatalogAndNamespace {
    def unapply(resolved: ResolvedNamespace): Option[(SupportsNamespaces, Seq[String])] =
      if (isSessionCatalog(resolved.catalog)) {
        Some(resolved.catalog -> resolved.namespace)
      } else {
        None
      }
  }

  private def assertTopLevelColumn(colName: Seq[String], command: String): Unit = {
    if (colName.length > 1) {
      throw new AnalysisException(s"$command does not support nested column: ${colName.quoted}")
    }
  }

  private def convertToStructField(col: QualifiedColType): StructField = {
    val builder = new MetadataBuilder
    col.comment.foreach(builder.putString("comment", _))

    val cleanedDataType = HiveStringType.replaceCharType(col.dataType)
    if (col.dataType != cleanedDataType) {
      builder.putString(HIVE_TYPE_STRING, col.dataType.catalogString)
    }

    StructField(
      col.name.head,
      cleanedDataType,
      nullable = true,
      builder.build())
  }

  private def isV2Provider(provider: String): Boolean = {
    DataSource.lookupDataSourceV2(provider, conf) match {
      // TODO(SPARK-28396): Currently file source v2 can't work with tables.
      case Some(_: FileDataSourceV2) => false
      case Some(_) => true
      case _ => false
    }
  }
}
