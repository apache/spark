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

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, LookupCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableRecoverPartitionsCommand, AlterTableSetLocationCommand, AlterTableSetPropertiesCommand, AlterTableUnsetPropertiesCommand, AnalyzeColumnCommand, AnalyzePartitionCommand, AnalyzeTableCommand, CacheTableCommand, CreateDatabaseCommand, DescribeColumnCommand, DescribeTableCommand, DropTableCommand, ShowCreateTableCommand, ShowPartitionsCommand, ShowTablesCommand, TruncateTableCommand, UncacheTableCommand}
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
         nameParts @ SessionCatalog(catalog, tableName), cols) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          cols.foreach(c => assertTopLevelColumn(c.name, "AlterTableAddColumnsCommand"))
          AlterTableAddColumnsCommand(tableName.asTableIdentifier, cols.map(convertToStructField))
      }.getOrElse {
        val changes = cols.map { col =>
          TableChange.addColumn(col.name.toArray, col.dataType, true, col.comment.orNull)
        }
        createAlterTable(nameParts, catalog, tableName, changes)
      }

    case AlterTableAlterColumnStatement(
         nameParts @ SessionCatalog(catalog, tableName), colName, dataType, comment) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          // TODO(SPARK-29353): we should fallback to the v1 `AlterTableChangeColumnCommand`.
          throw new AnalysisException("ALTER COLUMN is only supported with v2 tables.")
      }.getOrElse {
        val typeChange = dataType.map { newDataType =>
          TableChange.updateColumnType(colName.toArray, newDataType, true)
        }
        val commentChange = comment.map { newComment =>
          TableChange.updateColumnComment(colName.toArray, newComment)
        }
        createAlterTable(nameParts, catalog, tableName, typeChange.toSeq ++ commentChange)
      }

    case AlterTableRenameColumnStatement(
         nameParts @ SessionCatalog(catalog, tableName), col, newName) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          throw new AnalysisException("RENAME COLUMN is only supported with v2 tables.")
      }.getOrElse {
        val changes = Seq(TableChange.renameColumn(col.toArray, newName))
        createAlterTable(nameParts, catalog, tableName, changes)
      }

    case AlterTableDropColumnsStatement(
         nameParts @ SessionCatalog(catalog, tableName), cols) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          throw new AnalysisException("DROP COLUMN is only supported with v2 tables.")
      }.getOrElse {
        val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
        createAlterTable(nameParts, catalog, tableName, changes)
      }

    case AlterTableSetPropertiesStatement(
         nameParts @ SessionCatalog(catalog, tableName), props) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          AlterTableSetPropertiesCommand(tableName.asTableIdentifier, props, isView = false)
      }.getOrElse {
        val changes = props.map { case (key, value) =>
          TableChange.setProperty(key, value)
        }.toSeq
        createAlterTable(nameParts, catalog, tableName, changes)
      }

    case AlterTableUnsetPropertiesStatement(
         nameParts @ SessionCatalog(catalog, tableName), keys, ifExists) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          AlterTableUnsetPropertiesCommand(
            tableName.asTableIdentifier, keys, ifExists, isView = false)
      }.getOrElse {
        val changes = keys.map(key => TableChange.removeProperty(key))
        createAlterTable(nameParts, catalog, tableName, changes)
      }

    case AlterTableSetLocationStatement(
         nameParts @ SessionCatalog(catalog, tableName), newLoc) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          AlterTableSetLocationCommand(tableName.asTableIdentifier, None, newLoc)
      }.getOrElse {
        val changes = Seq(TableChange.setProperty("location", newLoc))
        createAlterTable(nameParts, catalog, tableName, changes)
      }

    // ALTER VIEW should always use v1 command if the resolved catalog is session catalog.
    case AlterViewSetPropertiesStatement(SessionCatalog(catalog, tableName), props) =>
      AlterTableSetPropertiesCommand(tableName.asTableIdentifier, props, isView = true)

    case AlterViewUnsetPropertiesStatement(SessionCatalog(catalog, tableName), keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(tableName.asTableIdentifier, keys, ifExists, isView = true)

    case DeleteFromStatement(
         nameParts @ SessionCatalog(catalog, tableName), tableAlias, condition) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          throw new AnalysisException("DELETE FROM is only supported with v2 tables.")
      }.getOrElse {
        val r = UnresolvedV2Relation(nameParts, catalog.asTableCatalog, tableName.asIdentifier)
        val aliased = tableAlias.map(SubqueryAlias(_, r)).getOrElse(r)
        DeleteFromTable(aliased, condition)
      }

    case DescribeTableStatement(
         nameParts @ SessionCatalog(catalog, tableName), partitionSpec, isExtended) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          DescribeTableCommand(tableName.asTableIdentifier, partitionSpec, isExtended)
      }.getOrElse {
        // The v1 `DescribeTableCommand` can describe view as well.
        if (isView(tableName)) {
          DescribeTableCommand(tableName.asTableIdentifier, partitionSpec, isExtended)
        } else {
          if (partitionSpec.nonEmpty) {
            throw new AnalysisException("DESCRIBE TABLE does not support partition for v2 tables.")
          }
          val r = UnresolvedV2Relation(nameParts, catalog.asTableCatalog, tableName.asIdentifier)
          DescribeTable(r, isExtended)
        }
      }

    case DescribeColumnStatement(SessionCatalog(catalog, tableName), colNameParts, isExtended) =>
      loadTable(catalog, tableName.asIdentifier).collect {
        case v1Table: V1Table =>
          DescribeColumnCommand(tableName.asTableIdentifier, colNameParts, isExtended)
      }.getOrElse {
        if (isView(tableName)) {
          DescribeColumnCommand(tableName.asTableIdentifier, colNameParts, isExtended)
        } else {
          throw new AnalysisException("Describing columns is not supported for v2 tables.")
        }
      }

    // For CREATE TABLE [AS SELECT], we should use the v1 command if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c @ CreateTableStatement(
         SessionCatalog(catalog, tableName), _, _, _, _, _, _, _, _, _) =>
      if (!isV2Provider(c.provider)) {
        val tableDesc = buildCatalogTable(c.tableName.asTableIdentifier, c.tableSchema,
          c.partitioning, c.bucketSpec, c.properties, c.provider, c.options, c.location,
          c.comment, c.ifNotExists)
        val mode = if (c.ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists
        CreateTable(tableDesc, mode, None)
      } else {
        CreateV2Table(
          catalog.asTableCatalog,
          tableName.asIdentifier,
          c.tableSchema,
          // convert the bucket spec and add it as a transform
          c.partitioning ++ c.bucketSpec.map(_.asTransform),
          convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
          ignoreIfExists = c.ifNotExists)
      }

    case c @ CreateTableAsSelectStatement(
         SessionCatalog(catalog, tableName), _, _, _, _, _, _, _, _, _) =>
      if (!isV2Provider(c.provider)) {
        val tableDesc = buildCatalogTable(c.tableName.asTableIdentifier, new StructType,
          c.partitioning, c.bucketSpec, c.properties, c.provider, c.options, c.location,
          c.comment, c.ifNotExists)
        val mode = if (c.ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists
        CreateTable(tableDesc, mode, Some(c.asSelect))
      } else {
        CreateTableAsSelect(
          catalog.asTableCatalog,
          tableName.asIdentifier,
          // convert the bucket spec and add it as a transform
          c.partitioning ++ c.bucketSpec.map(_.asTransform),
          c.asSelect,
          convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
          writeOptions = c.options.filterKeys(_ != "path"),
          ignoreIfExists = c.ifNotExists)
      }

    case RefreshTableStatement(SessionCatalog(_, tableName)) =>
      RefreshTable(tableName.asTableIdentifier)

    // For REPLACE TABLE [AS SELECT], we should fail if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c @ ReplaceTableStatement(
         SessionCatalog(catalog, tableName), _, _, _, _, _, _, _, _, _) =>
      if (!isV2Provider(c.provider)) {
        throw new AnalysisException("REPLACE TABLE is only supported with v2 tables.")
      } else {
        ReplaceTable(
          catalog.asTableCatalog,
          tableName.asIdentifier,
          c.tableSchema,
          // convert the bucket spec and add it as a transform
          c.partitioning ++ c.bucketSpec.map(_.asTransform),
          convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
          orCreate = c.orCreate)
      }

    case c @ ReplaceTableAsSelectStatement(
         SessionCatalog(catalog, tableName), _, _, _, _, _, _, _, _, _) =>
      if (!isV2Provider(c.provider)) {
        throw new AnalysisException("REPLACE TABLE AS SELECT is only supported with v2 tables.")
      } else {
        ReplaceTableAsSelect(
          catalog.asTableCatalog,
          tableName.asIdentifier,
          // convert the bucket spec and add it as a transform
          c.partitioning ++ c.bucketSpec.map(_.asTransform),
          c.asSelect,
          convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
          writeOptions = c.options.filterKeys(_ != "path"),
          orCreate = c.orCreate)
      }

    case d @ DropTableStatement(SessionCatalog(catalog, tableName), ifExists, purge) =>
      DropTableCommand(d.tableName.asTableIdentifier, ifExists, isView = false, purge = purge)

    case DropViewStatement(SessionCatalog(catalog, viewName), ifExists) =>
      DropTableCommand(viewName.asTableIdentifier, ifExists, isView = true, purge = false)

    case c @ CreateNamespaceStatement(SessionCatalog(catalog, nameParts), _, _) =>
      if (nameParts.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${nameParts.quoted}")
      }

      val comment = c.properties.get(CreateNamespaceStatement.COMMENT_PROPERTY_KEY)
      val location = c.properties.get(CreateNamespaceStatement.LOCATION_PROPERTY_KEY)
      val newProperties = c.properties -
        CreateNamespaceStatement.COMMENT_PROPERTY_KEY -
        CreateNamespaceStatement.LOCATION_PROPERTY_KEY
      CreateDatabaseCommand(nameParts.head, c.ifNotExists, location, comment, newProperties)

    case ShowTablesStatement(Some(SessionCatalog(catalog, nameParts)), pattern) =>
      if (nameParts.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${nameParts.quoted}")
      }
      ShowTablesCommand(Some(nameParts.head), pattern)

    case ShowTablesStatement(None, pattern) if isSessionCatalog(currentCatalog) =>
      ShowTablesCommand(None, pattern)

    case AnalyzeTableStatement(tableName, partitionSpec, noScan) =>
      val v1TableName = parseV1Table(tableName, "ANALYZE TABLE")
      if (partitionSpec.isEmpty) {
        AnalyzeTableCommand(v1TableName.asTableIdentifier, noScan)
      } else {
        AnalyzePartitionCommand(v1TableName.asTableIdentifier, partitionSpec, noScan)
      }

    case AnalyzeColumnStatement(tableName, columnNames, allColumns) =>
      val v1TableName = parseV1Table(tableName, "ANALYZE TABLE")
      AnalyzeColumnCommand(v1TableName.asTableIdentifier, columnNames, allColumns)

    case RepairTableStatement(tableName) =>
      val v1TableName = parseV1Table(tableName, "MSCK REPAIR TABLE")
      AlterTableRecoverPartitionsCommand(
        v1TableName.asTableIdentifier,
        "MSCK REPAIR TABLE")

    case ShowCreateTableStatement(tableName) =>
      val v1TableName = parseV1Table(tableName, "SHOW CREATE TABLE")
      ShowCreateTableCommand(v1TableName.asTableIdentifier)

    case CacheTableStatement(tableName, plan, isLazy, options) =>
      val v1TableName = parseV1Table(tableName, "CACHE TABLE")
      CacheTableCommand(v1TableName.asTableIdentifier, plan, isLazy, options)

    case UncacheTableStatement(tableName, ifExists) =>
      val v1TableName = parseV1Table(tableName, "UNCACHE TABLE")
      UncacheTableCommand(v1TableName.asTableIdentifier, ifExists)

    case TruncateTableStatement(tableName, partitionSpec) =>
      val v1TableName = parseV1Table(tableName, "TRUNCATE TABLE")
      TruncateTableCommand(
        v1TableName.asTableIdentifier,
        partitionSpec)

    case ShowPartitionsStatement(tableName, partitionSpec) =>
      val v1TableName = parseV1Table(tableName, "SHOW PARTITIONS")
      ShowPartitionsCommand(
        v1TableName.asTableIdentifier,
        partitionSpec)
  }

  private def parseV1Table(tableName: Seq[String], sql: String): Seq[String] = {
    val CatalogAndIdentifierParts(catalog, parts) = tableName
    if (!isSessionCatalog(catalog)) {
      throw new AnalysisException(s"$sql is only supported with v1 tables.")
    }
    parts
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

    val storage = DataSource.buildStorageFormatFromOptions(options)
    if (location.isDefined && storage.locationUri.isDefined) {
      throw new AnalysisException(
        "LOCATION and 'path' in OPTIONS are both used to indicate the custom table path, " +
            "you can only specify one of them.")
    }
    val customLocation = storage.locationUri.orElse(location.map(CatalogUtils.stringToURI))

    val tableType = if (customLocation.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    CatalogTable(
      identifier = table,
      tableType = tableType,
      storage = storage.copy(locationUri = customLocation),
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitioning.asPartitionColumns,
      bucketSpec = bucketSpec,
      properties = properties,
      comment = comment)
  }

  object SessionCatalog {
    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Seq[String])] = nameParts match {
      case CatalogAndIdentifierParts(catalog, parts) if isSessionCatalog(catalog) =>
        Some(catalog -> parts)
      case _ => None
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
