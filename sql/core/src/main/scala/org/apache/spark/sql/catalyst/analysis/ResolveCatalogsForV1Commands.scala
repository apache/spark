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
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateV2Table, LogicalPlan, ReplaceTable, ReplaceTableAsSelect}
import org.apache.spark.sql.catalyst.plans.logical.sql.{CreateTableAsSelectStatement, CreateTableStatement, DropTableStatement, DropViewStatement, ReplaceTableAsSelectStatement, ReplaceTableStatement, ShowNamespacesStatement, ShowTablesStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command.{DropTableCommand, ShowTablesCommand}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Resolves catalogs from the multi-part identifiers in SQL statements, and convert the statements
 * to the corresponding v1 commands if the resolved catalog is the session catalog.
 *
 * We can remove this rule once we implement all the catalog functionality in `V2SessionCatalog`.
 */
class ResolveCatalogsForV1Commands(val catalogManager: CatalogManager, conf: SQLConf)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    // For CREATE TABLE [AS SELECT], we should use the v1 command if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c @ CreateTableStatement(
         CatalogAndIdentifierParts(catalog, tableName), _, _, _, _, _, _, _, _, _)
         if isSessionCatalog(catalog) =>
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
         CatalogAndIdentifierParts(catalog, tableName), _, _, _, _, _, _, _, _, _)
         if isSessionCatalog(catalog) =>
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

    // For REPLACE TABLE [AS SELECT], we should fail if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c @ ReplaceTableStatement(
         CatalogAndIdentifierParts(catalog, tableName), _, _, _, _, _, _, _, _, _)
         if isSessionCatalog(catalog) =>
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
         CatalogAndIdentifierParts(catalog, tableName), _, _, _, _, _, _, _, _, _)
         if isSessionCatalog(catalog) =>
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

    case d @ DropTableStatement(
         CatalogAndIdentifierParts(c, tableName), ifExists, purge) if isSessionCatalog(c) =>
      DropTableCommand(d.tableName.asTableIdentifier, ifExists, isView = false, purge = purge)

    case DropViewStatement(
         CatalogAndIdentifierParts(c, tableName), ifExists) if isSessionCatalog(c) =>
      DropTableCommand(tableName.asTableIdentifier, ifExists, isView = true, purge = false)

    case ShowNamespacesStatement(
         Some(CatalogAndIdentifierParts(c, identParts)), pattern) if isSessionCatalog(c) =>
      throw new AnalysisException(
        "SHOW NAMESPACES is not supported with the session catalog.")

    // TODO (SPARK-29014): we should check if the current catalog is session catalog here.
    case ShowNamespacesStatement(None, pattern) if defaultCatalog.isEmpty =>
      throw new AnalysisException(
        "SHOW NAMESPACES is not supported with the session catalog.")

    case ShowTablesStatement(
         Some(CatalogAndIdentifierParts(c, identParts)), pattern) if isSessionCatalog(c) =>
      if (identParts.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${identParts.quoted}")
      }
      ShowTablesCommand(Some(identParts.head), pattern)

    // TODO (SPARK-29014): we should check if the current catalog is session catalog here.
    case ShowTablesStatement(None, pattern) if defaultCatalog.isEmpty =>
      ShowTablesCommand(None, pattern)
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

  private def isV2Provider(provider: String): Boolean = {
    DataSource.lookupDataSourceV2(provider, conf) match {
      // TODO(SPARK-28396): Currently file source v2 can't work with tables.
      case Some(_: FileDataSourceV2) => false
      case Some(_) => true
      case _ => false
    }
  }
}
