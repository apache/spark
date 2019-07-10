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

package org.apache.spark.sql.execution.datasources

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalog.v2.{CatalogPlugin, Identifier, LookupCatalog, TableCatalog}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateV2Table, DropTable, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.sql.{AlterTableAddColumnsStatement, AlterTableSetLocationStatement, AlterTableSetPropertiesStatement, AlterTableUnsetPropertiesStatement, AlterViewSetPropertiesStatement, AlterViewUnsetPropertiesStatement, CreateTableAsSelectStatement, CreateTableStatement, DropTableStatement, DropViewStatement, QualifiedColType}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableSetLocationCommand, AlterTableSetPropertiesCommand, AlterTableUnsetPropertiesCommand, DropTableCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.TableProvider
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, HiveStringType, MetadataBuilder, StructField, StructType}

case class DataSourceResolution(
    conf: SQLConf,
    findCatalog: String => CatalogPlugin)
  extends Rule[LogicalPlan] with CastSupport with LookupCatalog {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  override protected def lookupCatalog(name: String): CatalogPlugin = findCatalog(name)

  def defaultCatalog: Option[CatalogPlugin] = conf.defaultV2Catalog.map(findCatalog)

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateTableStatement(
        AsTableIdentifier(table), schema, partitionCols, bucketSpec, properties,
        V1WriteProvider(provider), options, location, comment, ifNotExists) =>

      val tableDesc = buildCatalogTable(table, schema, partitionCols, bucketSpec, properties,
        provider, options, location, comment, ifNotExists)
      val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

      CreateTable(tableDesc, mode, None)

    case create: CreateTableStatement =>
      // the provider was not a v1 source, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = create.tableName
      val catalog = maybeCatalog.orElse(defaultCatalog)
          .getOrElse(throw new AnalysisException(
            s"No catalog specified for table ${identifier.quoted} and no default catalog is set"))
          .asTableCatalog
      convertCreateTable(catalog, identifier, create)

    case CreateTableAsSelectStatement(
        AsTableIdentifier(table), query, partitionCols, bucketSpec, properties,
        V1WriteProvider(provider), options, location, comment, ifNotExists) =>

      val tableDesc = buildCatalogTable(table, new StructType, partitionCols, bucketSpec,
        properties, provider, options, location, comment, ifNotExists)
      val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

      CreateTable(tableDesc, mode, Some(query))

    case create: CreateTableAsSelectStatement =>
      // the provider was not a v1 source, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = create.tableName
      val catalog = maybeCatalog.orElse(defaultCatalog)
          .getOrElse(throw new AnalysisException(
            s"No catalog specified for table ${identifier.quoted} and no default catalog is set"))
          .asTableCatalog
      convertCTAS(catalog, identifier, create)

    case DropTableStatement(CatalogObjectIdentifier(Some(catalog), ident), ifExists, _) =>
      DropTable(catalog.asTableCatalog, ident, ifExists)

    case DropTableStatement(AsTableIdentifier(tableName), ifExists, purge) =>
      DropTableCommand(tableName, ifExists, isView = false, purge)

    case DropViewStatement(CatalogObjectIdentifier(Some(catalog), ident), _) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view $ident " +
          s"because view support in catalog has not been implemented yet")

    case DropViewStatement(AsTableIdentifier(tableName), ifExists) =>
      DropTableCommand(tableName, ifExists, isView = true, purge = false)

    case AlterTableSetPropertiesStatement(AsTableIdentifier(table), properties) =>
      AlterTableSetPropertiesCommand(table, properties, isView = false)

    case AlterViewSetPropertiesStatement(AsTableIdentifier(table), properties) =>
      AlterTableSetPropertiesCommand(table, properties, isView = true)

    case AlterTableUnsetPropertiesStatement(AsTableIdentifier(table), propertyKeys, ifExists) =>
      AlterTableUnsetPropertiesCommand(table, propertyKeys, ifExists, isView = false)

    case AlterViewUnsetPropertiesStatement(AsTableIdentifier(table), propertyKeys, ifExists) =>
      AlterTableUnsetPropertiesCommand(table, propertyKeys, ifExists, isView = true)

    case AlterTableSetLocationStatement(AsTableIdentifier(table), newLocation) =>
      AlterTableSetLocationCommand(table, None, newLocation)

    case AlterTableAddColumnsStatement(AsTableIdentifier(table), newColumns)
        if newColumns.forall(_.name.size == 1) =>
      // only top-level adds are supported using AlterTableAddColumnsCommand
      AlterTableAddColumnsCommand(table, newColumns.map(convertToStructField))
  }

  object V1WriteProvider {
    private val v1WriteOverrideSet =
      conf.useV1SourceWriterList.toLowerCase(Locale.ROOT).split(",").toSet

    def unapply(provider: String): Option[String] = {
      if (v1WriteOverrideSet.contains(provider.toLowerCase(Locale.ROOT))) {
        Some(provider)
      } else {
        lazy val providerClass = DataSource.lookupDataSource(provider, conf)
        provider match {
          case _ if classOf[TableProvider].isAssignableFrom(providerClass) =>
            None
          case _ =>
            Some(provider)
        }
      }
    }
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

  private def convertCTAS(
      catalog: TableCatalog,
      identifier: Identifier,
      ctas: CreateTableAsSelectStatement): CreateTableAsSelect = {
    // convert the bucket spec and add it as a transform
    val partitioning = ctas.partitioning ++ ctas.bucketSpec.map(_.asTransform)
    val properties = convertTableProperties(
      ctas.properties, ctas.options, ctas.location, ctas.comment, ctas.provider)

    CreateTableAsSelect(
      catalog,
      identifier,
      partitioning,
      ctas.asSelect,
      properties,
      writeOptions = ctas.options.filterKeys(_ != "path"),
      ignoreIfExists = ctas.ifNotExists)
  }

  private def convertCreateTable(
      catalog: TableCatalog,
      identifier: Identifier,
      create: CreateTableStatement): CreateV2Table = {
    // convert the bucket spec and add it as a transform
    val partitioning = create.partitioning ++ create.bucketSpec.map(_.asTransform)
    val properties = convertTableProperties(
      create.properties, create.options, create.location, create.comment, create.provider)

    CreateV2Table(
      catalog,
      identifier,
      create.tableSchema,
      partitioning,
      properties,
      ignoreIfExists = create.ifNotExists)
  }

  private def convertTableProperties(
      properties: Map[String, String],
      options: Map[String, String],
      location: Option[String],
      comment: Option[String],
      provider: String): Map[String, String] = {
    if (options.contains("path") && location.isDefined) {
      throw new AnalysisException(
        "LOCATION and 'path' in OPTIONS are both used to indicate the custom table path, " +
            "you can only specify one of them.")
    }

    if ((options.contains("comment") || properties.contains("comment"))
        && comment.isDefined) {
      throw new AnalysisException(
        "COMMENT and option/property 'comment' are both used to set the table comment, you can " +
            "only specify one of them.")
    }

    if (options.contains("provider") || properties.contains("provider")) {
      throw new AnalysisException(
        "USING and option/property 'provider' are both used to set the provider implementation, " +
            "you can only specify one of them.")
    }

    val filteredOptions = options.filterKeys(_ != "path")

    // create table properties from TBLPROPERTIES and OPTIONS clauses
    val tableProperties = new mutable.HashMap[String, String]()
    tableProperties ++= properties
    tableProperties ++= filteredOptions

    // convert USING, LOCATION, and COMMENT clauses to table properties
    tableProperties += ("provider" -> provider)
    comment.map(text => tableProperties += ("comment" -> text))
    location.orElse(options.get("path")).map(loc => tableProperties += ("location" -> loc))

    tableProperties.toMap
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
}
