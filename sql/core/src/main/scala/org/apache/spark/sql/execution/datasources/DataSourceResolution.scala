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
import org.apache.spark.sql.catalyst.analysis.{CastSupport, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateV2Table, DeleteFromTable, DropTable, Filter, LogicalPlan, ReplaceTable, ReplaceTableAsSelect, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.logical.sql.{AlterTableAddColumnsStatement, AlterTableSetLocationStatement, AlterTableSetPropertiesStatement, AlterTableUnsetPropertiesStatement, AlterViewSetPropertiesStatement, AlterViewUnsetPropertiesStatement, CreateTableAsSelectStatement, CreateTableStatement, DeleteFromStatement, DescribeColumnStatement, DescribeTableStatement, DropTableStatement, DropViewStatement, QualifiedColType, ReplaceTableAsSelectStatement, ReplaceTableStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableSetLocationCommand, AlterTableSetPropertiesCommand, AlterTableUnsetPropertiesCommand, DescribeColumnCommand, DescribeTableCommand, DropTableCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.TableProvider
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, HiveStringType, MetadataBuilder, StructField, StructType}

case class DataSourceResolution(
    conf: SQLConf,
    lookup: LookupCatalog)
  extends Rule[LogicalPlan] with CastSupport {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._
  import lookup._

  lazy val v2SessionCatalog: CatalogPlugin = lookup.sessionCatalog
      .getOrElse(throw new AnalysisException("No v2 session catalog implementation is available"))

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateTableStatement(
        AsTableIdentifier(table), schema, partitionCols, bucketSpec, properties,
        V1WriteProvider(provider), options, location, comment, ifNotExists) =>
      // the source is v1, the identifier has no catalog, and there is no default v2 catalog
      val tableDesc = buildCatalogTable(table, schema, partitionCols, bucketSpec, properties,
        provider, options, location, comment, ifNotExists)
      val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

      CreateTable(tableDesc, mode, None)

    case create: CreateTableStatement =>
      // the provider was not a v1 source or a v2 catalog is the default, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = create.tableName
      maybeCatalog match {
        case Some(catalog) =>
          // the identifier had a catalog, or there is a default v2 catalog
          convertCreateTable(catalog.asTableCatalog, identifier, create)
        case _ =>
          // the identifier had no catalog and no default catalog is set, but the source is v2.
          // use the v2 session catalog, which delegates to the global v1 session catalog
          convertCreateTable(v2SessionCatalog.asTableCatalog, identifier, create)
      }

    case CreateTableAsSelectStatement(
        AsTableIdentifier(table), query, partitionCols, bucketSpec, properties,
        V1WriteProvider(provider), options, location, comment, ifNotExists) =>
      // the source is v1, the identifier has no catalog, and there is no default v2 catalog
      val tableDesc = buildCatalogTable(table, new StructType, partitionCols, bucketSpec,
        properties, provider, options, location, comment, ifNotExists)
      val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

      CreateTable(tableDesc, mode, Some(query))

    case create: CreateTableAsSelectStatement =>
      // the provider was not a v1 source or a v2 catalog is the default, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = create.tableName
      maybeCatalog match {
        case Some(catalog) =>
          // the identifier had a catalog, or there is a default v2 catalog
          convertCTAS(catalog.asTableCatalog, identifier, create)
        case _ =>
          // the identifier had no catalog and no default catalog is set, but the source is v2.
          // use the v2 session catalog, which delegates to the global v1 session catalog
          convertCTAS(v2SessionCatalog.asTableCatalog, identifier, create)
      }

    case DescribeColumnStatement(
        AsTableIdentifier(tableName), colName, isExtended) =>
      DescribeColumnCommand(tableName, colName, isExtended)

    case DescribeColumnStatement(
        CatalogObjectIdentifier(Some(catalog), ident), colName, isExtended) =>
      throw new AnalysisException("Describing columns is not supported for v2 tables.")

    case DescribeTableStatement(
        AsTableIdentifier(tableName), partitionSpec, isExtended) =>
      DescribeTableCommand(tableName, partitionSpec, isExtended)

    case ReplaceTableStatement(
        AsTableIdentifier(table), schema, partitionCols, bucketSpec, properties,
        V1WriteProvider(provider), options, location, comment, orCreate) =>
        throw new AnalysisException(
          s"Replacing tables is not supported using the legacy / v1 Spark external catalog" +
            s" API. Write provider name: $provider, identifier: $table.")

    case ReplaceTableAsSelectStatement(
        AsTableIdentifier(table), query, partitionCols, bucketSpec, properties,
        V1WriteProvider(provider), options, location, comment, orCreate) =>
      throw new AnalysisException(
        s"Replacing tables is not supported using the legacy / v1 Spark external catalog" +
          s" API. Write provider name: $provider, identifier: $table.")

    case replace: ReplaceTableStatement =>
      // the provider was not a v1 source, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = replace.tableName
      val catalog = maybeCatalog.orElse(sessionCatalog)
        .getOrElse(throw new AnalysisException(
          s"No catalog specified for table ${identifier.quoted} and no default catalog is set"))
        .asTableCatalog
      convertReplaceTable(catalog, identifier, replace)

    case rtas: ReplaceTableAsSelectStatement =>
      // the provider was not a v1 source, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = rtas.tableName
      val catalog = maybeCatalog.orElse(sessionCatalog)
        .getOrElse(throw new AnalysisException(
          s"No catalog specified for table ${identifier.quoted} and no default catalog is set"))
        .asTableCatalog
      convertRTAS(catalog, identifier, rtas)

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

    case DeleteFromStatement(AsTableIdentifier(table), tableAlias, condition) =>
      throw new AnalysisException(
        s"Delete from tables is not supported using the legacy / v1 Spark external catalog" +
            s" API. Identifier: $table.")

    case delete: DeleteFromStatement =>
      val relation = UnresolvedRelation(delete.tableName)
      val aliased = delete.tableAlias.map(SubqueryAlias(_, relation)).getOrElse(relation)
      DeleteFromTable(aliased, delete.condition)

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

  private def convertRTAS(
      catalog: TableCatalog,
      identifier: Identifier,
      rtas: ReplaceTableAsSelectStatement): ReplaceTableAsSelect = {
    // convert the bucket spec and add it as a transform
    val partitioning = rtas.partitioning ++ rtas.bucketSpec.map(_.asTransform)
    val properties = convertTableProperties(
      rtas.properties, rtas.options, rtas.location, rtas.comment, rtas.provider)

    ReplaceTableAsSelect(
      catalog,
      identifier,
      partitioning,
      rtas.asSelect,
      properties,
      writeOptions = rtas.options.filterKeys(_ != "path"),
      orCreate = rtas.orCreate)
  }

  private def convertReplaceTable(
      catalog: TableCatalog,
      identifier: Identifier,
      replace: ReplaceTableStatement): ReplaceTable = {
    // convert the bucket spec and add it as a transform
    val partitioning = replace.partitioning ++ replace.bucketSpec.map(_.asTransform)
    val properties = convertTableProperties(
      replace.properties, replace.options, replace.location, replace.comment, replace.provider)

    ReplaceTable(
      catalog,
      identifier,
      replace.tableSchema,
      partitioning,
      properties,
      orCreate = replace.orCreate)
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
