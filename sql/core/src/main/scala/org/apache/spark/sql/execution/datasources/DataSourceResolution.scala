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
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.sql.{CreateTableAsSelectStatement, CreateTableStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.TableProvider
import org.apache.spark.sql.types.StructType

case class DataSourceResolution(
    conf: SQLConf,
    findCatalog: String => CatalogPlugin)
  extends Rule[LogicalPlan] with CastSupport with LookupCatalog {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  override def lookupCatalog: Option[String => CatalogPlugin] = Some(findCatalog)

  def defaultCatalog: Option[CatalogPlugin] = conf.defaultV2Catalog.map(findCatalog)

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateTableStatement(
        AsTableIdentifier(table), schema, partitionCols, bucketSpec, properties,
        V1WriteProvider(provider), options, location, comment, ifNotExists) =>

      val tableDesc = buildCatalogTable(table, schema, partitionCols, bucketSpec, properties,
        provider, options, location, comment, ifNotExists)
      val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

      CreateTable(tableDesc, mode, None)

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
    if (ctas.options.contains("path") && ctas.location.isDefined) {
      throw new AnalysisException(
        "LOCATION and 'path' in OPTIONS are both used to indicate the custom table path, " +
            "you can only specify one of them.")
    }

    if ((ctas.options.contains("provider") || ctas.properties.contains("provider"))
        && ctas.comment.isDefined) {
      throw new AnalysisException(
        "COMMENT and option/property 'comment' are both used to set the table comment, you can " +
            "only specify one of them.")
    }

    if (ctas.options.contains("provider") || ctas.properties.contains("provider")) {
      throw new AnalysisException(
        "USING and option/property 'provider' are both used to set the provider implementation, " +
            "you can only specify one of them.")
    }

    val options = ctas.options.filterKeys(_ != "path")

    // convert the bucket spec and add it as a transform
    val partitioning = ctas.partitioning ++ ctas.bucketSpec.map(_.asTransform)

    // create table properties from TBLPROPERTIES and OPTIONS clauses
    val properties = new mutable.HashMap[String, String]()
    properties ++= ctas.properties
    properties ++= options

    // convert USING, LOCATION, and COMMENT clauses to table properties
    properties += ("provider" -> ctas.provider)
    ctas.comment.map(text => properties += ("comment" -> text))
    ctas.location
        .orElse(ctas.options.get("path"))
        .map(loc => properties += ("location" -> loc))

    CreateTableAsSelect(
      catalog,
      identifier,
      partitioning,
      ctas.asSelect,
      properties.toMap,
      writeOptions = options,
      ignoreIfExists = ctas.ifNotExists)
  }
}
