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

package org.apache.spark.sql.execution.datasources.v2

import java.util.regex.Pattern

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.TimeTravelSpec
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogV2Util, DataSourceCatalogResolver, Identifier, SessionConfigSupport, StagedTable, StagingTableCatalog, SupportsCatalogOptions, SupportsRead, Table, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private[sql] object DataSourceV2Utils extends Logging {

  /**
   * Helper method that extracts and transforms session configs into k/v pairs, the k/v pairs will
   * be used to create data source options.
   * Only extract when `ds` implements [[SessionConfigSupport]], in this case we may fetch the
   * specified key-prefix from `ds`, and extract session configs with config keys that start with
   * `spark.datasource.$keyPrefix`. A session config `spark.datasource.$keyPrefix.xxx -> yyy` will
   * be transformed into `xxx -> yyy`.
   *
   * @param source a [[TableProvider]] object
   * @param conf the session conf
   * @return an immutable map that contains all the extracted and transformed k/v pairs.
   */
  def extractSessionConfigs(source: TableProvider, conf: SQLConf): Map[String, String] = {
    source match {
      case cs: SessionConfigSupport =>
        val keyPrefix = cs.keyPrefix()
        require(keyPrefix != null, "The data source config key prefix can't be null.")

        val pattern = Pattern.compile(s"^spark\\.datasource\\.$keyPrefix\\.(.+)")

        conf.getAllConfs.flatMap { case (key, value) =>
          val m = pattern.matcher(key)
          if (m.matches() && m.groupCount() > 0) {
            Seq((m.group(1), value))
          } else {
            Seq.empty
          }
        }

      case _ => Map.empty
    }
  }

  def getTableFromProvider(
      provider: TableProvider,
      options: CaseInsensitiveStringMap,
      userSpecifiedSchema: Option[StructType]): Table = {
    userSpecifiedSchema match {
      case Some(schema) =>
        if (provider.supportsExternalMetadata()) {
          provider.getTable(
            schema,
            provider.inferPartitioning(options),
            options.asCaseSensitiveMap())
        } else {
          throw QueryExecutionErrors.userSpecifiedSchemaUnsupportedByDataSourceError(provider)
        }

      case None =>
        provider.getTable(
          provider.inferSchema(options),
          provider.inferPartitioning(options),
          options.asCaseSensitiveMap())
    }
  }

  /**
   * Builds the [[CaseInsensitiveStringMap]] passed to a v2 [[TableProvider]]: session configs
   * extracted from the provider, merged with the caller-supplied options-with-path map.
   */
  def buildDsOptions(
      provider: TableProvider,
      conf: SQLConf,
      optionsWithPath: CaseInsensitiveMap[String]): CaseInsensitiveStringMap = {
    val sessionOptions = extractSessionConfigs(provider, conf)
    val finalOptions = sessionOptions.filter { case (k, _) => !optionsWithPath.contains(k) } ++
      optionsWithPath.originalMap
    new CaseInsensitiveStringMap(finalOptions.asJava)
  }

  /**
   * Extracts the catalog name and connector-canonical identifier from a
   * [[SupportsCatalogOptions]] provider. Shared by all SCO entry points (DataFrame reader, SQL
   * multipart-name resolution) so they observe identical null-handling semantics:
   * `extractCatalog` returning null falls back to the session catalog, matching
   * [[CatalogV2Util.getTableProviderCatalog]].
   */
  def extractCatalogAndIdentifier(
      provider: SupportsCatalogOptions,
      dsOptions: CaseInsensitiveStringMap): (String, Identifier) = {
    val ident = provider.extractIdentifier(dsOptions)
    val catalogName = Option(provider.extractCatalog(dsOptions))
      .getOrElse(CatalogManager.SESSION_CATALOG_NAME)
    (catalogName, ident)
  }

  /**
   * Resolver bound to a session [[SQLConf]] that maps a multipart SQL identifier
   * (e.g. `pathformat.\`/path/to/t\``) to a `(catalogName, identifier)` pair when the head
   * names a registered [[SupportsCatalogOptions]] data source. Returns `None` for non-SCO
   * sources or unknown format heads, letting the caller fall back to standard catalog
   * resolution.
   */
  def supportsCatalogOptionsResolver(conf: SQLConf): DataSourceCatalogResolver =
    (nameParts: Seq[String]) =>
      try {
        DataSource.lookupDataSourceV2(nameParts.head, conf).flatMap {
          case sco: SupportsCatalogOptions =>
            val optionsWithPath = getOptionsWithPaths(
              CaseInsensitiveMap(Map.empty), nameParts.tail.mkString("."))
            val dsOptions = buildDsOptions(sco, conf, optionsWithPath)
            Some(extractCatalogAndIdentifier(sco, dsOptions))
          case _ => None
        }
      } catch {
        // The format name is not a registered data source. Fall through and let the caller
        // treat it as a catalog/namespace name.
        case _: ClassNotFoundException => None
      }

  def loadV2Source(
      sparkSession: SparkSession,
      provider: TableProvider,
      userSpecifiedSchema: Option[StructType],
      extraOptions: CaseInsensitiveMap[String],
      source: String,
      paths: String*): Option[LogicalPlan] = {
    val catalogManager = sparkSession.sessionState.catalogManager
    val conf = sparkSession.sessionState.conf
    val optionsWithPath = getOptionsWithPaths(extraOptions, paths: _*)
    val dsOptions = buildDsOptions(provider, conf, optionsWithPath)
    val (table, catalog, ident, timeTravelSpec) = provider match {
      case _: SupportsCatalogOptions if userSpecifiedSchema.nonEmpty =>
        throw new IllegalArgumentException(
          s"$source does not support user specified schema. Please don't specify the schema.")
      case hasCatalog: SupportsCatalogOptions =>
        import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
        val (catalogName, ident) = extractCatalogAndIdentifier(hasCatalog, dsOptions)
        val catalog = catalogManager.catalog(catalogName).asTableCatalog

        val version = hasCatalog.extractTimeTravelVersion(dsOptions)
        val timestamp = hasCatalog.extractTimeTravelTimestamp(dsOptions)

        val timeTravelVersion = if (version.isPresent) Some(version.get) else None
        val timeTravelTimestamp = if (timestamp.isPresent) {
          if (timestamp.get.forall(_.isDigit)) {
            Some(Literal(timestamp.get.toLong, LongType))
          } else {
            Some(Literal(timestamp.get))
          }
        } else {
          None
        }
        val timeTravel = TimeTravelSpec.create(
          timeTravelTimestamp, timeTravelVersion, conf.sessionLocalTimeZone)
        val tbl = CatalogV2Util.getTable(catalog, ident, timeTravel)
        (tbl, Some(catalog), Some(ident), timeTravel)
      case _ =>
        // TODO: Non-catalog paths for DSV2 are currently not well defined.
        val tbl = DataSourceV2Utils.getTableFromProvider(provider, dsOptions, userSpecifiedSchema)
        (tbl, None, None, None)
    }
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    table match {
      case _: SupportsRead if table.supports(BATCH_READ) =>
        Option(DataSourceV2Relation.create(table, catalog, ident, dsOptions, timeTravelSpec))
      case _ => None
    }
  }

  /**
   * Returns the table provider for the given format, or None if it cannot be found.
   */
  def getTableProvider(provider: String, conf: SQLConf): Option[TableProvider] = {
    // Return earlier since `lookupDataSourceV2` may fail to resolve provider "hive" to
    // `HiveFileFormat`, when running tests in sql/core.
    if (DDLUtils.isHiveTable(Some(provider))) return None
    DataSource.lookupDataSourceV2(provider, conf) match {
      // TODO(SPARK-28396): Currently file source v2 can't work with tables.
      case Some(p) if !p.isInstanceOf[FileDataSourceV2] => Some(p)
      case _ => None
    }
  }

  private lazy val objectMapper = new ObjectMapper()
  def getOptionsWithPaths(
      extraOptions: CaseInsensitiveMap[String],
      paths: String*): CaseInsensitiveMap[String] = {
    if (paths.isEmpty) {
      extraOptions
    } else if (paths.length == 1) {
      extraOptions + ("path" -> paths.head)
    } else {
      extraOptions + ("paths" -> objectMapper.writeValueAsString(paths.toArray))
    }
  }

  /**
   * If `table` is a StagedTable, commit the staged changes and report the commit metrics.
   * Do nothing if the table is not a StagedTable.
   */
  def commitStagedChanges(
      sparkContext: SparkContext, table: Table, metrics: Map[String, SQLMetric]): Unit = {
    table match {
      case stagedTable: StagedTable =>
        stagedTable.commitStagedChanges()

        val driverMetrics = stagedTable.reportDriverMetrics()
        if (driverMetrics.nonEmpty) {
          for (taskMetric <- driverMetrics) {
            metrics.get(taskMetric.name()).foreach(_.set(taskMetric.value()))
          }

          val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
          SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        }
      case _ =>
    }
  }

  def commitMetrics(
      sparkContext: SparkContext, tableCatalog: StagingTableCatalog): Map[String, SQLMetric] = {
    tableCatalog.supportedCustomMetrics().map {
      metric => metric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, metric)
    }.toMap
  }
}
