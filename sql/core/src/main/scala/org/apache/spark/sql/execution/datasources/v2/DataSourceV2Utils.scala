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
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SessionConfigSupport, StagedTable, StagingTableCatalog, SupportsCatalogOptions, SupportsRead, Table, TableProvider}
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

  def loadV2Source(
      sparkSession: SparkSession,
      provider: TableProvider,
      userSpecifiedSchema: Option[StructType],
      extraOptions: CaseInsensitiveMap[String],
      source: String,
      paths: String*): Option[LogicalPlan] = {
    val catalogManager = sparkSession.sessionState.catalogManager
    val conf = sparkSession.sessionState.conf
    val sessionOptions = DataSourceV2Utils.extractSessionConfigs(provider, conf)

    val optionsWithPath = getOptionsWithPaths(extraOptions, paths: _*)

    val finalOptions = sessionOptions.filter { case (k, _) => !optionsWithPath.contains(k) } ++
      optionsWithPath.originalMap
    val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
    val (table, catalog, ident) = provider match {
      case _: SupportsCatalogOptions if userSpecifiedSchema.nonEmpty =>
        throw new IllegalArgumentException(
          s"$source does not support user specified schema. Please don't specify the schema.")
      case hasCatalog: SupportsCatalogOptions =>
        val ident = hasCatalog.extractIdentifier(dsOptions)
        val catalog = CatalogV2Util.getTableProviderCatalog(
          hasCatalog,
          catalogManager,
          dsOptions)

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
        (CatalogV2Util.getTable(catalog, ident, timeTravel), Some(catalog), Some(ident))
      case _ =>
        // TODO: Non-catalog paths for DSV2 are currently not well defined.
        val tbl = DataSourceV2Utils.getTableFromProvider(provider, dsOptions, userSpecifiedSchema)
        (tbl, None, None)
    }
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    table match {
      case _: SupportsRead if table.supports(BATCH_READ) =>
        Option(DataSourceV2Relation.create(table, catalog, ident, dsOptions))
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
