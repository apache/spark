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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SessionConfigSupport, SupportsCatalogOptions, SupportsRead, Table, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
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
      paths: String*): Option[DataFrame] = {
    val catalogManager = sparkSession.sessionState.catalogManager
    val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
      source = provider, conf = sparkSession.sessionState.conf)

    val optionsWithPath = getOptionsWithPaths(extraOptions, paths: _*)

    val finalOptions = sessionOptions.filterKeys(!optionsWithPath.contains(_)).toMap ++
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
        (catalog.loadTable(ident), Some(catalog), Some(ident))
      case _ =>
        // TODO: Non-catalog paths for DSV2 are currently not well defined.
        val tbl = DataSourceV2Utils.getTableFromProvider(provider, dsOptions, userSpecifiedSchema)
        (tbl, None, None)
    }
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    table match {
      case _: SupportsRead if table.supports(BATCH_READ) =>
        Option(Dataset.ofRows(
          sparkSession,
          DataSourceV2Relation.create(table, catalog, ident, dsOptions)))
      case _ => None
    }
  }

  private def getOptionsWithPaths(
      extraOptions: CaseInsensitiveMap[String],
      paths: String*): CaseInsensitiveMap[String] = {
    if (paths.isEmpty) {
      extraOptions
    } else if (paths.length == 1) {
      extraOptions + ("path" -> paths.head)
    } else {
      val objectMapper = new ObjectMapper()
      extraOptions + ("paths" -> objectMapper.writeValueAsString(paths.toArray))
    }
  }
}
