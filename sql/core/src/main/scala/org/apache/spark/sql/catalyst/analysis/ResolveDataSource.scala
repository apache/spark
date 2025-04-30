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

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedDataSource}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Utils, FileDataSourceV2}
import org.apache.spark.sql.execution.datasources.v2.python.PythonDataSourceV2
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Resolves the relations created from the DataFrameReader and DataStreamReader APIs. */
class ResolveDataSource(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case UnresolvedDataSource(source, userSpecifiedSchema, extraOptions, false, paths) =>
      // Batch data source created from DataFrameReader
      if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
        throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("read")
      }

      val legacyPathOptionBehavior = sparkSession.sessionState.conf.legacyPathOptionBehavior
      if (!legacyPathOptionBehavior &&
        (extraOptions.contains("path") || extraOptions.contains("paths")) && paths.nonEmpty) {
        throw QueryCompilationErrors.pathOptionNotSetCorrectlyWhenReadingError()
      }

      DataSource.lookupDataSourceV2(source, conf).flatMap { provider =>
        DataSourceV2Utils.loadV2Source(sparkSession, provider, userSpecifiedSchema, extraOptions,
          source, paths: _*)
      }.getOrElse(loadV1BatchSource(source, userSpecifiedSchema, extraOptions, paths: _*))

    case UnresolvedDataSource(source, userSpecifiedSchema, extraOptions, true, paths) =>
      // Streaming data source created from DataStreamReader
      if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
        throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("read")
      }
      val path = paths.headOption
      val optionsWithPath = if (path.isEmpty) {
        extraOptions
      } else {
        extraOptions + ("path" -> path.get)
      }

      val ds = DataSource.lookupDataSource(source, sparkSession.sessionState.conf)
        .getConstructor().newInstance()
      // We need to generate the V1 data source so we can pass it to the V2 relation as a shim.
      // We can't be sure at this point whether we'll actually want to use V2, since we don't know
      // the writer or whether the query is continuous.
      val v1DataSource = DataSource(
        sparkSession,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = optionsWithPath.originalMap)
      val v1Relation = ds match {
        case _: StreamSourceProvider => Some(StreamingRelation(v1DataSource))
        case _ => None
      }
      ds match {
        // file source v2 does not support streaming yet.
        case provider: TableProvider if !provider.isInstanceOf[FileDataSourceV2] =>
          val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
            source = provider, conf = sparkSession.sessionState.conf)
          val finalOptions =
            sessionOptions.filter { case (k, _) => !optionsWithPath.contains(k) } ++
            optionsWithPath.originalMap
          val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
          provider match {
            case p: PythonDataSourceV2 => p.setShortName(source)
            case _ =>
          }
          val table =
            DataSourceV2Utils.getTableFromProvider(provider, dsOptions, userSpecifiedSchema)
          import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
          table match {
            case _: SupportsRead if table.supportsAny(MICRO_BATCH_READ, CONTINUOUS_READ) =>
              import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
              StreamingRelationV2(
                  Some(provider), source, table, dsOptions,
                  toAttributes(table.columns.asSchema), None, None, v1Relation)

            // fallback to v1
            // TODO (SPARK-27483): we should move this fallback logic to an analyzer rule.
            case _ => StreamingRelation(v1DataSource)
          }

        case _ =>
          // Code path for data source v1.
          StreamingRelation(v1DataSource)
      }

  }

  private def loadV1BatchSource(
      source: String,
      userSpecifiedSchema: Option[StructType],
      extraOptions: CaseInsensitiveMap[String],
      paths: String*): LogicalPlan = {
    val legacyPathOptionBehavior = sparkSession.sessionState.conf.legacyPathOptionBehavior
    val (finalPaths, finalOptions) = if (!legacyPathOptionBehavior && paths.length == 1) {
      (Nil, extraOptions + ("path" -> paths.head))
    } else {
      (paths, extraOptions)
    }

    // Code path for data source v1.
    LogicalRelation(
      DataSource.apply(
        sparkSession,
        paths = finalPaths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = finalOptions.originalMap).resolveRelation(readOnly = true))
  }
}
