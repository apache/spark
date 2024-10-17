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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{
  CatalogTableType,
  TemporaryViewRelation,
  UnresolvedCatalogRelation
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{
  CatalogManager,
  CatalogPlugin,
  CatalogV2Util,
  Identifier,
  LookupCatalog,
  Table,
  V1Table,
  V2TableWithV1Fallback
}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

class RelationResolution(override val catalogManager: CatalogManager)
    extends DataTypeErrorsBase
    with Logging
    with LookupCatalog
    with SQLConfHelper {
  val v1SessionCatalog = catalogManager.v1SessionCatalog

  /**
   * If we are resolving database objects (relations, functions, etc.) inside views, we may need to
   * expand single or multi-part identifiers with the current catalog and namespace of when the
   * view was created.
   */
  def expandIdentifier(nameParts: Seq[String]): Seq[String] = {
    if (!isResolvingView || isReferredTempViewName(nameParts)) {
      return nameParts
    }

    if (nameParts.length == 1) {
      AnalysisContext.get.catalogAndNamespace :+ nameParts.head
    } else if (catalogManager.isCatalogRegistered(nameParts.head)) {
      nameParts
    } else {
      AnalysisContext.get.catalogAndNamespace.head +: nameParts
    }
  }

  /**
   * Lookup temporary view by `identifier`. Returns `None` if the view wasn't found.
   */
  def lookupTempView(identifier: Seq[String]): Option[TemporaryViewRelation] = {
    // We are resolving a view and this name is not a temp view when that view was created. We
    // return None earlier here.
    if (isResolvingView && !isReferredTempViewName(identifier)) {
      return None
    }

    v1SessionCatalog.getRawLocalOrGlobalTempView(identifier)
  }

  /**
   * Resolve relation `u` to v1 relation if it's a v1 table from the session catalog, or to v2
   * relation. This is for resolving DML commands and SELECT queries.
   */
  def resolveRelation(
      u: UnresolvedRelation,
      timeTravelSpec: Option[TimeTravelSpec] = None): Option[LogicalPlan] = {
    val timeTravelSpecFromOptions = TimeTravelSpec.fromOptions(
      u.options,
      conf.getConf(SQLConf.TIME_TRAVEL_TIMESTAMP_KEY),
      conf.getConf(SQLConf.TIME_TRAVEL_VERSION_KEY),
      conf.sessionLocalTimeZone
    )
    if (timeTravelSpec.nonEmpty && timeTravelSpecFromOptions.nonEmpty) {
      throw new AnalysisException("MULTIPLE_TIME_TRAVEL_SPEC", Map.empty[String, String])
    }
    val finalTimeTravelSpec = timeTravelSpec.orElse(timeTravelSpecFromOptions)
    resolveTempView(
      u.multipartIdentifier,
      u.isStreaming,
      finalTimeTravelSpec.isDefined
    ).orElse {
      expandIdentifier(u.multipartIdentifier) match {
        case CatalogAndIdentifier(catalog, ident) =>
          val key =
            (
              (catalog.name +: ident.namespace :+ ident.name).toImmutableArraySeq,
              finalTimeTravelSpec
            )
          AnalysisContext.get.relationCache
            .get(key)
            .map { cache =>
              val cachedRelation = cache.transform {
                case multi: MultiInstanceRelation =>
                  val newRelation = multi.newInstance()
                  newRelation.copyTagsFrom(multi)
                  newRelation
              }
              u.getTagValue(LogicalPlan.PLAN_ID_TAG)
                .map { planId =>
                  val cachedConnectRelation = cachedRelation.clone()
                  cachedConnectRelation.setTagValue(LogicalPlan.PLAN_ID_TAG, planId)
                  cachedConnectRelation
                }
                .getOrElse(cachedRelation)
            }
            .orElse {
              val writePrivilegesString =
                Option(u.options.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES))
              val table =
                CatalogV2Util.loadTable(catalog, ident, finalTimeTravelSpec, writePrivilegesString)
              val loaded = createRelation(
                catalog,
                ident,
                table,
                u.clearWritePrivileges.options,
                u.isStreaming
              )
              loaded.foreach(AnalysisContext.get.relationCache.update(key, _))
              u.getTagValue(LogicalPlan.PLAN_ID_TAG)
                .map { planId =>
                  loaded.map { loadedRelation =>
                    val loadedConnectRelation = loadedRelation.clone()
                    loadedConnectRelation.setTagValue(LogicalPlan.PLAN_ID_TAG, planId)
                    loadedConnectRelation
                  }
                }
                .getOrElse(loaded)
            }
        case _ => None
      }
    }
  }

  private def createRelation(
      catalog: CatalogPlugin,
      ident: Identifier,
      table: Option[Table],
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean): Option[LogicalPlan] = {
    table.map {
      // To utilize this code path to execute V1 commands, e.g. INSERT,
      // either it must be session catalog, or tracksPartitionsInCatalog
      // must be false so it does not require use catalog to manage partitions.
      // Obviously we cannot execute V1Table by V1 code path if the table
      // is not from session catalog and the table still requires its catalog
      // to manage partitions.
      case v1Table: V1Table
          if CatalogV2Util.isSessionCatalog(catalog)
          || !v1Table.catalogTable.tracksPartitionsInCatalog =>
        if (isStreaming) {
          if (v1Table.v1Table.tableType == CatalogTableType.VIEW) {
            throw QueryCompilationErrors.permanentViewNotSupportedByStreamingReadingAPIError(
              ident.quoted
            )
          }
          SubqueryAlias(
            catalog.name +: ident.asMultipartIdentifier,
            UnresolvedCatalogRelation(v1Table.v1Table, options, isStreaming = true)
          )
        } else {
          v1SessionCatalog.getRelation(v1Table.v1Table, options)
        }

      case table =>
        if (isStreaming) {
          val v1Fallback = table match {
            case withFallback: V2TableWithV1Fallback =>
              Some(UnresolvedCatalogRelation(withFallback.v1Table, isStreaming = true))
            case _ => None
          }
          SubqueryAlias(
            catalog.name +: ident.asMultipartIdentifier,
            StreamingRelationV2(
              None,
              table.name,
              table,
              options,
              table.columns.toAttributes,
              Some(catalog),
              Some(ident),
              v1Fallback
            )
          )
        } else {
          SubqueryAlias(
            catalog.name +: ident.asMultipartIdentifier,
            DataSourceV2Relation.create(table, Some(catalog), Some(ident), options)
          )
        }
    }
  }

  private def resolveTempView(
      identifier: Seq[String],
      isStreaming: Boolean = false,
      isTimeTravel: Boolean = false): Option[LogicalPlan] = {
    lookupTempView(identifier).map { v =>
      val tempViewPlan = v1SessionCatalog.getTempViewRelation(v)
      if (isStreaming && !tempViewPlan.isStreaming) {
        throw QueryCompilationErrors.readNonStreamingTempViewError(identifier.quoted)
      }
      if (isTimeTravel) {
        throw QueryCompilationErrors.timeTravelUnsupportedError(toSQLId(identifier))
      }
      tempViewPlan
    }
  }

  private def isResolvingView: Boolean = AnalysisContext.get.catalogAndNamespace.nonEmpty

  private def isReferredTempViewName(nameParts: Seq[String]): Boolean = {
    val resolver = conf.resolver
    AnalysisContext.get.referredTempViewNames.exists { n =>
      (n.length == nameParts.length) && n.zip(nameParts).forall {
        case (a, b) => resolver(a, b)
      }
    }
  }
}
