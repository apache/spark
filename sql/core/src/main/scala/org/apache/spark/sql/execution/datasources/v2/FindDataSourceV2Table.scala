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

import java.util.concurrent.Callable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.sources.v2.TableProvider

/**
 * Replaces v2 source [[UnresolvedCatalogRelation]] with concrete relation logical plans.
 */
class FindDataSourceV2Table(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def readDataSourceTable(
      table: CatalogTable): LogicalPlan = {
    val qualifiedTableName = QualifiedTableName(table.database, table.identifier.table)
    val catalog = sparkSession.sessionState.catalog
    catalog.getCachedPlan(qualifiedTableName, new Callable[LogicalPlan]() {
      override def call(): LogicalPlan = {
        val cls = DataSourceV2Utils.isV2Source(sparkSession, table.provider.get)
        val provider = cls.get.getConstructor().newInstance().asInstanceOf[TableProvider]
        val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
        val dsOptions = DataSourceV2Utils.
          extractSessionConfigs(provider, sparkSession.sessionState.conf, pathOption.toMap)
        val readTable = DataSourceV2Utils.
          getBatchReadTable(sparkSession, Option(table.schema), cls.get, dsOptions)
        DataSourceV2Relation.create(readTable.get, dsOptions)
      }
    })
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _)
        if DDLUtils.isDatasourceTable(tableMeta) =>
      val pathOption = tableMeta.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
      val shouldReadInV2 = DataSourceV2Utils.shouldReadWithV2(
        sparkSession,
        Option(tableMeta.schema),
        tableMeta.provider.get,
        pathOption.toMap)

      if (shouldReadInV2) {
        i.copy(table = readDataSourceTable(tableMeta))
      } else {
        i
      }

    case unresolved @ UnresolvedCatalogRelation(tableMeta) if
        DDLUtils.isDatasourceTable(tableMeta) =>
      val pathOption = tableMeta.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
      val shouldReadInV2 = DataSourceV2Utils.shouldReadWithV2(
        sparkSession,
        Option(tableMeta.schema),
        tableMeta.provider.get,
        pathOption.toMap)

      if (shouldReadInV2) {
        readDataSourceTable(tableMeta)
      } else {
        unresolved
      }
  }
}
