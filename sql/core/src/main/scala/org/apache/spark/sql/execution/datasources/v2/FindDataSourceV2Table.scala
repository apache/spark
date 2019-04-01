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

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.sources.v2.TableProvider
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/**
 * Replaces v2 source [[UnresolvedCatalogRelation]] with concrete relation logical plans.
 */
class FindDataSourceV2Table(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def readDataSourceTable(
      table: CatalogTable,
      clazz: Class[_]): LogicalPlan = {
    val qualifiedTableName = QualifiedTableName(table.database, table.identifier.table)
    val catalog = sparkSession.sessionState.catalog
    catalog.getCachedPlan(qualifiedTableName, new Callable[LogicalPlan]() {
      override def call(): LogicalPlan = {
        val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
        val provider = clazz.getConstructor().newInstance().asInstanceOf[TableProvider]
        val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
            source = provider, conf = sparkSession.sessionState.conf)
        val finalOptions = sessionOptions ++ pathOption
        val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
        val v2Table = provider.getTable(dsOptions)
        DataSourceV2Relation.create(v2Table, dsOptions)
      }
    })
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _)
        if DDLUtils.isDatasourceTable(tableMeta) =>
      val clsOpt = DataSourceV2Utils.isV2Source(tableMeta.provider.get, sparkSession)
      clsOpt.map { cls =>
        i.copy(table = readDataSourceTable(tableMeta, cls))
      }.getOrElse(i)

    case i @ UnresolvedCatalogRelation(tableMeta) if DDLUtils.isDatasourceTable(tableMeta) =>
      val clsOpt = DataSourceV2Utils.isV2Source(tableMeta.provider.get, sparkSession)
      clsOpt.map(readDataSourceTable(tableMeta, _)).getOrElse(i)
  }
}