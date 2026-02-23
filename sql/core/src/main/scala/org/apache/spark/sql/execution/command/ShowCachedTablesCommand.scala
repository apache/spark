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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.execution.datasources.{LogicalRelationWithTable}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StringType

/**
 * The command for `SHOW CACHED TABLES`.
 * Returns the qualified names of all cached tables/views.
 */
case class ShowCachedTablesCommand() extends LeafRunnableCommand {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("tableName", StringType, nullable = false)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val cacheManager = sparkSession.sharedState.cacheManager
    val fromEntries = listCachedTableNameParts(cacheManager.listCachedEntries())
    // Include temp view names that share a cached plan (e.g. t1 and t2 with same query).
    // Use the catalog's view plan instead of session.table(name) to avoid StackOverflow from
    // analysis/normalization during cache lookup.
    val catalog = sparkSession.sessionState.catalog
    val classicSession = sparkSession.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    val fromTempViews = catalog.getTempViewNames().flatMap { name =>
      try {
        catalog.getTempView(name).map { view =>
          val relation = SubqueryAlias(name, view)
          cacheManager.lookupCachedData(classicSession, relation).map(_ => Seq(name))
        }.flatten
      } catch {
        case _: Exception => None
      }
    }
    val namePartsList = (fromEntries ++ fromTempViews).distinct
    namePartsList.map { parts => Row(parts.mkString(".")) }
  }

  private def listCachedTableNameParts(
      entries: Seq[(LogicalPlan, Option[String])]): Seq[Seq[String]] = {
    entries.flatMap { case (plan, tableNameOpt) =>
      val fromPlan = EliminateSubqueryAliases(plan) match {
        case LogicalRelationWithTable(_, Some(catalogTable)) =>
          Some(catalogTable.identifier.nameParts)
        case DataSourceV2Relation(_, _, Some(catalog), Some(v2Ident), _, timeTravelSpec)
            if timeTravelSpec.isEmpty =>
          Some(v2Ident.toQualifiedNameParts(catalog))
        case v: View =>
          Some(v.desc.identifier.nameParts)
        case HiveTableRelation(catalogTable, _, _, _, _) =>
          Some(catalogTable.identifier.nameParts)
        case _ =>
          None
      }
      fromPlan.orElse(
        tableNameOpt.map(name => name.split("\\.").toSeq.filter(_.nonEmpty)))
    }.filter(_.nonEmpty).distinct
  }
}
