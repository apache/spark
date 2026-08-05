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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private[sql] sealed trait SharedRelationCacheTableMatch {
  def matches(table: Table): Boolean
}

private[sql] object SharedRelationCacheTableMatch {
  // Used before a query-scoped table pin exists, after loading the current table identity.
  case class ByTableId(tableId: String) extends SharedRelationCacheTableMatch {
    override def matches(table: Table): Boolean = table.id == tableId
  }

  // Used after a table pin exists, when the exact concrete Table must not be replaced.
  case class ByTableInstance(table: Table) extends SharedRelationCacheTableMatch {
    override def matches(candidate: Table): Boolean = candidate eq table
  }
}

/** Exact criteria supported by the shared relation cache. */
private[sql] case class SharedRelationCacheCriteria(
    catalog: CatalogPlugin,
    identifier: Identifier,
    options: CaseInsensitiveStringMap,
    tableMatch: SharedRelationCacheTableMatch) {

  def nameParts: Seq[String] = catalog.name +: identifier.namespace.toSeq :+ identifier.name

  def matches(plan: LogicalPlan): Boolean = plan match {
    case relation: DataSourceV2Relation =>
      relation.catalog.contains(catalog) &&
        relation.identifier.contains(identifier) &&
        relation.options == options &&
        tableMatch.matches(relation.table)
    case _ =>
      false
  }
}

private[sql] trait RelationCache {
  def lookup(
      criteria: SharedRelationCacheCriteria,
      resolver: Resolver): Option[LogicalPlan]
}

private[sql] object RelationCache {
  val empty: RelationCache = (_, _) => None
}
