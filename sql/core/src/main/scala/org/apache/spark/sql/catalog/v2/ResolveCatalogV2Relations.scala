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

package org.apache.spark.sql.catalog.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFrom, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

case class ResolveCatalogV2Relations(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case d @ DeleteFrom(u: UnresolvedRelation, _) if u.table.catalog.isDefined =>
      val catalogName = u.table.catalog.get
      val catalog = sparkSession.catalog(catalogName).asTableCatalog
      val table = catalog.loadTable(u.table.asTableIdentifier)
      d.copy(table = DataSourceV2Relation.create(catalogName, u.table.dropCatalog, table))
  }
}
