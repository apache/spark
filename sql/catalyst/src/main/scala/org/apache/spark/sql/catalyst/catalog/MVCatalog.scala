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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/*
 * Helper trait for the catalog of MaterializedViews.
 *
 */
trait MvCatalog {

  /**
    *
    * @param catalogTable the table object of MV
    * @return Option of LogicalPlan corresponding to the table
    */
  def getMaterializedViewPlan(catalogTable: CatalogTable): Option[LogicalPlan]

  /**
    * Get all materialized view information of a given table
    * @param tbl name of the table
    * @param db database name of the table
    * @return information about the MV of the given table
    */
  def getMaterializedViewForTable(tbl: String, db: String): CatalogCreationData

  /**
    *
    * @param sparkSession Init code for MV catalog
    */
  def init(sparkSession: Any): Unit
}
