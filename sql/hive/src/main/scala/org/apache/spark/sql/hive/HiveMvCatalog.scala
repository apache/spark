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
package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class HiveMvCatalog extends MvCatalog {

  private var sparkSession: SparkSession = SparkSession.getActiveSession.get

  override def init(session: Any): Unit = {
    val sparkSession = session.asInstanceOf[SparkSession]
    this.sparkSession = sparkSession
  }

  override def getMaterializedViewForTable(db: String, tblName: String): CatalogCreationData = {
    sparkSession.sessionState.catalog.externalCatalog.getMaterializedViewForTable(db, tblName)
  }

  override def getMaterializedViewPlan(catalogTable: CatalogTable): Option[LogicalPlan] = {
    val viewText = catalogTable.viewOriginalText
    // val plan = sparkSession.sessionState.sqlParser.parsePlan(viewText.get)
    val optimizedPlan = sparkSession.sql(viewText.get).queryExecution.optimizedPlan
    Some(optimizedPlan)
  }

  /* hive-3* onwards, there are simple metastore APIs to get all materialized views.
   * But till hive-3* is used, we will get all tables and filter the materialized views.
   * This part should be pluggable in another persistent service so that every spark-app
   * does not need to get all materialized views again and again
   */
  private def getAllMaterializedViewsFromMetastore(db: String,
       sessionCatalog: SessionCatalog): Seq[CatalogTable] = {
    val hiveSessionCatalog = sessionCatalog.asInstanceOf[HiveSessionCatalog]
    hiveSessionCatalog.listTables(db)
      .map(table => {
        hiveSessionCatalog.externalCatalog.getTable(db, table.identifier)
      })
      .filter(table => {
        table.tableType == CatalogTableType.MV
      })
  }
}
