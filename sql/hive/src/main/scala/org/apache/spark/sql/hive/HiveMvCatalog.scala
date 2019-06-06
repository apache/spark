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

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.google.common.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

class HiveMvCatalog extends MvCatalog {

  // load all MVs from metastore every hour?
  val cache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build(

    new CacheLoader[String, Seq[CatalogTable]] {
      override def load(db: String): Seq[CatalogTable] = {
        val mvs = getAllMaterializedViewsFromMetastore(db, sparkSession.sessionState.catalog)
        mvs.map(table => {
          // If a new MV or MV has changed . Currently this checks all fields of CatalogTable, i.e.
          // creation, comments etc. But we can change it to checking on the viewText field.

          if (modifiedChecker.get(table.identifier) != table) {
            val optimizedPlan = sparkSession.sql(table.viewText.get).queryExecution.optimizedPlan
            planCache.put(table.identifier,
              // mv table is always a HiveTableRelation
              (optimizedPlan, HiveTableRelation(
                table,
                // Hive table columns are always nullable.
                table.dataSchema.asNullable.toAttributes,
                table.partitionSchema.asNullable.toAttributes)))

            val mvDF = sparkSession.table(table.identifier)
              .queryExecution.optimizedPlan.asInstanceOf[HiveTableRelation]

            optimizedPlan.collectLeaves()
              .filter(p => p.isInstanceOf[HiveTableRelation] || p.isInstanceOf[LogicalRelation])
              .foreach(t => {
                t match {
                  case _@HiveTableRelation(hiveTable, dataCols, _) =>
                    mvCache.put(hiveTable.identifier,
                      mvCache.getOrElse(hiveTable.identifier, Set.empty[CatalogTable]) + table)
                    // considering only
                    (dataCols, mvDF.dataCols).zipped.map { (atrf, mvAtrf) => {
                      attributeRefMap.put(atrf.exprId, mvAtrf)
                    }
                    }
                  case _@LogicalRelation(_, dataCols, tableOpt: Option[CatalogTable], false) =>
                    mvCache.put(tableOpt.get.identifier,
                      mvCache.getOrElse(tableOpt.get.identifier, Set.empty[CatalogTable]) + table)
                    (dataCols, mvDF.dataCols).zipped.map { (atrf, mvAtrf) => {
                      attributeRefMap.put(atrf.exprId, mvAtrf)
                    }
                    }

                }
              })
          }
          modifiedChecker.put(table.identifier, table)
        })

        // Remove the MVs which are no longer in metastore
        val mvsSet = mvs.toSet
        for (tables <- mvCache.keys) {
          mvCache.put(tables, mvCache.getOrElse(tables, Set.empty) & mvsSet)
        }
        mvs
      }
    }
  )
  // Map a table to all the MVs it has
  private val mvCache = new mutable.HashMap[TableIdentifier, Set[CatalogTable]].empty
  // cache the optimized plan of an MV, and also the HiveTableRelation of the mv
  private val planCache = new mutable.HashMap[TableIdentifier, (LogicalPlan, HiveTableRelation)].empty
  // Check if MV was modified
  private val modifiedChecker = new mutable.HashMap[TableIdentifier, CatalogTable].empty
  private val attributeRefMap = new mutable.HashMap[ExprId, AttributeReference].empty
  private var sparkSession: SparkSession = SparkSession.getActiveSession.get

  override def init(session: Any): Unit = {
    val sparkSession = session.asInstanceOf[SparkSession]
    this.sparkSession = sparkSession

    val databases = sparkSession.sessionState.catalog.listDatabases();
    databases.map(d => cache.get(d))

    this.sparkSession = null
  }

  override def getMaterializedViewsOfTable(metaInfos: CatalogCreationData): Seq[CatalogTable] = {
    val db = (metaInfos.mvDetails(0))._1
    val tbls = (metaInfos.mvDetails)(1)._2
    Seq(sparkSession.sessionState.catalog.externalCatalog.getTable(db, tbls.split("\\.")(1)))
  }

  override def getMaterializedViewPlan(catalogTable: CatalogTable): Option[(LogicalPlan,
    HiveTableRelation)] = {

    val viewText = catalogTable.viewOriginalText
    val plan = sparkSession.sessionState.sqlParser.parsePlan(viewText.get);
    Some((plan, null))

  }

  def getCorrespondingMvProject(exprId: ExprId): Option[NamedExpression] = {
    attributeRefMap.get(exprId)
  }

  /* hive-3* onwards, there are simple metastore APIs to get all materialized views.
   * But till hive-3* is used, we will get all tables and filter the materialized views.
   * This part should be pluggable in another persistent service so that every spark-app
   * does not need to get all materialized views again and again
   */
  private def getAllMaterializedViewsFromMetastore(db: String, sessionCatalog: SessionCatalog)
  : Seq[CatalogTable] = {
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
