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

package org.apache.spark.sql.hive.execution

import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType

/**
 * Used to resolve UnResolvedStreamRelaTion, which is used in sqlstreaming
 * Change UnResolvedStreamRelation to StreamingRelation if table is stream_table,
 * otherwise, change UnResolvedStreamRelation to HiveTableRelation or other source Relation
 * @param catalog
 * @param conf
 * @param sparkSession
 */
class ResolveStreamRelation(catalog: SessionCatalog,
                            conf: SQLConf,
                            sparkSession: SparkSession)
  extends Rule[LogicalPlan] with CheckAnalysis {

  private def lookupRelation(relation: UnresolvedStreamRelation,
                             defaultDatabase: Option[String] = None): LogicalPlan = {

    val tableIdentWithDb = relation.tableIdentifier.copy(
      database = relation.tableIdentifier.database.orElse(defaultDatabase))

    try {
      val dbName = tableIdentWithDb.database.getOrElse(catalog.getCurrentDatabase)
      val db = formatDatabaseName(dbName)
      val table = formatTableName(tableIdentWithDb.table)
      val metadata = catalog.externalCatalog.getTable(db, table)

      if (metadata.isStreaming) {
        lookupStreamingRelation(metadata)
      } else {
        catalog.lookupRelation(tableIdentWithDb)
      }
    } catch {
      case _: NoSuchTableException =>
        relation.failAnalysis(
          s"Stream Table or view not found: ${tableIdentWithDb.unquotedString}")
      // If the database is defined and that database is not found, throw an AnalysisException.
      // Note that if the database is not defined, it is possible we are looking up a temp view.
      case e: NoSuchDatabaseException =>
        relation.failAnalysis(s"Stream Table or view not found: " +
          s"${tableIdentWithDb.unquotedString}, the database ${e.db} doesn't exsits.")
    }
  }

  /**
   * Format table name, taking into account case sensitivity.
   */
  protected[this] def formatTableName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  /**
   * Format database name, taking into account case sensitivity.
   */
  protected[this] def formatDatabaseName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  /**
   * Create StreamingRelation from table
   * @param table
   * @return
   */
  private[hive] def lookupStreamingRelation(table: CatalogTable): LogicalPlan = {
    table.provider match {
      case Some(sourceName) =>
        DataSource.lookupDataSource(sourceName, conf).newInstance() match {
          case s: StreamSourceProvider =>
            createStreamingRelation(table, usingFileStreamSource = false)
          case format: FileFormat =>
            createStreamingRelation(table, usingFileStreamSource = true)
          case _ =>
            throw new Exception(s"Cannot find Streaming Relation for provider $sourceName")
        }
      case _ =>
        throw new Exception("Invalid provider for Streaming Relation")
    }
  }

  /**
   * Create StreamingRelation from table for SourceProvider
   * @param table
   * @return
   */
  private def createStreamingRelation(
      table: CatalogTable,
      usingFileStreamSource: Boolean): LogicalPlan = {

    /**
     * Get Source or Sink meta data from table.
     */
    var sourceProperties = table.storage.properties
    val partitionColumnNames = table.partitionColumnNames
    val sourceName = table.provider.get
    sourceProperties += ("source" -> sourceName)
    var userSpecifiedSchema: Option[StructType] = Some(table.schema)

    if (usingFileStreamSource) {
      sourceProperties += ("path" -> table.location.getPath)
    }

    StreamingRelation(
      DataSource(
        sparkSession,
        sourceName,
        userSpecifiedSchema = userSpecifiedSchema,
        options = sourceProperties,
        partitionColumns = partitionColumnNames
      )
    )
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: UnresolvedStreamRelation =>
      val defaultDatabase = AnalysisContext.get.defaultDatabase
      lookupRelation(u, defaultDatabase)
  }
}
