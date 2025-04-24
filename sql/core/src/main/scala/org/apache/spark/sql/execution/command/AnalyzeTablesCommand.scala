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

import scala.util.control.NonFatal

import org.apache.spark.internal.LogKeys.{DATABASE_NAME, ERROR, TABLE_NAME}
import org.apache.spark.internal.MDC
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.classic.ClassicConversions.castToImpl

/**
 * Analyzes all tables in the given database to generate statistics.
 */
case class AnalyzeTablesCommand(
    databaseName: Option[String],
    noScan: Boolean) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val db = databaseName.getOrElse(catalog.getCurrentDatabase)
    catalog.listTables(db).foreach { tbl =>
      try {
        CommandUtils.analyzeTable(sparkSession, tbl, noScan)
      } catch {
        case NonFatal(e) =>
          logWarning(log"Failed to analyze table ${MDC(TABLE_NAME, tbl.table)} in the " +
            log"database ${MDC(DATABASE_NAME, db)} because of ${MDC(ERROR, e.toString)}}", e)
      }
    }
    Seq.empty[Row]
  }
}
