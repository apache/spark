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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShowFunctions, ShowTables, ShowTablesExtended, ShowViews}
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{ShowFunctionsCommand, ShowTablesCommand, ShowViewsCommand}
import org.apache.spark.sql.internal.SQLConf

/**
 * Lowers session-catalog SHOW TABLES, SHOW TABLE EXTENDED (without PARTITION), SHOW VIEWS and
 * SHOW FUNCTIONS, e.g. `SHOW VIEWS IN db` to [[ShowViewsCommand]]. Other plans pass through.
 */
private[sql] object ShowCommandHelper extends SQLConfHelper {

  /**
   * Lowers SHOW TABLES, SHOW TABLE EXTENDED, SHOW VIEWS and SHOW FUNCTIONS, preserving V1 namespace
   * validation and configuration guards. Other plans and V2/unresolved targets pass through
   * unchanged, except SHOW VIEWS on non-session catalogs without view support, which is rejected.
   *
   * Direct SHOW TABLES callers must first apply [[KeepLegacyOutputs.keepLegacyShowTablesOutput]];
   * its prepared output passes through unchanged. SHOW TABLE EXTENDED applies its legacy schema
   * here. Callers supply the command's origin through `CurrentOrigin.withOrigin` and propagate
   * its tags to any replacement plan. This helper does not traverse the plan.
   */
  def rewrite(command: LogicalPlan): LogicalPlan = command match {
    case ShowTables(ResolvedV1Database(db), pattern, output) if conf.useV1Command =>
      ShowTablesCommand(Some(db), pattern, output)

    case ShowTablesExtended(
        ResolvedV1Database(db),
        pattern,
        output) =>
      val newOutput = if (conf.getConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA)) {
        KeepLegacyOutputs.withLegacyShowTablesDatabaseColumn(output)
      } else {
        output
      }
      ShowTablesCommand(Some(db), Some(pattern), newOutput, isExtended = true)

    case ShowViews(ns: ResolvedNamespace, pattern, output)
        if !ns.catalog.isInstanceOf[ViewCatalog] =>
      ns match {
        case ResolvedDatabaseInSessionCatalog(db) => ShowViewsCommand(db, pattern, output)
        case _ =>
          throw QueryCompilationErrors.missingCatalogViewsAbilityError(ns.catalog)
      }

    case ShowFunctions(
        ResolvedDatabaseInSessionCatalog(db), userScope, systemScope, pattern, output) =>
      ShowFunctionsCommand(db, pattern, userScope, systemScope, output)

    case _ => command
  }
}
