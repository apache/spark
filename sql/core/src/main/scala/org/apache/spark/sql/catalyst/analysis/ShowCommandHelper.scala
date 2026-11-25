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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.V1CommandUtils.supportsV1Command
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShowColumns, ShowCreateTable, ShowFunctions, ShowPartitions, ShowTablePartition, ShowTableProperties, ShowTables, ShowTablesExtended, ShowViews}
import org.apache.spark.sql.connector.catalog.{V1Table, ViewCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{DDLUtils, ShowColumnsCommand, ShowCreateTableAsSerdeCommand, ShowCreateTableCommand, ShowFunctionsCommand, ShowPartitionsCommand, ShowTablePropertiesCommand, ShowTablesCommand, ShowViewsCommand}
import org.apache.spark.sql.internal.SQLConf

/**
 * Lowers SHOW commands for session-catalog tables, views and namespaces, e.g.
 * `SHOW CREATE TABLE t` to [[ShowCreateTableCommand]]. Other plans pass through.
 */
private[sql] object ShowCommandHelper {

  /**
   * Lowers SHOW commands, preserving V1 eligibility, namespace validation and configuration guards.
   * Unresolved targets and unsupported commands pass through, except SHOW VIEWS on non-session
   * catalogs without view support, which is rejected.
   *
   * `conf` is evaluated only by branches that need configuration, after their target extractors;
   * it defaults to the active SQL configuration.
   *
   * Callers first apply the legacy output preparation from [[KeepLegacyOutputs]] to SHOW TABLES
   * and keyed SHOW TBLPROPERTIES. SHOW TABLE EXTENDED applies its legacy schema here. Callers
   * supply the command's origin through `CurrentOrigin.withOrigin` and propagate its tags to any
   * replacement plan. This helper does not traverse the plan or resolve partition specifications.
   */
  def rewrite(
      command: LogicalPlan,
      conf: => SQLConf = SQLConf.get): LogicalPlan = command match {
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

    case ShowTablePartition(
        ResolvedTable(catalog, _, table: V1Table, _),
        partitionSpec,
        output) if supportsV1Command(catalog) =>
      val newOutput = if (conf.getConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA)) {
        KeepLegacyOutputs.withLegacyShowTablesDatabaseColumn(output)
      } else {
        output
      }
      val tablePartitionSpec = Option(partitionSpec).map(
        _.asInstanceOf[UnresolvedPartitionSpec].spec)
      ShowTablesCommand(table.catalogTable.identifier.database,
        Some(table.catalogTable.identifier.table), newOutput,
        isExtended = true, tablePartitionSpec)

    case ShowCreateTable(ResolvedTable(catalog, _, t: V1Table, _), asSerde, output)
        if supportsV1Command(catalog) && (asSerde ||
          (conf.hiveTableShowCreateTableAsSerde && DDLUtils.isHiveTable(t.catalogTable))) =>
      ShowCreateTableAsSerdeCommand(t.catalogTable.identifier, output)

    case ShowCreateTable(ResolvedViewIdentifier(ident), asSerde, output) if asSerde =>
      ShowCreateTableAsSerdeCommand(ident, output)

    // If target is view, force use v1 command
    case ShowCreateTable(ResolvedViewIdentifier(ident), _, output) =>
      ShowCreateTableCommand(ident, output)

    case ShowCreateTable(ResolvedV1TableIdentifier(ident), _, output)
      if conf.useV1Command => ShowCreateTableCommand(ident, output)

    case ShowCreateTable(ResolvedTable(catalog, _, table: V1Table, _), _, output)
        if supportsV1Command(catalog) && DDLUtils.isHiveTable(table.catalogTable) =>
      ShowCreateTableCommand(table.catalogTable.identifier, output)

    case ShowPartitions(
        ResolvedV1TableOrViewIdentifier(ident),
        pattern @ (None | Some(UnresolvedPartitionSpec(_, _))), output) =>
      ShowPartitionsCommand(
        ident,
        output,
        pattern.map(_.asInstanceOf[UnresolvedPartitionSpec].spec))

    case ShowColumns(resolvedChild @ ResolvedViewIdentifier(ident), ns, output) =>
      val resolver = conf.resolver
      val db = ns match {
        case Some(nsSeq) if ident.database.exists(!resolver(_, nsSeq.head)) =>
          throw QueryCompilationErrors.showColumnsWithConflictNamespacesError(
            Seq(nsSeq.head), Seq(ident.database.get))
        case _ => ns.map(_.head)
      }
      // Use unqualified table name when namespace comes from FROM clause so analyzer output
      // matches expected format (e.g. "SHOW COLUMNS IN showcolumn4 FROM global_temp").
      val tableNameForCommand =
        if (db.isDefined && ident.database == db) TableIdentifier(ident.table, None) else ident
      ShowColumnsCommand(resolvedChild, db, tableNameForCommand, output)

    case ShowColumns(
        resolvedChild @ ResolvedV1TableIdentifier(ident), ns, output) =>
      val v1TableName = ident
      val resolver = conf.resolver
      val db = ns match {
        case Some(db) if v1TableName.database.exists(!resolver(_, db.head)) =>
          throw QueryCompilationErrors.showColumnsWithConflictNamespacesError(
            Seq(db.head), Seq(v1TableName.database.get))
        case _ => ns.map(_.head)
      }
      ShowColumnsCommand(resolvedChild, db, v1TableName, output)

    // If target is view, force use v1 command
    case ShowTableProperties(ResolvedViewIdentifier(ident), propertyKey, output) =>
      ShowTablePropertiesCommand(ident, propertyKey, output)

    case ShowTableProperties(ResolvedV1TableIdentifier(ident), propertyKey, output)
        if conf.useV1Command =>
      ShowTablePropertiesCommand(ident, propertyKey, output)

    case _ => command
  }
}
