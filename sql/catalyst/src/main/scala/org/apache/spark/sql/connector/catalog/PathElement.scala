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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.connector.catalog.CatalogManager.{
  CurrentSchemaEntry, LiteralPathEntry, SessionPathEntry
}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * One element on the right-hand side of `SET PATH = ...`: either a well-known shortcut
 * keyword (DEFAULT_PATH, SYSTEM_PATH, PATH, CURRENT_SCHEMA / CURRENT_DATABASE) or a
 * fully qualified schema reference (`catalog.namespace...` with at least 2 parts).
 *
 * The same grammar is reused to parse the [[SQLConf.DEFAULT_PATH]] conf value, so this
 * AST node lives in catalyst beside [[CatalogManager]] rather than in the runtime
 * [[org.apache.spark.sql.execution.command.SetPathCommand]].
 */
sealed trait PathElement

object PathElement {
  case object DefaultPath extends PathElement
  case object SystemPath extends PathElement
  case object PathRef extends PathElement

  /**
   * Current database/schema (SQL aliases). Stored as the [[CurrentSchemaEntry]] marker
   * so resolution candidates expand against the live `USE SCHEMA`.
   */
  case object CurrentSchema extends PathElement

  /** Fully qualified schema reference (`catalog.namespace...`). Must have at least 2 parts. */
  case class SchemaInPath(parts: Seq[String]) extends PathElement

  /**
   * Expand a parsed [[PathElement]] list into concrete [[SessionPathEntry]] entries
   * suitable for storing in [[CatalogManager._sessionPath]] or returning from
   * [[CatalogManager.sessionPathEntries]].
   *
   * @param isWorkspaceDefaultExpansion when true, an inner [[DefaultPath]] token resolves
   *                                    to the spark-builtin default ordering (cycle break)
   *                                    rather than reading [[SQLConf.DEFAULT_PATH]] again.
   *                                    Set to true when this method is invoked while
   *                                    parsing [[SQLConf.DEFAULT_PATH]] itself.
   */
  def expand(
      elements: Seq[PathElement],
      conf: SQLConf,
      catalogManager: CatalogManager,
      isWorkspaceDefaultExpansion: Boolean = false): Seq[SessionPathEntry] = {
    val currentSchemaSentinel = Seq("__current_schema__")

    def toEntries(parts: Seq[Seq[String]]): Seq[SessionPathEntry] = parts.map {
      case p if p == currentSchemaSentinel => CurrentSchemaEntry
      case p => LiteralPathEntry(p)
    }

    def builtinDefaultWithCurrentSchema: Seq[SessionPathEntry] =
      toEntries(conf.defaultPathOrder(Seq(currentSchemaSentinel)))

    def defaultPathExpansion: Seq[SessionPathEntry] = {
      if (isWorkspaceDefaultExpansion) {
        // Cycle break: inner DEFAULT_PATH inside the workspace default conf value falls
        // back to the spark-builtin default ordering instead of recursing.
        builtinDefaultWithCurrentSchema
      } else {
        catalogManager.workspaceDefaultPathEntries.getOrElse(builtinDefaultWithCurrentSchema)
      }
    }

    elements.flatMap {
      case DefaultPath =>
        defaultPathExpansion
      case SystemPath =>
        toEntries(conf.systemPathOrder)
      case CurrentSchema =>
        Seq(CurrentSchemaEntry)
      case PathRef =>
        catalogManager.storedSessionPathEntries.getOrElse(defaultPathExpansion)
      case SchemaInPath(parts) =>
        if (parts.length < 2) {
          throw QueryCompilationErrors.invalidSqlPathSchemaReferenceError(parts.mkString("."))
        }
        Seq(LiteralPathEntry(parts))
    }
  }
}
