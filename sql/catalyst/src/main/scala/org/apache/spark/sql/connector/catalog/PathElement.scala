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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.CatalogManager.{
  CurrentSchemaEntry, LiteralPathEntry, SessionPathEntry
}
import org.apache.spark.sql.internal.SQLConf

/**
 * One element on the right-hand side of `SET PATH = ...`: either a well-known shortcut
 * keyword (DEFAULT_PATH, SYSTEM_PATH, PATH, CURRENT_SCHEMA / CURRENT_DATABASE) or a
 * fully qualified schema reference (`catalog.namespace...` with at least 2 parts).
 *
 * The same grammar is reused to parse the
 * [[org.apache.spark.sql.internal.SQLConf#DEFAULT_PATH()]] conf value, so this
 * AST node lives in catalyst beside [[CatalogManager]] rather than in the runtime
 * [[org.apache.spark.sql.execution.command.SetPathCommand]].
 */
private[sql] sealed trait PathElement

private[sql] object PathElement {
  case object DefaultPath extends PathElement
  case object SystemPath extends PathElement
  case object PathRef extends PathElement

  /**
   * Current database/schema (SQL aliases). Stored as the
   * [[org.apache.spark.sql.connector.catalog.CatalogManager.CurrentSchemaEntry$]]
   * marker so resolution candidates expand against the live `USE SCHEMA`.
   */
  case object CurrentSchema extends PathElement

  /** Fully qualified schema reference (`catalog.namespace...`). Must have at least 2 parts. */
  case class SchemaInPath(parts: Seq[String]) extends PathElement

  /**
   * Expand a parsed [[PathElement]] list into concrete [[SessionPathEntry]] entries
   * suitable for storing in [[CatalogManager._sessionPath]] or returning from
   * [[CatalogManager#sessionPathEntries]].
   *
   * @param isConfDefaultExpansion when true, an inner [[DefaultPath]] token resolves
   *                               to the spark-builtin default ordering (cycle break)
   *                               rather than reading
   *                               [[org.apache.spark.sql.internal.SQLConf#DEFAULT_PATH()]] again.
   *                               Set to true when this method is invoked while
   *                               parsing [[org.apache.spark.sql.internal.SQLConf#DEFAULT_PATH()]]
   *                               itself.
   */
  def expand(
      elements: Seq[PathElement],
      conf: SQLConf,
      catalogManager: CatalogManager,
      isConfDefaultExpansion: Boolean = false): Seq[SessionPathEntry] = {
    val currentSchemaSentinel = Seq("__current_schema__")

    def toEntries(parts: Seq[Seq[String]]): Seq[SessionPathEntry] = parts.map {
      case p if p == currentSchemaSentinel => CurrentSchemaEntry
      case p => LiteralPathEntry(p)
    }

    def builtinDefaultWithCurrentSchema: Seq[SessionPathEntry] =
      toEntries(conf.defaultPathOrder(Seq(currentSchemaSentinel)))

    def defaultPathExpansion: Seq[SessionPathEntry] = {
      if (isConfDefaultExpansion) {
        // Cycle break: inner DEFAULT_PATH inside the conf default value falls back to the
        // spark-builtin default ordering instead of recursing.
        builtinDefaultWithCurrentSchema
      } else {
        catalogManager.confDefaultPathEntries.getOrElse(builtinDefaultWithCurrentSchema)
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
        Seq(LiteralPathEntry(parts))
    }
  }

  /**
   * Reject *static* duplicates in a SET PATH entry list: identical
   * [[CatalogManager#LiteralPathEntry]] parts and repeated
   * [[org.apache.spark.sql.connector.catalog.CatalogManager.CurrentSchemaEntry$]] markers
   * (the `current_schema` / `current_database`
   * cross-alias case). Used for the interactive `SET PATH` form to surface user typos at
   * statement time.
   *
   * Deliberately does NOT compare a [[CatalogManager#LiteralPathEntry]] against a
   * [[org.apache.spark.sql.connector.catalog.CatalogManager.CurrentSchemaEntry$]]: such a
   * "duplicate" depends on the live `USE SCHEMA`
   * and is harmless at lookup (first-match resolution skips the dead literal).
   * [[org.apache.spark.sql.internal.SQLConf#DEFAULT_PATH()]] expansion skips this check
   * entirely so transient `USE`-induced
   * collisions don't wedge unqualified resolution.
   */
  def validateNoStaticDuplicates(
      entries: Seq[SessionPathEntry],
      caseSensitive: Boolean): Seq[SessionPathEntry] = {
    val seenLiterals = new mutable.HashSet[Seq[String]]
    var seenCurrentSchema = false
    entries.foreach {
      case CurrentSchemaEntry =>
        if (seenCurrentSchema) {
          throw new AnalysisException(
            errorClass = "DUPLICATE_SQL_PATH_ENTRY",
            messageParameters = Map("pathEntry" -> "current_schema"))
        }
        seenCurrentSchema = true
      case LiteralPathEntry(parts) =>
        val key = if (caseSensitive) parts else parts.map(_.toLowerCase(Locale.ROOT))
        if (!seenLiterals.add(key)) {
          throw new AnalysisException(
            errorClass = "DUPLICATE_SQL_PATH_ENTRY",
            messageParameters = Map(
              "pathEntry" ->
                parts.map(p => if (p.contains(".")) s"`$p`" else p).mkString(".")))
        }
    }
    entries
  }
}
