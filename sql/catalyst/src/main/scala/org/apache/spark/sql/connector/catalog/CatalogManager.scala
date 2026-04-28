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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, TempVariableManager}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * A thread-safe manager for [[CatalogPlugin]]s. It tracks all the registered catalogs, and allow
 * the caller to look up a catalog by name.
 *
 * There are still many commands (e.g. ANALYZE TABLE) that do not support v2 catalog API. They
 * ignore the current catalog and blindly go to the v1 `SessionCatalog`. To avoid tracking current
 * namespace in both `SessionCatalog` and `CatalogManger`, we let `CatalogManager` to set/get
 * current database of `SessionCatalog` when the current catalog is the session catalog.
 */
// TODO: all commands should look up table from the current catalog. The `SessionCatalog` doesn't
//       need to track current database at all.
private[sql]
class CatalogManager(
    defaultSessionCatalog: CatalogPlugin,
    val v1SessionCatalog: SessionCatalog) extends SQLConfHelper with Logging {
  import CatalogManager.SESSION_CATALOG_NAME
  import CatalogV2Util._

  private val catalogs = mutable.HashMap.empty[String, CatalogPlugin]

  // TODO: create a real SYSTEM catalog to host `TempVariableManager` under the SESSION namespace.
  val tempVariableManager: TempVariableManager = new TempVariableManager

  def catalog(name: String): CatalogPlugin = synchronized {
    if (name.equalsIgnoreCase(SESSION_CATALOG_NAME)) {
      v2SessionCatalog
    } else {
      catalogs.getOrElseUpdate(name, Catalogs.load(name, conf))
    }
  }

  def isCatalogRegistered(name: String): Boolean = {
    try {
      catalog(name)
      true
    } catch {
      case _: CatalogNotFoundException => false
    }
  }

  private def loadV2SessionCatalog(): CatalogPlugin = {
    Catalogs.load(SESSION_CATALOG_NAME, conf) match {
      case extension: CatalogExtension =>
        extension.setDelegateCatalog(defaultSessionCatalog)
        extension
      case other => other
    }
  }

  /**
   * If the V2_SESSION_CATALOG config is specified, we try to instantiate the user-specified v2
   * session catalog. Otherwise, return the default session catalog.
   *
   * This catalog is a v2 catalog that delegates to the v1 session catalog. it is used when the
   * session catalog is responsible for an identifier, but the source requires the v2 catalog API.
   * This happens when the source implementation extends the v2 TableProvider API and is not listed
   * in the fallback configuration, spark.sql.sources.useV1SourceList
   */
  private[sql] def v2SessionCatalog: CatalogPlugin = {
    conf.getConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION) match {
      case "builtin" => defaultSessionCatalog
      case _ => catalogs.getOrElseUpdate(SESSION_CATALOG_NAME, loadV2SessionCatalog())
    }
  }

  private var _currentNamespace: Option[Array[String]] = None

  def currentNamespace: Array[String] = {
    val defaultNamespace = if (currentCatalog.name() == SESSION_CATALOG_NAME) {
      Array(v1SessionCatalog.getCurrentDatabase)
    } else {
      currentCatalog.defaultNamespace()
    }

    this.synchronized {
      _currentNamespace.getOrElse {
        defaultNamespace
      }
    }
  }

  private def assertNamespaceExist(namespace: Array[String]): Unit = {
    currentCatalog match {
      case catalog: SupportsNamespaces if !catalog.namespaceExists(namespace) =>
        throw QueryCompilationErrors.noSuchNamespaceError(catalog.name() +: namespace)
      case _ =>
    }
  }

  def setCurrentNamespace(namespace: Array[String]): Unit = synchronized {
    if (isSessionCatalog(currentCatalog) && namespace.length == 1) {
      v1SessionCatalog.setCurrentDatabaseWithNameCheck(
        namespace.head,
        _ => assertNamespaceExist(namespace))
    } else {
      assertNamespaceExist(namespace)
    }
    _currentNamespace = Some(namespace)
  }

  import CatalogManager.SessionPathEntry

  private var _sessionPath: Option[Seq[SessionPathEntry]] = None

  /** Returns the raw stored session path entries, or None if no path is set. */
  def sessionPathEntries: Option[Seq[SessionPathEntry]] = synchronized { _sessionPath }

  def setSessionPath(entries: Seq[SessionPathEntry]): Unit = synchronized {
    _sessionPath = Some(entries)
  }

  def clearSessionPath(): Unit = synchronized {
    _sessionPath = None
  }

  private[sql] def copySessionPathFrom(other: CatalogManager): Unit = synchronized {
    _sessionPath = other.sessionPathEntries
  }

  /**
   * String form of the current resolution path for CURRENT_PATH().
   * When PATH is enabled and a session path is stored, formats the effective path entries
   * with markers expanded. Otherwise falls back to the legacy resolutionSearchPath.
   */
  def currentPathString: String = synchronized {
    import CatalogV2Implicits._
    val stored = if (conf.pathEnabled) _sessionPath else None
    stored match {
      case Some(entries) =>
        val resolved = CatalogManager.resolvePathEntries(
          entries, currentCatalog.name(), currentNamespace.toSeq)
        resolved.map(_.quoted).mkString(",")
      case None =>
        val catalogPath = (currentCatalog.name() +: currentNamespace).toSeq
        conf.resolutionSearchPath(catalogPath).map(_.quoted).mkString(",")
    }
  }

  /**
   * Ordered catalog/schema path entries for resolving unqualified SQL object names.
   * When PATH is off or unset, applies [[SQLConf.defaultPathOrder]] (legacy).
   * When PATH is explicitly set, uses the resolved stored path entries.
   */
  def sqlResolutionPathEntries(
      pathDefaultCatalog: String,
      pathDefaultNamespace: Seq[String],
      expandCatalog: String,
      expandNamespace: Seq[String]): Seq[Seq[String]] = synchronized {
    val defaultEntry =
      if (pathDefaultNamespace.isEmpty) Seq(pathDefaultCatalog)
      else pathDefaultCatalog +: pathDefaultNamespace
    val stored = if (conf.pathEnabled) _sessionPath else None
    stored match {
      case Some(entries) =>
        CatalogManager.resolvePathEntries(entries, expandCatalog, expandNamespace)
      case None =>
        conf.defaultPathOrder(Seq(defaultEntry))
    }
  }

  /** Session-catalog overload. */
  def sqlResolutionPathEntries(
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[Seq[String]] =
    sqlResolutionPathEntries(
      currentCatalog, currentNamespace,
      currentCatalog, currentNamespace)

  /** True if [[sqlResolutionPathEntries]] includes `system.session`. */
  def sessionScopeUnqualifiedAllowed(
      currentCatalog: String,
      currentNamespace: Seq[String]): Boolean =
    sqlResolutionPathEntries(currentCatalog, currentNamespace)
      .exists(CatalogManager.isSystemSessionPathEntry)

  private var _currentCatalogName: Option[String] = None

  def currentCatalog: CatalogPlugin = synchronized {
    catalog(_currentCatalogName.getOrElse(conf.getConf(SQLConf.DEFAULT_CATALOG)))
  }

  def setCurrentCatalog(catalogName: String): Unit = synchronized {
    // `setCurrentCatalog` is noop if it doesn't switch to a different catalog.
    if (currentCatalog.name() != catalogName) {
      catalog(catalogName)
      _currentCatalogName = Some(catalogName)
      _currentNamespace = None
      // Reset the current database of v1 `SessionCatalog` when switching current catalog, so that
      // when we switch back to session catalog, the current namespace definitely is ["default"].
      v1SessionCatalog.setCurrentDatabase(conf.defaultDatabase)
    }
  }

  def listCatalogs(pattern: Option[String]): Seq[String] = {
    val allCatalogs = (synchronized(catalogs.keys.toSeq) :+ SESSION_CATALOG_NAME).distinct.sorted
    pattern.map(StringUtils.filterPattern(allCatalogs, _)).getOrElse(allCatalogs)
  }

  // Clear all the registered catalogs. Only used in tests.
  private[sql] def reset(): Unit = synchronized {
    catalogs.clear()
    _currentNamespace = None
    _currentCatalogName = None
    _sessionPath = None
    v1SessionCatalog.setCurrentDatabase(conf.defaultDatabase)
  }
}

private[sql] object CatalogManager {

  val SESSION_CATALOG_NAME: String = "spark_catalog"
  val SYSTEM_CATALOG_NAME = "system"
  val SESSION_NAMESPACE = "session"
  val BUILTIN_NAMESPACE = "builtin"

  /**
   * For a view identifier's namespace (e.g. from Identifier.namespace()), returns the database
   * name to use with v1 TableIdentifier when the view is a session temp view.
   * - system.session (2 parts) -> Some("session") so SessionCatalog finds the local temp view
   * - session (1 part) -> Some("session")
   * - other non-empty namespace -> Some(namespace.head)
   * - empty -> None
   */
  def databaseForSessionQualifiedViewIdentifier(namespace: Seq[String]): Option[String] = {
    if (namespace.isEmpty) {
      None
    } else if (namespace.length == 2 &&
        namespace(0).equalsIgnoreCase(SYSTEM_CATALOG_NAME) &&
        namespace(1).equalsIgnoreCase(SESSION_NAMESPACE)) {
      Some(SESSION_NAMESPACE)
    } else {
      Some(namespace.head)
    }
  }

  /**
   * True only for fully qualified `system.session.view` (3 parts). Persistent catalog is never
   * consulted for this form; see [[isSessionQualifiedViewName]] for 2-part `session.view`.
   */
  def isFullyQualifiedSystemSessionViewName(nameParts: Seq[String]): Boolean = {
    nameParts.length == 3 &&
      nameParts(0).equalsIgnoreCase(SYSTEM_CATALOG_NAME) &&
      nameParts(1).equalsIgnoreCase(SESSION_NAMESPACE)
  }

  /**
   * True if the multipart name uses the session temp view namespace: two-part `session.view`
   * or three-part `system.session.view`. The two-part form can also denote a persistent relation
   * in schema `session`; resolution order is controlled by [[SQLConf.prioritizeSystemCatalog]].
   */
  def isSessionQualifiedViewName(nameParts: Seq[String]): Boolean = {
    (nameParts.length == 2 && nameParts.head.equalsIgnoreCase(SESSION_NAMESPACE)) ||
      isFullyQualifiedSystemSessionViewName(nameParts)
  }

  /** True if a SQL path entry is the well-known `system.session` entry. */
  def isSystemSessionPathEntry(parts: Seq[String]): Boolean =
    parts == Seq(SYSTEM_CATALOG_NAME, SESSION_NAMESPACE)

  /**
   * A single entry in the session SQL path: either a literal schema
   * or the current-schema marker.
   */
  sealed trait SessionPathEntry {
    /** Resolve to concrete catalog + namespace parts. */
    def resolve(
        currentCatalog: String,
        currentNamespace: Seq[String]): Seq[String] = this match {
      case CurrentSchemaEntry =>
        if (currentNamespace.isEmpty) Seq(currentCatalog)
        else currentCatalog +: currentNamespace
      case LiteralPathEntry(parts) => parts
    }
  }

  /** Marker for CURRENT_SCHEMA / CURRENT_DATABASE: expands dynamically with USE SCHEMA. */
  case object CurrentSchemaEntry extends SessionPathEntry

  /** A fully qualified schema reference (catalog.namespace...). */
  case class LiteralPathEntry(parts: Seq[String]) extends SessionPathEntry

  /** Resolve all entries in a session path to concrete catalog + namespace parts. */
  def resolvePathEntries(
      entries: Seq[SessionPathEntry],
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[Seq[String]] =
    entries.map(_.resolve(currentCatalog, currentNamespace))

  /**
   * Compute the resolved path entries to persist in view or SQL function metadata.
   * When PATH is enabled, resolves the stored session path (or falls back to the
   * legacy resolutionSearchPath). If `stripSession` is true, removes `system.session`
   * entries (persisted objects cannot reference temporary objects).
   */
  def pathEntriesForPersistence(
      catalogManager: CatalogManager,
      conf: SQLConf,
      stripSession: Boolean): Seq[Seq[String]] = {
    if (!conf.pathEnabled) return Seq.empty
    val currentCatalog = catalogManager.currentCatalog.name()
    val currentNamespace = catalogManager.currentNamespace.toSeq
    val entries = catalogManager.sessionPathEntries match {
      case Some(stored) =>
        resolvePathEntries(stored, currentCatalog, currentNamespace)
      case None =>
        val catalogPath =
          (currentCatalog +: currentNamespace).toSeq
        conf.resolutionSearchPath(catalogPath)
    }
    if (stripSession) {
      entries.filterNot(isSystemSessionPathEntry)
    } else {
      entries
    }
  }

  /** Serialize resolved path entries to JSON for storage in view/function properties. */
  def serializePathEntries(entries: Seq[Seq[String]]): String = {
    import org.json4s.JsonAST.{JArray, JString}
    import org.json4s.jackson.JsonMethods.compact
    compact(JArray(entries.map(parts =>
      JArray(parts.map(JString(_)).toList)).toList))
  }
}
