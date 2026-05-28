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

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, TempVariableManager}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.SessionFunctionKind
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.CatalogManager.SessionPathEntry
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * A thread-safe contract for managing [[CatalogPlugin]]s. Implementations resolve catalogs by
 * name and maintain the current catalog and namespace for a session.
 *
 * There are still many commands (e.g. ANALYZE TABLE) that do not support v2 catalog API. They
 * ignore the current catalog and blindly go to the v1 `SessionCatalog`. To avoid tracking current
 * namespace in both `SessionCatalog` and `CatalogManager`, implementations set/get the
 * current database of `SessionCatalog` when the current catalog is the session catalog.
 *
 * Two implementations exist: [[DefaultCatalogManager]] owns the mutable session state;
 * [[TransactionAwareCatalogManager]] wraps another manager and redirects catalog lookups to the
 * active transaction's catalog.
 */
// TODO: all commands should look up table from the current catalog. The `SessionCatalog` doesn't
//       need to track current database at all.
private[sql] trait CatalogManager extends SQLConfHelper with Logging {

  // ---- Underlying state exposed by implementations ----
  def defaultSessionCatalog: CatalogPlugin
  def v1SessionCatalog: SessionCatalog
  def tempVariableManager: TempVariableManager

  // ---- Catalog access ----
  def catalog(name: String): CatalogPlugin
  private[sql] def v2SessionCatalog: CatalogPlugin
  def listCatalogs(pattern: Option[String]): Seq[String]
  def currentCatalog: CatalogPlugin
  def setCurrentCatalog(catalogName: String): Unit
  def isCatalogRegistered(name: String): Boolean = {
    try {
      catalog(name)
      true
    } catch {
      case _: CatalogNotFoundException => false
    }
  }

  /**
   * Returns the catalog name that owns path-based tables for the given data source format name,
   * or None if the format is unknown or does not implement SupportsCatalogOptions.
   * Overridden in sql/core via [[BaseSessionStateBuilder]] to use the real DataSource API.
   */
  def catalogForDataSource(formatName: String): Option[String] = None

  // ---- Transactions ----
  def transaction: Option[Transaction] = None

  def withTransaction(transaction: Transaction): CatalogManager

  // ---- Namespace ----
  def currentNamespace: Array[String]
  def setCurrentNamespace(namespace: Array[String]): Unit

  // ---- Session path ----
  def sessionPathEntries: Option[Seq[SessionPathEntry]]
  def storedSessionPathEntries: Option[Seq[SessionPathEntry]]
  def confDefaultPathEntries: Option[Seq[SessionPathEntry]]
  def setSessionPath(entries: Seq[SessionPathEntry]): Unit
  def clearSessionPath(): Unit
  private[sql] def copySessionPathFrom(other: CatalogManager): Unit
  def currentPathString: String
  def sqlResolutionPathEntries(
      pathDefaultCatalog: String,
      pathDefaultNamespace: Seq[String],
      expandCatalog: String,
      expandNamespace: Seq[String]): Seq[Seq[String]]
  def sqlResolutionPathEntries(
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[Seq[String]]
  def isSystemSessionOnPath: Boolean
  def resolutionPathEntriesForAnalysis(
      pinnedEntries: Option[Seq[Seq[String]]],
      viewCatalogAndNamespace: Seq[String]): Seq[Seq[String]]
  def sessionFunctionKindsForUnqualifiedResolution(): Seq[SessionFunctionKind]

  // Reset the manager to its initial state. Only used in tests.
  private[sql] def reset(): Unit
}

/**
 * Default [[CatalogManager]] implementation. Owns the mutable session state
 * (registered catalogs, current catalog/namespace, session path).
 */
private[sql] class DefaultCatalogManager(
    override val defaultSessionCatalog: CatalogPlugin,
    override val v1SessionCatalog: SessionCatalog) extends CatalogManager {
  import CatalogManager.SESSION_CATALOG_NAME
  import CatalogV2Util._

  private val catalogs = mutable.HashMap.empty[String, CatalogPlugin]

  // TODO: create a real SYSTEM catalog to host `TempVariableManager` under the SESSION namespace.
  override val tempVariableManager: TempVariableManager = new TempVariableManager

  // Wire `SessionCatalog`'s fast-path kinds to the live SQL PATH. The kinds list itself is
  // pure data conversion (system entries from the path, in path order); the *decision* to use
  // path-order kinds for unqualified lookups lives at the Strategy layer (see callers of
  // [[CatalogManager.systemFunctionKindsFromPath]]).
  v1SessionCatalog.bindCatalogManagerForSessionFunctionKinds(this)

  override def catalog(name: String): CatalogPlugin = synchronized {
    if (name.equalsIgnoreCase(SESSION_CATALOG_NAME)) {
      v2SessionCatalog
    } else {
      catalogs.getOrElseUpdate(name, Catalogs.load(name, conf))
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
  override private[sql] def v2SessionCatalog: CatalogPlugin = {
    conf.getConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION) match {
      case "builtin" => defaultSessionCatalog
      case _ => catalogs.getOrElseUpdate(SESSION_CATALOG_NAME, loadV2SessionCatalog())
    }
  }

  override def withTransaction(transaction: Transaction): CatalogManager =
    new TransactionAwareCatalogManager(this, transaction)

  private var _currentNamespace: Option[Array[String]] = None

  override def currentNamespace: Array[String] = {
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

  override def setCurrentNamespace(namespace: Array[String]): Unit = {
    // SPARK-56939: do NOT hold [[CatalogManager]]'s intrinsic lock across the callbacks below.
    // [[v1SessionCatalog.setCurrentDatabaseWithNameCheck]] briefly synchronizes on
    // [[SessionCatalog]], and concurrent unqualified function resolution acquires the
    // [[SessionCatalog]] lock and then reaches into [[CatalogManager]] via
    // [[sqlResolutionPathEntries]]; nesting the manager lock outside the catalog lock here
    // would invert that order and deadlock. Snapshot the dispatch decision under the lock,
    // run callbacks outside it, then publish the new namespace under the lock again.
    //
    // Concurrency trade-offs versus the pre-SPARK-56939 atomic version (v1-side and
    // CM-side drift modes):
    //
    // (a) v1-side drift. The `isSession` snapshot can drift if a concurrent
    //     [[setCurrentCatalog]] switches to a v2 catalog between this read and the v1
    //     callback below -- the callback would still touch `v1.currentDb` even though
    //     the active catalog is no longer the session catalog. A later switch back to
    //     the session catalog resets `v1.currentDb` to `default` (see
    //     [[setCurrentCatalog]]), so long-term state remains consistent; only the
    //     intermediate observation is novel.
    //
    // (b) CM-side publish-overwrite drift (sticky). Between the v1 callback returning
    //     and the publish below, a concurrent [[setCurrentCatalog]] can complete fully
    //     -- switching `_currentCatalogName` to (say) a v2 catalog and clearing
    //     `_currentNamespace = None` -- before this method's publish overwrites that
    //     with `Some(namespace)`. End state: `_currentNamespace = Some(namespace)` is
    //     published under a different `_currentCatalogName` than the one observed when
    //     [[isSession]] was snapshotted at the top. Unlike (a) there is no analogous
    //     auto-recovery; the mismatch sticks until the next `USE`. This is still
    //     last-writer-wins for two racing `USE` commands, which is the conventional
    //     expectation, so it is accepted as a trade-off against the deadlock alternative.
    val isSession = synchronized(isSessionCatalog(currentCatalog))
    if (isSession && namespace.length == 1) {
      v1SessionCatalog.setCurrentDatabaseWithNameCheck(
        namespace.head,
        _ => assertNamespaceExist(namespace))
    } else {
      assertNamespaceExist(namespace)
    }
    synchronized {
      _currentNamespace = Some(namespace)
    }
  }

  private var _sessionPath: Option[Seq[SessionPathEntry]] = None

  /**
   * Cache for [[confDefaultPathEntries]]: stores the expanded [[SessionPathEntry]] list keyed
   * on the trimmed [[SQLConf#DEFAULT_PATH]] string and
   * [[SQLConf#SESSION_FUNCTION_RESOLUTION_ORDER]] value (the only conf that affects the
   * expansion of `DEFAULT_PATH` / `SYSTEM_PATH` tokens).
   * `CurrentSchemaEntry` markers are preserved unresolved so the cache stays valid across
   * `USE SCHEMA`.
   */
  private val confDefaultPathCache =
    new AtomicReference[Option[(String, String, Seq[SessionPathEntry])]](None)

  /**
   * Returns the effective session path entries: the explicit `SET PATH` value if stored,
   * else the parsed [[SQLConf#DEFAULT_PATH]] conf if non-empty (mirroring how
   * [[currentCatalog]] falls back to [[SQLConf#DEFAULT_CATALOG]]). Returns `None` when
   * [[SQLConf#PATH_ENABLED]] is false or both sources are empty.
   */
  override def sessionPathEntries: Option[Seq[SessionPathEntry]] = synchronized {
    if (!conf.pathEnabled) None
    else _sessionPath.orElse(confDefaultPathEntries)
  }

  /** Raw `_sessionPath` (post-`SET PATH`), without the [[SQLConf#DEFAULT_PATH]] fallback. */
  override def storedSessionPathEntries: Option[Seq[SessionPathEntry]] =
    synchronized { _sessionPath }

  /**
   * Parsed and expanded [[SQLConf#DEFAULT_PATH]] value, or `None` when the conf is empty.
   * Reuses the SET PATH grammar via
   * [[org.apache.spark.sql.catalyst.parser.AbstractSqlParser#parsePathElements]] (via
   * [[org.apache.spark.sql.catalyst.parser.CatalystSqlParser]]). An inner
   * `DEFAULT_PATH` token resolves to the spark-builtin default ordering (cycle break).
   *
   * Unlike `SET PATH`, this does NOT run a duplicate check: lookup uses first-match
   * resolution, so any redundant entry (including ones that only collide after a later
   * `USE SCHEMA`) is dead code rather than an error. Cached so the hot path is a single
   * atomic load on conf-stable sessions.
   */
  override def confDefaultPathEntries: Option[Seq[SessionPathEntry]] = {
    val confValue = conf.defaultPath
    if (confValue == null || confValue.trim.isEmpty) {
      confDefaultPathCache.set(None)
      None
    } else {
      val trimmed = confValue.trim
      val sessionOrder = conf.sessionFunctionResolutionOrder
      val expanded = confDefaultPathCache.get() match {
        case Some((k, ord, cached)) if k == trimmed && ord == sessionOrder => cached
        case _ =>
          val elements = CatalystSqlParser.parsePathElements(trimmed)
          val computed = PathElement.expand(elements, conf, this, isConfDefaultExpansion = true)
          confDefaultPathCache.set(Some((trimmed, sessionOrder, computed)))
          computed
      }
      if (expanded.isEmpty) None else Some(expanded)
    }
  }

  override def setSessionPath(entries: Seq[SessionPathEntry]): Unit = synchronized {
    _sessionPath = Some(entries)
  }

  override def clearSessionPath(): Unit = synchronized {
    _sessionPath = None
  }

  override private[sql] def copySessionPathFrom(other: CatalogManager): Unit = synchronized {
    _sessionPath = other.storedSessionPathEntries
  }

  /**
   * String form of the current resolution path for CURRENT_PATH().
   * When PATH is enabled and a session path is in effect (stored or via
   * [[SQLConf#DEFAULT_PATH]]), formats the resolved entries. Otherwise falls back to the legacy
   * resolutionSearchPath.
   *
   * SPARK-56939 note: this is currently the only intentional `CatalogManager.synchronized ->
   * SessionCatalog.synchronized` nest left in this class. The transitive call into
   * [[v1SessionCatalog.getCurrentDatabase]] happens via [[currentNamespace]], which fetches
   * the v1 current database under the CM lock. It is safe today because no code path holds
   * [[SessionCatalog]]'s intrinsic lock while waiting on [[CatalogManager]]'s -- the
   * SPARK-56939 fix removed every such SC->CM ordering. Any future change that introduces a
   * new SC->CM ordering must take `currentPathString` (or any other CM->SC nest) into
   * account to avoid resurrecting the deadlock.
   */
  override def currentPathString: String = synchronized {
    import CatalogV2Implicits._
    sessionPathEntries match {
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
   * When PATH is off or unset, applies [[SQLConf#defaultPathOrder]] (legacy).
   * When PATH is in effect (stored or via the [[SQLConf#DEFAULT_PATH]] conf), uses the
   * resolved entries.
   */
  override def sqlResolutionPathEntries(
      pathDefaultCatalog: String,
      pathDefaultNamespace: Seq[String],
      expandCatalog: String,
      expandNamespace: Seq[String]): Seq[Seq[String]] = synchronized {
    val defaultEntry =
      if (pathDefaultNamespace.isEmpty) Seq(pathDefaultCatalog)
      else pathDefaultCatalog +: pathDefaultNamespace
    sessionPathEntries match {
      case Some(entries) =>
        CatalogManager.resolvePathEntries(entries, expandCatalog, expandNamespace)
      case None =>
        conf.defaultPathOrder(Seq(defaultEntry))
    }
  }

  /** Session-catalog overload. */
  override def sqlResolutionPathEntries(
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[Seq[String]] =
    sqlResolutionPathEntries(
      currentCatalog, currentNamespace,
      currentCatalog, currentNamespace)

  /**
   * Snapshot the live PATH-derived [[SessionCatalog.SessionFunctionKind]] order used by
   * unqualified function/table-function resolution.
   *
   * The `(currentCatalog, _currentNamespace, sessionPath)` triple is read together inside a
   * single CM critical section so a concurrent `USE` / `SET PATH` cannot return a torn
   * snapshot for those three fields (e.g. catalog from one observation, explicit namespace
   * from another).
   *
   * The `v1SessionCatalog.getCurrentDatabase` read needed for the default-namespace fallback
   * is taken OUTSIDE the CM lock and is therefore intentionally racy w.r.t. a concurrent
   * `USE SCHEMA`. That staleness is harmless for this helper's output: this method consumes
   * `effectiveNs` only to expand `CURRENT_SCHEMA` markers in the SQL path, and
   * [[CatalogManager.systemFunctionKindsFromPath]] only retains literal `system.builtin` /
   * `system.session` entries from the resolved path -- it never inspects any
   * `(catalog, namespace)` derived from `v1`. So if `v1CurrentDb` lags by one `USE SCHEMA`,
   * a `CURRENT_SCHEMA` entry might briefly resolve to the previous database, but the kinds
   * list (the only thing returned here) is unaffected. Moving the read inside the CM lock
   * would re-introduce the SPARK-56939 lock-order inversion this helper exists to avoid.
   *
   * Callers (e.g. [[SessionCatalog.sessionFunctionKindsInResolutionOrder]],
   * [[org.apache.spark.sql.catalyst.analysis.FunctionResolution.isSessionBeforeBuiltinInPath]])
   * MUST NOT hold [[SessionCatalog]]'s intrinsic lock when invoking this method.
   */
  override def sessionFunctionKindsForUnqualifiedResolution(): Seq[SessionFunctionKind] = {
    // SPARK-56939: read v1's current database before taking the CM lock; see the method
    // doc for why the resulting staleness is harmless for the kinds list.
    val v1CurrentDb = v1SessionCatalog.getCurrentDatabase
    val pathEntries = synchronized {
      val catName = currentCatalog.name()
      val effectiveNs: Seq[String] = _currentNamespace.map(_.toSeq).getOrElse {
        if (catName == SESSION_CATALOG_NAME) {
          Seq(v1CurrentDb)
        } else {
          currentCatalog.defaultNamespace().toSeq
        }
      }
      sqlResolutionPathEntries(catName, effectiveNs)
    }
    CatalogManager.systemFunctionKindsFromPath(pathEntries)
  }

  /**
   * True if `system.session` is on the SQL path. Only literal path entries can match: the
   * [[org.apache.spark.sql.connector.catalog.CatalogManager.CurrentSchemaEntry$]] marker expands to
   * `currentCatalog.name() +: currentNamespace`, and
   * `system` is not a registered catalog (it is a synthetic namespace served via
   * [[org.apache.spark.sql.catalyst.analysis.FakeSystemCatalog]] / `lookupBuiltinOrTempFunction`,
   * not loadable via [[catalog]]), so `currentCatalog.name()` cannot be `"system"`. If that
   * invariant ever changes, this short-circuit must be revisited.
   * Inspecting effective entries directly avoids loading the configured default catalog.
   */
  override def isSystemSessionOnPath: Boolean = synchronized {
    if (!conf.pathEnabled) return true
    sessionPathEntries match {
      case None => true
      case Some(entries) => entries.exists {
        case CatalogManager.LiteralPathEntry(parts) =>
          CatalogManager.isSystemSessionPathEntry(parts)
        case _ => false
      }
    }
  }

  /**
   * Single source of truth for analysis-time resolution path entries used by relation, routine,
   * and procedure resolution. When `pinnedEntries` are set (a view or SQL function body's
   * persisted frozen path) and PATH is enabled, returns them as-is so unqualified lookups follow
   * the creation-time path. Otherwise falls back to [[sqlResolutionPathEntries]] using the view's
   * catalog/namespace as the path default (so unqualified names inside a view body see the view's
   * home schema first), while always expanding markers like CURRENT_SCHEMA against the live
   * session catalog/namespace.
   *
   * @param pinnedEntries persisted frozen path entries from view / SQL function metadata
   *                      (typically `AnalysisContext.resolutionPathEntries`).
   * @param viewCatalogAndNamespace the view's catalog and namespace
   *                               (typically `AnalysisContext.catalogAndNamespace`); empty when
   *                               not resolving a view body.
   */
  override def resolutionPathEntriesForAnalysis(
      pinnedEntries: Option[Seq[Seq[String]]],
      viewCatalogAndNamespace: Seq[String]): Seq[Seq[String]] = {
    pinnedEntries match {
      case Some(entries) if conf.pathEnabled => entries
      case _ =>
        val expandCatalog = currentCatalog.name()
        val expandNamespace = currentNamespace.toSeq
        val (pathCatalog, pathNamespace) =
          if (viewCatalogAndNamespace.nonEmpty) {
            (viewCatalogAndNamespace.head, viewCatalogAndNamespace.tail.toSeq)
          } else {
            (expandCatalog, expandNamespace)
          }
        sqlResolutionPathEntries(
          pathCatalog,
          pathNamespace,
          expandCatalog,
          expandNamespace)
    }
  }

  private var _currentCatalogName: Option[String] = None

  override def currentCatalog: CatalogPlugin = synchronized {
    catalog(_currentCatalogName.getOrElse(conf.getConf(SQLConf.DEFAULT_CATALOG)))
  }

  override def setCurrentCatalog(catalogName: String): Unit = {
    // SPARK-56939: see [[setCurrentNamespace]]. Avoid nesting [[CatalogManager]]'s lock
    // across [[v1SessionCatalog.setCurrentDatabase]] (which synchronizes on
    // [[SessionCatalog]]) to prevent a lock-order inversion with concurrent unqualified
    // function resolution.
    val needsSwitch = synchronized {
      // `setCurrentCatalog` is noop if it doesn't switch to a different catalog.
      if (currentCatalog.name() != catalogName) {
        // Force-load the named catalog while holding the manager lock to keep the
        // not-found error semantics; if loading fails, throw before mutating state.
        catalog(catalogName)
        true
      } else {
        false
      }
    }
    if (needsSwitch) {
      // Reset the current database of v1 `SessionCatalog` when switching current catalog, so that
      // when we switch back to session catalog, the current namespace definitely is ["default"].
      // Run this BEFORE publishing the new catalog name so that if a reader observes the new
      // catalog, the v1 state is already consistent with it.
      //
      // Concurrency trade-off versus the pre-SPARK-56939 atomic version: between this v1 write
      // and the publish below, a concurrent reader of `currentNamespace` sees
      // `(oldCatalog, v1.currentDb = default)`. When the old catalog is the session catalog
      // (the common case for `USE CATALOG`), the user's previous namespace is briefly invisible
      // to that reader until the new name is published. The opposite torn observation
      // (`newCatalog`, stale `v1.currentDb`) is avoided by this ordering. This trade-off
      // (transient invisibility instead of transient inconsistency, exchanged for breaking the
      // deadlock cycle) is accepted; the long-term post-switch state is the same as before.
      v1SessionCatalog.setCurrentDatabase(conf.defaultDatabase)
      synchronized {
        _currentCatalogName = Some(catalogName)
        _currentNamespace = None
      }
    }
  }

  override def listCatalogs(pattern: Option[String]): Seq[String] = {
    val allCatalogs = (synchronized(catalogs.keys.toSeq) :+ SESSION_CATALOG_NAME).distinct.sorted
    pattern.map(StringUtils.filterPattern(allCatalogs, _)).getOrElse(allCatalogs)
  }

  // Clear all the registered catalogs. Only used in tests.
  //
  // SPARK-56939: apply the same split-lock pattern as [[setCurrentNamespace]] /
  // [[setCurrentCatalog]] so the locking contract is uniform across every CM mutator that
  // calls back into [[v1SessionCatalog]]. Test-only callers don't race against unqualified
  // function resolution today, but keeping the contract symmetric prevents future test
  // helpers (e.g. session reset in a concurrent harness) from reintroducing the cycle.
  override private[sql] def reset(): Unit = {
    synchronized {
      catalogs.clear()
      _currentNamespace = None
      _currentCatalogName = None
      _sessionPath = None
      confDefaultPathCache.set(None)
    }
    v1SessionCatalog.setCurrentDatabase(conf.defaultDatabase)
  }
}

private[sql] object CatalogManager extends Logging {

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
   * in schema `session`; resolution order is controlled by [[SQLConf#prioritizeSystemCatalog]].
   */
  def isSessionQualifiedViewName(nameParts: Seq[String]): Boolean = {
    (nameParts.length == 2 && nameParts.head.equalsIgnoreCase(SESSION_NAMESPACE)) ||
      isFullyQualifiedSystemSessionViewName(nameParts)
  }

  /** True if a SQL path entry is the well-known `system.session` entry (case-insensitive). */
  def isSystemSessionPathEntry(parts: Seq[String]): Boolean =
    parts.length == 2 &&
      parts.head.equalsIgnoreCase(SYSTEM_CATALOG_NAME) &&
      parts(1).equalsIgnoreCase(SESSION_NAMESPACE)

  /** True if a SQL path entry is the well-known `system.builtin` entry (case-insensitive). */
  def isSystemBuiltinPathEntry(parts: Seq[String]): Boolean =
    parts.length == 2 &&
      parts.head.equalsIgnoreCase(SYSTEM_CATALOG_NAME) &&
      parts(1).equalsIgnoreCase(BUILTIN_NAMESPACE)

  /**
   * Extract `system.builtin` / `system.session` entries from a resolved PATH, mapped to
   * [[SessionCatalog.SessionFunctionKind]] in path order. Pure data conversion -- callers
   * decide whether and how to use this list.
   */
  def systemFunctionKindsFromPath(
      path: Seq[Seq[String]]): Seq[SessionCatalog.SessionFunctionKind] =
    path.flatMap { e =>
      if (isSystemBuiltinPathEntry(e)) Some(SessionCatalog.Builtin)
      else if (isSystemSessionPathEntry(e)) Some(SessionCatalog.Temp)
      else None
    }

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

  private def parsePathEntries(storedPathStr: String): Either[String, Seq[Seq[String]]] = {
    import org.json4s.JsonAST.{JArray, JString}
    import org.json4s.jackson.JsonMethods.parse

    Try(parse(storedPathStr)).toOption match {
      case Some(JArray(entries)) =>
        entries.foldLeft(Right(Seq.empty[Seq[String]]): Either[String, Seq[Seq[String]]]) {
          (acc, entry) =>
            acc.flatMap { collected =>
              entry match {
                case JArray(parts) =>
                  val strings = parts.collect { case JString(s) => s }
                  if (strings.size == parts.size) Right(collected :+ strings)
                  else Left("expected all array entry parts to be JSON strings")
                case _ =>
                  Left("expected each top-level array entry to be a JSON array")
              }
            }
        }
      case Some(_) =>
        Left("expected top-level JSON array")
      case None =>
        Left("failed to parse JSON payload")
    }
  }

  /**
   * Parse a stored frozen path string from view/function metadata.
   * Returns None if the payload is malformed.
   */
  def deserializePathEntries(storedPathStr: String): Option[Seq[Seq[String]]] = {
    parsePathEntries(storedPathStr) match {
      case Right(entries) => Some(entries)
      case Left(reason) =>
        logWarning(
          s"Invalid stored SQL path metadata: $reason. Raw payload: $storedPathStr")
        None
    }
  }

  /**
   * Parse stored frozen path metadata and fail analysis if malformed.
   */
  def deserializePathEntriesOrFail(
      storedPathStr: String,
      objectType: String,
      objectName: String): Seq[Seq[String]] = {
    parsePathEntries(storedPathStr) match {
      case Right(entries) => entries
      case Left(reason) =>
        throw new AnalysisException(
          message = s"Invalid stored SQL path metadata for $objectType '$objectName': " +
            s"$reason. Raw payload: $storedPathStr",
          line = None,
          startPosition = None,
          cause = None,
          errorClass = None,
          messageParameters = Map.empty,
          context = Array.empty)
    }
  }
}
