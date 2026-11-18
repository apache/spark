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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, TempVariableManager}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.SessionFunctionKind
import org.apache.spark.sql.connector.catalog.CatalogManager.SessionPathEntry
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.internal.SQLConf

/**
 * A read-only [[CatalogManager]] for one ANALYZE call. The call's current catalog and
 * namespace are fixed here, and the v1 [[SessionCatalog]] is a per-call copy whose current
 * database is the call's schema. Nothing is written to the invoker session, so overlapping
 * calls cannot observe each other and a failed call cannot leave the session changed.
 *
 * Path and temp state are read from the wrapped manager. Mutators throw, because an
 * analysis-scoped manager must not change anything.
 */
private[sql] class AnalysisCatalogManager(
    delegate: CatalogManager,
    catalogName: String,
    namespace: Array[String],
    perCallV1: SessionCatalog) extends CatalogManager {

  // ---- Underlying state: the call's own catalog, namespace and v1 catalog. ----
  override def defaultSessionCatalog: CatalogPlugin = delegate.defaultSessionCatalog
  override def v1SessionCatalog: SessionCatalog = perCallV1
  override def tempVariableManager: TempVariableManager = delegate.tempVariableManager

  override def catalog(name: String): CatalogPlugin = delegate.catalog(name)
  override private[sql] def v2SessionCatalog: CatalogPlugin = delegate.v2SessionCatalog
  override def listCatalogs(pattern: Option[String]): Seq[String] =
    delegate.listCatalogs(pattern)

  override def transaction: Option[Transaction] = None

  override def withTransaction(transaction: Transaction): CatalogManager =
    throw SparkException.internalError("ANALYZE does not run in a transaction.")

  override def currentCatalog: CatalogPlugin = delegate.catalog(catalogName)

  override def setCurrentCatalog(name: String): Unit =
    throw SparkException.internalError("The ANALYZE catalog context is read-only.")

  override def currentNamespace: Array[String] = namespace.clone()

  override def setCurrentNamespace(ns: Array[String]): Unit =
    throw SparkException.internalError("The ANALYZE catalog context is read-only.")

  // ---- SQL path: read from the wrapped manager, with this call's catalog and namespace. ----
  override def sessionPathEntries: Option[Seq[SessionPathEntry]] =
    delegate.sessionPathEntries

  override def storedSessionPathEntries: Option[Seq[SessionPathEntry]] =
    delegate.storedSessionPathEntries

  override def confDefaultPathEntries: Option[Seq[SessionPathEntry]] =
    delegate.confDefaultPathEntries

  override def setSessionPath(entries: Seq[SessionPathEntry]): Unit =
    throw SparkException.internalError("The ANALYZE SQL path is read-only.")

  override def clearSessionPath(): Unit =
    throw SparkException.internalError("The ANALYZE SQL path is read-only.")

  override private[sql] def copySessionPathFrom(other: CatalogManager): Unit =
    throw SparkException.internalError("The ANALYZE SQL path is read-only.")

  override def currentPathString: String = delegate.currentPathString

  override def sqlResolutionPathEntries(
      pathDefaultCatalog: String,
      pathDefaultNamespace: Seq[String],
      expandCatalog: String,
      expandNamespace: Seq[String]): Seq[Seq[String]] =
    delegate.sqlResolutionPathEntries(
      pathDefaultCatalog, pathDefaultNamespace, expandCatalog, expandNamespace)

  override def sqlResolutionPathEntries(
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[Seq[String]] =
    delegate.sqlResolutionPathEntries(currentCatalog, currentNamespace)

  override def isSystemSessionOnPath: Boolean = delegate.isSystemSessionOnPath

  /** As [[CatalogManager.resolutionPathEntriesForAnalysis]], with this call's namespace. */
  override def resolutionPathEntriesForAnalysis(
      pinnedEntries: Option[Seq[Seq[String]]],
      viewCatalogAndNamespace: Seq[String]): Seq[Seq[String]] = {
    pinnedEntries match {
      case Some(entries) if SQLConf.get.pathEnabled => entries
      case _ =>
        val expandCatalog = currentCatalog.name()
        val expandNamespace = currentNamespace.toSeq
        val (pathCatalog, pathNamespace) =
          if (viewCatalogAndNamespace.nonEmpty) {
            (viewCatalogAndNamespace.head, viewCatalogAndNamespace.tail.toSeq)
          } else {
            (expandCatalog, expandNamespace)
          }
        sqlResolutionPathEntries(pathCatalog, pathNamespace, expandCatalog, expandNamespace)
    }
  }

  /** As [[CatalogManager.sessionFunctionKindsForUnqualifiedResolution]], with this namespace. */
  override def sessionFunctionKindsForUnqualifiedResolution(): Seq[SessionFunctionKind] = {
    val pathEntries = sqlResolutionPathEntries(catalogName, currentNamespace.toSeq)
    CatalogManager.systemFunctionKindsFromPath(pathEntries)
  }

  override private[sql] def reset(): Unit =
    throw SparkException.internalError("The ANALYZE catalog context cannot be reset.")
}
