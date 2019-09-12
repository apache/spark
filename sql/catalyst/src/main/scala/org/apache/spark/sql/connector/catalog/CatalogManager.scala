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
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

/**
 * A thread-safe manager for [[CatalogPlugin]]s. It tracks all the registered catalogs, and allow
 * the caller to look up a catalog by name.
 */
private[sql]
class CatalogManager(conf: SQLConf, defaultSessionCatalog: TableCatalog) extends Logging {

  private val catalogs = mutable.HashMap.empty[String, CatalogPlugin]

  def catalog(name: String): CatalogPlugin = synchronized {
    if (name.equalsIgnoreCase(CatalogManager.SESSION_CATALOG_NAME)) {
      v2SessionCatalog
    } else {
      catalogs.getOrElseUpdate(name, Catalogs.load(name, conf))
    }
  }

  def defaultCatalog: Option[CatalogPlugin] = {
    conf.defaultV2Catalog.flatMap { catalogName =>
      try {
        Some(catalog(catalogName))
      } catch {
        case NonFatal(e) =>
          logError(s"Cannot load default v2 catalog: $catalogName", e)
          None
      }
    }
  }

  private def loadV2SessionCatalog(): CatalogPlugin = {
    Catalogs.load(CatalogManager.SESSION_CATALOG_NAME, conf) match {
      case extension: CatalogExtension =>
        extension.setDelegateCatalog(defaultSessionCatalog)
        extension
      case other => other
    }
  }

  // If the V2_SESSION_CATALOG config is specified, we try to instantiate the user-specified v2
  // session catalog. Otherwise, return the default session catalog.
  def v2SessionCatalog: CatalogPlugin = {
    conf.getConf(SQLConf.V2_SESSION_CATALOG).map { customV2SessionCatalog =>
      try {
        catalogs.getOrElseUpdate(CatalogManager.SESSION_CATALOG_NAME, loadV2SessionCatalog())
      } catch {
        case NonFatal(_) =>
          logError(
            "Fail to instantiate the custom v2 session catalog: " + customV2SessionCatalog)
          defaultSessionCatalog
      }
    }.getOrElse(defaultSessionCatalog)
  }

  private def getDefaultNamespace(c: CatalogPlugin) = c match {
    case c: SupportsNamespaces => c.defaultNamespace()
    case _ => Array.empty[String]
  }

  private var _currentNamespace: Option[Array[String]] = None

  def currentNamespace: Array[String] = synchronized {
    _currentNamespace.getOrElse {
      currentCatalog.map { catalogName =>
        getDefaultNamespace(catalog(catalogName))
      }.getOrElse(Array("default")) // The builtin catalog use "default" as the default database.
    }
  }

  def setCurrentNamespace(namespace: Array[String]): Unit = synchronized {
    _currentNamespace = Some(namespace)
  }

  private var _currentCatalog: Option[String] = None

  // Returns the name of current catalog. None means the current catalog is the builtin catalog.
  def currentCatalog: Option[String] = synchronized {
    _currentCatalog.orElse(conf.defaultV2Catalog)
  }

  def setCurrentCatalog(catalogName: String): Unit = synchronized {
    _currentCatalog = Some(catalogName)
    _currentNamespace = None
  }

  // Clear all the registered catalogs. Only used in tests.
  private[sql] def reset(): Unit = synchronized {
    catalogs.clear()
    _currentNamespace = None
    _currentCatalog = None
  }
}

private[sql] object CatalogManager {
  val SESSION_CATALOG_NAME: String = "session"
}
