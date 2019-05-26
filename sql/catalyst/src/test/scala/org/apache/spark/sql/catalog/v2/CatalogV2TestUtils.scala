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

package org.apache.spark.sql.catalog.v2

import org.apache.spark.sql.internal.SQLConf

private[sql] trait CatalogV2TestUtils {

  protected lazy val catalogManager: CatalogManager = new CatalogManager(SQLConf.get)

  /**
   * Adds a catalog.
   */
  protected def addCatalog(name: String, pluginClassName: String): Unit =
    catalogManager.add(name, pluginClassName)

  /**
   * Removes catalogs.
   */
  protected def removeCatalog(catalogNames: String*): Unit =
    catalogNames.foreach { catalogName =>
      catalogManager.remove(catalogName)
    }

  /**
   * Sets the default catalog.
   *
   * @param catalog the new default catalog
   */
  protected def setDefaultCatalog(catalog: String): Unit =
    SQLConf.get.setConfString(SQLConf.DEFAULT_V2_CATALOG.key, catalog)

  /**
   * Returns the current default catalog.
   */
  protected def defaultCatalog: Option[String] = SQLConf.get.defaultV2Catalog

  /**
   * Restores the default catalog to the previously saved value.
   */
  protected def restoreDefaultCatalog(previous: Option[String]): Unit =
    previous.foreach(SQLConf.get.setConfString(SQLConf.DEFAULT_V2_CATALOG.key, _))
}
