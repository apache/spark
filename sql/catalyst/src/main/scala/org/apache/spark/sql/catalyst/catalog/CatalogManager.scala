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

package org.apache.spark.sql.catalyst.catalog

import scala.collection.mutable

import org.apache.spark.sql.catalog.v2.{CatalogPlugin, Catalogs}
import org.apache.spark.sql.internal.SQLConf

/**
 * A thread-safe manager for [[CatalogPlugin]]s. It tracks all the registered catalogs, and allow
 * the caller to look up a catalog by name.
 *
 * TODO: it keeps an [[ExternalCatalog]] because we haven't migrated it to the new
 * catalog API.
 */
class CatalogManager(conf: SQLConf, val externalCatalog: ExternalCatalog) {

  /**
   * Tracks all the registered catalogs.
   */
  private val catalogs = mutable.HashMap.empty[String, CatalogPlugin]

  /**
   * Looks up a catalog by name.
   */
  def getCatalog(name: String): CatalogPlugin = synchronized {
    catalogs.getOrElseUpdate(name, Catalogs.load(name, conf))
  }

  def getDefaultCatalog(): Option[CatalogPlugin] = {
    conf.defaultV2Catalog.map(getCatalog)
  }
}
