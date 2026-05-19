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

import java.util.concurrent.ConcurrentHashMap

/**
 * An InMemoryTableCatalog that simulates a caching connector like
 * Iceberg's CachingCatalog. On first loadTable, returns a fresh
 * copy. On subsequent loads, returns the CACHED (stale) copy,
 * making external changes invisible.
 *
 * Session writes go through the SQL path which modifies the
 * original table and invalidates, but direct catalog API
 * modifications are not visible until the cache is cleared.
 *
 * Call [[CachingInMemoryTableCatalog.clearCache()]] to simulate
 * cache expiration (like Iceberg's 30-second TTL).
 */
class CachingInMemoryTableCatalog extends InMemoryTableCatalog {
  import CachingInMemoryTableCatalog._

  // Note: The write-path loadTable(ident, writePrivileges) is NOT overridden,
  // so writes bypass the cache and go directly to the underlying table.
  // This is intentional: it simulates connectors where writes update the real
  // table while reads may return stale cached copies.
  //
  // Note: dropTable, createTable, and alterTable are NOT overridden, so they
  // do not invalidate the cache. Only explicit invalidateTable calls (e.g.,
  // via REFRESH TABLE) clear cached entries. This simulates real-world
  // caching connectors where external DDL is invisible until refresh.

  override def loadTable(ident: Identifier): Table = {
    cachedTables.computeIfAbsent(cacheKey(name, ident), _ => {
      super.loadTable(ident)
    })
  }

  override def invalidateTable(ident: Identifier): Unit = {
    super.invalidateTable(ident)
    cachedTables.remove(cacheKey(name, ident))
  }

  private def cacheKey(
      catalog: String, ident: Identifier): String = {
    s"$catalog.${ident.toString}"
  }
}

object CachingInMemoryTableCatalog {
  private val cachedTables =
    new ConcurrentHashMap[String, Table]()

  def clearCache(): Unit = cachedTables.clear()
}
