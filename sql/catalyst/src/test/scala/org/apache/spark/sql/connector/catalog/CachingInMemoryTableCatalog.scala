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
 * Iceberg's CachingCatalog. On first [[loadTable]], returns a fresh
 * copy. On subsequent loads, returns the CACHED (stale) copy,
 * making external changes invisible.
 *
 * Session writes go through the write-variant [[loadTable]], which is not
 * cached, so they modify the underlying table directly. Cached [[loadTable]]
 * results may still be stale until [[clearCache]] or REFRESH TABLE (which
 * invokes [[invalidateTable]]) is called.
 *
 * Note: [[dropTable]], [[createTable]], and [[alterTable]] do not invalidate
 * the cache, matching the behavior of real caching connectors like Iceberg's
 * CachingCatalog.
 */
class CachingInMemoryTableCatalog extends InMemoryTableCatalog {
  private val cachedTables = new ConcurrentHashMap[Identifier, Table]()

  override def loadTable(ident: Identifier): Table =
    cachedTables.computeIfAbsent(ident, _ => super.loadTable(ident))

  override def invalidateTable(ident: Identifier): Unit = {
    super.invalidateTable(ident)
    cachedTables.remove(ident)
  }

  def clearCache(): Unit = cachedTables.clear()
}
