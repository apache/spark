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
package org.apache.spark.sql.internal

import org.apache.spark.sql.SparkSession

/**
 * Used by CatalogImpl; not part of SparkSession public API.
 * Implemented by SessionState (core, uses SQL parser) and Connect SparkSession (uses
 * ConnectIdentifierParser).
 */
private[sql] trait CatalogSupport {
  private[sql] def parseMultipartIdentifier(identifier: String): Seq[String]
  private[sql] def quoteIdentifier(identifier: String): String
  private[sql] def currentDatabase: String
  private[sql] def currentCatalog(): String
  /**
   * Load function metadata from a V2 catalog (when DESCRIBE FUNCTION is not supported).
   * Returns (description, className) or None if the function does not exist or catalog
   * does not support functions.
   */
  private[sql] def getFunctionMetadata(
      catalogName: String,
      namespace: Array[String],
      functionName: String): Option[(String, String)]

  /**
   * Refresh cache and file metadata for the given path. Used by Catalog.refreshByPath to
   * avoid recursion (REFRESH SQL command calls catalog.refreshByPath again).
   * Default no-op; SessionState overrides to call CacheManager.recacheByPath.
   */
  private[sql] def refreshByPath(session: SparkSession, path: String): Unit = {}

  /**
   * Invalidate all cache entries that reference the given table/view (cascade = true).
   * Used by Catalog.refreshTable so we explicitly invalidate before recache, matching the
   * original pre-SQL-DDL catalog behavior (SPARK-33290, SPARK-33729). Default no-op;
   * SessionState overrides to call CacheManager.uncacheTableOrView.
   */
  private[sql] def uncacheTableOrView(session: SparkSession, tableName: String): Unit = {}

  /**
   * Invalidate and recache all cache entries that reference the given table/view.
   * Used by Catalog.refreshTable so REFRESH TABLE reliably refreshes cached data (SPARK-33290,
   * SPARK-33729). Default no-op; SessionState overrides to call CacheManager.recacheTableOrView.
   */
  private[sql] def recacheTableOrView(session: SparkSession, tableName: String): Unit = {}

  /**
   * Returns whether the given table/view is cached. When the table references a dropped view
   * or otherwise fails to resolve, resolution throws and the exception propagates.
   * Default: SHOW CACHED TABLES and match by name. SessionState overrides to resolve the
   * table then check cache so that invalid references throw AnalysisException.
   */
  private[sql] def isTableCached(session: SparkSession, tableName: String): Boolean = {
    val nameParts = parseMultipartIdentifier(tableName)
    val fullName = nameParts.mkString(".")
    val rows = session.sql("SHOW CACHED TABLES").collect()
    rows.exists { row =>
      val cachedName = row.getString(0)
      cachedName.equalsIgnoreCase(fullName) ||
        cachedName.equalsIgnoreCase(nameParts.last) ||
        (cachedName.contains(".") && cachedName.substring(cachedName.lastIndexOf(".") + 1)
          .equalsIgnoreCase(nameParts.last))
    }
  }
}
