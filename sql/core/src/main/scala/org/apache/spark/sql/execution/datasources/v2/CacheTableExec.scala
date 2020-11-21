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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.CacheTableUtils
import org.apache.spark.storage.StorageLevel

/**
 * Physical plan node for caching a table.
 */
case class CacheTableExec(
    session: SparkSession,
    catalog: TableCatalog,
    table: Table,
    ident: Identifier,
    isLazy: Boolean,
    options: Map[String, String]) extends V2CommandExec {
  override def run(): Seq[InternalRow] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper

    val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
    val df = Dataset.ofRows(session, v2Relation)
    val tableName = Some(ident.quoted)
    val optStorageLevel = CacheTableUtils.getStorageLevel(options)
    if (optStorageLevel.nonEmpty) {
      session.sharedState.cacheManager.cacheQuery(
        df, tableName, StorageLevel.fromString(optStorageLevel.get))
    } else {
      session.sharedState.cacheManager.cacheQuery(df, tableName)
    }

    if (!isLazy) {
      // Performs eager caching.
      df.count()
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * Physical plan node for uncaching a table.
 */
case class UncacheTableExec(
    session: SparkSession,
    catalog: TableCatalog,
    table: Table,
    ident: Identifier) extends V2CommandExec {
  override def run(): Seq[InternalRow] = {
    val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
    val df = Dataset.ofRows(session, v2Relation)
    // Cascade should be true unless a temporary view is uncached.
    session.sharedState.cacheManager.uncacheQuery(df, cascade = true)
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
