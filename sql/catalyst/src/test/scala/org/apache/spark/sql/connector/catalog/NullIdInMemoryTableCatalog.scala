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

/**
 * An InMemoryTableCatalog that creates tables WITHOUT table IDs
 * (id() returns null). This simulates connectors that don't
 * support table identity tracking.
 *
 * When table ID is null, the validateTableIdentity check in
 * V2TableRefreshUtil is skipped entirely, meaning drop/recreate
 * of a table is NOT detected as an error.
 */
class NullIdInMemoryTableCatalog extends InMemoryTableCatalog {

  override def createTable(
      ident: Identifier,
      info: TableInfo): Table = {
    val created = super.createTable(ident, info)
    // Replace with a null-ID wrapper
    val nullIdTable = created match {
      case t: InMemoryTable =>
        new InMemoryTable(
          t.name,
          t.columns(),
          t.partitioning,
          t.properties,
          t.constraints,
          id = null) // null table ID
      case other => other
    }
    tables.put(ident, nullIdTable)
    nullIdTable
  }
}
