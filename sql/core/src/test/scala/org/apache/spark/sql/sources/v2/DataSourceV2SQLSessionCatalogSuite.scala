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

package org.apache.spark.sql.sources.v2

import org.apache.spark.sql.catalog.v2.Identifier

class DataSourceV2SQLSessionCatalogSuite
  extends AlterTableSQLTests
  with SessionCatalogTest[InMemoryTable, InMemoryTableSessionCatalog] {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  override val catalogAndNamespace = "default."
  override protected val v2Source: String = classOf[InMemoryTableProvider].getName

  override def loadAlterTableMetadata(tableName: String): Table = {
    spark.sessionState.catalogManager.v2SessionCatalog.get.asTableCatalog
      .loadTable(Identifier.of(Array(), tableName))
  }
}
