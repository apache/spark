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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystConf, TableIdentifier, SimpleCatalystConf}

class CatalogSuite extends AnalysisTest {

  test("check deprecated methods") {
    val caseSensitiveConf = new SimpleCatalystConf(true)
    val caseInsensitiveConf = new SimpleCatalystConf(false)
    val caseSensitiveCatalog = new SimpleCatalog(caseSensitiveConf)
    val caseInsensitiveCatalog = new SimpleCatalog(caseInsensitiveConf)
    val catalogs = Seq(caseSensitiveCatalog, caseInsensitiveCatalog)

    val plan = UnresolvedRelation(TableIdentifier("table"), Some("alias"))
    val seqId = Seq("db", "table")
    val tableId = TableIdentifier("table", Some("db"))

    for (catalog <- catalogs) {
      catalog.registerTable(seqId, plan)
      assert(catalog.lookupRelation(seqId) == catalog.lookupRelation(tableId))
      assert(catalog.tableExists(seqId))
      assert(catalog.tableExists(tableId))
      catalog.unregisterTable(seqId)
      assert(!catalog.tableExists(seqId))
      assert(!catalog.tableExists(tableId))
    }
  }

  test("check protected deprecated methods") {
    val publicCatalog = new SimpleCatalog(new SimpleCatalystConf(true)) {
      override def processTableIdentifier(tableIdentifier: Seq[String]): Seq[String] =
        super.processTableIdentifier(tableIdentifier)
      override def getDbTableName(tableIdent: Seq[String]): String =
        super.getDbTableName(tableIdent)
      override def getDBTable(tableIdent: Seq[String]): (Option[String], String) =
        super.getDBTable(tableIdent)
      override def checkTableIdentifier(tableIdentifier: Seq[String]): Unit =
        super.checkTableIdentifier(tableIdentifier)
    }

  }
}
