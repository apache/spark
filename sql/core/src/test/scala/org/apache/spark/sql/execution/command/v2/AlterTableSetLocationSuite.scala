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

package org.apache.spark.sql.execution.command.v2

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.CatalogV2Util.withDefaultOwnership
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `ALTER TABLE .. SET LOCATION` command to
 * check V2 table catalogs.
 */
class AlterTableSetLocationSuite
  extends command.AlterTableSetLocationSuiteBase with CommandSuiteBase {

  private def getTableMetadata(tableName: String): Table = {
    val nameParts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val v2Catalog = spark.sessionState.catalogManager.catalog(nameParts.head).asTableCatalog
    val namespace = nameParts.drop(1).init.toArray
    v2Catalog.loadTable(Identifier.of(namespace, nameParts.last))
  }

  test("alter table set location") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t SET LOCATION 's3://bucket/path'")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.properties === withDefaultOwnership(
        Map("provider" -> "foo", "location" -> "s3://bucket/path")).asJava)
    }
  }

  test("alter table set partition location") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) USING foo")

      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t PARTITION(ds='2017-06-10') SET LOCATION 's3://bucket/path'")
      }
      assert(e.getMessage.contains(
        "ALTER TABLE SET LOCATION does not support partition for v2 tables"))
    }
  }
}
