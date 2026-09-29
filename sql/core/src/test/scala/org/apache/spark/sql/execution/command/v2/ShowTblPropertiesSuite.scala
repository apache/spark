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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `SHOW TBLPROPERTIES` command to check V2 table catalogs.
 */
class ShowTblPropertiesSuite extends command.ShowTblPropertiesSuiteBase with CommandSuiteBase {
  test("display properties supplement current properties and exclude reserved keys") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint) $defaultUsing " +
        "TBLPROPERTIES ('persisted' = 'stored')")
      loadTable(catalog, "ns", "table").setDisplayProperties(Map(
        "catalog-label" -> "catalog-value",
        "persisted" -> "display-value",
        TableCatalog.PROP_EXTERNAL -> "true",
        TableCatalog.PROP_LOCATION -> "file:/display-location").asJava)

      checkAnswer(sql(s"SHOW TBLPROPERTIES $tbl"),
        Seq(Row("catalog-label", "catalog-value"), Row("persisted", "stored")))
      checkAnswer(sql(s"SHOW TBLPROPERTIES $tbl ('persisted')"), Row("persisted", "stored"))
      checkAnswer(sql(s"SHOW TBLPROPERTIES $tbl ('catalog-label')"),
        Row("catalog-label", "catalog-value"))

      sql(s"ALTER TABLE $tbl SET TBLPROPERTIES ('persisted' = 'updated')")
      checkAnswer(sql(s"SHOW TBLPROPERTIES $tbl"),
        Seq(Row("catalog-label", "catalog-value"), Row("persisted", "updated")))
    }
  }
}
