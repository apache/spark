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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.execution.command.v1
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `SHOW TABLES` command to check V1 Hive external table catalog.
 */
class ShowTablesSuite extends v1.ShowTablesSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowTablesSuiteBase].commandVersion

  test("hive client calls") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")
      checkHiveClientCalls(expected = 3) {
        sql(s"SHOW TABLES IN $catalog.ns")
      }
    }
  }

  override protected def extendedPartInNonPartedTableError(
      catalog: String,
      namespace: String,
      table: String): (String, Map[String, String]) = {
    ("_LEGACY_ERROR_TEMP_1231",
      Map("key" -> "id", "tblName" -> s"`$catalog`.`$namespace`.`$table`"))
  }

  protected override def extendedPartExpectedResult: String =
    super.extendedPartExpectedResult +
    """
      |Location: <location>
      |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
      |InputFormat: org.apache.hadoop.mapred.TextInputFormat
      |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
      |Storage Properties: [serialization.format=1]
      |Partition Parameters: <partition parameters>
      |Created Time: <created time>
      |Last Access: <last access>""".stripMargin

  protected override def extendedTableInfo: String =
    s"""Owner: ${Utils.getCurrentUserName()}
       |Created Time: <created time>
       |Last Access: <last access>
       |Created By: <created by>
       |Type: MANAGED
       |Provider: hive
       |Table Properties: <table properties>
       |Location: <location>
       |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
       |InputFormat: org.apache.hadoop.mapred.TextInputFormat
       |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
       |Storage Properties: [serialization.format=1]""".stripMargin

  test("show table extended in permanent view") {
    val namespace = "ns"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")
      val viewName = table + "_view"
      withView(viewName) {
        sql(s"CREATE VIEW $catalog.$namespace.$viewName AS SELECT id FROM $t")
        val result = sql(s"SHOW TABLE EXTENDED in $namespace LIKE '$viewName*'").sort("tableName")
        assert(result.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        val resultCollect = result.collect()
        assert(resultCollect.length == 1)
        assert(resultCollect(0).length == 4)
        assert(resultCollect(0)(1) === viewName)
        assert(resultCollect(0)(2) === false)
        val actualResult = replace(resultCollect(0)(3).toString)
        val expectedResult =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $viewName
             |Owner: ${Utils.getCurrentUserName()}
             |Created Time: <created time>
             |Last Access: <last access>
             |Created By: <created by>
             |Type: VIEW
             |View Text: SELECT id FROM $catalog.$namespace.$table
             |View Original Text: SELECT id FROM $catalog.$namespace.$table
             |View Schema Mode: COMPENSATION
             |View Catalog and Namespace: $catalog.$namespace
             |View Query Output Columns: [`id`]
             |Table Properties: <table properties>
             |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
             |InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat
             |OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat
             |Storage Properties: [serialization.format=1]
             |Schema: root
             | |-- id: integer (nullable = true)""".stripMargin
        assert(actualResult === expectedResult)
      }
    }
  }
}
