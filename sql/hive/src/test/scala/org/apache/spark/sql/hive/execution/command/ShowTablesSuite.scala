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

import org.apache.spark.sql.AnalysisException
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

  test("show table extended in non-partitioned table") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing")
      val e = intercept[AnalysisException] {
        sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table' PARTITION(id = 1)")
      }
      checkError(
        exception = e,
        errorClass = "_LEGACY_ERROR_TEMP_1231",
        parameters = Map("key" -> "id", "tblName" -> s"`$catalog`.`$namespace`.`$table`")
      )
    }
  }

  test("show table extended in multi partition key - " +
    "the command's partition parameters are complete") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id1 bigint, id2 bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id1, id2)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id1 = 1, id2 = 2)")

      val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace " +
        s"LIKE '$table' PARTITION(id1 = 1, id2 = 2)")
      assert(result.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result.collect()(0).length == 4)
      assert(result.collect()(0)(0) === namespace)
      assert(result.collect()(0)(1) === table)
      assert(result.collect()(0)(2) === false)
      val actualResult = exclude(result.collect()(0)(3).toString)
      val expectedResult =
        """Partition Values: [id1=1, id2=2]
          |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          |InputFormat: org.apache.hadoop.mapred.TextInputFormat
          |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
          |Storage Properties: [serialization.format=1]""".stripMargin
      assert(actualResult === expectedResult)
    }
  }

  test("show table extended in multi tables") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { _ =>
      sql(s"CREATE TABLE $catalog.$namespace.$table (id bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id)")
      val table1 = "tbl1"
      val table2 = "tbl2"
      withTable(table1, table2) {
        sql(s"CREATE TABLE $catalog.$namespace.$table1 (id1 bigint, data1 string) " +
          s"$defaultUsing PARTITIONED BY (id1)")
        sql(s"CREATE TABLE $catalog.$namespace.$table2 (id2 bigint, data2 string) " +
          s"$defaultUsing PARTITIONED BY (id2)")

        val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE '$table*'")
          .sort("tableName")
        assert(result.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        assert(result.collect().length == 3)

        assert(result.collect()(0).length == 4)
        assert(result.collect()(0)(1) === table)
        assert(result.collect()(0)(2) === false)
        val actualResult_0_3 = exclude(result.collect()(0)(3).toString)

        // exclude "Table Properties"
        val expectedResult_0_3 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table
             |Owner: ${Utils.getCurrentUserName()}
             |Type: MANAGED
             |Provider: hive
             |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
             |InputFormat: org.apache.hadoop.mapred.TextInputFormat
             |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
             |Storage Properties: [serialization.format=1]
             |Partition Provider: Catalog
             |Partition Columns: [`id`]
             |Schema: root
             | |-- data: string (nullable = true)
             | |-- id: long (nullable = true)""".stripMargin
        assert(actualResult_0_3 === expectedResult_0_3)

        assert(result.collect()(1).length == 4)
        assert(result.collect()(1)(1) === table1)
        assert(result.collect()(1)(2) === false)
        val actualResult_1_3 = exclude(result.collect()(1)(3).toString)

        // exclude "Table Properties"
        val expectedResult_1_3 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table1
             |Owner: ${Utils.getCurrentUserName()}
             |Type: MANAGED
             |Provider: hive
             |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
             |InputFormat: org.apache.hadoop.mapred.TextInputFormat
             |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
             |Storage Properties: [serialization.format=1]
             |Partition Provider: Catalog
             |Partition Columns: [`id1`]
             |Schema: root
             | |-- data1: string (nullable = true)
             | |-- id1: long (nullable = true)""".stripMargin
        assert(actualResult_1_3 === expectedResult_1_3)

        assert(result.collect()(2).length == 4)
        assert(result.collect()(2)(1) === table2)
        assert(result.collect()(2)(2) === false)
        val actualResult_2_3 = exclude(result.collect()(2)(3).toString)

        // exclude "Table Properties"
        val expectedResult_2_3 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table2
             |Owner: ${Utils.getCurrentUserName()}
             |Type: MANAGED
             |Provider: hive
             |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
             |InputFormat: org.apache.hadoop.mapred.TextInputFormat
             |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
             |Storage Properties: [serialization.format=1]
             |Partition Provider: Catalog
             |Partition Columns: [`id2`]
             |Schema: root
             | |-- data2: string (nullable = true)
             | |-- id2: long (nullable = true)""".stripMargin
        assert(actualResult_2_3 === expectedResult_2_3)
      }
    }
  }
}
