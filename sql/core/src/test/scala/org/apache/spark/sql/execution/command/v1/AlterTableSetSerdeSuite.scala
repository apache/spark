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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

/**
 * This base suite contains unified tests for the `ALTER TABLE .. SET [SERDE|SERDEPROPERTIES]`
 * command that check V1 table catalogs. The tests that cannot run for all V1 catalogs
 * are located in more specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.AlterTableSetSerdeSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.AlterTableSetSerdeSuite`
 */
trait AlterTableSetSerdeSuiteBase extends command.AlterTableSetSerdeSuiteBase {

  private[sql] lazy val sessionCatalog = spark.sessionState.catalog

  private def isUsingHiveMetastore: Boolean = {
    spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive"
  }

  private def normalizeSerdeProp(props: Map[String, String]): Map[String, String] = {
    props.filterNot(p => Seq("serialization.format", "path").contains(p._1))
  }

  private[sql] def checkSerdeProps(tableIdent: TableIdentifier,
    expectedSerdeProps: Map[String, String]): Unit = {
    val serdeProp = sessionCatalog.getTableMetadata(tableIdent).storage.properties
    if (isUsingHiveMetastore) {
      assert(normalizeSerdeProp(serdeProp) == expectedSerdeProps)
    } else {
      assert(serdeProp == expectedSerdeProps)
    }
  }

  private[sql] def checkPartitionSerdeProps(
    tableIdent: TableIdentifier,
    spec: Map[String, String],
    expectedSerdeProps: Map[String, String]): Unit = {
    val serdeProp = sessionCatalog.getPartition(tableIdent, spec).storage.properties
    if (isUsingHiveMetastore) {
      assert(normalizeSerdeProp(serdeProp) == expectedSerdeProps)
    } else {
      assert(serdeProp == expectedSerdeProps)
    }
  }
}

/**
 * The class contains tests for the `ALTER TABLE .. SET [SERDE|SERDEPROPERTIES]` command to check
 * V1 In-Memory table catalog.
 */
class AlterTableSetSerdeSuite extends AlterTableSetSerdeSuiteBase with CommandSuiteBase {

  test("In-Memory catalog - datasource table: alter table set serde") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) $defaultUsing " +
        s"PARTITIONED BY (a, b)")

      val tableIdent = TableIdentifier("tbl", Some("ns"))
      assert(sessionCatalog.getTableMetadata(tableIdent).storage.serde.isEmpty)
      checkSerdeProps(tableIdent, Map.empty[String, String])

      // set table serde and/or properties (should fail on datasource tables)
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t SET SERDE 'whatever'")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1248",
        parameters = Map.empty)
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t SET SERDE 'org.apache.madoop' " +
            "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1248",
        parameters = Map.empty)

      // set serde properties only
      sql(s"ALTER TABLE $t SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      checkSerdeProps(tableIdent, Map("k" -> "vvv", "kay" -> "vee"))

      // set things without explicitly specifying database
      sessionCatalog.setCurrentDatabase("ns")
      sql(s"ALTER TABLE tbl SET SERDEPROPERTIES ('kay' = 'veee')")
      checkSerdeProps(tableIdent, Map("k" -> "vvv", "kay" -> "veee"))

      // table to alter does not exist
      val e3 = intercept[AnalysisException] {
        sql("ALTER TABLE does_not_exist SET SERDEPROPERTIES ('x' = 'y')")
      }
      checkErrorTableNotFound(e3, "`does_not_exist`",
        ExpectedContext("does_not_exist", 12, 11 + "does_not_exist".length))
    }
  }

  test("In-Memory catalog - datasource table: alter table set serde partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) $defaultUsing " +
        s"PARTITIONED BY (a, b)")
      sql(s"INSERT INTO $t PARTITION (a = '1', b = '2') SELECT 1, 'abc'")
      sql(s"INSERT INTO $t PARTITION (a = '1', b = '3') SELECT 2, 'def'")
      sql(s"INSERT INTO $t PARTITION (a = '2', b = '2') SELECT 3, 'ghi'")
      sql(s"INSERT INTO $t PARTITION (a = '2', b = '3') SELECT 4, 'jkl'")

      val tableIdent = TableIdentifier("tbl", Some("ns"))
      val spec = Map("a" -> "1", "b" -> "2")
      assert(sessionCatalog.getPartition(tableIdent, spec).storage.serde.isEmpty)
      checkPartitionSerdeProps(tableIdent, spec, Map.empty[String, String])

      // set table serde and/or properties (should fail on datasource tables)
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDE 'whatever'")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1247",
        parameters = Map.empty)
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
            "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1247",
        parameters = Map.empty)

      // set serde properties only
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t PARTITION (a=1, b=2) " +
            "SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1247",
        parameters = Map.empty)

      // set things without explicitly specifying database
      sessionCatalog.setCurrentDatabase("ns")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE tbl PARTITION (a=1, b=2) SET SERDEPROPERTIES ('kay' = 'veee')")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1247",
        parameters = Map.empty)

      // table to alter does not exist
      val e5 = intercept[AnalysisException] {
        sql("ALTER TABLE does_not_exist PARTITION (a=1, b=2) SET SERDEPROPERTIES ('x' = 'y')")
      }
      checkErrorTableNotFound(e5, "`does_not_exist`",
        ExpectedContext("does_not_exist", 12, 11 + "does_not_exist".length))
    }
  }
}
