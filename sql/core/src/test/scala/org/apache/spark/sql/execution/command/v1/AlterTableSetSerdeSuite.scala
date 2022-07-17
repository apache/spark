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

  protected val isDatasourceTable = true

  private def isUsingHiveMetastore: Boolean = {
    spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive"
  }

  private def normalizeSerdeProp(props: Map[String, String]): Map[String, String] = {
    props.filterNot(p => Seq("serialization.format", "path").contains(p._1))
  }

  private def maybeWrapException[T](expectException: Boolean)(body: => T): Unit = {
    if (expectException) intercept[AnalysisException] { body } else body
  }

  protected def testSetSerde(): Unit = {
    withNamespaceAndTable("ns", "tbl") { t =>
      if (!isUsingHiveMetastore) {
        assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
      }
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) $defaultUsing " +
        s"PARTITIONED BY (a, b)")

      val catalog = spark.sessionState.catalog
      val tableIdent = TableIdentifier("tbl", Some("ns"))
      def checkSerdeProps(expectedSerdeProps: Map[String, String]): Unit = {
        val serdeProp = catalog.getTableMetadata(tableIdent).storage.properties
        if (isUsingHiveMetastore) {
          assert(normalizeSerdeProp(serdeProp) == expectedSerdeProps)
        } else {
          assert(serdeProp == expectedSerdeProps)
        }
      }
      if (isUsingHiveMetastore) {
        val expectedSerde = if (isDatasourceTable) {
          "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        } else {
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        }
        assert(catalog.getTableMetadata(tableIdent).storage.serde == Some(expectedSerde))
      } else {
        assert(catalog.getTableMetadata(tableIdent).storage.serde.isEmpty)
      }
      checkSerdeProps(Map.empty[String, String])
      // set table serde and/or properties (should fail on datasource tables)
      if (isDatasourceTable) {
        val e1 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t SET SERDE 'whatever'")
        }
        val e2 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t SET SERDE 'org.apache.madoop' " +
            "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
        }
        assert(e1.getMessage.contains("datasource"))
        assert(e2.getMessage.contains("datasource"))
      } else {
        val newSerde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        sql(s"ALTER TABLE $t SET SERDE '$newSerde'")
        assert(catalog.getTableMetadata(tableIdent).storage.serde == Some(newSerde))
        checkSerdeProps(Map.empty[String, String])
        val serde2 = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"
        sql(s"ALTER TABLE $t SET SERDE '$serde2' " +
          "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
        assert(catalog.getTableMetadata(tableIdent).storage.serde == Some(serde2))
        checkSerdeProps(Map("k" -> "v", "kay" -> "vee"))
      }
      // set serde properties only
      sql(s"ALTER TABLE $t SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      checkSerdeProps(Map("k" -> "vvv", "kay" -> "vee"))
      // set things without explicitly specifying database
      catalog.setCurrentDatabase("ns")
      sql("ALTER TABLE tbl SET SERDEPROPERTIES ('kay' = 'veee')")
      checkSerdeProps(Map("k" -> "vvv", "kay" -> "veee"))
      // table to alter does not exist
      intercept[AnalysisException] {
        sql("ALTER TABLE does_not_exist SET SERDEPROPERTIES ('x' = 'y')")
      }
    }
  }

  protected def testSetSerdePartition(): Unit = {
    withNamespaceAndTable("ns", "tbl") { t =>
      if (!isUsingHiveMetastore) {
        assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
      }
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) $defaultUsing " +
        s"PARTITIONED BY (a, b)")
      sql(s"INSERT INTO $t PARTITION (a = '1', b = '2') SELECT 1, 'abc'")
      sql(s"INSERT INTO $t PARTITION (a = '1', b = '3') SELECT 2, 'def'")
      sql(s"INSERT INTO $t PARTITION (a = '2', b = '2') SELECT 3, 'ghi'")
      sql(s"INSERT INTO $t PARTITION (a = '2', b = '3') SELECT 4, 'jkl'")

      val catalog = spark.sessionState.catalog
      val tableIdent = TableIdentifier("tbl", Some("ns"))
      val spec = Map("a" -> "1", "b" -> "2")
      def checkPartitionSerdeProps(expectedSerdeProps: Map[String, String]): Unit = {
        val serdeProp = catalog.getPartition(tableIdent, spec).storage.properties
        if (isUsingHiveMetastore) {
          assert(normalizeSerdeProp(serdeProp) == expectedSerdeProps)
        } else {
          assert(serdeProp == expectedSerdeProps)
        }
      }
      if (isUsingHiveMetastore) {
        val expectedSerde = if (isDatasourceTable) {
          "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        } else {
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        }
        assert(catalog.getPartition(tableIdent, spec).storage.serde == Some(expectedSerde))
      } else {
        assert(catalog.getPartition(tableIdent, spec).storage.serde.isEmpty)
      }
      checkPartitionSerdeProps(Map.empty[String, String])
      // set table serde and/or properties (should fail on datasource tables)
      if (isDatasourceTable) {
        val e1 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDE 'whatever'")
        }
        val e2 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
            "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
        }
        assert(e1.getMessage.contains("datasource"))
        assert(e2.getMessage.contains("datasource"))
      } else {
        sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDE 'org.apache.jadoop'")
        assert(catalog.getPartition(tableIdent, spec).storage.serde == Some("org.apache.jadoop"))
        checkPartitionSerdeProps(Map.empty[String, String])
        sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
          "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
        assert(catalog.getPartition(tableIdent, spec).storage.serde == Some("org.apache.madoop"))
        checkPartitionSerdeProps(Map("k" -> "v", "kay" -> "vee"))
      }
      // set serde properties only
      maybeWrapException(isDatasourceTable) {
        sql(s"ALTER TABLE $t PARTITION (a=1, b=2) " +
          "SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
        checkPartitionSerdeProps(Map("k" -> "vvv", "kay" -> "vee"))
      }
      // set things without explicitly specifying database
      catalog.setCurrentDatabase("ns")
      maybeWrapException(isDatasourceTable) {
        sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDEPROPERTIES ('kay' = 'veee')")
        checkPartitionSerdeProps(Map("k" -> "vvv", "kay" -> "veee"))
      }
      // table to alter does not exist
      intercept[AnalysisException] {
        sql("ALTER TABLE does_not_exist PARTITION (a=1, b=2) SET SERDEPROPERTIES ('x' = 'y')")
      }

    }
  }
}

/**
 * The class contains tests for the `ALTER TABLE .. SET [SERDE|SERDEPROPERTIES]` command to check
 * V1 In-Memory table catalog.
 */
class AlterTableSetSerdeSuite extends AlterTableSetSerdeSuiteBase with CommandSuiteBase {

  test("datasource table: alter table set serde") {
    testSetSerde()
  }

  test("datasource table: alter table set serde partition") {
    testSetSerdePartition()
  }
}
