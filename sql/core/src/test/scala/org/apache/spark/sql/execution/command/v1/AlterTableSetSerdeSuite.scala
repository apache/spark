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

import java.io.File

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.execution.command.DDLSuiteBase
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * This base suite contains unified tests for the `ALTER TABLE .. SET [SERDE|SERDEPROPERTIES]`
 * command that check V1 table catalogs. The tests that cannot run for all V1 catalogs
 * are located in more specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.AlterTableSetSerdeSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.AlterTableSetSerdeSuite`
 */
trait AlterTableSetSerdeSuiteBase extends command.AlterTableSetSerdeSuiteBase with DDLSuiteBase {

  protected def testSetSerde(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
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
        sql("ALTER TABLE dbx.tab1 SET SERDE 'whatever'")
      }
      val e2 = intercept[AnalysisException] {
        sql("ALTER TABLE dbx.tab1 SET SERDE 'org.apache.madoop' " +
          "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      }
      assert(e1.getMessage.contains("datasource"))
      assert(e2.getMessage.contains("datasource"))
    } else {
      val newSerde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      sql(s"ALTER TABLE dbx.tab1 SET SERDE '$newSerde'")
      assert(catalog.getTableMetadata(tableIdent).storage.serde == Some(newSerde))
      checkSerdeProps(Map.empty[String, String])
      val serde2 = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"
      sql(s"ALTER TABLE dbx.tab1 SET SERDE '$serde2' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(catalog.getTableMetadata(tableIdent).storage.serde == Some(serde2))
      checkSerdeProps(Map("k" -> "v", "kay" -> "vee"))
    }
    // set serde properties only
    sql("ALTER TABLE dbx.tab1 SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
    checkSerdeProps(Map("k" -> "vvv", "kay" -> "vee"))
    // set things without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET SERDEPROPERTIES ('kay' = 'veee')")
    checkSerdeProps(Map("k" -> "vvv", "kay" -> "veee"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist SET SERDEPROPERTIES ('x' = 'y')")
    }
  }

  protected def testSetSerdePartition(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val spec = Map("a" -> "1", "b" -> "2")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    createTablePartition(catalog, spec, tableIdent)
    createTablePartition(catalog, Map("a" -> "1", "b" -> "3"), tableIdent)
    createTablePartition(catalog, Map("a" -> "2", "b" -> "2"), tableIdent)
    createTablePartition(catalog, Map("a" -> "2", "b" -> "3"), tableIdent)
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
        sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'whatever'")
      }
      val e2 = intercept[AnalysisException] {
        sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
          "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      }
      assert(e1.getMessage.contains("datasource"))
      assert(e2.getMessage.contains("datasource"))
    } else {
      sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'org.apache.jadoop'")
      assert(catalog.getPartition(tableIdent, spec).storage.serde == Some("org.apache.jadoop"))
      checkPartitionSerdeProps(Map.empty[String, String])
      sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(catalog.getPartition(tableIdent, spec).storage.serde == Some("org.apache.madoop"))
      checkPartitionSerdeProps(Map("k" -> "v", "kay" -> "vee"))
    }
    // set serde properties only
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) " +
        "SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      checkPartitionSerdeProps(Map("k" -> "vvv", "kay" -> "vee"))
    }
    // set things without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 PARTITION (a=1, b=2) SET SERDEPROPERTIES ('kay' = 'veee')")
      checkPartitionSerdeProps(Map("k" -> "vvv", "kay" -> "veee"))
    }
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist PARTITION (a=1, b=2) SET SERDEPROPERTIES ('x' = 'y')")
    }
  }
}

/**
 * The class contains tests for the `ALTER TABLE .. SET [SERDE|SERDEPROPERTIES]` command to check
 * V1 In-Memory table catalog.
 */
class AlterTableSetSerdeSuite extends AlterTableSetSerdeSuiteBase with CommandSuiteBase {

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      Utils.deleteRecursively(new File(spark.sessionState.conf.warehousePath))
      super.afterEach()
    }
  }

  protected override def generateTable(
    catalog: SessionCatalog,
    name: TableIdentifier,
    isDataSource: Boolean = true,
    partitionCols: Seq[String] = Seq("a", "b")): CatalogTable = {
    val storage =
      CatalogStorageFormat.empty.copy(locationUri = Some(catalog.defaultTablePath(name)))
    val metadata = new MetadataBuilder()
      .putString("key", "value")
      .build()
    val schema = new StructType()
      .add("col1", "int", nullable = true, metadata = metadata)
      .add("col2", "string")
    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = schema.copy(
        fields = schema.fields ++ partitionCols.map(StructField(_, IntegerType))),
      provider = Some("parquet"),
      partitionColumnNames = partitionCols,
      createTime = 0L,
      createVersion = org.apache.spark.SPARK_VERSION,
      tracksPartitionsInCatalog = true)
  }

  test("alter table: set serde (datasource table)") {
    testSetSerde(isDatasourceTable = true)
  }

  test("alter table: set serde partition (datasource table)") {
    testSetSerdePartition(isDatasourceTable = true)
  }
}
