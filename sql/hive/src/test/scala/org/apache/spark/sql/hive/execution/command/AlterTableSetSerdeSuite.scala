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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.v1

/**
 * The class contains tests for the `ALTER TABLE .. SET [SERDE|SERDEPROPERTIES]` command to check
 * V1 Hive external table catalog.
 */
class AlterTableSetSerdeSuite extends v1.AlterTableSetSerdeSuiteBase with CommandSuiteBase {

  test("Hive external catalog - hiveformat table: alter table set serde") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) " +
        s"PARTITIONED BY (a, b) " +
        s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
        s"STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat' " +
        s"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'")

      val tableIdent = TableIdentifier("tbl", Some("ns"))
      val expectedSerde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      assert(sessionCatalog.getTableMetadata(tableIdent).storage.serde == Some(expectedSerde))
      checkSerdeProps(tableIdent, Map.empty[String, String])

      // set table serde and/or properties (should success on hiveformat tables)
      val newSerde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      sql(s"ALTER TABLE $t SET SERDE '$newSerde'")
      assert(sessionCatalog.getTableMetadata(tableIdent).storage.serde == Some(newSerde))
      checkSerdeProps(tableIdent, Map.empty[String, String])
      val serde2 = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"
      sql(s"ALTER TABLE $t SET SERDE '$serde2' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(sessionCatalog.getTableMetadata(tableIdent).storage.serde == Some(serde2))
      checkSerdeProps(tableIdent, Map("k" -> "v", "kay" -> "vee"))

      // set serde properties only
      sql(s"ALTER TABLE $t SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      checkSerdeProps(tableIdent, Map("k" -> "vvv", "kay" -> "vee"))

      // set things without explicitly specifying database
      sessionCatalog.setCurrentDatabase("ns")
      sql("ALTER TABLE tbl SET SERDEPROPERTIES ('kay' = 'veee')")
      checkSerdeProps(tableIdent, Map("k" -> "vvv", "kay" -> "veee"))

      // table to alter does not exist
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE does_not_exist SET SERDEPROPERTIES ('x' = 'y')")
      }
      checkErrorTableNotFound(e, "`does_not_exist`",
        ExpectedContext("does_not_exist", 12, 11 + "does_not_exist".length))
    }
  }

  test("Hive external catalog - hiveformat table: alter table set serde partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) " +
        s"PARTITIONED BY (a, b) " +
        s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
        s"STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat' " +
        s"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'")
      sql(s"INSERT INTO $t PARTITION (a = '1', b = '2') SELECT 1, 'abc'")
      sql(s"INSERT INTO $t PARTITION (a = '1', b = '3') SELECT 2, 'def'")
      sql(s"INSERT INTO $t PARTITION (a = '2', b = '2') SELECT 3, 'ghi'")
      sql(s"INSERT INTO $t PARTITION (a = '2', b = '3') SELECT 4, 'jkl'")

      val tableIdent = TableIdentifier("tbl", Some("ns"))
      val spec = Map("a" -> "1", "b" -> "2")
      val expectedSerde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      assert(sessionCatalog.getPartition(tableIdent, spec).storage.serde == Some(expectedSerde))
      checkPartitionSerdeProps(tableIdent, spec, Map.empty[String, String])

      // set table serde and/or properties (should success on hiveformat tables)
      sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDE 'org.apache.jadoop'")
      assert(sessionCatalog.getPartition(tableIdent, spec).storage.serde ==
        Some("org.apache.jadoop"))
      checkPartitionSerdeProps(tableIdent, spec, Map.empty[String, String])
      sql(s"ALTER TABLE $t PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(sessionCatalog.getPartition(tableIdent, spec).storage.serde ==
        Some("org.apache.madoop"))
      checkPartitionSerdeProps(tableIdent, spec, Map("k" -> "v", "kay" -> "vee"))

      // set serde properties only
      sql(s"ALTER TABLE $t PARTITION (a=1, b=2) " +
        "SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      checkPartitionSerdeProps(tableIdent, spec, Map("k" -> "vvv", "kay" -> "vee"))

      // set things without explicitly specifying database
      sessionCatalog.setCurrentDatabase("ns")
      sql(s"ALTER TABLE tbl PARTITION (a=1, b=2) SET SERDEPROPERTIES ('kay' = 'veee')")
      checkPartitionSerdeProps(tableIdent, spec, Map("k" -> "vvv", "kay" -> "veee"))

      // table to alter does not exist
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE does_not_exist PARTITION (a=1, b=2) SET SERDEPROPERTIES ('x' = 'y')")
      }
      checkErrorTableNotFound(e, "`does_not_exist`",
        ExpectedContext("does_not_exist", 12, 11 + "does_not_exist".length))
    }
  }
}
