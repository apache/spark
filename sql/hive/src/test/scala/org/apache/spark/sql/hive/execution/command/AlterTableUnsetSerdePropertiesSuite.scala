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
 * The class contains tests for the `ALTER TABLE ... UNSET SERDEPROPERTIES` command
 * to check V1 Hive external table catalog.
 */
class AlterTableUnsetSerdePropertiesSuite extends v1.AlterTableUnsetSerdePropertiesSuiteBase
    with CommandSuiteBase {

  test("Hive external catalog - hiveformat table: alter table unset serde properties") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) " +
          s"PARTITIONED BY (a, b) " +
          s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
          s"STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat' " +
          s"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'")

      val tableIdent = TableIdentifier("tbl", Some("ns"))
      checkSerdeProps(tableIdent, None, Map.empty[String, String])

      // set serde properties
      sql(s"ALTER TABLE $t SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      checkSerdeProps(tableIdent, None, Map("k" -> "vvv", "kay" -> "vee"))

      // unset serde properties
      sql(s"ALTER TABLE $t UNSET SERDEPROPERTIES ('k', 'key_non_exist')")
      checkSerdeProps(tableIdent, None, Map("kay" -> "vee"))

      // table to alter does not exist
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE does_not_exist UNSET SERDEPROPERTIES ('x')")
      }
      checkErrorTableNotFound(e, "`does_not_exist`",
        ExpectedContext("does_not_exist", 12, 11 + "does_not_exist".length))
    }
  }

  test("Hive external catalog - hiveformat table: alter table unset partition serde properties") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) " +
          s"PARTITIONED BY (a, b) " +
          s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
          s"STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat' " +
          s"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'")
      sql(s"INSERT INTO $t PARTITION (a = 1, b = 2) SELECT 1, 'abc'")
      sql(s"INSERT INTO $t PARTITION (a = 1, b = 3) SELECT 2, 'def'")
      sql(s"INSERT INTO $t PARTITION (a = 2, b = 2) SELECT 3, 'ghi'")
      sql(s"INSERT INTO $t PARTITION (a = 2, b = 3) SELECT 4, 'jkl'")

      val tableIdent = TableIdentifier("tbl", Some("ns"))
      val spec = Map("a" -> "1", "b" -> "2")
      checkSerdeProps(tableIdent, Some(spec), Map.empty[String, String])

      // set partition serde properties
      sql(s"ALTER TABLE $t PARTITION (a = 1, b = 2) " +
        "SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      checkSerdeProps(tableIdent, Some(spec), Map("k" -> "vvv", "kay" -> "vee"))

      // unset serde properties
      sql(s"ALTER TABLE $t PARTITION (a = 1, b = 2) UNSET SERDEPROPERTIES ('k', 'key_non_exist')")
      checkSerdeProps(tableIdent, Some(spec), Map("kay" -> "vee"))

      // table to alter does not exist
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE does_not_exist UNSET SERDEPROPERTIES ('x')")
      }
      checkErrorTableNotFound(e, "`does_not_exist`",
        ExpectedContext("does_not_exist", 12, 11 + "does_not_exist".length))
    }
  }
}
