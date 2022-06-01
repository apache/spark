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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * This base suite contains unified tests for the `SHOW TBLPROPERTIES` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.ShowTblPropertiesSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.ShowTblPropertiesSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowTblPropertiesSuite`
 *     - V1 Hive External catalog:
*        `org.apache.spark.sql.hive.execution.command.ShowTblPropertiesSuite`
 */
trait ShowTblPropertiesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW TBLPROPERTIES"

  test("SHOW TBLPROPERTIES BASIC") {
    withNamespaceAndTable("ns1", "tbl") { tbl =>
      val user = "andrew"
      val status = "new"
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        s"TBLPROPERTIES ('user'='$user', 'status'='$status', 'password' = 'password')")
      val properties = sql(s"SHOW TBLPROPERTIES $tbl")
        .filter("key != 'transient_lastDdlTime'")
        .filter("key != 'option.serialization.format'")
      val schema = new StructType()
        .add("key", StringType, nullable = false)
        .add("value", StringType, nullable = false)
      val expected = Seq(
        Row("password", "*********(redacted)"),
        Row("status", status),
        Row("user", user))

      assert(properties.schema === schema)
      checkAnswer(properties, expected)
    }
  }

  test("SHOW TBLPROPERTIES(KEY)") {
    withNamespaceAndTable("ns1", "tbl") { tbl =>
      val user = "andrew"
      val status = "new"
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        s"TBLPROPERTIES ('user'='$user', 'status'='$status')")

      val properties = sql(s"SHOW TBLPROPERTIES $tbl ('status')")
      val expected = Seq(Row("status", status))
      checkAnswer(properties, expected)
    }
  }

  test("SHOW TBLPROPERTIES WITH TABLE NOT EXIST") {
    val message = intercept[AnalysisException] {
      sql("SHOW TBLPROPERTIES BADTABLE")
    }.getMessage
    assert(message.contains("Table or view not found: BADTABLE"))
  }

  test("SHOW TBLPROPERTIES(KEY) KEY NOT FOUND") {
    withNamespaceAndTable("ns1", "tbl") { tbl =>
      val nonExistingKey = "nonExistingKey"
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        s"TBLPROPERTIES ('user'='andrew', 'status'='new')")

      val res = sql(s"SHOW TBLPROPERTIES $tbl ('$nonExistingKey')").collect()
      assert(res.length == 1)
      assert(res.head.getString(0) == nonExistingKey)
      assert(res.head.getString(1).contains(s"does not have property: $nonExistingKey"))
    }
  }

  test("KEEP THE LEGACY OUTPUT SCHEMA") {
    Seq(true, false).foreach { keepLegacySchema =>
      withSQLConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA.key -> keepLegacySchema.toString) {
        withNamespaceAndTable("ns1", "tbl") { tbl =>
          spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
            "TBLPROPERTIES ('user'='spark', 'status'='new')")

          val properties = sql(s"SHOW TBLPROPERTIES $tbl ('status')")
          val schema = properties.schema.fieldNames.toSeq
          if (keepLegacySchema) {
            assert(schema === Seq("value"))
            checkAnswer(properties, Seq(Row("new")))
          } else {
            assert(schema === Seq("key", "value"))
            checkAnswer(properties, Seq(Row("status", "new")))
          }
        }
      }
    }
  }
}
