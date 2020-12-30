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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan

class AnalysisExceptionPositionSuite extends AnalysisTest {
  test("SPARK-33886: UnresolvedTable should retain sql text position") {
    verifyTablePosition("MSCK REPAIR TABLE unknown", "unknown")
    verifyTablePosition("LOAD DATA LOCAL INPATH 'filepath' INTO TABLE unknown", "unknown")
    verifyTablePosition("TRUNCATE TABLE unknown", "unknown")
    verifyTablePosition("SHOW PARTITIONS unknown", "unknown")
    verifyTablePosition("ALTER TABLE unknown RECOVER PARTITIONS", "unknown")
    verifyTablePosition("ALTER TABLE unknown ADD PARTITION (p=1)", "unknown")
    verifyTablePosition("ALTER TABLE unknown PARTITION (p=1) RENAME TO PARTITION (p=2)", "unknown")
    verifyTablePosition("ALTER TABLE unknown DROP PARTITION (p=1)", "unknown")
    verifyTablePosition("ALTER TABLE unknown SET SERDEPROPERTIES ('a'='b')", "unknown")
    verifyTablePosition("COMMENT ON TABLE unknown IS 'hello'", "unknown")
  }

  test("SPARK-33918: UnresolvedView should retain sql text position") {
    verifyViewPosition("DROP VIEW unknown", "unknown")
    verifyViewPosition("ALTER VIEW unknown SET TBLPROPERTIES ('k'='v')", "unknown")
    verifyViewPosition("ALTER VIEW unknown UNSET TBLPROPERTIES ('k')", "unknown")
    verifyViewPosition("ALTER VIEW unknown AS SELECT 1", "unknown")
  }

  private def verifyTablePosition(sql: String, table: String): Unit = {
    verifyPosition(sql, table, "Table")
  }

  private def verifyViewPosition(sql: String, table: String): Unit = {
    verifyPosition(sql, table, "View")
  }

  private def verifyPosition(sql: String, table: String, msgPrefix: String): Unit = {
    val expectedPos = sql.indexOf(table)
    assert(expectedPos != -1)
    assertAnalysisError(
      parsePlan(sql),
      Seq(s"$msgPrefix not found: $table; line 1 pos $expectedPos"))
  }
}
