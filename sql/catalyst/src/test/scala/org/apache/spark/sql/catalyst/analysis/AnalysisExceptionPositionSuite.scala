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
    verifyViewPosition("ALTER VIEW unknown SET TBLPROPERTIES ('k'='v')", "unknown")
    verifyViewPosition("ALTER VIEW unknown UNSET TBLPROPERTIES ('k')", "unknown")
    verifyViewPosition("ALTER VIEW unknown AS SELECT 1", "unknown")
  }

  test("SPARK-34057: UnresolvedTableOrView should retain sql text position") {
    verifyTableOrPermanentViewPosition("ANALYZE TABLE unknown COMPUTE STATISTICS", "unknown")
    verifyTableOrViewPosition("ANALYZE TABLE unknown COMPUTE STATISTICS FOR COLUMNS col", "unknown")
    verifyTableOrViewPosition("ANALYZE TABLE unknown COMPUTE STATISTICS FOR ALL COLUMNS", "unknown")
    verifyTableOrPermanentViewPosition("SHOW CREATE TABLE unknown", "unknown")
    verifyTableOrViewPosition("REFRESH TABLE unknown", "unknown")
    verifyTableOrViewPosition("SHOW COLUMNS FROM unknown", "unknown")
    // Special case where namespace is prepended to the table name.
    assertAnalysisErrorCondition(
      parsePlan("SHOW COLUMNS FROM unknown IN db"),
      "TABLE_OR_VIEW_NOT_FOUND",
      Map("relationName" -> "`db`.`unknown`"),
      Array(ExpectedContext("unknown", 18, 24))
    )
    verifyTableOrViewPosition("ALTER TABLE unknown RENAME TO t", "unknown")
    verifyTableOrViewPosition("ALTER VIEW unknown RENAME TO v", "unknown")
  }

  test("SPARK-34139: UnresolvedRelation should retain sql text position") {
    verifyTableOrViewPosition("CACHE TABLE unknown", "unknown")
    verifyTableOrViewPosition("UNCACHE TABLE unknown", "unknown")
    verifyTableOrViewPosition("DELETE FROM unknown", "unknown")
    verifyTableOrViewPosition("UPDATE unknown SET name='abc'", "unknown")
    verifyTableOrViewPosition(
      "MERGE INTO unknownTarget AS target USING TaBlE AS source " +
        "ON target.col = source.col WHEN MATCHED THEN DELETE",
      "unknownTarget")
    verifyTableOrViewPosition(
      "MERGE INTO TaBlE AS target USING unknownSource AS source " +
        "ON target.col = source.col WHEN MATCHED THEN DELETE",
      "unknownSource")
    verifyTablePosition("INSERT INTO TABLE unknown SELECT 1", "unknown")
    verifyTablePosition("INSERT OVERWRITE TABLE unknown VALUES (1, 'a')", "unknown")
  }

  private def verifyTablePosition(sql: String, table: String): Unit = {
    verifyPosition(sql, table)
  }

  private def verifyViewPosition(sql: String, table: String): Unit = {
    verifyPosition(sql, table)
  }

  private def verifyTableOrViewPosition(sql: String, table: String): Unit = {
    verifyPosition(sql, table)
  }

  private def verifyTableOrPermanentViewPosition(sql: String, table: String): Unit = {
    verifyPosition(sql, table)
  }

  private def verifyPosition(sql: String, table: String): Unit = {
    val startPos = sql.indexOf(table)
    assert(startPos != -1)
    assertAnalysisErrorCondition(
      parsePlan(sql),
      "TABLE_OR_VIEW_NOT_FOUND",
      Map("relationName" -> s"`$table`"),
      Array(ExpectedContext(table, startPos, startPos + table.length - 1))
    )
  }
}
