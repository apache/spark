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

package org.apache.spark.sql.connector

import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTable

/**
 * Regression tests for TRUNCATE TABLE against `InMemoryRowLevelOperationTableCatalog`.
 *
 * SPARK-56995 made the catalog's read-path `loadTable` return a snapshot copy per call so that
 * version-aware `Table.equals` can detect stale cached relations. TRUNCATE TABLE resolves its
 * target through that read path and mutates the resolved instance, so without redirection the
 * mutation would land on a disposable snapshot and the live table would silently keep its rows
 * (originally observed as AutoCdcScd1FullRefreshSuite failures in the pipelines module).
 */
class RowLevelOperationTruncateTableSuite extends RowLevelOperationSuiteBase {

  test("SPARK-56995: TRUNCATE TABLE mutates the live catalog table, not only the snapshot") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }""".stripMargin)

    val versionBefore = table.version().toLong

    sql(s"TRUNCATE TABLE $tableNameAsString")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
    assert(table.data.isEmpty, "the live table must be truncated")
    assert(table.version().toLong === versionBefore + 1,
      "TRUNCATE must bump the live table version so pre-truncate snapshots become stale")
  }

  test("SPARK-56995: snapshots loaded before TRUNCATE stay pinned and become stale") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }""".stripMargin)

    val snapshot = catalog.loadTable(ident).asInstanceOf[InMemoryRowLevelOperationTable]

    sql(s"TRUNCATE TABLE $tableNameAsString")

    // Pin-at-load semantics: the snapshot keeps the data frozen at load time.
    assert(snapshot.data.nonEmpty, "a pre-truncate snapshot must keep its pinned data")
    // Version-aware equality detects that the snapshot is stale after the truncate.
    assert(snapshot !== catalog.loadTable(ident),
      "a pre-truncate snapshot must compare unequal to a fresh load")
  }
}
