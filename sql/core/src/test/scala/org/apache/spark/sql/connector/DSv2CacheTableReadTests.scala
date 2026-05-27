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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Column, InMemoryTableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.types.IntegerType

/**
 * Shared CACHE TABLE impact on reads tests for DSv2 tables. Write operations ignore the
 * cache, so these tests verify how reads behave when a cached table is mutated by session
 * SQL or external catalog API calls:
 *
 *  - Scenario 1 (external write): cache pins the read, external write invisible until REFRESH.
 *  - Scenario 2 (session write then external write): session write rebuilds cache,
 *    subsequent external write invisible until REFRESH.
 *  - Scenario 3 (external schema change): cache pinned at original schema until REFRESH.
 *  - Scenario 4 (session schema change then external write): session ALTER rebuilds
 *    cache, subsequent external write invisible until REFRESH.
 *  - Scenario 5 (external drop and recreate table): query sees new empty table.
 *
 * Unlike the peer traits [[DSv2TempViewWithStoredPlanTests]] and
 * [[DSv2RepeatedTableAccessTests]], this trait does not include `cachingcat` (caching
 * connector) variants. When CACHE TABLE is active, Spark's CacheManager pins reads
 * regardless of the connector, and REFRESH TABLE calls both
 * [[org.apache.spark.sql.connector.catalog.TableCatalog#invalidateTable]] (clearing
 * any connector cache) and the CacheManager rebuild, so the observable behavior is the
 * same with either catalog. cachingcat variants can be added in a follow-up if needed.
 *
 * NOTE: All `session.sql(...)` calls append `.collect()` because Connect client DataFrames
 * are lazy and require an action to trigger execution. In classic mode `.collect()` on
 * DDL / DML is a no-op (these execute eagerly), so this is harmless.
 */
trait DSv2CacheTableReadTests extends DSv2ExternalMutationTestBase {

  private def assertTableCached(session: SparkSession, tableName: String): Unit =
    assert(session.catalog.isCached(tableName))

  test(s"${testPrefix}SPARK-54022: cached table pinned against external data write") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        session.table(testTable).cache()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100)))

        session.sql(s"REFRESH TABLE $testTable").collect()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test(s"${testPrefix}SPARK-54022: session write invalidates cache, " +
      "then external write invisible") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        session.table(testTable).cache()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100)))

        session.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100), Row(2, 200)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(3, 300))

        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100), Row(2, 200)))

        session.sql(s"REFRESH TABLE $testTable").collect()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      }
    }
  }

  test(s"${testPrefix}SPARK-54022: cached table pinned against external schema change") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        session.table(testTable).cache()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        catalog.alterTable(testIdent, addCol)
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100)))

        session.sql(s"REFRESH TABLE $testTable").collect()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test(s"${testPrefix}SPARK-54022: session schema change invalidates cache, " +
      "external write invisible") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        session.table(testTable).cache()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100)))

        session.sql(s"ALTER TABLE $testTable ADD COLUMN new_column INT").collect()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100, null)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100, null)))

        session.sql(s"REFRESH TABLE $testTable").collect()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test(s"${testPrefix}SPARK-54022: cached table after external drop and " +
      "recreate sees empty table") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        session.table(testTable).cache()
        assertTableCached(session, testTable)
        checkRows(session.table(testTable), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val originalTableId = catalog.loadTable(testIdent).id

        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())

        val newTableId = catalog.loadTable(testIdent).id
        assert(originalTableId != newTableId)

        val result = session.table(testTable)
        assert(result.schema.fieldNames.toSeq == Seq("id", "salary"))
        checkRows(result, Seq.empty)

        // External drop+recreate produces a new table identity, so the prior cache entry
        // is unreachable via name lookup (unlike external write/schema change where the
        // cache stays pinned).
        assert(!session.catalog.isCached(testTable))

        session.sql(s"REFRESH TABLE $testTable").collect()
        checkRows(session.table(testTable), Seq.empty)
      }
    }
  }
}
