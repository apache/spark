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

import java.util

import scala.reflect.ClassTag

import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{BufferedRows, CatalogV2Util, Identifier, InMemoryBaseTable, TableCatalog, TableWritePrivilege}

/**
 * Base trait for DSv2 tests that involve external table mutations (writes, schema changes,
 * drop/recreate) via the catalog API.
 *
 * Provides abstract methods so that the same test scenarios can run in both classic mode
 * (where the test session IS the server session) and Connect mode (where the test session
 * is a Connect client and catalog access requires the server session).
 *
 * Concrete suites override the abstract methods and mix in the test trait
 * [[DSv2TempViewWithStoredPlanTests]].
 */
trait DSv2ExternalMutationTestBase extends QueryTest {

  /** Prefix for test names, e.g. "[classic] " or "[connect] ". */
  protected def testPrefix: String

  /** Execute a test body with a session. */
  protected def withTestSession(fn: SparkSession => Unit): Unit

  /**
   * Assert that a DataFrame's rows match the expected rows (order-agnostic).
   */
  protected def checkRows(df: => DataFrame, expected: Seq[Row]): Unit

  /**
   * Get a [[TableCatalog]] by name from the underlying session.
   */
  protected def getTableCatalog[C <: TableCatalog: ClassTag](
      session: SparkSession,
      catalogName: String): C

  /**
   * Cleanup wrapper: drop views and the table after the test body, even on failure.
   * Classic: delegates to `withTable` + manual view drops.
   * Connect: `session.sql("DROP ...")` in a finally block.
   */
  protected def withTestTableAndViews(
      session: SparkSession,
      table: String,
      views: Seq[String] = Seq.empty)(fn: => Unit): Unit

  /** Appends a row to a DSv2 table via the catalog API, bypassing the session. */
  protected def externalAppend(
      catalog: TableCatalog,
      ident: Identifier,
      row: InternalRow): Unit = {
    val extTable = catalog
      .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
      .asInstanceOf[InMemoryBaseTable]
    val schema = CatalogV2Util.v2ColumnsToStructType(extTable.columns())
    extTable.withData(Array(new BufferedRows(Seq.empty, schema).withRow(row)))
  }
}
