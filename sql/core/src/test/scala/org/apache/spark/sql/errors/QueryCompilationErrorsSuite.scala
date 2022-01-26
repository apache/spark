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

package org.apache.spark.sql.errors

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.connector.{DatasourceV2SQLBase, FakeV2Provider, InMemoryTableSessionCatalog, InMemoryTableWithV1Fallback, InMemoryV1Provider, TestV2SessionCatalogBase, V1FallbackTableCatalog}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, InMemoryTable, Table}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SharedSparkSession

abstract class QueryCompilationErrorsSuiteBase extends QueryTest with SharedSparkSession {
  protected val v2Format: String
  protected val catalogAndNamespace: String

  /** Check that the results in `tableName` match the `expected` DataFrame. */
  protected def verifyTable(tableName: String, expected: DataFrame): Unit

  private def withTableAndData(tableName: String)(testFn: String => Unit): Unit = {
    withTable(tableName) {
      val viewName = "tmp_view"
      val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
      df.createOrReplaceTempView(viewName)
      withTempView(viewName) {
        testFn(viewName)
      }
    }
  }

  test("UNSUPPORTED_FEATURE: IF PARTITION NOT EXISTS not supported by INSERT") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTableAndData(t1) { view =>
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")

      val exc = intercept[AnalysisException] {
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id = 1) IF NOT EXISTS SELECT * FROM $view")
      }

      verifyTable(t1, spark.emptyDataFrame)
      assert(exc.getMessage.contains("The feature is not supported: " +
        "IF NOT EXISTS for the table"))
      assert(exc.getMessage.contains(t1))
      assert(exc.getErrorClass === "UNSUPPORTED_FEATURE")
      assert(exc.getSqlState === "0A000")
    }
  }
}

class QueryCompilationErrorsDSv2Suite
  extends QueryCompilationErrorsSuiteBase
  with DatasourceV2SQLBase {

  override protected val v2Format = classOf[FakeV2Provider].getName
  override protected val catalogAndNamespace = "testcat.ns1.ns2."

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }
}

trait SessionCatalogTestBase[T <: Table, Catalog <: TestV2SessionCatalogBase[T]]
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfter {

  protected def catalog(name: String): CatalogPlugin =
    spark.sessionState.catalogManager.catalog(name)
  protected val v2Format: String = classOf[FakeV2Provider].getName
  protected val catalogClassName: String = classOf[InMemoryTableSessionCatalog].getName

  before {
    spark.conf.set(V2_SESSION_CATALOG_IMPLEMENTATION.key, catalogClassName)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    catalog(SESSION_CATALOG_NAME).asInstanceOf[Catalog].clearTables()
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
  }
}

class QueryCompilationErrorsDSv2SessionCatalogSuite
  extends QueryCompilationErrorsSuiteBase
  with SessionCatalogTestBase[InMemoryTable, InMemoryTableSessionCatalog] {

  override protected val catalogAndNamespace = ""

  override protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
    checkAnswer(sql(s"SELECT * FROM $tableName"), expected)
    checkAnswer(sql(s"SELECT * FROM default.$tableName"), expected)
    checkAnswer(sql(s"TABLE $tableName"), expected)
  }
}

class QueryCompilationErrorsV1WriteFallbackSuite
  extends QueryCompilationErrorsSuiteBase
  with SessionCatalogTestBase[InMemoryTableWithV1Fallback, V1FallbackTableCatalog] {

  override protected val v2Format = classOf[InMemoryV1Provider].getName
  override protected val catalogClassName: String = classOf[V1FallbackTableCatalog].getName
  override protected val catalogAndNamespace: String = ""

  override protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(InMemoryV1Provider.getTableData(spark, s"default.$tableName"), expected)
  }
}
