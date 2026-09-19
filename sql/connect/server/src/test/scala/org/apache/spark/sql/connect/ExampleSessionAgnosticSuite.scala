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

package org.apache.spark.sql.connect

import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.classic
import org.apache.spark.sql.connector.catalog.InMemoryPartitionTableCatalog

/**
 * Example of the session-agnostic test pattern, demonstrating shared, classic-only, and
 * connect-only tests colocated in one file.
 *
 * {{{
 *   // Shared tests run against BOTH a classic and a connect session.
 *   class FooSuite extends sql.SessionQueryTest {
 *     test("shared") { checkAnswer(sql("SELECT 1"), Row(1)) }
 *   }
 *
 *   // Classic-only tests. Extend classic.SessionQueryTest (not FooSuite, which already runs
 *   // the shared tests on classic) so that classic-only APIs are visible.
 *   class FooClassicSuite extends classic.SessionQueryTest {
 *     test("classic only") { spark.sql("SELECT 1").queryExecution }
 *   }
 *
 *   // Connect variant: re-runs FooSuite's shared tests on connect, plus connect-only tests.
 *   class FooConnectSuite extends FooSuite with connect.SessionQueryTest {
 *     test("connect only") { assert(spark.range(1).count() == 1) }
 *   }
 * }}}
 *
 * In real suites the shared and classic-only parts are written in sql/core and only the connect
 * variant lives in sql/connect; they are colocated here so the whole pattern is on one page.
 */
class ExampleSessionAgnosticSuite extends sql.SessionQueryTest {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.sql.catalog.testcat", classOf[InMemoryPartitionTableCatalog].getName)
      .set("spark.sql.defaultCatalog", "testcat")

  test("Example classic/connect-agnostic testcase") {
    withTable("t") {
      spark.sql(s"CREATE TABLE t (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO t VALUES (1, 100)").collect()

      val df1 = spark.table("t")

      spark.sql(s"ALTER TABLE t ADD COLUMN new_column INT").collect()
      spark.sql(s"INSERT INTO t VALUES (2, 200, -1)").collect()

      val df2 = spark.table("t")
      val selfJoin = df1.join(df2, df1("id") === df2("id"))

      // diverging behaviour can be documented via `isConnect`
      if (isConnect) {
        // Connect re-resolves df1 with the new 3-column schema (id, salary, new_column).
        assert(
          selfJoin.columns.length == 6,
          s"Expected 6 columns (3 + 3) but got: ${selfJoin.columns.mkString(", ")}")
        checkAnswer(selfJoin, Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
      } else {
        // Classic: df1 keeps its original 2-column schema (id, salary).
        assert(
          selfJoin.columns.length == 5,
          s"Expected 5 columns (2 + 3) but got: ${selfJoin.columns.mkString(", ")}")
        checkAnswer(selfJoin, Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
      }
    }
  }

  test("testcase that uses withConf") {
    // since SQLConf is not part of the public API,
    // `withConf` can be used to temporarily change the RuntimeConfig.
    withConf("spark.sql.charAsVarchar" -> "true") {
      withTable("t") {
        spark.sql(s"CREATE TABLE t(col CHAR(5)) USING foo")
        checkAnswer(spark.sql(s"desc t").selectExpr("data_type"), Seq(Row("varchar(5)")))
      }
    }
  }
}

/**
 * Example of a classic-only suite. Extends [[classic.SessionQueryTest]], so `spark` is a
 * [[classic.SparkSession]] and classic-only APIs are visible. The shared tests already run on
 * classic via [[ExampleSessionAgnosticSuite]], so they are not repeated here.
 */
class ExampleClassicSuite extends classic.SessionQueryTest {

  test("classic-only testcase") {
    // `spark` is a classic.SparkSession, so classic-only APIs like queryExecution are visible.
    val df = spark.sql("SELECT 1")
    assert(df.queryExecution.analyzed.output.length == 1)
  }
}

/**
 * Connect variant of [[ExampleSessionAgnosticSuite]]: re-runs its shared tests against a connect
 * session and adds connect-only tests.
 */
class ExampleSessionAgnosticConnectSuite
    extends ExampleSessionAgnosticSuite
    with SessionQueryTest {

  test("connect-only testcase") {
    // Tests declared in the connect variant run only on connect (not classic).
    // Here `spark` is a connect SparkSession.
    assert(isConnect)
    assert(spark.range(1).count() == 1)
  }
}
