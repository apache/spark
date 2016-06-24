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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.test.SharedSQLContext

class DDLSemanticAnalysisSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  test("create table - duplicate column names in the table definition") {
    val query = "CREATE TABLE default.tab1 (key INT, key STRING)"
    val e = intercept[AnalysisException] { sql(query) }
    assert(e.getMessage.contains(
      "Found duplicate column(s) in table definition of `default`.`tab1`: `key`"))
  }

  test("create table - column repeated in partitioning columns") {
    val query = "CREATE TABLE tab1 (key INT, value STRING) PARTITIONED BY (key INT, hr STRING)"
    val e = intercept[AnalysisException] { sql(query) }
    assert(e.getMessage.contains(
      "Operation not allowed: Partition columns may not be specified in the schema: `key`"))
  }

  test("duplicate column names in bucketBy") {
    import testImplicits._
    val df = (0 until 5).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("tab123") {
      val e = intercept[AnalysisException] {
        df.write.format("json").bucketBy(8, "j", "j").saveAsTable("tab123")
      }.getMessage
      assert(e.contains("Found duplicate column(s) in Bucketing: `j`"))
    }
  }

  test("duplicate column names in sortBy") {
    import testImplicits._
    val df = (0 until 5).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("tab123") {
      val e = intercept[AnalysisException] {
        df.write.format("json").bucketBy(8, "j", "k").sortBy("k", "k").saveAsTable("tab123")
      }.getMessage
      assert(e.contains("Found duplicate column(s) in Sorting: `k`"))
    }
  }

  test("duplicate column names in partitionBy") {
    import testImplicits._
    val df = (0 until 5).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("tab123") {
      val e = intercept[AnalysisException] {
        df.write.format("json").partitionBy("i", "i").saveAsTable("tab123")
      }.getMessage
      assert(e.contains("Found duplicate column(s) in Partition: `i`"))
    }
  }

}
