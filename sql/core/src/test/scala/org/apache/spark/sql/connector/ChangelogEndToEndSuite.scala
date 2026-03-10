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

import java.sql.Timestamp
import java.util

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * End-to-end tests for Change Data Capture (CDC) queries using
 * [[InMemoryChangelogCatalog]].
 */
class ChangelogEndToEndSuite extends QueryTest with SharedSparkSession {

  private val catalogName = "cdc_e2e"
  private val testTableName = "test_table"
  private val fullTableName = s"$catalogName.$testTableName"
  private val ident = Identifier.of(Array.empty, testTableName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      s"spark.sql.catalog.$catalogName",
      classOf[InMemoryChangelogCatalog].getName)
  }

  override def afterAll(): Unit = {
    spark.conf.unset(s"spark.sql.catalog.$catalogName")
    super.afterAll()
  }

  private def catalog: InMemoryChangelogCatalog = {
    spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val cat = catalog
    if (cat.tableExists(ident)) {
      cat.dropTable(ident)
    }
    cat.createTable(
      ident,
      Array(
        Column.create("id", LongType),
        Column.create("data", StringType)),
      Array.empty,
      new util.HashMap[String, String]())
    cat.clearChangeRows(ident)
  }

  private def makeChangeRow(
      id: Long,
      data: String,
      changeType: String,
      commitVersion: Long,
      commitTimestamp: Long): InternalRow = {
    InternalRow(
      id,
      UTF8String.fromString(data),
      UTF8String.fromString(changeType),
      commitVersion,
      commitTimestamp)
  }

  test("SQL CHANGES clause returns change data") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(1L, "a", "delete", 2L, 2000000L)))

    val df = sql(
      s"SELECT * FROM $fullTableName " +
        "CHANGES FROM VERSION 1 TO VERSION 2")
    checkAnswer(df, Seq(
      Row(1L, "a", "insert", 1L, new Timestamp(1000L)),
      Row(2L, "b", "insert", 1L, new Timestamp(1000L)),
      Row(1L, "a", "delete", 2L, new Timestamp(2000L))))
  }

  test("SQL CHANGES clause with column projection") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L)))

    val df = sql(
      s"SELECT id, _change_type FROM $fullTableName " +
        "CHANGES FROM VERSION 1")
    checkAnswer(df, Seq(
      Row(1L, "insert"),
      Row(2L, "insert")))
  }

  test("SQL CHANGES clause with filter") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(3L, "c", "insert", 2L, 2000000L)))

    val df = sql(
      s"SELECT id, data FROM $fullTableName " +
        "CHANGES FROM VERSION 1 TO VERSION 2 " +
        "WHERE _commit_version = 1")
    checkAnswer(df, Seq(
      Row(1L, "a"),
      Row(2L, "b")))
  }

  test("DataFrame API - changes() returns change data") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "delete", 2L, 2000000L)))

    val df = spark.read
      .option("startingVersion", "1")
      .option("endingVersion", "2")
      .changes(fullTableName)
    checkAnswer(df, Seq(
      Row(1L, "a", "insert", 1L, new Timestamp(1000L)),
      Row(2L, "b", "delete", 2L, new Timestamp(2000L))))
  }

  test("DataFrame API - changes() with projection and filter") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(1L, "a2", "insert", 2L, 2000000L)))

    val df = spark.read
      .option("startingVersion", "1")
      .changes(fullTableName)
      .filter("_commit_version = 2")
      .select("id", "data")
    checkAnswer(df, Seq(Row(1L, "a2")))
  }

  test("schema includes CDC metadata columns") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L)))

    val df = spark.read
      .option("startingVersion", "1")
      .changes(fullTableName)
    val colNames = df.schema.fieldNames
    assert(colNames === Array(
      "id", "data", "_change_type",
      "_commit_version", "_commit_timestamp"))
  }

  test("empty change data returns no rows") {
    val df = sql(
      s"SELECT * FROM $fullTableName " +
        "CHANGES FROM VERSION 1 TO VERSION 5")
    assert(df.collect().isEmpty)
    val colNames = df.schema.fieldNames
    assert(colNames.contains("_change_type"))
  }

  test("streaming CDC reads change data via STREAM CHANGES") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(1L, "a", "delete", 2L, 2000000L)))

    val streamDf = sql(
      s"SELECT * FROM STREAM $fullTableName CHANGES FROM VERSION 1")

    val queryName = "cdc_streaming_test"
    val query = streamDf.writeStream
      .format("memory")
      .queryName(queryName)
      .start()
    try {
      query.processAllAvailable()
      val result = spark.sql(s"SELECT * FROM $queryName")
      checkAnswer(result, Seq(
        Row(1L, "a", "insert", 1L, new Timestamp(1000L)),
        Row(2L, "b", "insert", 1L, new Timestamp(1000L)),
        Row(1L, "a", "delete", 2L, new Timestamp(2000L))))
    } finally {
      query.stop()
    }
  }

  test("streaming CDC reads change data via STREAM CHANGES FROM VERSION 2") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(1L, "a", "delete", 2L, 2000000L)))

    val streamDf = sql(
      s"SELECT * FROM STREAM $fullTableName CHANGES FROM VERSION 2")

    val queryName = "cdc_streaming_test_v2"
    val query = streamDf.writeStream
      .format("memory")
      .queryName(queryName)
      .start()
    try {
      query.processAllAvailable()
      val result = spark.sql(s"SELECT * FROM $queryName")
      checkAnswer(result, Seq(
        Row(1L, "a", "delete", 2L, new Timestamp(2000L))))
    } finally {
      query.stop()
    }
  }
}
