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

package org.apache.spark.sql.jdbc.v2

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

@DockerTest
trait V2JDBCTest extends SharedSparkSession {
  val catalogName: String
  // dialect specific update column type test
  def testUpdateColumnType(tbl: String): Unit

  def testUpdateColumnNullability(tbl: String): Unit = {
    sql(s"CREATE TABLE $catalogName.alt_table (ID STRING NOT NULL) USING _")
    var t = spark.table(s"$catalogName.alt_table")
    // nullable is true in the expecteSchema because Spark always sets nullable to true
    // regardless of the JDBC metadata https://github.com/apache/spark/pull/18445
    var expectedSchema = new StructType().add("ID", StringType, nullable = true)
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $catalogName.alt_table ALTER COLUMN ID DROP NOT NULL")
    t = spark.table(s"$catalogName.alt_table")
    expectedSchema = new StructType().add("ID", StringType, nullable = true)
    assert(t.schema === expectedSchema)
    // Update nullability of not existing column
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.alt_table ALTER COLUMN bad_column DROP NOT NULL")
    }.getMessage
    assert(msg.contains("Cannot update missing field bad_column"))
  }

  test("SPARK-33034: ALTER TABLE ... add new columns") {
    withTable(s"$catalogName.alt_table") {
      sql(s"CREATE TABLE $catalogName.alt_table (ID STRING) USING _")
      var t = spark.table(s"$catalogName.alt_table")
      var expectedSchema = new StructType().add("ID", StringType)
      assert(t.schema === expectedSchema)
      sql(s"ALTER TABLE $catalogName.alt_table ADD COLUMNS (C1 STRING, C2 STRING)")
      t = spark.table(s"$catalogName.alt_table")
      expectedSchema = expectedSchema.add("C1", StringType).add("C2", StringType)
      assert(t.schema === expectedSchema)
      sql(s"ALTER TABLE $catalogName.alt_table ADD COLUMNS (C3 STRING)")
      t = spark.table(s"$catalogName.alt_table")
      expectedSchema = expectedSchema.add("C3", StringType)
      assert(t.schema === expectedSchema)
      // Add already existing column
      val msg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalogName.alt_table ADD COLUMNS (C3 DOUBLE)")
      }.getMessage
      assert(msg.contains("Cannot add column, because C3 already exists"))
    }
    // Add a column to not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table ADD COLUMNS (C4 STRING)")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("SPARK-33034: ALTER TABLE ... drop column") {
    withTable(s"$catalogName.alt_table") {
      sql(s"CREATE TABLE $catalogName.alt_table (C1 INTEGER, C2 STRING, c3 INTEGER) USING _")
      sql(s"ALTER TABLE $catalogName.alt_table DROP COLUMN C1")
      sql(s"ALTER TABLE $catalogName.alt_table DROP COLUMN c3")
      val t = spark.table(s"$catalogName.alt_table")
      val expectedSchema = new StructType().add("C2", StringType)
      assert(t.schema === expectedSchema)
      // Drop not existing column
      val msg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalogName.alt_table DROP COLUMN bad_column")
      }.getMessage
      assert(msg.contains("Cannot delete missing field bad_column in alt_table schema"))
    }
    // Drop a column from a not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table DROP COLUMN C1")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("SPARK-33034: ALTER TABLE ... update column type") {
    withTable(s"$catalogName.alt_table") {
      testUpdateColumnType(s"$catalogName.alt_table")
      // Update not existing column
      val msg2 = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalogName.alt_table ALTER COLUMN bad_column TYPE DOUBLE")
      }.getMessage
      assert(msg2.contains("Cannot update missing field bad_column"))
    }
    // Update column type in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table ALTER COLUMN id TYPE DOUBLE")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("SPARK-33034: ALTER TABLE ... update column nullability") {
    withTable(s"$catalogName.alt_table") {
      testUpdateColumnNullability(s"$catalogName.alt_table")
    }
    // Update column nullability in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table ALTER COLUMN ID DROP NOT NULL")
    }.getMessage
    assert(msg.contains("Table not found"))
  }
}
