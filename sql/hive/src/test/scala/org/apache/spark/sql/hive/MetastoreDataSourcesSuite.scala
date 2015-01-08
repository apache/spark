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

package org.apache.spark.sql.hive

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.BeforeAndAfterEach

/* Implicits */
import org.apache.spark.sql.hive.test.TestHive._

/**
 * Tests for persisting tables created though the data sources API into the metastore.
 */
class MetastoreDataSourcesSuite extends QueryTest with BeforeAndAfterEach {
  override def afterEach(): Unit = {
    reset()
  }

  test ("persistent JSON table") {
    sql(
      """
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path 'src/test/resources/data/files/sample.json'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      jsonFile("src/test/resources/data/files/sample.json").collect().toSeq)

  }

  test ("persistent JSON table with a user specified schema") {
    sql(
      """
        |CREATE TABLE jsonTable (a string, b String)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path 'src/test/resources/data/files/sample.json'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      jsonFile("src/test/resources/data/files/sample.json").collect().toSeq)

  }

  test ("persistent JSON table with a user specified schema with a subset of fields") {
    // This works because JSON objects are self-describing and JSONRelation can get needed
    // field values based on field names.
    sql(
      """
        |CREATE TABLE jsonTable (b String)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path 'src/test/resources/data/files/sample.json'
        |)
      """.stripMargin)

    val expectedSchema = StructType(StructField("b", StringType, true) :: Nil)

    assert(expectedSchema == table("jsonTable").schema)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      jsonFile("src/test/resources/data/files/sample.json").collect().map(
        r => Seq(r.getString(1))).toSeq)

  }

  test("resolve shortened provider names") {
    sql(
      """
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path 'src/test/resources/data/files/sample.json'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      jsonFile("src/test/resources/data/files/sample.json").collect().toSeq)
  }

  test("drop table") {
    sql(
      """
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path 'src/test/resources/data/files/sample.json'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      jsonFile("src/test/resources/data/files/sample.json").collect().toSeq)

    sql("DROP TABLE jsonTable")

    intercept[Exception] {
      sql("SELECT * FROM jsonTable").collect()
    }
  }

  test("check change without refresh") {
    val tempDir = File.createTempFile("sparksql", "json")
    tempDir.delete()
    sparkContext.parallelize(("a", "b") :: Nil).toJSON.saveAsTextFile(tempDir.getCanonicalPath)

    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '${tempDir.getCanonicalPath}'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      ("a", "b") :: Nil)

    FileUtils.deleteDirectory(tempDir)
    sparkContext.parallelize(("a1", "b1", "c1") :: Nil).toJSON.saveAsTextFile(tempDir.getCanonicalPath)

    // Schema is cached so the new column does not show. The updated values in existing columns
    // will show.
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      ("a1", "b1") :: Nil)

    refreshTable("jsonTable")

    // Check that the refresh worked
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      ("a1", "b1", "c1") :: Nil)
    FileUtils.deleteDirectory(tempDir)
  }

  test("drop, change, recreate") {
    val tempDir = File.createTempFile("sparksql", "json")
    tempDir.delete()
    sparkContext.parallelize(("a", "b") :: Nil).toJSON.saveAsTextFile(tempDir.getCanonicalPath)

    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '${tempDir.getCanonicalPath}'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      ("a", "b") :: Nil)

    FileUtils.deleteDirectory(tempDir)
    sparkContext.parallelize(("a", "b", "c") :: Nil).toJSON.saveAsTextFile(tempDir.getCanonicalPath)

    sql("DROP TABLE jsonTable")

    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '${tempDir.getCanonicalPath}'
        |)
      """.stripMargin)

    // New table should reflect new schema.
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      ("a", "b", "c") :: Nil)
    FileUtils.deleteDirectory(tempDir)
  }
}
