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

import org.scalatest.BeforeAndAfterEach

import org.apache.commons.io.FileUtils

import org.apache.spark.sql._
import org.apache.spark.util.Utils
import org.apache.spark.sql.types._

/* Implicits */
import org.apache.spark.sql.hive.test.TestHive._

/**
 * Tests for persisting tables created though the data sources API into the metastore.
 */
class MetastoreDataSourcesSuite extends QueryTest with BeforeAndAfterEach {
  override def afterEach(): Unit = {
    reset()
  }

  val filePath = Utils.getSparkClassLoader.getResource("sample.json").getFile

  test ("persistent JSON table") {
    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      jsonFile(filePath).collect().toSeq)
  }

  test ("persistent JSON table with a user specified schema") {
    sql(
      s"""
        |CREATE TABLE jsonTable (
        |a string,
        |b String,
        |`c_!@(3)` int,
        |`<d>` Struct<`d!`:array<int>, `=`:array<struct<Dd2: boolean>>>)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    jsonFile(filePath).registerTempTable("expectedJsonTable")

    checkAnswer(
      sql("SELECT a, b, `c_!@(3)`, `<d>`.`d!`, `<d>`.`=` FROM jsonTable"),
      sql("SELECT a, b, `c_!@(3)`, `<d>`.`d!`, `<d>`.`=` FROM expectedJsonTable").collect().toSeq)
  }

  test ("persistent JSON table with a user specified schema with a subset of fields") {
    // This works because JSON objects are self-describing and JSONRelation can get needed
    // field values based on field names.
    sql(
      s"""
        |CREATE TABLE jsonTable (`<d>` Struct<`=`:array<struct<Dd2: boolean>>>, b String)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    val innerStruct = StructType(
      StructField("=", ArrayType(StructType(StructField("Dd2", BooleanType, true) :: Nil))) :: Nil)
    val expectedSchema = StructType(
      StructField("<d>", innerStruct, true) ::
      StructField("b", StringType, true) :: Nil)

    assert(expectedSchema == table("jsonTable").schema)

    jsonFile(filePath).registerTempTable("expectedJsonTable")

    checkAnswer(
      sql("SELECT b, `<d>`.`=` FROM jsonTable"),
      sql("SELECT b, `<d>`.`=` FROM expectedJsonTable").collect().toSeq)
  }

  test("resolve shortened provider names") {
    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      jsonFile(filePath).collect().toSeq)
  }

  test("drop table") {
    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      jsonFile(filePath).collect().toSeq)

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
      Row("a", "b"))

    FileUtils.deleteDirectory(tempDir)
    sparkContext.parallelize(("a1", "b1", "c1") :: Nil).toJSON.saveAsTextFile(tempDir.getCanonicalPath)

    // Schema is cached so the new column does not show. The updated values in existing columns
    // will show.
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      Row("a1", "b1"))

    refreshTable("jsonTable")

    // Check that the refresh worked
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      Row("a1", "b1", "c1"))
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
      Row("a", "b"))

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
      Row("a", "b", "c"))
    FileUtils.deleteDirectory(tempDir)
  }

  test("invalidate cache and reload") {
    sql(
      s"""
        |CREATE TABLE jsonTable (`c_!@(3)` int)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    jsonFile(filePath).registerTempTable("expectedJsonTable")

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

    // Discard the cached relation.
    invalidateTable("jsonTable")

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

    invalidateTable("jsonTable")
    val expectedSchema = StructType(StructField("c_!@(3)", IntegerType, true) :: Nil)

    assert(expectedSchema == table("jsonTable").schema)
  }

  test("SPARK-5286 Fail to drop an invalid table when using the data source API") {
    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path 'it is not a path at all!'
        |)
      """.stripMargin)

    sql("DROP TABLE jsonTable").collect().foreach(println)
  }
}
