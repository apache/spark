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

package org.apache.spark.sql.sources

import java.io.File

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.util
import org.apache.spark.sql.types.{StringType, StructType, StructField}
import org.apache.spark.util.Utils

class CreateTableAsSelectSuite extends DataSourceTest with BeforeAndAfterAll {

  import caseInsensisitiveContext._

  var path: File = null

  override def beforeAll(): Unit = {
    path = util.getTempFilePath("jsonCTAS").getCanonicalFile
    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    jsonRDD(rdd).registerTempTable("jt")
  }

  override def afterAll(): Unit = {
    dropTempTable("jt")
  }

  after {
    if (path.exists()) Utils.deleteRecursively(path)
  }

  test("CTAS") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt").collect())

    dropTempTable("jsonTable")
  }

  test("CTAS with IF NOT EXISTS") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT b FROM jt
      """.stripMargin)

    sql(
      s"""
        |CREATE TEMPORARY TABLE IF NOT EXISTS jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)

    // jsonTable should only have a single column.
    val expectedSchema = StructType(StructField("b", StringType, true) :: Nil)
    assert(expectedSchema === table("jsonTable").schema)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      sql("SELECT b FROM jt").collect())

    // This statement does not have IF NOT EXISTS, so we will overwrite the table.
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)

    assert(table("jt").schema === table("jsonTable").schema)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt").collect())

    dropTempTable("jsonTable")
  }

  test("create a table, drop it and create another one with the same name") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt").collect())

    dropTempTable("jsonTable")

    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a * 4 FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      sql("SELECT a * 4 FROM jt").collect())

    dropTempTable("jsonTable")
  }

  test("a CTAS statement with column definitions is not allowed") {
    intercept[DDLException]{
      sql(
        s"""
        |CREATE TEMPORARY TABLE jsonTable (a int, b string)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)
    }
  }
}
