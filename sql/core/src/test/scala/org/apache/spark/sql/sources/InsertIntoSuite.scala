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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util
import org.apache.spark.util.Utils

class InsertIntoSuite extends DataSourceTest with BeforeAndAfterAll {

  import caseInsensisitiveContext._

  var path: File = null

  override def beforeAll: Unit = {
    path = util.getTempFilePath("jsonCTAS").getCanonicalFile
    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    jsonRDD(rdd).registerTempTable("jt")
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable (a int, b string)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |)
      """.stripMargin)
  }

  override def afterAll: Unit = {
    dropTempTable("jsonTable")
    dropTempTable("jt")
    if (path.exists()) Utils.deleteRecursively(path)
  }

  test("Simple INSERT OVERWRITE a JSONRelation") {
    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )
  }

  test("INSERT OVERWRITE a JSONRelation multiple times") {
    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)

    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)

    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )
  }

  test("INSERT INTO not supported for JSONRelation for now") {
    intercept[RuntimeException]{
      sql(
        s"""
        |INSERT INTO TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)
    }
  }
}
