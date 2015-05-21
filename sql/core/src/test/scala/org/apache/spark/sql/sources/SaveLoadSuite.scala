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

import org.apache.spark.sql.{SaveMode, SQLConf, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class SaveLoadSuite extends DataSourceTest with BeforeAndAfterAll {

  import caseInsensitiveContext._

  var originalDefaultSource: String = null

  var path: File = null

  var df: DataFrame = null

  override def beforeAll(): Unit = {
    originalDefaultSource = conf.defaultDataSourceName

    path = Utils.createTempDir()
    path.delete()

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    df = read.json(rdd)
    df.registerTempTable("jsonTable")
  }

  override def afterAll(): Unit = {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
  }

  after {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
    Utils.deleteRecursively(path)
  }

  def checkLoad(): Unit = {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    checkAnswer(read.load(path.toString), df.collect())

    // Test if we can pick up the data source name passed in load.
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    checkAnswer(read.format("json").load(path.toString), df.collect())
    checkAnswer(read.format("json").load(path.toString), df.collect())
    val schema = StructType(StructField("b", StringType, true) :: Nil)
    checkAnswer(
      read.format("json").schema(schema).load(path.toString),
      sql("SELECT b FROM jsonTable").collect())
  }

  test("save with path and load") {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    df.write.save(path.toString)
    checkLoad()
  }

  test("save with string mode and path, and load") {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    path.createNewFile()
    df.write.mode("overwrite").save(path.toString)
    checkLoad()
  }

  test("save with path and datasource, and load") {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.write.json(path.toString)
    checkLoad()
  }

  test("save with data source and options, and load") {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.write.mode(SaveMode.ErrorIfExists).json(path.toString)
    checkLoad()
  }

  test("save and save again") {
    df.write.json(path.toString)

    var message = intercept[RuntimeException] {
      df.write.json(path.toString)
    }.getMessage

    assert(
      message.contains("already exists"),
      "We should complain that the path already exists.")

    if (path.exists()) Utils.deleteRecursively(path)

    df.write.json(path.toString)
    checkLoad()

    df.write.mode(SaveMode.Overwrite).json(path.toString)
    checkLoad()

    message = intercept[RuntimeException] {
      df.write.mode(SaveMode.Append).json(path.toString)
    }.getMessage

    assert(
      message.contains("Append mode is not supported"),
      "We should complain that 'Append mode is not supported' for JSON source.")
  }
}
