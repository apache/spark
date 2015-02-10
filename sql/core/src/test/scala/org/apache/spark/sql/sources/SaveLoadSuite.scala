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
import org.apache.spark.sql.{SQLConf, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class SaveLoadSuite extends DataSourceTest with BeforeAndAfterAll {

  import caseInsensisitiveContext._

  var originalDefaultSource: String = null

  var path: File = null

  var df: DataFrame = null

  override def beforeAll(): Unit = {
    originalDefaultSource = conf.defaultDataSourceName

    path = util.getTempFilePath("datasource").getCanonicalFile

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    df = jsonRDD(rdd)
    df.registerTempTable("jsonTable")
  }

  override def afterAll(): Unit = {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
  }

  after {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
    if (path.exists()) Utils.deleteRecursively(path)
  }

  def checkLoad(): Unit = {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    checkAnswer(load(path.toString), df.collect())

    // Test if we can pick up the data source name passed in load.
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    checkAnswer(load(path.toString, "org.apache.spark.sql.json"), df.collect())
    checkAnswer(load("org.apache.spark.sql.json", Map("path" -> path.toString)), df.collect())
    val schema = StructType(StructField("b", StringType, true) :: Nil)
    checkAnswer(
      load("org.apache.spark.sql.json", schema, Map("path" -> path.toString)),
      sql("SELECT b FROM jsonTable").collect())
  }

  test("save with path and load") {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    df.save(path.toString)
    checkLoad()
  }

  test("save with path and datasource, and load") {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.save(path.toString, "org.apache.spark.sql.json")
    checkLoad()
  }

  test("save with data source and options, and load") {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.save("org.apache.spark.sql.json", SaveMode.ErrorIfExists, Map("path" -> path.toString))
    checkLoad()
  }

  test("save and save again") {
    df.save(path.toString, "org.apache.spark.sql.json")

    var message = intercept[RuntimeException] {
      df.save(path.toString, "org.apache.spark.sql.json")
    }.getMessage

    assert(
      message.contains("already exists"),
      "We should complain that the path already exists.")

    if (path.exists()) Utils.deleteRecursively(path)

    df.save(path.toString, "org.apache.spark.sql.json")
    checkLoad()

    df.save("org.apache.spark.sql.json", SaveMode.Overwrite, Map("path" -> path.toString))
    checkLoad()

    message = intercept[RuntimeException] {
      df.save("org.apache.spark.sql.json", SaveMode.Append, Map("path" -> path.toString))
    }.getMessage

    assert(
      message.contains("Append mode is not supported"),
      "We should complain that 'Append mode is not supported' for JSON source.")
  }
}