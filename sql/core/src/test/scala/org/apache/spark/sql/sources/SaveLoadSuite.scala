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

import org.apache.spark.sql.DataFrame
import org.apache.spark.util.Utils

import org.apache.spark.sql.catalyst.util

class SaveLoadSuite extends DataSourceTest with BeforeAndAfterAll {

  import caseInsensisitiveContext._

  var originalDefaultSource: String = null

  var path: File = null

  var df: DataFrame = null

  override def beforeAll(): Unit = {
    originalDefaultSource = conf.defaultDataSourceName
    conf.setConf("spark.sql.default.datasource", "org.apache.spark.sql.json")

    path = util.getTempFilePath("datasource").getCanonicalFile

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    df = jsonRDD(rdd)
  }

  override def afterAll(): Unit = {
    conf.setConf("spark.sql.default.datasource", originalDefaultSource)
  }

  after {
    if (path.exists()) Utils.deleteRecursively(path)
  }

  def checkLoad(): Unit = {
    checkAnswer(load(path.toString), df.collect())
    checkAnswer(load("org.apache.spark.sql.json", ("path", path.toString)), df.collect())
  }

  test("save with overwrite and load") {
    df.save(path.toString)
    checkLoad
  }

  test("save with data source and options, and load") {
    df.save("org.apache.spark.sql.json", ("path", path.toString))
    checkLoad
  }

  test("save and save again") {
    df.save(path.toString)

    val message = intercept[RuntimeException] {
      df.save(path.toString)
    }.getMessage

    assert(
      message.contains("already exists"),
      "We should complain that the path already exists.")

    if (path.exists()) Utils.deleteRecursively(path)

    df.save(path.toString)
    checkLoad
  }
}