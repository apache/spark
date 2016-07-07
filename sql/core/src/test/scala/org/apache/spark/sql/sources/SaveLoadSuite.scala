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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class SaveLoadSuite extends DataSourceTest with SharedSQLContext with BeforeAndAfter {
  protected override lazy val sql = spark.sql _
  private var originalDefaultSource: String = null
  private var path: File = null
  private var df: DataFrame = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalDefaultSource = spark.sessionState.conf.defaultDataSourceName

    path = Utils.createTempDir()
    path.delete()

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    df = spark.read.json(rdd)
    df.createOrReplaceTempView("jsonTable")
  }

  override def afterAll(): Unit = {
    try {
      spark.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, originalDefaultSource)
    } finally {
      super.afterAll()
    }
  }

  after {
    Utils.deleteRecursively(path)
  }

  def checkLoad(expectedDF: DataFrame = df, tbl: String = "jsonTable"): Unit = {
    spark.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, "org.apache.spark.sql.json")
    checkAnswer(spark.read.load(path.toString), expectedDF.collect())

    // Test if we can pick up the data source name passed in load.
    spark.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, "not a source name")
    checkAnswer(spark.read.format("json").load(path.toString),
      expectedDF.collect())
    checkAnswer(spark.read.format("json").load(path.toString),
      expectedDF.collect())
    val schema = StructType(StructField("b", StringType, true) :: Nil)
    checkAnswer(
      spark.read.format("json").schema(schema).load(path.toString),
      sql(s"SELECT b FROM $tbl").collect())
  }

  test("save with path and load") {
    spark.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, "org.apache.spark.sql.json")
    df.write.save(path.toString)
    checkLoad()
  }

  test("save with string mode and path, and load") {
    spark.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, "org.apache.spark.sql.json")
    path.createNewFile()
    df.write.mode("overwrite").save(path.toString)
    checkLoad()
  }

  test("save with path and datasource, and load") {
    spark.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, "not a source name")
    df.write.json(path.toString)
    checkLoad()
  }

  test("save with data source and options, and load") {
    spark.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, "not a source name")
    df.write.mode(SaveMode.ErrorIfExists).json(path.toString)
    checkLoad()
  }

  test("save and save again") {
    df.write.json(path.toString)

    val message = intercept[AnalysisException] {
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

    // verify the append mode
    df.write.mode(SaveMode.Append).json(path.toString)
    val df2 = df.union(df)
    df2.createOrReplaceTempView("jsonTable2")

    checkLoad(df2, "jsonTable2")
  }
}
