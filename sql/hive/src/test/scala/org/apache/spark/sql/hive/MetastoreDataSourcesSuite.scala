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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidInputException

import org.apache.spark.sql.catalyst.util
import org.apache.spark.sql._
import org.apache.spark.util.Utils
import org.apache.spark.sql.types._

/* Implicits */
import org.apache.spark.sql.hive.test.TestHive._

/**
 * Tests for persisting tables created though the data sources API into the metastore.
 */
class MetastoreDataSourcesSuite extends QueryTest with BeforeAndAfterEach {

  import org.apache.spark.sql.hive.test.TestHive.implicits._

  override def afterEach(): Unit = {
    reset()
    if (tempPath.exists()) Utils.deleteRecursively(tempPath)
  }

  val filePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
  var tempPath: File = util.getTempFilePath("jsonCTAS").getCanonicalFile

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

    assert(expectedSchema === table("jsonTable").schema)

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

    assert(
      (new File(filePath)).exists(),
      "The table with specified path is considered as an external table, " +
        "its data should not deleted after DROP TABLE.")
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

    assert(expectedSchema === table("jsonTable").schema)
  }

  test("CTAS") {
    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    sql(
      s"""
        |CREATE TABLE ctasJsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${tempPath}'
        |) AS
        |SELECT * FROM jsonTable
      """.stripMargin)

    assert(table("ctasJsonTable").schema === table("jsonTable").schema)

    checkAnswer(
      sql("SELECT * FROM ctasJsonTable"),
      sql("SELECT * FROM jsonTable").collect())
  }

  test("CTAS with IF NOT EXISTS") {
    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    sql(
      s"""
        |CREATE TABLE ctasJsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${tempPath}'
        |) AS
        |SELECT * FROM jsonTable
      """.stripMargin)

    // Create the table again should trigger a AlreadyExistsException.
    val message = intercept[RuntimeException] {
      sql(
        s"""
        |CREATE TABLE ctasJsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${tempPath}'
        |) AS
        |SELECT * FROM jsonTable
      """.stripMargin)
    }.getMessage
    assert(message.contains("Table ctasJsonTable already exists."),
      "We should complain that ctasJsonTable already exists")

    // The following statement should be fine if it has IF NOT EXISTS.
    // It tries to create a table ctasJsonTable with a new schema.
    // The actual table's schema and data should not be changed.
    sql(
      s"""
        |CREATE TABLE IF NOT EXISTS ctasJsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${tempPath}'
        |) AS
        |SELECT a FROM jsonTable
      """.stripMargin)

    // Discard the cached relation.
    invalidateTable("ctasJsonTable")

    // Schema should not be changed.
    assert(table("ctasJsonTable").schema === table("jsonTable").schema)
    // Table data should not be changed.
    checkAnswer(
      sql("SELECT * FROM ctasJsonTable"),
      sql("SELECT * FROM jsonTable").collect())
  }

  test("CTAS a managed table") {
    sql(
      s"""
        |CREATE TABLE jsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${filePath}'
        |)
      """.stripMargin)

    val expectedPath = catalog.hiveDefaultTableFilePath("ctasJsonTable")
    val filesystemPath = new Path(expectedPath)
    val fs = filesystemPath.getFileSystem(sparkContext.hadoopConfiguration)
    if (fs.exists(filesystemPath)) fs.delete(filesystemPath, true)

    // It is a managed table when we do not specify the location.
    sql(
      s"""
        |CREATE TABLE ctasJsonTable
        |USING org.apache.spark.sql.json.DefaultSource
        |AS
        |SELECT * FROM jsonTable
      """.stripMargin)

    assert(fs.exists(filesystemPath), s"$expectedPath should exist after we create the table.")

    sql(
      s"""
        |CREATE TABLE loadedTable
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${expectedPath}'
        |)
      """.stripMargin)

    assert(table("ctasJsonTable").schema === table("loadedTable").schema)

    checkAnswer(
      sql("SELECT * FROM ctasJsonTable"),
      sql("SELECT * FROM loadedTable").collect()
    )

    sql("DROP TABLE ctasJsonTable")
    assert(!fs.exists(filesystemPath), s"$expectedPath should not exist after we drop the table.")
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

  test("save table") {
    val originalDefaultSource = conf.defaultDataSourceName

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    val df = jsonRDD(rdd)

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    // Save the df as a managed table (by not specifiying the path).
    df.saveAsTable("savedJsonTable")

    checkAnswer(
      sql("SELECT * FROM savedJsonTable"),
      df.collect())

    // Right now, we cannot append to an existing JSON table.
    intercept[RuntimeException] {
      df.saveAsTable("savedJsonTable", SaveMode.Append)
    }

    // We can overwrite it.
    df.saveAsTable("savedJsonTable", SaveMode.Overwrite)
    checkAnswer(
      sql("SELECT * FROM savedJsonTable"),
      df.collect())

    // When the save mode is Ignore, we will do nothing when the table already exists.
    df.select("b").saveAsTable("savedJsonTable", SaveMode.Ignore)
    assert(df.schema === table("savedJsonTable").schema)
    checkAnswer(
      sql("SELECT * FROM savedJsonTable"),
      df.collect())

    // Drop table will also delete the data.
    sql("DROP TABLE savedJsonTable")
    intercept[InvalidInputException] {
      jsonFile(catalog.hiveDefaultTableFilePath("savedJsonTable"))
    }

    // Create an external table by specifying the path.
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.saveAsTable(
      "savedJsonTable",
      "org.apache.spark.sql.json",
      SaveMode.Append,
      Map("path" -> tempPath.toString))
    checkAnswer(
      sql("SELECT * FROM savedJsonTable"),
      df.collect())

    // Data should not be deleted after we drop the table.
    sql("DROP TABLE savedJsonTable")
    checkAnswer(
      jsonFile(tempPath.toString),
      df.collect())

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
  }

  test("create external table") {
    val originalDefaultSource = conf.defaultDataSourceName

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    val df = jsonRDD(rdd)

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.saveAsTable(
      "savedJsonTable",
      "org.apache.spark.sql.json",
      SaveMode.Append,
      Map("path" -> tempPath.toString))

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    createExternalTable("createdJsonTable", tempPath.toString)
    assert(table("createdJsonTable").schema === df.schema)
    checkAnswer(
      sql("SELECT * FROM createdJsonTable"),
      df.collect())

    var message = intercept[RuntimeException] {
      createExternalTable("createdJsonTable", filePath.toString)
    }.getMessage
    assert(message.contains("Table createdJsonTable already exists."),
      "We should complain that ctasJsonTable already exists")

    // Data should not be deleted.
    sql("DROP TABLE createdJsonTable")
    checkAnswer(
      jsonFile(tempPath.toString),
      df.collect())

    // Try to specify the schema.
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    val schema = StructType(StructField("b", StringType, true) :: Nil)
    createExternalTable(
      "createdJsonTable",
      "org.apache.spark.sql.json",
      schema,
      Map("path" -> tempPath.toString))
    checkAnswer(
      sql("SELECT * FROM createdJsonTable"),
      sql("SELECT b FROM savedJsonTable").collect())

    sql("DROP TABLE createdJsonTable")

    message = intercept[RuntimeException] {
      createExternalTable(
        "createdJsonTable",
        "org.apache.spark.sql.json",
        schema,
        Map.empty[String, String])
    }.getMessage
    assert(
      message.contains("Option 'path' not specified"),
      "We should complain that path is not specified.")

    sql("DROP TABLE savedJsonTable")
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
  }
}
