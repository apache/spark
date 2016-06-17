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

package org.apache.spark.sql.test

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.Utils


object LastOptions {

  var parameters: Map[String, String] = null
  var schema: Option[StructType] = null
  var saveMode: SaveMode = null

  def clear(): Unit = {
    parameters = null
    schema = null
    saveMode = null
  }
}


/** Dummy provider. */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  case class FakeRelation(sqlContext: SQLContext) extends BaseRelation {
    override def schema: StructType = StructType(Seq(StructField("a", StringType)))
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType
    ): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = Some(schema)
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
    ): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = None
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = None
    LastOptions.saveMode = mode
    FakeRelation(sqlContext)
  }
}


class DataFrameReaderWriterSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {


  private val input = Utils.createTempDir(namePrefix = "input").getCanonicalPath
  private var output: String = _
  private val userSchema = new StructType().add("s", StringType)
  private val textSchema = new StructType().add("value", StringType)

  before {
    val f = Utils.createTempDir(namePrefix = "output")
    f.delete()
    output = f.getCanonicalPath
  }

  test("writeStream cannot be called on non-streaming datasets") {
    val e = intercept[AnalysisException] {
      spark.read
        .format("org.apache.spark.sql.test")
        .load()
        .writeStream
        .start()
    }
    Seq("'writeStream'", "only", "streaming Dataset/DataFrame").foreach { s =>
      assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
    }
  }


  test("resolve default source") {
    spark.read
      .format("org.apache.spark.sql.test")
      .load()
      .write
      .format("org.apache.spark.sql.test")
      .save()
  }

  test("resolve full class") {
    spark.read
      .format("org.apache.spark.sql.test.DefaultSource")
      .load()
      .write
      .format("org.apache.spark.sql.test")
      .save()
  }

  test("options") {
    val map = new java.util.HashMap[String, String]
    map.put("opt3", "3")

    val df = spark.read
        .format("org.apache.spark.sql.test")
        .option("opt1", "1")
        .options(Map("opt2" -> "2"))
        .options(map)
        .load()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")

    LastOptions.clear()

    df.write
      .format("org.apache.spark.sql.test")
      .option("opt1", "1")
      .options(Map("opt2" -> "2"))
      .options(map)
      .save()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")
  }

  test("save mode") {
    val df = spark.read
      .format("org.apache.spark.sql.test")
      .load()

    df.write
      .format("org.apache.spark.sql.test")
      .mode(SaveMode.ErrorIfExists)
      .save()
    assert(LastOptions.saveMode === SaveMode.ErrorIfExists)
  }

  test("test different data types for options") {
    val df = spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .load("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")

    LastOptions.clear()
    df.write
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .save("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")
  }

  test("check jdbc() does not support partitioning or bucketing") {
    val df = spark.read.text(Utils.createTempDir(namePrefix = "text").getCanonicalPath)

    var w = df.write.partitionBy("value")
    var e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "partitioning").foreach { s =>
      assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
    }

    w = df.write.bucketBy(2, "value")
    e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "bucketing").foreach { s =>
      assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
    }
  }

  test("prevent all column partitioning") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      intercept[AnalysisException] {
        spark.range(10).write.format("parquet").mode("overwrite").partitionBy("id").save(path)
      }
      intercept[AnalysisException] {
        spark.range(10).write.format("orc").mode("overwrite").partitionBy("id").save(path)
      }
    }
  }

  test("load API") {
    spark.read.format("org.apache.spark.sql.test").load()
    spark.read.format("org.apache.spark.sql.test").load(input)
    spark.read.format("org.apache.spark.sql.test").load(input, input, input)
    spark.read.format("org.apache.spark.sql.test").load(Seq(input, input): _*)
    Option(input).map(spark.read.format("org.apache.spark.sql.test").load)
  }

  test("text - API and common behavior") {
    // Reader, without user specified schema
    assert(spark.read.text().schema === textSchema)
    assert(spark.read.text(input).schema === textSchema)
    assert(spark.read.text(input, input, input).schema === textSchema)
    assert(spark.read.text(Seq(input, input): _*).schema === textSchema)
    assert(Option(input).map(spark.read.text).get.schema === textSchema) // SPARK-16009

    // Reader, with user specified schema
    assert(spark.read.schema(userSchema).text().schema === userSchema)
    assert(spark.read.schema(userSchema).text(input).schema === userSchema)
    assert(spark.read.schema(userSchema).text(input, input, input).schema === userSchema)
    assert(spark.read.schema(userSchema).text(Seq(input, input): _*).schema === userSchema)

    // Writer
    spark.read.text().write.text(output)
  }

  test("textFile - API and common behavior") {
    // Reader, without user specified schema
    assert(spark.read.textFile().schema === textSchema)
    assert(spark.read.textFile(input).schema === textSchema)
    assert(spark.read.textFile(input, input, input).schema === textSchema)
    assert(spark.read.textFile(Seq(input, input): _*).schema === textSchema)
    assert(Option(input).map(spark.read.textFile).get.schema === textSchema) // SPARK-16009
  }

  test("csv - API and common behavior") {
    // Reader, with user specified schema
    // Refer to csv-specific test suites for behavior without user specified schema
    assert(spark.read.schema(userSchema).csv().schema === userSchema)
    assert(spark.read.schema(userSchema).csv(input).schema === userSchema)
    assert(spark.read.schema(userSchema).csv(input, input, input).schema === userSchema)
    assert(spark.read.schema(userSchema).csv(Seq(input, input): _*).schema === userSchema)

    // Test explicit calls to single arg method - SPARK-16009
    assert(Option(input).map(spark.read.schema(userSchema).csv).get.schema === userSchema)

    // Writer
    spark.range(10).write.csv(output)
  }

  test("json - API and common behavior") {
    // Reader, with user specified schema
    // Refer to csv-specific test suites for behavior without user specified schema
    assert(spark.read.schema(userSchema).json().schema === userSchema)
    assert(spark.read.schema(userSchema).json(input).schema === userSchema)
    assert(spark.read.schema(userSchema).json(input, input, input).schema === userSchema)
    assert(spark.read.schema(userSchema).json(Seq(input, input): _*).schema === userSchema)

    // Test explicit calls to single arg method - SPARK-16009
    assert(Option(input).map(spark.read.schema(userSchema).json).get.schema === userSchema)

    // Writer
    spark.range(10).write.json(output)
  }

  test("parquet - API and common behavior") {
    // Reader, with user specified schema
    // Refer to csv-specific test suites for behavior without user specified schema
    assert(spark.read.schema(userSchema).parquet().schema === userSchema)
    assert(spark.read.schema(userSchema).parquet(input).schema === userSchema)
    assert(spark.read.schema(userSchema).parquet(input, input, input).schema === userSchema)
    assert(spark.read.schema(userSchema).parquet(Seq(input, input): _*).schema === userSchema)

    // Test explicit calls to single arg method - SPARK-16009
    assert(Option(input).map(spark.read.schema(userSchema).parquet).get.schema === userSchema)

    // Writer
    spark.range(10).write.parquet(output)
  }

  /**
   * This only tests whether API compiles, but does not run it as orc()
   * cannot be run with Hive classes.
   */
  ignore("orc - API") {
    // Reader, with user specified schema
    // Refer to csv-specific test suites for behavior without user specified schema
    spark.read.schema(userSchema).orc()
    spark.read.schema(userSchema).orc(input)
    spark.read.schema(userSchema).orc(input, input, input)
    spark.read.schema(userSchema).orc(Seq(input, input): _*)
    Option(input).map(spark.read.schema(userSchema).orc)

    // Writer
    spark.range(10).write.orc(output)
  }
}
