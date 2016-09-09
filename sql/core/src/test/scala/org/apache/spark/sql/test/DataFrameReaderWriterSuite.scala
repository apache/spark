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

import java.io.File

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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

/** Dummy provider with only RelationProvider and CreatableRelationProvider. */
class DefaultSourceWithoutUserSpecifiedSchema
  extends RelationProvider
  with CreatableRelationProvider {

  case class FakeRelation(sqlContext: SQLContext) extends BaseRelation {
    override def schema: StructType = StructType(Seq(StructField("a", StringType)))
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    FakeRelation(sqlContext)
  }
}

class DataFrameReaderWriterSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {


  private val userSchema = new StructType().add("s", StringType)
  private val textSchema = new StructType().add("value", StringType)
  private val data = Seq("1", "2", "3")
  private val dir = Utils.createTempDir(namePrefix = "input").getCanonicalPath
  private implicit var enc: Encoder[String] = _

  before {
    enc = spark.implicits.newStringEncoder
    Utils.deleteRecursively(new File(dir))
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

  test("resolve default source without extending SchemaRelationProvider") {
    spark.read
      .format("org.apache.spark.sql.test.DefaultSourceWithoutUserSpecifiedSchema")
      .load()
      .write
      .format("org.apache.spark.sql.test.DefaultSourceWithoutUserSpecifiedSchema")
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

  test("test path option in load") {
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .load("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("path") == "/test")

    LastOptions.clear()
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 55)
      .load()

    assert(LastOptions.parameters("intOpt") == "55")
    assert(!LastOptions.parameters.contains("path"))

    LastOptions.clear()
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 54)
      .load("/test", "/test1", "/test2")

    assert(LastOptions.parameters("intOpt") == "54")
    assert(!LastOptions.parameters.contains("path"))
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
        spark.range(10).write.format("csv").mode("overwrite").partitionBy("id").save(path)
      }
      spark.emptyDataFrame.write.format("parquet").mode("overwrite").save(path)
    }
  }

  test("load API") {
    spark.read.format("org.apache.spark.sql.test").load()
    spark.read.format("org.apache.spark.sql.test").load(dir)
    spark.read.format("org.apache.spark.sql.test").load(dir, dir, dir)
    spark.read.format("org.apache.spark.sql.test").load(Seq(dir, dir): _*)
    Option(dir).map(spark.read.format("org.apache.spark.sql.test").load)
  }

  test("text - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).write.mode(SaveMode.Overwrite).text(dir)
    testRead(spark.read.text(dir), data, textSchema)

    // Reader, without user specified schema
    testRead(spark.read.text(), Seq.empty, textSchema)
    testRead(spark.read.text(dir, dir, dir), data ++ data ++ data, textSchema)
    testRead(spark.read.text(Seq(dir, dir): _*), data ++ data, textSchema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.text).get, data, textSchema)

    // Reader, with user specified schema, should just apply user schema on the file data
    testRead(spark.read.schema(userSchema).text(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).text(dir), data, userSchema)
    testRead(spark.read.schema(userSchema).text(dir, dir), data ++ data, userSchema)
    testRead(spark.read.schema(userSchema).text(Seq(dir, dir): _*), data ++ data, userSchema)
  }

  test("textFile - API and behavior regarding schema") {
    spark.createDataset(data).write.mode(SaveMode.Overwrite).text(dir)

    // Reader, without user specified schema
    testRead(spark.read.textFile().toDF(), Seq.empty, textSchema)
    testRead(spark.read.textFile(dir).toDF(), data, textSchema)
    testRead(spark.read.textFile(dir, dir).toDF(), data ++ data, textSchema)
    testRead(spark.read.textFile(Seq(dir, dir): _*).toDF(), data ++ data, textSchema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.text).get, data, textSchema)

    // Reader, with user specified schema, should just apply user schema on the file data
    val e = intercept[AnalysisException] { spark.read.schema(userSchema).textFile() }
    assert(e.getMessage.toLowerCase.contains("user specified schema not supported"))
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(dir) }
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(dir, dir) }
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(Seq(dir, dir): _*) }
  }

  test("csv - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).csv(dir)
    val df = spark.read.csv(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    intercept[IllegalArgumentException] {
      testRead(spark.read.csv(), Seq.empty, schema)
    }
    testRead(spark.read.csv(dir), data, schema)
    testRead(spark.read.csv(dir, dir), data ++ data, schema)
    testRead(spark.read.csv(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.csv).get, data, schema)

    // Reader, with user specified schema, should just apply user schema on the file data
    testRead(spark.read.schema(userSchema).csv(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).csv(dir), data, userSchema)
    testRead(spark.read.schema(userSchema).csv(dir, dir), data ++ data, userSchema)
    testRead(spark.read.schema(userSchema).csv(Seq(dir, dir): _*), data ++ data, userSchema)
  }

  test("json - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).json(dir)
    val df = spark.read.json(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    intercept[AnalysisException] {
      testRead(spark.read.json(), Seq.empty, schema)
    }
    testRead(spark.read.json(dir), data, schema)
    testRead(spark.read.json(dir, dir), data ++ data, schema)
    testRead(spark.read.json(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.json).get, data, schema)

    // Reader, with user specified schema, data should be nulls as schema in file different
    // from user schema
    val expData = Seq[String](null, null, null)
    testRead(spark.read.schema(userSchema).json(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).json(dir), expData, userSchema)
    testRead(spark.read.schema(userSchema).json(dir, dir), expData ++ expData, userSchema)
    testRead(spark.read.schema(userSchema).json(Seq(dir, dir): _*), expData ++ expData, userSchema)
  }

  test("parquet - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).parquet(dir)
    val df = spark.read.parquet(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    intercept[AnalysisException] {
      testRead(spark.read.parquet(), Seq.empty, schema)
    }
    testRead(spark.read.parquet(dir), data, schema)
    testRead(spark.read.parquet(dir, dir), data ++ data, schema)
    testRead(spark.read.parquet(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.parquet).get, data, schema)

    // Reader, with user specified schema, data should be nulls as schema in file different
    // from user schema
    val expData = Seq[String](null, null, null)
    testRead(spark.read.schema(userSchema).parquet(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).parquet(dir), expData, userSchema)
    testRead(spark.read.schema(userSchema).parquet(dir, dir), expData ++ expData, userSchema)
    testRead(
      spark.read.schema(userSchema).parquet(Seq(dir, dir): _*), expData ++ expData, userSchema)
  }

  /**
   * This only tests whether API compiles, but does not run it as orc()
   * cannot be run without Hive classes.
   */
  ignore("orc - API") {
    // Reader, with user specified schema
    // Refer to csv-specific test suites for behavior without user specified schema
    spark.read.schema(userSchema).orc()
    spark.read.schema(userSchema).orc(dir)
    spark.read.schema(userSchema).orc(dir, dir, dir)
    spark.read.schema(userSchema).orc(Seq(dir, dir): _*)
    Option(dir).map(spark.read.schema(userSchema).orc)

    // Writer
    spark.range(10).write.orc(dir)
  }

  test("column nullability and comment - write and then read") {
    import testImplicits._

    Seq("json", "parquet", "csv").foreach { format =>
      val schema = StructType(
        StructField("cl1", IntegerType, nullable = false).withComment("test") ::
          StructField("cl2", IntegerType, nullable = true) ::
          StructField("cl3", IntegerType, nullable = true) :: Nil)
      val row = Row(3, null, 4)
      val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

      val tableName = "tab"
      withTable(tableName) {
        df.write.format(format).mode("overwrite").saveAsTable(tableName)
        // Verify the DDL command result: DESCRIBE TABLE
        checkAnswer(
          sql(s"desc $tableName").select("col_name", "comment").where($"comment" === "test"),
          Row("cl1", "test") :: Nil)
        // Verify the schema
        val expectedFields = schema.fields.map(f => f.copy(nullable = true))
        assert(spark.table(tableName).schema == schema.copy(fields = expectedFields))
      }
    }
  }

  test("SPARK-17230: write out results of decimal calculation") {
    val df = spark.range(99, 101)
      .selectExpr("id", "cast(id as long) * cast('1.0' as decimal(38, 18)) as num")
    df.write.mode(SaveMode.Overwrite).parquet(dir)
    val df2 = spark.read.parquet(dir)
    checkAnswer(df2, df)
  }

  private def testRead(
      df: => DataFrame,
      expectedResult: Seq[String],
      expectedSchema: StructType): Unit = {
    checkAnswer(df, spark.createDataset(expectedResult).toDF())
    assert(df.schema === expectedSchema)
  }
}
