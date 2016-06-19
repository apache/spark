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


class DataFrameReaderWriterSuite extends QueryTest with SharedSQLContext {

  private def newMetadataDir =
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

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

  test("paths") {
    val df = spark.read
      .format("org.apache.spark.sql.test")
      .option("checkpointLocation", newMetadataDir)
      .load("/test")

    assert(LastOptions.parameters("path") == "/test")

    LastOptions.clear()

    df.write
      .format("org.apache.spark.sql.test")
      .option("checkpointLocation", newMetadataDir)
      .save("/test")

    assert(LastOptions.parameters("path") == "/test")
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
      .option("checkpointLocation", newMetadataDir)
      .save("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")
  }

  test("reading catalog table") {
    val schema = StructType(StructField("c1", IntegerType) :: Nil)
    val format = "parquet"
    val df = spark.createDataFrame(sparkContext.parallelize(Row(3) :: Nil), schema)

    val tableName = "tab"
    withTable(tableName) {
      df.write.format(format).mode("overwrite").saveAsTable(tableName)
      // correct way:
      spark.read.table(tableName)

      // not allowed to specify format.
      var e = intercept[IllegalArgumentException] {
        spark.read.format(format).table(tableName)
      }.getMessage
      assert(e.contains("Operation not allowed: specifying the input data source format " +
        "when reading table from catalog. table: `tab`, format: `parquet`"))

      // not allowed to specify schema.
      e = intercept[IllegalArgumentException] {
        spark.read.schema(schema).table(tableName)
      }.getMessage
      assert(e.contains("Operation not allowed: specifying the input schema when reading table " +
        "from catalog. table: `tab`, schema: `StructType(StructField(c1,IntegerType,true))`"))

      // not allowed to specify options.
      e = intercept[IllegalArgumentException] {
        spark.read.options(Map("header" -> "true", "mode" -> "dropmalformed")).table(tableName)
      }.getMessage
      assert(e.contains("Operation not allowed: specifying the input option when reading table " +
        "from catalog. table: `tab`, option: `header -> true, mode -> dropmalformed`"))
    }
  }

  test("format or schema are specified twice") {
    val schema1 = StructType(StructField("c1", IntegerType) :: Nil)
    val schema2 = StructType(StructField("c2", IntegerType) :: Nil)
    val format = "parquet"
    val df = spark.createDataFrame(sparkContext.parallelize(Row(3) :: Nil), schema1)

    withTempPath { path =>
      df.write.format("json").mode("overwrite").save(path.getCanonicalPath)
      // correct way:
      spark.read.json(path.getCanonicalPath)

      // not allowed to specify the format more than once
      var e = intercept[IllegalArgumentException] {
        spark.read.format(format).json(path.getCanonicalPath)
      }.getMessage
      assert(e.contains("Operation not allowed: the input data source format has already been " +
        "set. Existing: `parquet`, new: `json`"))

      // not allowed to specify the schema more than once.
      e = intercept[IllegalArgumentException] {
        spark.read.schema(schema1).schema(schema2).json(path.getCanonicalPath)
      }.getMessage
      assert(e.contains("Operation not allowed: the input schema has already been set. " +
        "Existing: `StructType(StructField(c1,IntegerType,true))`, " +
        "new: `StructType(StructField(c2,IntegerType,true))`"))
    }
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
}
