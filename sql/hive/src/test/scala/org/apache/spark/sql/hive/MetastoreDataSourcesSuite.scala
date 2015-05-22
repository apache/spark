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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidInputException
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql._
import org.apache.spark.sql.hive.client.{HiveTable, ManagedTable}
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.parquet.ParquetRelation2
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Tests for persisting tables created though the data sources API into the metastore.
 */
class MetastoreDataSourcesSuite extends QueryTest with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    reset()
    Utils.deleteRecursively(tempPath)
  }

  val filePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
  var tempPath: File = Utils.createTempDir()
  tempPath.delete()

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
      read.json(filePath).collect().toSeq)
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

    read.json(filePath).registerTempTable("expectedJsonTable")

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

    read.json(filePath).registerTempTable("expectedJsonTable")

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
      read.json(filePath).collect().toSeq)
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
      read.json(filePath).collect().toSeq)

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
    val tempDir = File.createTempFile("sparksql", "json", Utils.createTempDir())
    tempDir.delete()
    sparkContext.parallelize(("a", "b") :: Nil).toDF()
      .toJSON.saveAsTextFile(tempDir.getCanonicalPath)

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

    Utils.deleteRecursively(tempDir)
    sparkContext.parallelize(("a1", "b1", "c1") :: Nil).toDF()
      .toJSON.saveAsTextFile(tempDir.getCanonicalPath)

    // Schema is cached so the new column does not show. The updated values in existing columns
    // will show.
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      Row("a1", "b1"))

    sql("REFRESH TABLE jsonTable")

    // Check that the refresh worked
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      Row("a1", "b1", "c1"))
    Utils.deleteRecursively(tempDir)
  }

  test("drop, change, recreate") {
    val tempDir = File.createTempFile("sparksql", "json", Utils.createTempDir())
    tempDir.delete()
    sparkContext.parallelize(("a", "b") :: Nil).toDF()
      .toJSON.saveAsTextFile(tempDir.getCanonicalPath)

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

    Utils.deleteRecursively(tempDir)
    sparkContext.parallelize(("a", "b", "c") :: Nil).toDF()
      .toJSON.saveAsTextFile(tempDir.getCanonicalPath)

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
    Utils.deleteRecursively(tempDir)
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

    read.json(filePath).registerTempTable("expectedJsonTable")

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

    // Create the table again should trigger a AnalysisException.
    val message = intercept[AnalysisException] {
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

  test("SPARK-5839 HiveMetastoreCatalog does not recognize table aliases of data source tables.") {
    val originalDefaultSource = conf.defaultDataSourceName

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    val df = read.json(rdd)

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    // Save the df as a managed table (by not specifiying the path).
    df.write.saveAsTable("savedJsonTable")

    checkAnswer(
      sql("SELECT * FROM savedJsonTable where savedJsonTable.a < 5"),
      (1 to 4).map(i => Row(i, s"str${i}")))

    checkAnswer(
      sql("SELECT * FROM savedJsonTable tmp where tmp.a > 5"),
      (6 to 10).map(i => Row(i, s"str${i}")))

    invalidateTable("savedJsonTable")

    checkAnswer(
      sql("SELECT * FROM savedJsonTable where savedJsonTable.a < 5"),
      (1 to 4).map(i => Row(i, s"str${i}")))

    checkAnswer(
      sql("SELECT * FROM savedJsonTable tmp where tmp.a > 5"),
      (6 to 10).map(i => Row(i, s"str${i}")))

    // Drop table will also delete the data.
    sql("DROP TABLE savedJsonTable")

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
  }

  test("save table") {
    val originalDefaultSource = conf.defaultDataSourceName

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    val df = read.json(rdd)

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    // Save the df as a managed table (by not specifiying the path).
    df.write.saveAsTable("savedJsonTable")

    checkAnswer(
      sql("SELECT * FROM savedJsonTable"),
      df.collect())

    // Right now, we cannot append to an existing JSON table.
    intercept[RuntimeException] {
      df.write.mode(SaveMode.Append).saveAsTable("savedJsonTable")
    }

    // We can overwrite it.
    df.write.mode(SaveMode.Overwrite).saveAsTable("savedJsonTable")
    checkAnswer(
      sql("SELECT * FROM savedJsonTable"),
      df.collect())

    // When the save mode is Ignore, we will do nothing when the table already exists.
    df.select("b").write.mode(SaveMode.Ignore).saveAsTable("savedJsonTable")
    assert(df.schema === table("savedJsonTable").schema)
    checkAnswer(
      sql("SELECT * FROM savedJsonTable"),
      df.collect())

    // Drop table will also delete the data.
    sql("DROP TABLE savedJsonTable")
    intercept[InvalidInputException] {
      read.json(catalog.hiveDefaultTableFilePath("savedJsonTable"))
    }

    // Create an external table by specifying the path.
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.write
      .format("org.apache.spark.sql.json")
      .mode(SaveMode.Append)
      .option("path", tempPath.toString)
      .saveAsTable("savedJsonTable")
    checkAnswer(
      sql("SELECT * FROM savedJsonTable"),
      df.collect())

    // Data should not be deleted after we drop the table.
    sql("DROP TABLE savedJsonTable")
    checkAnswer(
      read.json(tempPath.toString),
      df.collect())

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
  }

  test("create external table") {
    val originalDefaultSource = conf.defaultDataSourceName

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    val df = read.json(rdd)

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.write.format("org.apache.spark.sql.json")
      .mode(SaveMode.Append)
      .option("path", tempPath.toString)
      .saveAsTable("savedJsonTable")

    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    createExternalTable("createdJsonTable", tempPath.toString)
    assert(table("createdJsonTable").schema === df.schema)
    checkAnswer(
      sql("SELECT * FROM createdJsonTable"),
      df.collect())

    var message = intercept[AnalysisException] {
      createExternalTable("createdJsonTable", filePath.toString)
    }.getMessage
    assert(message.contains("Table createdJsonTable already exists."),
      "We should complain that ctasJsonTable already exists")

    // Data should not be deleted.
    sql("DROP TABLE createdJsonTable")
    checkAnswer(
      read.json(tempPath.toString),
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
      message.contains("'path' must be specified for json data."),
      "We should complain that path is not specified.")

    sql("DROP TABLE savedJsonTable")
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
  }

  if (HiveShim.version == "0.13.1") {
    test("scan a parquet table created through a CTAS statement") {
      val originalConvertMetastore = getConf("spark.sql.hive.convertMetastoreParquet", "true")
      val originalUseDataSource = getConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "true")
      setConf("spark.sql.hive.convertMetastoreParquet", "true")
      setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "true")

      val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
      read.json(rdd).registerTempTable("jt")
      sql(
        """
          |create table test_parquet_ctas STORED AS parquET
          |AS select tmp.a from jt tmp where tmp.a < 5
        """.stripMargin)

      checkAnswer(
        sql(s"SELECT a FROM test_parquet_ctas WHERE a > 2 "),
        Row(3) :: Row(4) :: Nil
      )

      table("test_parquet_ctas").queryExecution.optimizedPlan match {
        case LogicalRelation(p: ParquetRelation2) => // OK
        case _ =>
          fail(
            "test_parquet_ctas should be converted to " +
            s"${classOf[ParquetRelation2].getCanonicalName}")
      }

      // Clenup and reset confs.
      sql("DROP TABLE IF EXISTS jt")
      sql("DROP TABLE IF EXISTS test_parquet_ctas")
      setConf("spark.sql.hive.convertMetastoreParquet", originalConvertMetastore)
      setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, originalUseDataSource)
    }
  }

  test("Pre insert nullability check (ArrayType)") {
    val df1 =
      createDataFrame(Tuple1(Seq(Int.box(1), null.asInstanceOf[Integer])) :: Nil).toDF("a")
    val expectedSchema1 =
      StructType(
        StructField("a", ArrayType(IntegerType, containsNull = true), nullable = true) :: Nil)
    assert(df1.schema === expectedSchema1)
    df1.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("arrayInParquet")

    val df2 =
      createDataFrame(Tuple1(Seq(2, 3)) :: Nil).toDF("a")
    val expectedSchema2 =
      StructType(
        StructField("a", ArrayType(IntegerType, containsNull = false), nullable = true) :: Nil)
    assert(df2.schema === expectedSchema2)
    df2.insertInto("arrayInParquet", overwrite = false)
    createDataFrame(Tuple1(Seq(4, 5)) :: Nil).toDF("a").write.mode(SaveMode.Append)
      .saveAsTable("arrayInParquet") // This one internally calls df2.insertInto.
    createDataFrame(Tuple1(Seq(Int.box(6), null.asInstanceOf[Integer])) :: Nil).toDF("a").write
      .mode(SaveMode.Append).saveAsTable("arrayInParquet")
    refreshTable("arrayInParquet")

    checkAnswer(
      sql("SELECT a FROM arrayInParquet"),
      Row(ArrayBuffer(1, null)) ::
        Row(ArrayBuffer(2, 3)) ::
        Row(ArrayBuffer(4, 5)) ::
        Row(ArrayBuffer(6, null)) :: Nil)

    sql("DROP TABLE arrayInParquet")
  }

  test("Pre insert nullability check (MapType)") {
    val df1 =
      createDataFrame(Tuple1(Map(1 -> null.asInstanceOf[Integer])) :: Nil).toDF("a")
    val mapType1 = MapType(IntegerType, IntegerType, valueContainsNull = true)
    val expectedSchema1 =
      StructType(
        StructField("a", mapType1, nullable = true) :: Nil)
    assert(df1.schema === expectedSchema1)
    df1.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("mapInParquet")

    val df2 =
      createDataFrame(Tuple1(Map(2 -> 3)) :: Nil).toDF("a")
    val mapType2 = MapType(IntegerType, IntegerType, valueContainsNull = false)
    val expectedSchema2 =
      StructType(
        StructField("a", mapType2, nullable = true) :: Nil)
    assert(df2.schema === expectedSchema2)
    df2.insertInto("mapInParquet", overwrite = false)
    createDataFrame(Tuple1(Map(4 -> 5)) :: Nil).toDF("a").write.mode(SaveMode.Append)
      .saveAsTable("mapInParquet") // This one internally calls df2.insertInto.
    createDataFrame(Tuple1(Map(6 -> null.asInstanceOf[Integer])) :: Nil).toDF("a").write
      .format("parquet").mode(SaveMode.Append).saveAsTable("mapInParquet")
    refreshTable("mapInParquet")

    checkAnswer(
      sql("SELECT a FROM mapInParquet"),
      Row(Map(1 -> null)) ::
        Row(Map(2 -> 3)) ::
        Row(Map(4 -> 5)) ::
        Row(Map(6 -> null)) :: Nil)

    sql("DROP TABLE mapInParquet")
  }

  test("SPARK-6024 wide schema support") {
    // We will need 80 splits for this schema if the threshold is 4000.
    val schema = StructType((1 to 5000).map(i => StructField(s"c_${i}", StringType, true)))
    assert(
      schema.json.size > conf.schemaStringLengthThreshold,
      "To correctly test the fix of SPARK-6024, the value of " +
      s"spark.sql.sources.schemaStringLengthThreshold needs to be less than ${schema.json.size}")
    // Manually create a metastore data source table.
    catalog.createDataSourceTable(
      tableName = "wide_schema",
      userSpecifiedSchema = Some(schema),
      partitionColumns = Array.empty[String],
      provider = "json",
      options = Map("path" -> "just a dummy path"),
      isExternal = false)

    invalidateTable("wide_schema")

    val actualSchema = table("wide_schema").schema
    assert(schema === actualSchema)
  }

  test("SPARK-6655 still support a schema stored in spark.sql.sources.schema") {
    val tableName = "spark6655"
    val schema = StructType(StructField("int", IntegerType, true) :: Nil)

    val hiveTable = HiveTable(
      specifiedDatabase = Some("default"),
      name = tableName,
      schema = Seq.empty,
      partitionColumns = Seq.empty,
      properties = Map(
        "spark.sql.sources.provider" -> "json",
        "spark.sql.sources.schema" -> schema.json,
        "EXTERNAL" -> "FALSE"),
      tableType = ManagedTable,
      serdeProperties = Map(
        "path" -> catalog.hiveDefaultTableFilePath(tableName)))

    catalog.client.createTable(hiveTable)

    invalidateTable(tableName)
    val actualSchema = table(tableName).schema
    assert(schema === actualSchema)
    sql(s"drop table $tableName")
  }

  test("Saving partition columns information") {
    val df =
      sparkContext.parallelize(1 to 10, 4).map { i =>
        Tuple4(i, i + 1, s"str$i", s"str${i + 1}")
      }.toDF("a", "b", "c", "d")

    val tableName = s"partitionInfo_${System.currentTimeMillis()}"
    df.write.format("parquet").partitionBy("d", "b").saveAsTable(tableName)
    invalidateTable(tableName)
    val metastoreTable = catalog.client.getTable("default", tableName)
    val expectedPartitionColumns =
      StructType(df.schema("d") :: df.schema("b") :: Nil)
    val actualPartitionColumns =
      StructType(
        metastoreTable.partitionColumns.map(c =>
          StructField(c.name, HiveMetastoreTypes.toDataType(c.hiveType))))
    // Make sure partition columns are correctly stored in metastore.
    assert(
      expectedPartitionColumns.sameType(actualPartitionColumns),
      s"Partitions columns stored in metastore $actualPartitionColumns is not the " +
        s"partition columns defined by the saveAsTable operation $expectedPartitionColumns.")

    // Check the content of the saved table.
    checkAnswer(
      table(tableName).selectExpr("c", "b", "d", "a"),
      df.selectExpr("c", "b", "d", "a").collect())

    sql(s"drop table $tableName")
  }

  test("insert into a table") {
    def createDF(from: Int, to: Int): DataFrame =
      createDataFrame((from to to).map(i => Tuple2(i, s"str$i"))).toDF("c1", "c2")

    createDF(0, 9).write.format("parquet").saveAsTable("insertParquet")
    checkAnswer(
      sql("SELECT p.c1, p.c2 FROM insertParquet p WHERE p.c1 > 5"),
      (6 to 9).map(i => Row(i, s"str$i")))

    intercept[AnalysisException] {
      createDF(10, 19).write.format("parquet").saveAsTable("insertParquet")
    }

    createDF(10, 19).write.mode(SaveMode.Append).format("parquet").saveAsTable("insertParquet")
    checkAnswer(
      sql("SELECT p.c1, p.c2 FROM insertParquet p WHERE p.c1 > 5"),
      (6 to 19).map(i => Row(i, s"str$i")))

    createDF(20, 29).write.mode(SaveMode.Append).format("parquet").saveAsTable("insertParquet")
    checkAnswer(
      sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 25"),
      (6 to 24).map(i => Row(i, s"str$i")))

    intercept[AnalysisException] {
      createDF(30, 39).write.saveAsTable("insertParquet")
    }

    createDF(30, 39).write.mode(SaveMode.Append).saveAsTable("insertParquet")
    checkAnswer(
      sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 35"),
      (6 to 34).map(i => Row(i, s"str$i")))

    createDF(40, 49).insertInto("insertParquet")
    checkAnswer(
      sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 45"),
      (6 to 44).map(i => Row(i, s"str$i")))

    createDF(50, 59).write.mode(SaveMode.Overwrite).saveAsTable("insertParquet")
    checkAnswer(
      sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 51 AND p.c1 < 55"),
      (52 to 54).map(i => Row(i, s"str$i")))
    createDF(60, 69).write.mode(SaveMode.Ignore).saveAsTable("insertParquet")
    checkAnswer(
      sql("SELECT p.c1, c2 FROM insertParquet p"),
      (50 to 59).map(i => Row(i, s"str$i")))

    createDF(70, 79).insertInto("insertParquet", overwrite = true)
    checkAnswer(
      sql("SELECT p.c1, c2 FROM insertParquet p"),
      (70 to 79).map(i => Row(i, s"str$i")))
  }
}
