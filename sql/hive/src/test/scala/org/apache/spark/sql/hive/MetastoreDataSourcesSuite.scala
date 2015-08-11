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

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.client.{HiveTable, ManagedTable}
import org.apache.spark.sql.hive.test.HiveTestUtils
import org.apache.spark.sql.parquet.ParquetRelation
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Tests for persisting tables created though the data sources API into the metastore.
 */
class MetastoreDataSourcesSuite
  extends QueryTest
  with HiveTestUtils
  with Logging {

  import testImplicits._

  var jsonFilePath: String = _

  override def beforeAll(): Unit = {
    jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
  }

  test("persistent JSON table") {
    withTable("jsonTable") {
      ctx.sql(
        s"""CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        ctx.sql("SELECT * FROM jsonTable"),
        ctx.read.json(jsonFilePath).collect().toSeq)
    }
  }

  test("persistent JSON table with a user specified schema") {
    withTable("jsonTable") {
      ctx.sql(
        s"""CREATE TABLE jsonTable (
           |a string,
           |b String,
           |`c_!@(3)` int,
           |`<d>` Struct<`d!`:array<int>, `=`:array<struct<Dd2: boolean>>>)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      withTempTable("expectedJsonTable") {
        ctx.read.json(jsonFilePath).registerTempTable("expectedJsonTable")
        checkAnswer(
          ctx.sql("SELECT a, b, `c_!@(3)`, `<d>`.`d!`, `<d>`.`=` FROM jsonTable"),
          ctx.sql("SELECT a, b, `c_!@(3)`, `<d>`.`d!`, `<d>`.`=` FROM expectedJsonTable"))
      }
    }
  }

  test("persistent JSON table with a user specified schema with a subset of fields") {
    withTable("jsonTable") {
      // This works because JSON objects are self-describing and JSONRelation can get needed
      // field values based on field names.
      ctx.sql(
        s"""CREATE TABLE jsonTable (`<d>` Struct<`=`:array<struct<Dd2: boolean>>>, b String)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      val innerStruct = StructType(Seq(
        StructField("=", ArrayType(StructType(StructField("Dd2", BooleanType, true) :: Nil)))))

      val expectedSchema = StructType(Seq(
        StructField("<d>", innerStruct, true),
        StructField("b", StringType, true)))

      assert(expectedSchema === ctx.table("jsonTable").schema)

      withTempTable("expectedJsonTable") {
        ctx.read.json(jsonFilePath).registerTempTable("expectedJsonTable")
        checkAnswer(
          ctx.sql("SELECT b, `<d>`.`=` FROM jsonTable"),
          ctx.sql("SELECT b, `<d>`.`=` FROM expectedJsonTable"))
      }
    }
  }

  test("resolve shortened provider names") {
    withTable("jsonTable") {
      ctx.sql(
        s"""
           |CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        ctx.sql("SELECT * FROM jsonTable"),
        ctx.read.json(jsonFilePath).collect().toSeq)
    }
  }

  test("drop table") {
    withTable("jsonTable") {
      ctx.sql(
        s"""
           |CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        ctx.sql("SELECT * FROM jsonTable"),
        ctx.read.json(jsonFilePath))

      ctx.sql("DROP TABLE jsonTable")

      intercept[Exception] {
        ctx.sql("SELECT * FROM jsonTable").collect()
      }

      assert(
        new File(jsonFilePath).exists(),
        "The table with specified path is considered as an external table, " +
          "its data should not deleted after DROP TABLE.")
    }
  }

  test("check change without refresh") {
    withTempPath { tempDir =>
      withTable("jsonTable") {
        (("a", "b") :: Nil).toDF().toJSON.saveAsTextFile(tempDir.getCanonicalPath)

        ctx.sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.getCanonicalPath}'
             |)
           """.stripMargin)

        checkAnswer(
          ctx.sql("SELECT * FROM jsonTable"),
          Row("a", "b"))

        Utils.deleteRecursively(tempDir)
        (("a1", "b1", "c1") :: Nil).toDF().toJSON.saveAsTextFile(tempDir.getCanonicalPath)

        // Schema is cached so the new column does not show. The updated values in existing columns
        // will show.
        checkAnswer(
          ctx.sql("SELECT * FROM jsonTable"),
          Row("a1", "b1"))

        ctx.sql("REFRESH TABLE jsonTable")

        // Check that the refresh worked
        checkAnswer(
          ctx.sql("SELECT * FROM jsonTable"),
          Row("a1", "b1", "c1"))
      }
    }
  }

  test("drop, change, recreate") {
    withTempPath { tempDir =>
      (("a", "b") :: Nil).toDF().toJSON.saveAsTextFile(tempDir.getCanonicalPath)

      withTable("jsonTable") {
        ctx.sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.getCanonicalPath}'
             |)
           """.stripMargin)

        checkAnswer(
          ctx.sql("SELECT * FROM jsonTable"),
          Row("a", "b"))

        Utils.deleteRecursively(tempDir)
        (("a", "b", "c") :: Nil).toDF().toJSON.saveAsTextFile(tempDir.getCanonicalPath)

        ctx.sql("DROP TABLE jsonTable")

        ctx.sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.getCanonicalPath}'
             |)
           """.stripMargin)

        // New table should reflect new schema.
        checkAnswer(
          ctx.sql("SELECT * FROM jsonTable"),
          Row("a", "b", "c"))
      }
    }
  }

  test("invalidate cache and reload") {
    withTable("jsonTable") {
      ctx.sql(
        s"""CREATE TABLE jsonTable (`c_!@(3)` int)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      withTempTable("expectedJsonTable") {
        ctx.read.json(jsonFilePath).registerTempTable("expectedJsonTable")

        checkAnswer(
          ctx.sql("SELECT * FROM jsonTable"),
          ctx.sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

        // Discard the cached relation.
        ctx.invalidateTable("jsonTable")

        checkAnswer(
          ctx.sql("SELECT * FROM jsonTable"),
          ctx.sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

        ctx.invalidateTable("jsonTable")
        val expectedSchema = StructType(StructField("c_!@(3)", IntegerType, true) :: Nil)

        assert(expectedSchema === ctx.table("jsonTable").schema)
      }
    }
  }

  test("CTAS") {
    withTempPath { tempPath =>
      withTable("jsonTable", "ctasJsonTable") {
        ctx.sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$jsonFilePath'
             |)
           """.stripMargin)

        ctx.sql(
          s"""CREATE TABLE ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$tempPath'
             |) AS
             |SELECT * FROM jsonTable
           """.stripMargin)

        assert(ctx.table("ctasJsonTable").schema === ctx.table("jsonTable").schema)

        checkAnswer(
          ctx.sql("SELECT * FROM ctasJsonTable"),
          ctx.sql("SELECT * FROM jsonTable").collect())
      }
    }
  }

  test("CTAS with IF NOT EXISTS") {
    withTempPath { path =>
      val tempPath = path.getCanonicalPath

      withTable("jsonTable", "ctasJsonTable") {
        ctx.sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$jsonFilePath'
             |)
           """.stripMargin)

        ctx.sql(
          s"""CREATE TABLE ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$tempPath'
             |) AS
             |SELECT * FROM jsonTable
           """.stripMargin)

        // Create the table again should trigger a AnalysisException.
        val message = intercept[AnalysisException] {
          ctx.sql(
            s"""CREATE TABLE ctasJsonTable
               |USING org.apache.spark.sql.json.DefaultSource
               |OPTIONS (
               |  path '$tempPath'
               |) AS
               |SELECT * FROM jsonTable
             """.stripMargin)
        }.getMessage

        assert(
          message.contains("Table ctasJsonTable already exists."),
          "We should complain that ctasJsonTable already exists")

        // The following statement should be fine if it has IF NOT EXISTS.
        // It tries to create a table ctasJsonTable with a new schema.
        // The actual table's schema and data should not be changed.
        ctx.sql(
          s"""CREATE TABLE IF NOT EXISTS ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$tempPath'
             |) AS
             |SELECT a FROM jsonTable
           """.stripMargin)

        // Discard the cached relation.
        ctx.invalidateTable("ctasJsonTable")

        // Schema should not be changed.
        assert(ctx.table("ctasJsonTable").schema === ctx.table("jsonTable").schema)
        // Table data should not be changed.
        checkAnswer(
          ctx.sql("SELECT * FROM ctasJsonTable"),
          ctx.sql("SELECT * FROM jsonTable").collect())
      }
    }
  }

  test("CTAS a managed table") {
    withTable("jsonTable", "ctasJsonTable", "loadedTable") {
      ctx.sql(
        s"""CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      val expectedPath = ctx.catalog.hiveDefaultTableFilePath("ctasJsonTable")
      val filesystemPath = new Path(expectedPath)
      val fs = filesystemPath.getFileSystem(ctx.sparkContext.hadoopConfiguration)
      if (fs.exists(filesystemPath)) fs.delete(filesystemPath, true)

      // It is a managed table when we do not specify the location.
      ctx.sql(
        s"""CREATE TABLE ctasJsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |AS
           |SELECT * FROM jsonTable
         """.stripMargin)

      assert(fs.exists(filesystemPath), s"$expectedPath should exist after we create the table.")

      ctx.sql(
        s"""CREATE TABLE loadedTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$expectedPath'
           |)
         """.stripMargin)

      assert(ctx.table("ctasJsonTable").schema === ctx.table("loadedTable").schema)

      checkAnswer(
        ctx.sql("SELECT * FROM ctasJsonTable"),
        ctx.sql("SELECT * FROM loadedTable"))

      ctx.sql("DROP TABLE ctasJsonTable")
      assert(!fs.exists(filesystemPath), s"$expectedPath should not exist after we drop the table.")
    }
  }

  test("SPARK-5286 Fail to drop an invalid table when using the data source API") {
    withTable("jsonTable") {
      ctx.sql(
        s"""CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path 'it is not a path at all!'
           |)
         """.stripMargin)

      ctx.sql("DROP TABLE jsonTable").collect().foreach(i => logInfo(i.toString))
    }
  }

  test("SPARK-5839 HiveMetastoreCatalog does not recognize table aliases of data source tables.") {
    withTable("savedJsonTable") {
      // Save the df as a managed table (by not specifying the path).
      (1 to 10)
        .map(i => i -> s"str$i")
        .toDF("a", "b")
        .write
        .format("json")
        .saveAsTable("savedJsonTable")

      checkAnswer(
        ctx.sql("SELECT * FROM savedJsonTable where savedJsonTable.a < 5"),
        (1 to 4).map(i => Row(i, s"str$i")))

      checkAnswer(
        ctx.sql("SELECT * FROM savedJsonTable tmp where tmp.a > 5"),
        (6 to 10).map(i => Row(i, s"str$i")))

      ctx.invalidateTable("savedJsonTable")

      checkAnswer(
        ctx.sql("SELECT * FROM savedJsonTable where savedJsonTable.a < 5"),
        (1 to 4).map(i => Row(i, s"str$i")))

      checkAnswer(
        ctx.sql("SELECT * FROM savedJsonTable tmp where tmp.a > 5"),
        (6 to 10).map(i => Row(i, s"str$i")))
    }
  }

  test("save table") {
    withTempPath { path =>
      val tempPath = path.getCanonicalPath

      withTable("savedJsonTable") {
        val df = (1 to 10).map(i => i -> s"str$i").toDF("a", "b")

        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
          // Save the df as a managed table (by not specifying the path).
          df.write.saveAsTable("savedJsonTable")

          checkAnswer(ctx.sql("SELECT * FROM savedJsonTable"), df)

          // Right now, we cannot append to an existing JSON table.
          intercept[RuntimeException] {
            df.write.mode(SaveMode.Append).saveAsTable("savedJsonTable")
          }

          // We can overwrite it.
          df.write.mode(SaveMode.Overwrite).saveAsTable("savedJsonTable")
          checkAnswer(ctx.sql("SELECT * FROM savedJsonTable"), df)

          // When the save mode is Ignore, we will do nothing when the table already exists.
          df.select("b").write.mode(SaveMode.Ignore).saveAsTable("savedJsonTable")
          assert(df.schema === ctx.table("savedJsonTable").schema)
          checkAnswer(ctx.sql("SELECT * FROM savedJsonTable"), df)

          // Drop table will also delete the data.
          ctx.sql("DROP TABLE savedJsonTable")
          intercept[InvalidInputException] {
            ctx.read.json(ctx.catalog.hiveDefaultTableFilePath("savedJsonTable"))
          }
        }

        // Create an external table by specifying the path.
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          df.write
            .format("org.apache.spark.sql.json")
            .mode(SaveMode.Append)
            .option("path", tempPath.toString)
            .saveAsTable("savedJsonTable")

          checkAnswer(ctx.sql("SELECT * FROM savedJsonTable"), df)
        }

        // Data should not be deleted after we drop the table.
        ctx.sql("DROP TABLE savedJsonTable")
        checkAnswer(ctx.read.json(tempPath.toString), df)
      }
    }
  }

  test("create external table") {
    withTempPath { tempPath =>
      withTable("savedJsonTable", "createdJsonTable") {
        val df = ctx.read.json(ctx.sparkContext.parallelize((1 to 10).map { i =>
          s"""{ "a": $i, "b": "str$i" }"""
        }))

        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          df.write
            .format("json")
            .mode(SaveMode.Append)
            .option("path", tempPath.toString)
            .saveAsTable("savedJsonTable")
        }

        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
          ctx.createExternalTable("createdJsonTable", tempPath.toString)
          assert(ctx.table("createdJsonTable").schema === df.schema)
          checkAnswer(ctx.sql("SELECT * FROM createdJsonTable"), df)

          assert(
            intercept[AnalysisException] {
              ctx.createExternalTable("createdJsonTable", jsonFilePath.toString)
            }.getMessage.contains("Table createdJsonTable already exists."),
            "We should complain that createdJsonTable already exists")
        }

        // Data should not be deleted.
        ctx.sql("DROP TABLE createdJsonTable")
        checkAnswer(ctx.read.json(tempPath.toString), df)

        // Try to specify the schema.
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          val schema = StructType(StructField("b", StringType, true) :: Nil)
          ctx.createExternalTable(
            "createdJsonTable",
            "org.apache.spark.sql.json",
            schema,
            Map("path" -> tempPath.toString))

          checkAnswer(
            ctx.sql("SELECT * FROM createdJsonTable"),
            ctx.sql("SELECT b FROM savedJsonTable"))

          ctx.sql("DROP TABLE createdJsonTable")

          assert(
            intercept[RuntimeException] {
              ctx.createExternalTable(
                "createdJsonTable",
                "org.apache.spark.sql.json",
                schema,
                Map.empty[String, String])
            }.getMessage.contains("'path' must be specified for json data."),
            "We should complain that path is not specified.")
        }
      }
    }
  }

  test("scan a parquet table created through a CTAS statement") {
    withSQLConf(HiveContext.CONVERT_METASTORE_PARQUET.key -> "true") {
      withTempTable("jt") {
        (1 to 10).map(i => i -> s"str$i").toDF("a", "b").registerTempTable("jt")

        withTable("test_parquet_ctas") {
          ctx.sql(
            """CREATE TABLE test_parquet_ctas STORED AS PARQUET
              |AS SELECT tmp.a FROM jt tmp WHERE tmp.a < 5
            """.stripMargin)

          checkAnswer(
            ctx.sql(s"SELECT a FROM test_parquet_ctas WHERE a > 2 "),
            Row(3) :: Row(4) :: Nil)

          ctx.table("test_parquet_ctas").queryExecution.optimizedPlan match {
            case LogicalRelation(p: ParquetRelation) => // OK
            case _ =>
              fail(s"test_parquet_ctas should have be converted to ${classOf[ParquetRelation]}")
          }
        }
      }
    }
  }

  test("Pre insert nullability check (ArrayType)") {
    withTable("arrayInParquet") {
      {
        val df = (Tuple1(Seq(Int.box(1), null: Integer)) :: Nil).toDF("a")
        val expectedSchema =
          StructType(
            StructField(
              "a",
              ArrayType(IntegerType, containsNull = true),
              nullable = true) :: Nil)

        assert(df.schema === expectedSchema)

        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .saveAsTable("arrayInParquet")
      }

      {
        val df = (Tuple1(Seq(2, 3)) :: Nil).toDF("a")
        val expectedSchema =
          StructType(
            StructField(
              "a",
              ArrayType(IntegerType, containsNull = false),
              nullable = true) :: Nil)

        assert(df.schema === expectedSchema)

        df.write
          .format("parquet")
          .mode(SaveMode.Append)
          .insertInto("arrayInParquet")
      }

      (Tuple1(Seq(4, 5)) :: Nil).toDF("a")
        .write
        .mode(SaveMode.Append)
        .saveAsTable("arrayInParquet") // This one internally calls df2.insertInto.

      (Tuple1(Seq(Int.box(6), null: Integer)) :: Nil).toDF("a")
        .write
        .mode(SaveMode.Append)
        .saveAsTable("arrayInParquet")

      ctx.refreshTable("arrayInParquet")

      checkAnswer(
        ctx.sql("SELECT a FROM arrayInParquet"),
        Row(ArrayBuffer(1, null)) ::
          Row(ArrayBuffer(2, 3)) ::
          Row(ArrayBuffer(4, 5)) ::
          Row(ArrayBuffer(6, null)) :: Nil)
    }
  }

  test("Pre insert nullability check (MapType)") {
    withTable("mapInParquet") {
      {
        val df = (Tuple1(Map(1 -> (null: Integer))) :: Nil).toDF("a")
        val expectedSchema =
          StructType(
            StructField(
              "a",
              MapType(IntegerType, IntegerType, valueContainsNull = true),
              nullable = true) :: Nil)

        assert(df.schema === expectedSchema)

        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .saveAsTable("mapInParquet")
      }

      {
        val df = (Tuple1(Map(2 -> 3)) :: Nil).toDF("a")
        val expectedSchema =
          StructType(
            StructField(
              "a",
              MapType(IntegerType, IntegerType, valueContainsNull = false),
              nullable = true) :: Nil)

        assert(df.schema === expectedSchema)

        df.write
          .format("parquet")
          .mode(SaveMode.Append)
          .insertInto("mapInParquet")
      }

      (Tuple1(Map(4 -> 5)) :: Nil).toDF("a")
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .saveAsTable("mapInParquet") // This one internally calls df2.insertInto.

      (Tuple1(Map(6 -> null.asInstanceOf[Integer])) :: Nil).toDF("a")
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .saveAsTable("mapInParquet")

      ctx.refreshTable("mapInParquet")

      checkAnswer(
        ctx.sql("SELECT a FROM mapInParquet"),
        Row(Map(1 -> null)) ::
          Row(Map(2 -> 3)) ::
          Row(Map(4 -> 5)) ::
          Row(Map(6 -> null)) :: Nil)
    }
  }

  test("SPARK-6024 wide schema support") {
    withSQLConf(SQLConf.SCHEMA_STRING_LENGTH_THRESHOLD.key -> "4000") {
      withTable("wide_schema") {
        // We will need 80 splits for this schema if the threshold is 4000.
        val schema = StructType((1 to 5000).map(i => StructField(s"c_$i", StringType, true)))

        // Manually create a metastore data source table.
        ctx.catalog.createDataSourceTable(
          tableName = "wide_schema",
          userSpecifiedSchema = Some(schema),
          partitionColumns = Array.empty[String],
          provider = "json",
          options = Map("path" -> "just a dummy path"),
          isExternal = false)

        ctx.invalidateTable("wide_schema")

        val actualSchema = ctx.table("wide_schema").schema
        assert(schema === actualSchema)
      }
    }
  }

  test("SPARK-6655 still support a schema stored in spark.sql.sources.schema") {
    val tableName = "spark6655"
    withTable(tableName) {
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
          "path" -> ctx.catalog.hiveDefaultTableFilePath(tableName)))

      ctx.catalog.client.createTable(hiveTable)

      ctx.invalidateTable(tableName)
      val actualSchema = ctx.table(tableName).schema
      assert(schema === actualSchema)
    }
  }

  test("Saving partition columns information") {
    val df = (1 to 10).map(i => (i, i + 1, s"str$i", s"str${i + 1}")).toDF("a", "b", "c", "d")
    val tableName = s"partitionInfo_${System.currentTimeMillis()}"

    withTable(tableName) {
      df.write.format("parquet").partitionBy("d", "b").saveAsTable(tableName)
      ctx.invalidateTable(tableName)
      val metastoreTable = ctx.catalog.client.getTable("default", tableName)
      val expectedPartitionColumns = StructType(df.schema("d") :: df.schema("b") :: Nil)
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
        ctx.table(tableName).select("c", "b", "d", "a"),
        df.select("c", "b", "d", "a"))
    }
  }

  test("insert into a table") {
    def createDF(from: Int, to: Int): DataFrame = {
      (from to to).map(i => i -> s"str$i").toDF("c1", "c2")
    }

    withTable("insertParquet") {
      createDF(0, 9).write.format("parquet").saveAsTable("insertParquet")
      checkAnswer(
        ctx.sql("SELECT p.c1, p.c2 FROM insertParquet p WHERE p.c1 > 5"),
        (6 to 9).map(i => Row(i, s"str$i")))

      intercept[AnalysisException] {
        createDF(10, 19).write.format("parquet").saveAsTable("insertParquet")
      }

      createDF(10, 19).write.mode(SaveMode.Append).format("parquet").saveAsTable("insertParquet")
      checkAnswer(
        ctx.sql("SELECT p.c1, p.c2 FROM insertParquet p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))

      createDF(20, 29).write.mode(SaveMode.Append).format("parquet").saveAsTable("insertParquet")
      checkAnswer(
        ctx.sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 25"),
        (6 to 24).map(i => Row(i, s"str$i")))

      intercept[AnalysisException] {
        createDF(30, 39).write.saveAsTable("insertParquet")
      }

      createDF(30, 39).write.mode(SaveMode.Append).saveAsTable("insertParquet")
      checkAnswer(
        ctx.sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 35"),
        (6 to 34).map(i => Row(i, s"str$i")))

      createDF(40, 49).write.mode(SaveMode.Append).insertInto("insertParquet")
      checkAnswer(
        ctx.sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 45"),
        (6 to 44).map(i => Row(i, s"str$i")))

      createDF(50, 59).write.mode(SaveMode.Overwrite).saveAsTable("insertParquet")
      checkAnswer(
        ctx.sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 51 AND p.c1 < 55"),
        (52 to 54).map(i => Row(i, s"str$i")))
      createDF(60, 69).write.mode(SaveMode.Ignore).saveAsTable("insertParquet")
      checkAnswer(
        ctx.sql("SELECT p.c1, c2 FROM insertParquet p"),
        (50 to 59).map(i => Row(i, s"str$i")))

      createDF(70, 79).write.mode(SaveMode.Overwrite).insertInto("insertParquet")
      checkAnswer(
        ctx.sql("SELECT p.c1, c2 FROM insertParquet p"),
        (70 to 79).map(i => Row(i, s"str$i")))
    }
  }

  test("SPARK-8156:create table to specific database by 'use dbname' ") {

    val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
    ctx.sql("""create database if not exists testdb8156""")
    ctx.sql("""use testdb8156""")
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ttt3")

    checkAnswer(
      ctx.sql("show TABLES in testdb8156").filter("tableName = 'ttt3'"),
      Row("ttt3", false))
    ctx.sql("""use default""")
    ctx.sql("""drop database if exists testdb8156 CASCADE""")
  }
}
