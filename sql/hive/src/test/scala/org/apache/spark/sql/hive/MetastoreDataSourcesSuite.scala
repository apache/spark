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

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.CreateTableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.hive.HiveExternalCatalog._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Tests for persisting tables created though the data sources API into the metastore.
 */
class MetastoreDataSourcesSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext._
  import spark.implicits._

  var jsonFilePath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
  }

  // To test `HiveExternalCatalog`, we need to read the raw table metadata(schema, partition
  // columns and bucket specification are still in table properties) from hive client.
  private def hiveClient: HiveClient =
    sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client

  test("persistent JSON table") {
    withTable("jsonTable") {
      sql(
        s"""CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        read.json(jsonFilePath).collect().toSeq)
    }
  }

  test("persistent JSON table with a user specified schema") {
    withTable("jsonTable") {
      sql(
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

      withTempView("expectedJsonTable") {
        read.json(jsonFilePath).createOrReplaceTempView("expectedJsonTable")
        checkAnswer(
          sql("SELECT a, b, `c_!@(3)`, `<d>`.`d!`, `<d>`.`=` FROM jsonTable"),
          sql("SELECT a, b, `c_!@(3)`, `<d>`.`d!`, `<d>`.`=` FROM expectedJsonTable"))
      }
    }
  }

  test("persistent JSON table with a user specified schema with a subset of fields") {
    withTable("jsonTable") {
      // This works because JSON objects are self-describing and JSONRelation can get needed
      // field values based on field names.
      sql(
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

      assert(expectedSchema === table("jsonTable").schema)

      withTempView("expectedJsonTable") {
        read.json(jsonFilePath).createOrReplaceTempView("expectedJsonTable")
        checkAnswer(
          sql("SELECT b, `<d>`.`=` FROM jsonTable"),
          sql("SELECT b, `<d>`.`=` FROM expectedJsonTable"))
      }
    }
  }

  test("resolve shortened provider names") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        read.json(jsonFilePath).collect().toSeq)
    }
  }

  test("drop table") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        read.json(jsonFilePath))

      sql("DROP TABLE jsonTable")

      intercept[Exception] {
        sql("SELECT * FROM jsonTable").collect()
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
        (("a", "b") :: Nil).toDF().toJSON.rdd.saveAsTextFile(tempDir.getCanonicalPath)

        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.toURI}'
             |)
           """.stripMargin)

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a", "b"))

        Utils.deleteRecursively(tempDir)
        (("a1", "b1", "c1") :: Nil).toDF().toJSON.rdd.saveAsTextFile(tempDir.getCanonicalPath)

        // Schema is cached so the new column does not show. The updated values in existing columns
        // will show.
        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a1", "b1"))

        sql("REFRESH TABLE jsonTable")

        // After refresh, schema is not changed.
        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a1", "b1"))
      }
    }
  }

  test("drop, change, recreate") {
    withTempPath { tempDir =>
      (("a", "b") :: Nil).toDF().toJSON.rdd.saveAsTextFile(tempDir.getCanonicalPath)

      withTable("jsonTable") {
        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.toURI}'
             |)
           """.stripMargin)

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a", "b"))

        Utils.deleteRecursively(tempDir)
        (("a", "b", "c") :: Nil).toDF().toJSON.rdd.saveAsTextFile(tempDir.getCanonicalPath)

        sql("DROP TABLE jsonTable")

        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.toURI}'
             |)
           """.stripMargin)

        // New table should reflect new schema.
        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a", "b", "c"))
      }
    }
  }

  test("invalidate cache and reload") {
    withTable("jsonTable") {
      sql(
        s"""CREATE TABLE jsonTable (`c_!@(3)` int)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      withTempView("expectedJsonTable") {
        read.json(jsonFilePath).createOrReplaceTempView("expectedJsonTable")

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

        // Discard the cached relation.
        sessionState.refreshTable("jsonTable")

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

        sessionState.refreshTable("jsonTable")
        val expectedSchema = StructType(StructField("c_!@(3)", IntegerType, true) :: Nil)

        assert(expectedSchema === table("jsonTable").schema)
      }
    }
  }

  test("CTAS") {
    withTempPath { tempPath =>
      withTable("jsonTable", "ctasJsonTable") {
        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$jsonFilePath'
             |)
           """.stripMargin)

        sql(
          s"""CREATE TABLE ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '${tempPath.toURI}'
             |) AS
             |SELECT * FROM jsonTable
           """.stripMargin)

        assert(table("ctasJsonTable").schema === table("jsonTable").schema)

        checkAnswer(
          sql("SELECT * FROM ctasJsonTable"),
          sql("SELECT * FROM jsonTable").collect())
      }
    }
  }

  test("CTAS with IF NOT EXISTS") {
    withTempPath { path =>
      val tempPath = path.toURI

      withTable("jsonTable", "ctasJsonTable") {
        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$jsonFilePath'
             |)
           """.stripMargin)

        sql(
          s"""CREATE TABLE ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$tempPath'
             |) AS
             |SELECT * FROM jsonTable
           """.stripMargin)

        // Create the table again should trigger a AnalysisException.
        val message = intercept[AnalysisException] {
          sql(
            s"""CREATE TABLE ctasJsonTable
               |USING org.apache.spark.sql.json.DefaultSource
               |OPTIONS (
               |  path '$tempPath'
               |) AS
               |SELECT * FROM jsonTable
             """.stripMargin)
        }.getMessage

        assert(
          message.contains("Table default.ctasJsonTable already exists."),
          "We should complain that ctasJsonTable already exists")

        // The following statement should be fine if it has IF NOT EXISTS.
        // It tries to create a table ctasJsonTable with a new schema.
        // The actual table's schema and data should not be changed.
        sql(
          s"""CREATE TABLE IF NOT EXISTS ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$tempPath'
             |) AS
             |SELECT a FROM jsonTable
           """.stripMargin)

        // Discard the cached relation.
        sessionState.refreshTable("ctasJsonTable")

        // Schema should not be changed.
        assert(table("ctasJsonTable").schema === table("jsonTable").schema)
        // Table data should not be changed.
        checkAnswer(
          sql("SELECT * FROM ctasJsonTable"),
          sql("SELECT * FROM jsonTable").collect())
      }
    }
  }

  test("CTAS a managed table") {
    withTable("jsonTable", "ctasJsonTable", "loadedTable") {
      sql(
        s"""CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      val expectedPath = sessionState.catalog.defaultTablePath(TableIdentifier("ctasJsonTable"))
      val filesystemPath = new Path(expectedPath)
      val fs = filesystemPath.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(filesystemPath, true)

      // It is a managed table when we do not specify the location.
      sql(
        s"""CREATE TABLE ctasJsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |AS
           |SELECT * FROM jsonTable
         """.stripMargin)

      assert(fs.exists(filesystemPath), s"$expectedPath should exist after we create the table.")

      sql(
        s"""CREATE TABLE loadedTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$expectedPath'
           |)
         """.stripMargin)

      assert(table("ctasJsonTable").schema === table("loadedTable").schema)

      checkAnswer(
        sql("SELECT * FROM ctasJsonTable"),
        sql("SELECT * FROM loadedTable"))

      sql("DROP TABLE ctasJsonTable")
      assert(!fs.exists(filesystemPath), s"$expectedPath should not exist after we drop the table.")
    }
  }

  test("saveAsTable(CTAS) using append and insertInto when the target table is Hive serde") {
    val tableName = "tab1"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName STORED AS SEQUENCEFILE AS SELECT 1 AS key, 'abc' AS value")

      val df = sql(s"SELECT key, value FROM $tableName")
      df.write.insertInto(tableName)
      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Row(1, "abc") :: Row(1, "abc") :: Nil
      )
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
        sql("SELECT * FROM savedJsonTable where savedJsonTable.a < 5"),
        (1 to 4).map(i => Row(i, s"str$i")))

      checkAnswer(
        sql("SELECT * FROM savedJsonTable tmp where tmp.a > 5"),
        (6 to 10).map(i => Row(i, s"str$i")))

      sessionState.refreshTable("savedJsonTable")

      checkAnswer(
        sql("SELECT * FROM savedJsonTable where savedJsonTable.a < 5"),
        (1 to 4).map(i => Row(i, s"str$i")))

      checkAnswer(
        sql("SELECT * FROM savedJsonTable tmp where tmp.a > 5"),
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

          checkAnswer(sql("SELECT * FROM savedJsonTable"), df)

          // We can overwrite it.
          df.write.mode(SaveMode.Overwrite).saveAsTable("savedJsonTable")
          checkAnswer(sql("SELECT * FROM savedJsonTable"), df)

          // When the save mode is Ignore, we will do nothing when the table already exists.
          df.select("b").write.mode(SaveMode.Ignore).saveAsTable("savedJsonTable")
          // TODO in ResolvedDataSource, will convert the schema into nullable = true
          // hence the df.schema is not exactly the same as table("savedJsonTable").schema
          // assert(df.schema === table("savedJsonTable").schema)
          checkAnswer(sql("SELECT * FROM savedJsonTable"), df)

          // Drop table will also delete the data.
          sql("DROP TABLE savedJsonTable")
          intercept[AnalysisException] {
            read.json(
              sessionState.catalog.defaultTablePath(TableIdentifier("savedJsonTable")).toString)
          }
        }

        // Create an external table by specifying the path.
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          df.write
            .format("org.apache.spark.sql.json")
            .mode(SaveMode.Append)
            .option("path", tempPath.toString)
            .saveAsTable("savedJsonTable")

          checkAnswer(sql("SELECT * FROM savedJsonTable"), df)
        }

        // Data should not be deleted after we drop the table.
        sql("DROP TABLE savedJsonTable")
        checkAnswer(read.json(tempPath.toString), df)
      }
    }
  }

  test("create external table") {
    withTempPath { tempPath =>
      withTable("savedJsonTable", "createdJsonTable") {
        val df = read.json((1 to 10).map { i =>
          s"""{ "a": $i, "b": "str$i" }"""
        }.toDS())

        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          df.write
            .format("json")
            .mode(SaveMode.Append)
            .option("path", tempPath.toString)
            .saveAsTable("savedJsonTable")
        }

        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
          sparkSession.catalog.createExternalTable("createdJsonTable", tempPath.toString)
          assert(table("createdJsonTable").schema === df.schema)
          checkAnswer(sql("SELECT * FROM createdJsonTable"), df)

          assert(
            intercept[AnalysisException] {
              sparkSession.catalog.createExternalTable("createdJsonTable", jsonFilePath.toString)
            }.getMessage.contains("Table createdJsonTable already exists."),
            "We should complain that createdJsonTable already exists")
        }

        // Data should not be deleted.
        sql("DROP TABLE createdJsonTable")
        checkAnswer(read.json(tempPath.toString), df)

        // Try to specify the schema.
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          val schema = StructType(StructField("b", StringType, true) :: Nil)
          sparkSession.catalog.createExternalTable(
            "createdJsonTable",
            "org.apache.spark.sql.json",
            schema,
            Map("path" -> tempPath.toString))

          checkAnswer(
            sql("SELECT * FROM createdJsonTable"),
            sql("SELECT b FROM savedJsonTable"))

          sql("DROP TABLE createdJsonTable")
        }
      }
    }
  }

  test("path required error") {
    assert(
      intercept[AnalysisException] {
        sparkSession.catalog.createExternalTable(
          "createdJsonTable",
          "org.apache.spark.sql.json",
          Map.empty[String, String])

        table("createdJsonTable")
      }.getMessage.contains("Unable to infer schema"),
      "We should complain that path is not specified.")

    sql("DROP TABLE IF EXISTS createdJsonTable")
  }

  test("scan a parquet table created through a CTAS statement") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "true") {
      withTempView("jt") {
        (1 to 10).map(i => i -> s"str$i").toDF("a", "b").createOrReplaceTempView("jt")

        withTable("test_parquet_ctas") {
          sql(
            """CREATE TABLE test_parquet_ctas STORED AS PARQUET
              |AS SELECT tmp.a FROM jt tmp WHERE tmp.a < 5
            """.stripMargin)

          checkAnswer(
            sql(s"SELECT a FROM test_parquet_ctas WHERE a > 2 "),
            Row(3) :: Row(4) :: Nil)

          table("test_parquet_ctas").queryExecution.optimizedPlan match {
            case LogicalRelation(p: HadoopFsRelation, _, _) => // OK
            case _ =>
              fail(s"test_parquet_ctas should have be converted to ${classOf[HadoopFsRelation]}")
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

      sparkSession.catalog.refreshTable("arrayInParquet")

      checkAnswer(
        sql("SELECT a FROM arrayInParquet"),
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

      sparkSession.catalog.refreshTable("mapInParquet")

      checkAnswer(
        sql("SELECT a FROM mapInParquet"),
        Row(Map(1 -> null)) ::
          Row(Map(2 -> 3)) ::
          Row(Map(4 -> 5)) ::
          Row(Map(6 -> null)) :: Nil)
    }
  }

  test("SPARK-6024 wide schema support") {
    assert(spark.sparkContext.conf.get(SCHEMA_STRING_LENGTH_THRESHOLD) == 4000)
    withTable("wide_schema") {
      withTempDir { tempDir =>
        // We will need 80 splits for this schema if the threshold is 4000.
        val schema = StructType((1 to 5000).map(i => StructField(s"c_$i", StringType)))

        val tableDesc = CatalogTable(
          identifier = TableIdentifier("wide_schema"),
          tableType = CatalogTableType.EXTERNAL,
          storage = CatalogStorageFormat.empty.copy(
            properties = Map("path" -> tempDir.getCanonicalPath)
          ),
          schema = schema,
          provider = Some("json")
        )
        spark.sessionState.catalog.createTable(tableDesc, ignoreIfExists = false)

        sessionState.refreshTable("wide_schema")

        val actualSchema = table("wide_schema").schema
        assert(schema === actualSchema)
      }
    }
  }

  test("SPARK-6655 still support a schema stored in spark.sql.sources.schema") {
    val tableName = "spark6655"
    withTable(tableName) {
      val schema = StructType(StructField("int", IntegerType, true) :: Nil)
      val hiveTable = CatalogTable(
        identifier = TableIdentifier(tableName, Some("default")),
        tableType = CatalogTableType.MANAGED,
        schema = new StructType,
        provider = Some("json"),
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          compressed = false,
          properties = Map(
            "path" -> sessionState.catalog.defaultTablePath(TableIdentifier(tableName)).toString)
        ),
        properties = Map(
          DATASOURCE_PROVIDER -> "json",
          DATASOURCE_SCHEMA -> schema.json,
          "EXTERNAL" -> "FALSE"))

      hiveClient.createTable(hiveTable, ignoreIfExists = false)

      sessionState.refreshTable(tableName)
      val actualSchema = table(tableName).schema
      assert(schema === actualSchema)
    }
  }

  test("Saving partitionBy columns information") {
    val df = (1 to 10).map(i => (i, i + 1, s"str$i", s"str${i + 1}")).toDF("a", "b", "c", "d")
    val tableName = s"partitionInfo_${System.currentTimeMillis()}"

    withTable(tableName) {
      df.write.format("parquet").partitionBy("d", "b").saveAsTable(tableName)
      sessionState.refreshTable(tableName)
      val metastoreTable = hiveClient.getTable("default", tableName)
      val expectedPartitionColumns = StructType(df.schema("d") :: df.schema("b") :: Nil)

      val numPartCols = metastoreTable.properties(DATASOURCE_SCHEMA_NUMPARTCOLS).toInt
      assert(numPartCols == 2)

      val actualPartitionColumns =
        StructType(
          (0 until numPartCols).map { index =>
            df.schema(metastoreTable.properties(s"$DATASOURCE_SCHEMA_PARTCOL_PREFIX$index"))
          })
      // Make sure partition columns are correctly stored in metastore.
      assert(
        expectedPartitionColumns.sameType(actualPartitionColumns),
        s"Partitions columns stored in metastore $actualPartitionColumns is not the " +
          s"partition columns defined by the saveAsTable operation $expectedPartitionColumns.")

      // Check the content of the saved table.
      checkAnswer(
        table(tableName).select("c", "b", "d", "a"),
        df.select("c", "b", "d", "a"))
    }
  }

  test("Saving information for sortBy and bucketBy columns") {
    val df = (1 to 10).map(i => (i, i + 1, s"str$i", s"str${i + 1}")).toDF("a", "b", "c", "d")
    val tableName = s"bucketingInfo_${System.currentTimeMillis()}"

    withTable(tableName) {
      df.write
        .format("parquet")
        .bucketBy(8, "d", "b")
        .sortBy("c")
        .saveAsTable(tableName)
      sessionState.refreshTable(tableName)
      val metastoreTable = hiveClient.getTable("default", tableName)
      val expectedBucketByColumns = StructType(df.schema("d") :: df.schema("b") :: Nil)
      val expectedSortByColumns = StructType(df.schema("c") :: Nil)

      val numBuckets = metastoreTable.properties(DATASOURCE_SCHEMA_NUMBUCKETS).toInt
      assert(numBuckets == 8)

      val numBucketCols = metastoreTable.properties(DATASOURCE_SCHEMA_NUMBUCKETCOLS).toInt
      assert(numBucketCols == 2)

      val numSortCols = metastoreTable.properties(DATASOURCE_SCHEMA_NUMSORTCOLS).toInt
      assert(numSortCols == 1)

      val actualBucketByColumns =
        StructType(
          (0 until numBucketCols).map { index =>
            df.schema(metastoreTable.properties(s"$DATASOURCE_SCHEMA_BUCKETCOL_PREFIX$index"))
          })
      // Make sure bucketBy columns are correctly stored in metastore.
      assert(
        expectedBucketByColumns.sameType(actualBucketByColumns),
        s"Partitions columns stored in metastore $actualBucketByColumns is not the " +
          s"partition columns defined by the saveAsTable operation $expectedBucketByColumns.")

      val actualSortByColumns =
        StructType(
          (0 until numSortCols).map { index =>
            df.schema(metastoreTable.properties(s"$DATASOURCE_SCHEMA_SORTCOL_PREFIX$index"))
          })
      // Make sure sortBy columns are correctly stored in metastore.
      assert(
        expectedSortByColumns.sameType(actualSortByColumns),
        s"Partitions columns stored in metastore $actualSortByColumns is not the " +
          s"partition columns defined by the saveAsTable operation $expectedSortByColumns.")

      // Check the content of the saved table.
      checkAnswer(
        table(tableName).select("c", "b", "d", "a"),
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

      createDF(40, 49).write.mode(SaveMode.Append).insertInto("insertParquet")
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

      createDF(70, 79).write.mode(SaveMode.Overwrite).insertInto("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM insertParquet p"),
        (70 to 79).map(i => Row(i, s"str$i")))
    }
  }

  test("append table using different formats") {
    def createDF(from: Int, to: Int): DataFrame = {
      (from to to).map(i => i -> s"str$i").toDF("c1", "c2")
    }

    withTable("appendOrcToParquet") {
      createDF(0, 9).write.format("parquet").saveAsTable("appendOrcToParquet")
      val e = intercept[AnalysisException] {
        createDF(10, 19).write.mode(SaveMode.Append).format("orc").saveAsTable("appendOrcToParquet")
      }
      assert(e.getMessage.contains(
        "The format of the existing table default.appendOrcToParquet is `ParquetFileFormat`. " +
          "It doesn't match the specified format `OrcFileFormat`"))
    }

    withTable("appendParquetToJson") {
      createDF(0, 9).write.format("json").saveAsTable("appendParquetToJson")
      val e = intercept[AnalysisException] {
        createDF(10, 19).write.mode(SaveMode.Append).format("parquet")
          .saveAsTable("appendParquetToJson")
      }
      assert(e.getMessage.contains(
        "The format of the existing table default.appendParquetToJson is `JsonFileFormat`. " +
        "It doesn't match the specified format `ParquetFileFormat`"))
    }

    withTable("appendTextToJson") {
      createDF(0, 9).write.format("json").saveAsTable("appendTextToJson")
      val e = intercept[AnalysisException] {
        createDF(10, 19).write.mode(SaveMode.Append).format("text")
          .saveAsTable("appendTextToJson")
      }
      assert(e.getMessage.contains(
        "The format of the existing table default.appendTextToJson is `JsonFileFormat`. " +
        "It doesn't match the specified format `TextFileFormat`"))
    }
  }

  test("append a table using the same formats but different names") {
    def createDF(from: Int, to: Int): DataFrame = {
      (from to to).map(i => i -> s"str$i").toDF("c1", "c2")
    }

    withTable("appendParquet") {
      createDF(0, 9).write.format("parquet").saveAsTable("appendParquet")
      createDF(10, 19).write.mode(SaveMode.Append).format("org.apache.spark.sql.parquet")
        .saveAsTable("appendParquet")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM appendParquet p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))
    }

    withTable("appendParquet") {
      createDF(0, 9).write.format("org.apache.spark.sql.parquet").saveAsTable("appendParquet")
      createDF(10, 19).write.mode(SaveMode.Append).format("parquet").saveAsTable("appendParquet")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM appendParquet p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))
    }

    withTable("appendParquet") {
      createDF(0, 9).write.format("org.apache.spark.sql.parquet.DefaultSource")
        .saveAsTable("appendParquet")
      createDF(10, 19).write.mode(SaveMode.Append)
        .format("org.apache.spark.sql.execution.datasources.parquet.DefaultSource")
        .saveAsTable("appendParquet")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM appendParquet p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))
    }
  }

  test("SPARK-8156:create table to specific database by 'use dbname' ") {

    val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
    spark.sql("""create database if not exists testdb8156""")
    spark.sql("""use testdb8156""")
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ttt3")

    checkAnswer(
      spark.sql("show TABLES in testdb8156").filter("tableName = 'ttt3'"),
      Row("testdb8156", "ttt3", false))
    spark.sql("""use default""")
    spark.sql("""drop database if exists testdb8156 CASCADE""")
  }


  test("skip hive metadata on table creation") {
    withTempDir { tempPath =>
      val schema = StructType((1 to 5).map(i => StructField(s"c_$i", StringType)))

      val tableDesc1 = CatalogTable(
        identifier = TableIdentifier("not_skip_hive_metadata"),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty.copy(
          locationUri = Some(tempPath.toURI),
          properties = Map("skipHiveMetadata" -> "false")
        ),
        schema = schema,
        provider = Some("parquet")
      )
      spark.sessionState.catalog.createTable(tableDesc1, ignoreIfExists = false)

      // As a proxy for verifying that the table was stored in Hive compatible format,
      // we verify that each column of the table is of native type StringType.
      assert(hiveClient.getTable("default", "not_skip_hive_metadata").schema
        .forall(_.dataType == StringType))

      val tableDesc2 = CatalogTable(
        identifier = TableIdentifier("skip_hive_metadata", Some("default")),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty.copy(
          properties = Map("path" -> tempPath.getCanonicalPath, "skipHiveMetadata" -> "true")
        ),
        schema = schema,
        provider = Some("parquet")
      )
      spark.sessionState.catalog.createTable(tableDesc2, ignoreIfExists = false)

      // As a proxy for verifying that the table was stored in SparkSQL format,
      // we verify that the table has a column type as array of StringType.
      assert(hiveClient.getTable("default", "skip_hive_metadata").schema
        .forall(_.dataType == ArrayType(StringType)))
    }
  }

  test("CTAS: persisted partitioned data source table") {
    withTempPath { dir =>
      withTable("t") {
        sql(
          s"""CREATE TABLE t USING PARQUET
             |OPTIONS (PATH '${dir.toURI}')
             |PARTITIONED BY (a)
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )

        val metastoreTable = hiveClient.getTable("default", "t")
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMPARTCOLS).toInt === 1)
        assert(!metastoreTable.properties.contains(DATASOURCE_SCHEMA_NUMBUCKETS))
        assert(!metastoreTable.properties.contains(DATASOURCE_SCHEMA_NUMBUCKETCOLS))
        assert(!metastoreTable.properties.contains(DATASOURCE_SCHEMA_NUMSORTCOLS))

        checkAnswer(table("t"), Row(2, 1))
      }
    }
  }

  test("CTAS: persisted bucketed data source table") {
    withTempPath { dir =>
      withTable("t") {
        sql(
          s"""CREATE TABLE t USING PARQUET
             |OPTIONS (PATH '${dir.toURI}')
             |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )

        val metastoreTable = hiveClient.getTable("default", "t")
        assert(!metastoreTable.properties.contains(DATASOURCE_SCHEMA_NUMPARTCOLS))
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMBUCKETS).toInt === 2)
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMBUCKETCOLS).toInt === 1)
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMSORTCOLS).toInt === 1)

        checkAnswer(table("t"), Row(1, 2))
      }
    }

    withTempPath { dir =>
      withTable("t") {
        sql(
          s"""CREATE TABLE t USING PARQUET
             |OPTIONS (PATH '${dir.toURI}')
             |CLUSTERED BY (a) INTO 2 BUCKETS
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )

        val metastoreTable = hiveClient.getTable("default", "t")
        assert(!metastoreTable.properties.contains(DATASOURCE_SCHEMA_NUMPARTCOLS))
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMBUCKETS).toInt === 2)
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMBUCKETCOLS).toInt === 1)
        assert(!metastoreTable.properties.contains(DATASOURCE_SCHEMA_NUMSORTCOLS))

        checkAnswer(table("t"), Row(1, 2))
      }
    }
  }

  test("CTAS: persisted partitioned bucketed data source table") {
    withTempPath { dir =>
      withTable("t") {
        sql(
          s"""CREATE TABLE t USING PARQUET
             |OPTIONS (PATH '${dir.toURI}')
             |PARTITIONED BY (a)
             |CLUSTERED BY (b) SORTED BY (c) INTO 2 BUCKETS
             |AS SELECT 1 AS a, 2 AS b, 3 AS c
           """.stripMargin
        )

        val metastoreTable = hiveClient.getTable("default", "t")
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMPARTCOLS).toInt === 1)
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMBUCKETS).toInt === 2)
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMBUCKETCOLS).toInt === 1)
        assert(metastoreTable.properties(DATASOURCE_SCHEMA_NUMSORTCOLS).toInt === 1)

        checkAnswer(table("t"), Row(2, 3, 1))
      }
    }
  }

  test("saveAsTable[append]: the column order doesn't matter") {
    withTable("saveAsTable_column_order") {
      Seq((1, 2)).toDF("i", "j").write.saveAsTable("saveAsTable_column_order")
      Seq((3, 4)).toDF("j", "i").write.mode("append").saveAsTable("saveAsTable_column_order")
      checkAnswer(
        table("saveAsTable_column_order"),
        Seq((1, 2), (4, 3)).toDF("i", "j"))
    }
  }

  test("saveAsTable[append]: mismatch column names") {
    withTable("saveAsTable_mismatch_column_names") {
      Seq((1, 2)).toDF("i", "j").write.saveAsTable("saveAsTable_mismatch_column_names")
      val e = intercept[AnalysisException] {
        Seq((3, 4)).toDF("i", "k")
          .write.mode("append").saveAsTable("saveAsTable_mismatch_column_names")
      }
      assert(e.getMessage.contains("cannot resolve"))
    }
  }

  test("saveAsTable[append]: too many columns") {
    withTable("saveAsTable_too_many_columns") {
      Seq((1, 2)).toDF("i", "j").write.saveAsTable("saveAsTable_too_many_columns")
      val e = intercept[AnalysisException] {
        Seq((3, 4, 5)).toDF("i", "j", "k")
          .write.mode("append").saveAsTable("saveAsTable_too_many_columns")
      }
      assert(e.getMessage.contains("doesn't match"))
    }
  }

  test("create a temp view using hive") {
    val tableName = "tab1"
    withTable(tableName) {
      val e = intercept[AnalysisException] {
        sql(
          s"""
             |CREATE TEMPORARY VIEW $tableName
             |(col1 int)
             |USING hive
           """.stripMargin)
      }.getMessage
      assert(e.contains("Hive data source can only be used with tables, you can't use it with " +
        "CREATE TEMP VIEW USING"))
    }
  }

  test("saveAsTable - source and target are the same table") {
    val tableName = "tab1"
    withTable(tableName) {
      Seq((1, 2)).toDF("i", "j").write.saveAsTable(tableName)

      table(tableName).write.mode(SaveMode.Append).saveAsTable(tableName)
      checkAnswer(table(tableName),
        Seq(Row(1, 2), Row(1, 2)))

      table(tableName).write.mode(SaveMode.Ignore).saveAsTable(tableName)
      checkAnswer(table(tableName),
        Seq(Row(1, 2), Row(1, 2)))

      var e = intercept[AnalysisException] {
        table(tableName).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
      }.getMessage
      assert(e.contains(s"Cannot overwrite table default.$tableName that is also being read from"))

      e = intercept[AnalysisException] {
        table(tableName).write.mode(SaveMode.ErrorIfExists).saveAsTable(tableName)
      }.getMessage
      assert(e.contains(s"Table `$tableName` already exists"))
    }
  }

  test("insertInto - source and target are the same table") {
    val tableName = "tab1"
    withTable(tableName) {
      Seq((1, 2)).toDF("i", "j").write.saveAsTable(tableName)

      table(tableName).write.mode(SaveMode.Append).insertInto(tableName)
      checkAnswer(
        table(tableName),
        Seq(Row(1, 2), Row(1, 2)))

      table(tableName).write.mode(SaveMode.Ignore).insertInto(tableName)
      checkAnswer(
        table(tableName),
        Seq(Row(1, 2), Row(1, 2), Row(1, 2), Row(1, 2)))

      table(tableName).write.mode(SaveMode.ErrorIfExists).insertInto(tableName)
      checkAnswer(
        table(tableName),
        Seq(Row(1, 2), Row(1, 2), Row(1, 2), Row(1, 2), Row(1, 2), Row(1, 2), Row(1, 2), Row(1, 2)))

      val e = intercept[AnalysisException] {
        table(tableName).write.mode(SaveMode.Overwrite).insertInto(tableName)
      }.getMessage
      assert(e.contains(s"Cannot overwrite a path that is also being read from"))
    }
  }

  test("saveAsTable[append]: less columns") {
    withTable("saveAsTable_less_columns") {
      Seq((1, 2)).toDF("i", "j").write.saveAsTable("saveAsTable_less_columns")
      val e = intercept[AnalysisException] {
        Seq((4)).toDF("j")
          .write.mode("append").saveAsTable("saveAsTable_less_columns")
      }
      assert(e.getMessage.contains("doesn't match"))
    }
  }

  test("SPARK-15025: create datasource table with path with select") {
    withTempPath { dir =>
      withTable("t") {
        sql(
          s"""CREATE TABLE t USING PARQUET
             |OPTIONS (PATH '${dir.toURI}')
             |AS SELECT 1 AS a, 2 AS b, 3 AS c
           """.stripMargin
        )
        sql("insert into t values (2, 3, 4)")
        checkAnswer(table("t"), Seq(Row(1, 2, 3), Row(2, 3, 4)))
        val catalogTable = hiveClient.getTable("default", "t")
        assert(catalogTable.storage.locationUri.isDefined)
      }
    }
  }

  test("SPARK-15269 external data source table creation") {
    withTempPath { dir =>
      val path = dir.toURI.toString
      spark.range(1).write.json(path)

      withTable("t") {
        sql(s"CREATE TABLE t USING json OPTIONS (PATH '$path')")
        sql("DROP TABLE t")
        sql(s"CREATE TABLE t USING json AS SELECT 1 AS c")
      }
    }
  }

  test("read table with corrupted schema") {
    try {
      val schema = StructType(StructField("int", IntegerType, true) :: Nil)
      val hiveTable = CatalogTable(
        identifier = TableIdentifier("t", Some("default")),
        tableType = CatalogTableType.MANAGED,
        schema = new StructType,
        provider = Some("json"),
        storage = CatalogStorageFormat.empty,
        properties = Map(
          DATASOURCE_PROVIDER -> "json",
          // no DATASOURCE_SCHEMA_NUMPARTS
          DATASOURCE_SCHEMA_PART_PREFIX + 0 -> schema.json))

      hiveClient.createTable(hiveTable, ignoreIfExists = false)

      val e = intercept[AnalysisException] {
        sharedState.externalCatalog.getTable("default", "t")
      }.getMessage
      assert(e.contains(s"Could not read schema from the hive metastore because it is corrupted"))

      withDebugMode {
        val tableMeta = sharedState.externalCatalog.getTable("default", "t")
        assert(tableMeta.identifier == TableIdentifier("t", Some("default")))
        assert(tableMeta.properties(DATASOURCE_PROVIDER) == "json")
      }
    } finally {
      hiveClient.dropTable("default", "t", ignoreIfNotExists = true, purge = true)
    }
  }

  test("should keep data source entries in table properties when debug mode is on") {
    withDebugMode {
      val newSession = sparkSession.newSession()
      newSession.sql("CREATE TABLE abc(i int) USING json")
      val tableMeta = newSession.sessionState.catalog.getTableMetadata(TableIdentifier("abc"))
      assert(tableMeta.properties(DATASOURCE_SCHEMA_NUMPARTS).toInt == 1)
      assert(tableMeta.properties(DATASOURCE_PROVIDER) == "json")
    }
  }

  test("Infer schema for Hive serde tables") {
    val tableName = "tab1"
    val avroSchema =
      """{
        |  "name": "test_record",
        |  "type": "record",
        |  "fields": [ {
        |    "name": "f0",
        |    "type": "int"
        |  }]
        |}
      """.stripMargin

    Seq(true, false).foreach { isPartitioned =>
      withTable(tableName) {
        val partitionClause = if (isPartitioned) "PARTITIONED BY (ds STRING)" else ""
        // Creates the (non-)partitioned Avro table
        val plan = sql(
          s"""
             |CREATE TABLE $tableName
             |$partitionClause
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
        ).queryExecution.analyzed

        assert(plan.isInstanceOf[CreateTableCommand] &&
          plan.asInstanceOf[CreateTableCommand].table.dataSchema.nonEmpty)

        if (isPartitioned) {
          sql(s"INSERT OVERWRITE TABLE $tableName partition (ds='a') SELECT 1")
          checkAnswer(spark.table(tableName), Row(1, "a"))
        } else {
          sql(s"INSERT OVERWRITE TABLE $tableName SELECT 1")
          checkAnswer(spark.table(tableName), Row(1))
        }
      }
    }
  }

  private def withDebugMode(f: => Unit): Unit = {
    val previousValue = sparkSession.sparkContext.conf.get(DEBUG_MODE)
    try {
      sparkSession.sparkContext.conf.set(DEBUG_MODE, true)
      f
    } finally {
      sparkSession.sparkContext.conf.set(DEBUG_MODE, previousValue)
    }
  }

  test("SPARK-18464: support old table which doesn't store schema in table properties") {
    withTable("old") {
      withTempPath { path =>
        Seq(1 -> "a").toDF("i", "j").write.parquet(path.getAbsolutePath)
        val tableDesc = CatalogTable(
          identifier = TableIdentifier("old", Some("default")),
          tableType = CatalogTableType.EXTERNAL,
          storage = CatalogStorageFormat.empty.copy(
            properties = Map("path" -> path.getAbsolutePath)
          ),
          schema = new StructType(),
          provider = Some("parquet"),
          properties = Map(
            HiveExternalCatalog.DATASOURCE_PROVIDER -> "parquet"))
        hiveClient.createTable(tableDesc, ignoreIfExists = false)

        checkAnswer(spark.table("old"), Row(1, "a"))

        val expectedSchema = StructType(Seq(
          StructField("i", IntegerType, nullable = true),
          StructField("j", StringType, nullable = true)))
        assert(table("old").schema === expectedSchema)
      }
    }
  }
}
