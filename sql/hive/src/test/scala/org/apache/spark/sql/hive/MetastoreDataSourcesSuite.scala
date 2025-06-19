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
import org.apache.logging.log4j.Level

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.execution.command.CreateTableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelationWithTable}
import org.apache.spark.sql.hive.HiveExternalCatalog._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Tests for persisting tables created though the data sources API into the metastore.
 */
class MetastoreDataSourcesSuite extends QueryTest
  with SQLTestUtils
  with TestHiveSingleton
  with QueryErrorsBase {
  import hiveContext._
  import spark.implicits._

  var jsonFilePath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
  }

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
        spark.catalog.refreshTable("jsonTable")

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

        spark.catalog.refreshTable("jsonTable")
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
        val e = intercept[AnalysisException] {
          sql(
            s"""CREATE TABLE ctasJsonTable
               |USING org.apache.spark.sql.json.DefaultSource
               |OPTIONS (
               |  path '$tempPath'
               |) AS
               |SELECT * FROM jsonTable
             """.stripMargin)
        }

        checkErrorTableAlreadyExists(e, s"`$SESSION_CATALOG_NAME`.`default`.`ctasJsonTable`")

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
        spark.catalog.refreshTable("ctasJsonTable")

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

      spark.catalog.refreshTable("savedJsonTable")

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
          sparkSession.catalog.createTable("createdJsonTable", tempPath.toString)
          assert(table("createdJsonTable").schema === df.schema)
          checkAnswer(sql("SELECT * FROM createdJsonTable"), df)

          Seq("true", "false").foreach { caseSensitive =>
            withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
              val e = intercept[AnalysisException] {
                sparkSession.catalog.createTable("createdJsonTable", tempPath.toString)
              }
              val expectedTableName = s"`$SESSION_CATALOG_NAME`.`default`." + {
                if (caseSensitive.toBoolean) {
                  "`createdJsonTable`"
                } else {
                  "`createdjsontable`"
                }
              }
              checkErrorTableAlreadyExists(e, expectedTableName)
            }
          }
        }

        // Data should not be deleted.
        sql("DROP TABLE createdJsonTable")
        checkAnswer(read.json(tempPath.toString), df)

        // Try to specify the schema.
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          val schema = StructType(StructField("b", StringType, true) :: Nil)
          sparkSession.catalog.createTable(
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
    checkError(
      exception = intercept[AnalysisException] {
        sparkSession.catalog.createTable(
          "createdJsonTable",
          "org.apache.spark.sql.json",
          Map.empty[String, String])

        table("createdJsonTable")
      },
      condition = "UNABLE_TO_INFER_SCHEMA",
      parameters = Map("format" -> "JSON")
    )

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
            case LogicalRelationWithTable(_: HadoopFsRelation, _) => // OK
            case _ =>
              fail(s"test_parquet_ctas should have be converted to ${classOf[HadoopFsRelation]}")
          }
        }
      }
    }
  }

  test("Pre insert nullability check (ArrayType)") {
    withTable("array") {
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
          .mode(SaveMode.Overwrite)
          .saveAsTable("array")
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
          .mode(SaveMode.Append)
          .insertInto("array")
      }

      (Tuple1(Seq(4, 5)) :: Nil).toDF("a")
        .write
        .mode(SaveMode.Append)
        .saveAsTable("array") // This one internally calls df2.insertInto.

      (Tuple1(Seq(Int.box(6), null: Integer)) :: Nil).toDF("a")
        .write
        .mode(SaveMode.Append)
        .saveAsTable("array")

      sparkSession.catalog.refreshTable("array")

      checkAnswer(
        sql("SELECT a FROM array"),
        Row(ArrayBuffer(1, null)) ::
          Row(ArrayBuffer(2, 3)) ::
          Row(ArrayBuffer(4, 5)) ::
          Row(ArrayBuffer(6, null)) :: Nil)
    }
  }

  test("Pre insert nullability check (MapType)") {
    withTable("map") {
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
          .mode(SaveMode.Overwrite)
          .saveAsTable("map")
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
          .mode(SaveMode.Append)
          .insertInto("map")
      }

      (Tuple1(Map(4 -> 5)) :: Nil).toDF("a")
        .write
        .mode(SaveMode.Append)
        .saveAsTable("map") // This one internally calls df2.insertInto.

      (Tuple1(Map(6 -> null.asInstanceOf[Integer])) :: Nil).toDF("a")
        .write
        .mode(SaveMode.Append)
        .saveAsTable("map")

      sparkSession.catalog.refreshTable("map")

      checkAnswer(
        sql("SELECT a FROM map"),
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
            locationUri = Some(tempDir.toURI)
          ),
          schema = schema,
          provider = Some("json")
        )
        spark.sessionState.catalog.createTable(tableDesc, ignoreIfExists = false)

        spark.catalog.refreshTable("wide_schema")

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
        schema = HiveExternalCatalog.EMPTY_DATA_SCHEMA,
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

      spark.catalog.refreshTable(tableName)
      val actualSchema = table(tableName).schema
      assert(schema === actualSchema)
    }
  }

  test("Saving partitionBy columns information") {
    val df = (1 to 10).map(i => (i, i + 1, s"str$i", s"str${i + 1}")).toDF("a", "b", "c", "d")
    val tableName = s"partitionInfo_${System.currentTimeMillis()}"

    withTable(tableName) {
      df.write.format("parquet").partitionBy("d", "b").saveAsTable(tableName)
      spark.catalog.refreshTable(tableName)
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
        DataTypeUtils.sameType(expectedPartitionColumns, actualPartitionColumns),
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
      spark.catalog.refreshTable(tableName)
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
        DataTypeUtils.sameType(expectedBucketByColumns, actualBucketByColumns),
        s"Partitions columns stored in metastore $actualBucketByColumns is not the " +
          s"partition columns defined by the saveAsTable operation $expectedBucketByColumns.")

      val actualSortByColumns =
        StructType(
          (0 until numSortCols).map { index =>
            df.schema(metastoreTable.properties(s"$DATASOURCE_SCHEMA_SORTCOL_PREFIX$index"))
          })
      // Make sure sortBy columns are correctly stored in metastore.
      assert(
        DataTypeUtils.sameType(expectedSortByColumns, actualSortByColumns),
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

    withTable("t") {
      createDF(0, 9).write.saveAsTable("t")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM t p WHERE p.c1 > 5"),
        (6 to 9).map(i => Row(i, s"str$i")))

      intercept[AnalysisException] {
        createDF(10, 19).write.saveAsTable("t")
      }

      createDF(10, 19).write.mode(SaveMode.Append).saveAsTable("t")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM t p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))

      createDF(20, 29).write.mode(SaveMode.Append).saveAsTable("t")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM t p WHERE p.c1 > 5 AND p.c1 < 25"),
        (6 to 24).map(i => Row(i, s"str$i")))

      intercept[AnalysisException] {
        createDF(30, 39).write.saveAsTable("t")
      }

      createDF(30, 39).write.mode(SaveMode.Append).saveAsTable("t")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM t p WHERE p.c1 > 5 AND p.c1 < 35"),
        (6 to 34).map(i => Row(i, s"str$i")))

      createDF(40, 49).write.mode(SaveMode.Append).insertInto("t")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM t p WHERE p.c1 > 5 AND p.c1 < 45"),
        (6 to 44).map(i => Row(i, s"str$i")))

      createDF(50, 59).write.mode(SaveMode.Overwrite).saveAsTable("t")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM t p WHERE p.c1 > 51 AND p.c1 < 55"),
        (52 to 54).map(i => Row(i, s"str$i")))
      createDF(60, 69).write.mode(SaveMode.Ignore).saveAsTable("t")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM t p"),
        (50 to 59).map(i => Row(i, s"str$i")))

      createDF(70, 79).write.mode(SaveMode.Overwrite).insertInto("t")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM t p"),
        (70 to 79).map(i => Row(i, s"str$i")))
    }
  }

  test("append table using different formats") {
    def createDF(from: Int, to: Int): DataFrame = {
      (from to to).map(i => i -> s"str$i").toDF("c1", "c2")
    }

    withTable("appendOrcToParquet") {
      createDF(0, 9).write.format("parquet").saveAsTable("appendOrcToParquet")
      checkError(
        exception = intercept[AnalysisException] {
          createDF(10, 19).write.mode(SaveMode.Append).format("orc").
            saveAsTable("appendOrcToParquet")
        },
        condition = "_LEGACY_ERROR_TEMP_1159",
        parameters = Map(
          "tableName" -> s"$SESSION_CATALOG_NAME.default.appendorctoparquet",
          "existingProvider" -> "ParquetDataSourceV2",
          "specifiedProvider" -> "OrcDataSourceV2"
        )
      )
    }

    withTable("appendParquetToJson") {
      createDF(0, 9).write.format("json").saveAsTable("appendParquetToJson")
      checkError(
        exception = intercept[AnalysisException] {
          createDF(10, 19).write.mode(SaveMode.Append).format("parquet")
            .saveAsTable("appendParquetToJson")
        },
        condition = "_LEGACY_ERROR_TEMP_1159",
        parameters = Map(
          "tableName" -> s"$SESSION_CATALOG_NAME.default.appendparquettojson",
          "existingProvider" -> "JsonDataSourceV2",
          "specifiedProvider" -> "ParquetDataSourceV2"
        )
      )
    }

    withTable("appendTextToJson") {
      createDF(0, 9).write.format("json").saveAsTable("appendTextToJson")
      checkError(
        exception = intercept[AnalysisException] {
          createDF(10, 19).write.mode(SaveMode.Append).format("text")
            .saveAsTable("appendTextToJson")
        },
        condition = "_LEGACY_ERROR_TEMP_1159",
        // The format of the existing table can be JsonDataSourceV2 or JsonFileFormat.
        parameters = Map(
          "tableName" -> s"$SESSION_CATALOG_NAME.default.appendtexttojson",
          "existingProvider" -> "JsonDataSourceV2",
          "specifiedProvider" -> "TextDataSourceV2"
        )
      )
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

  test("append a table with file source V2 provider using the v1 file format") {
    def createDF(from: Int, to: Int): DataFrame = {
      (from to to).map(i => i -> s"str$i").toDF("c1", "c2")
    }

    withTable("appendCSV") {
      createDF(0, 9)
        .write
        .mode(SaveMode.Append)
        .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
        .saveAsTable("appendCSV")
      createDF(10, 19)
        .write
        .mode(SaveMode.Append)
        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
        .saveAsTable("appendCSV")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM appendCSV p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))
    }

    withTable("appendCSV") {
      createDF(0, 9).write.mode(SaveMode.Append).format("csv").saveAsTable("appendCSV")
      createDF(10, 19)
        .write
        .mode(SaveMode.Append)
        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
        .saveAsTable("appendCSV")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM appendCSV p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))
    }
  }

  test("append a table with v1 file format provider using file source V2 format") {
    def createDF(from: Int, to: Int): DataFrame = {
      (from to to).map(i => i -> s"str$i").toDF("c1", "c2")
    }

    withTable("appendCSV") {
      createDF(0, 9)
        .write
        .mode(SaveMode.Append)
        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
        .saveAsTable("appendCSV")
      createDF(10, 19)
        .write
        .mode(SaveMode.Append)
        .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
        .saveAsTable("appendCSV")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM appendCSV p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))
    }

    withTable("appendCSV") {
      createDF(0, 9)
        .write
        .mode(SaveMode.Append)
        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
        .saveAsTable("appendCSV")
      createDF(10, 19).write.mode(SaveMode.Append).format("csv").saveAsTable("appendCSV")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM appendCSV p WHERE p.c1 > 5"),
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
          locationUri = Some(tempPath.toURI),
          properties = Map("skipHiveMetadata" -> "true")
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
      checkError(
        exception = intercept[AnalysisException] {
          Seq((3, 4)).toDF("i", "k")
            .write.mode("append").saveAsTable("saveAsTable_mismatch_column_names")
        },
        condition = "_LEGACY_ERROR_TEMP_1162",
        parameters = Map("col" -> "j", "inputColumns" -> "i, k"))
    }
  }

  test("saveAsTable[append]: too many columns") {
    withTable("saveAsTable_too_many_columns") {
      Seq((1, 2)).toDF("i", "j").write.saveAsTable("saveAsTable_too_many_columns")
      checkError(
        exception = intercept[AnalysisException] {
          Seq((3, 4, 5)).toDF("i", "j", "k")
            .write.mode("append").saveAsTable("saveAsTable_too_many_columns")
        },
        condition = "_LEGACY_ERROR_TEMP_1161",
        parameters = Map(
          "tableName" -> "spark_catalog.default.saveastable_too_many_columns",
          "existingTableSchema" -> "struct<i:int,j:int>",
          "querySchema" -> "struct<i:int,j:int,k:int>"))
    }
  }

  test("create a temp view using hive") {
    val tableName = "tab1"
    withTempView(tableName) {
      checkError(
        exception = intercept[AnalysisException] {
          sql(
            s"""
               |CREATE TEMPORARY VIEW $tableName
               |(col1 int)
               |USING hive
             """.stripMargin)
        },
        condition = "_LEGACY_ERROR_TEMP_1293",
        parameters = Map.empty
      )
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

      checkError(
        exception = intercept[AnalysisException] {
          table(tableName).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
        },
        condition = "UNSUPPORTED_OVERWRITE.TABLE",
        parameters = Map("table" -> s"`$SESSION_CATALOG_NAME`.`default`.`tab1`")
      )

      checkError(
        exception = intercept[AnalysisException] {
          table(tableName).write.mode(SaveMode.ErrorIfExists).saveAsTable(tableName)
        },
        condition = "TABLE_OR_VIEW_ALREADY_EXISTS",
        parameters = Map("relationName" -> s"`$SESSION_CATALOG_NAME`.`default`.`tab1`")
      )
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

      checkError(
        exception = intercept[AnalysisException] {
          table(tableName).write.mode(SaveMode.Overwrite).insertInto(tableName)
        },
        condition = "UNSUPPORTED_OVERWRITE.TABLE",
        parameters = Map("table" -> s"`$SESSION_CATALOG_NAME`.`default`.`tab1`")
      )
    }
  }

  test("saveAsTable[append]: less columns") {
    withTable("saveAsTable_less_columns") {
      Seq((1, 2)).toDF("i", "j").write.saveAsTable("saveAsTable_less_columns")
      checkError(
        exception = intercept[AnalysisException] {
          Seq(4).toDF("j").write.mode("append").saveAsTable("saveAsTable_less_columns")
        },
        condition = "_LEGACY_ERROR_TEMP_1161",
        parameters = Map(
          "tableName" -> "spark_catalog.default.saveastable_less_columns",
          "existingTableSchema" -> "struct<i:int,j:int>",
          "querySchema" -> "struct<j:int>"))
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
      val schema = StructType(StructField("int", IntegerType) :: Nil)
      val hiveTableWithoutNumPartsProp = CatalogTable(
        identifier = TableIdentifier("t", Some("default")),
        tableType = CatalogTableType.MANAGED,
        schema = HiveExternalCatalog.EMPTY_DATA_SCHEMA,
        provider = Some("json"),
        storage = CatalogStorageFormat.empty,
        properties = Map(
          DATASOURCE_PROVIDER -> "json",
          DATASOURCE_SCHEMA_PART_PREFIX + 0 -> schema.json))

      hiveClient.createTable(hiveTableWithoutNumPartsProp, ignoreIfExists = false)

      checkError(
        exception = intercept[AnalysisException] {
          sharedState.externalCatalog.getTable("default", "t")
        },
        condition = "INSUFFICIENT_TABLE_PROPERTY.MISSING_KEY",
        parameters = Map("key" -> toSQLConf("spark.sql.sources.schema"))
      )

      val hiveTableWithNumPartsProp = CatalogTable(
        identifier = TableIdentifier("t2", Some("default")),
        tableType = CatalogTableType.MANAGED,
        schema = HiveExternalCatalog.EMPTY_DATA_SCHEMA,
        provider = Some("json"),
        storage = CatalogStorageFormat.empty,
        properties = Map(
          DATASOURCE_PROVIDER -> "json",
          DATASOURCE_SCHEMA_PREFIX + "numParts" -> "3",
          DATASOURCE_SCHEMA_PART_PREFIX + 0 -> schema.json))

      hiveClient.createTable(hiveTableWithNumPartsProp, ignoreIfExists = false)

      checkError(
        exception = intercept[AnalysisException] {
          sharedState.externalCatalog.getTable("default", "t2")
        },
        condition = "INSUFFICIENT_TABLE_PROPERTY.MISSING_KEY_PART",
        parameters = Map(
          "key" -> toSQLConf("spark.sql.sources.schema.part.1"),
          "totalAmountOfParts" -> "3")
      )

      withDebugMode {
        val tableMeta = sharedState.externalCatalog.getTable("default", "t")
        assert(tableMeta.identifier == TableIdentifier("t", Some("default")))
        assert(tableMeta.properties(DATASOURCE_PROVIDER) == "json")
        val tableMeta2 = sharedState.externalCatalog.getTable("default", "t2")
        assert(tableMeta2.identifier == TableIdentifier("t2", Some("default")))
        assert(tableMeta2.properties(DATASOURCE_PROVIDER) == "json")
      }
    } finally {
      hiveClient.dropTable("default", "t", ignoreIfNotExists = true, purge = true)
      hiveClient.dropTable("default", "t2", ignoreIfNotExists = true, purge = true)
    }
  }

  test("should keep data source entries in table properties when debug mode is on") {
    withDebugMode {
      val newSession = sparkSession.newSession()
      newSession.sql("CREATE TABLE abc(i int) USING json")
      val tableMeta = newSession.sessionState.catalog.getTableMetadata(TableIdentifier("abc"))
      assert(tableMeta.properties.contains(DATASOURCE_SCHEMA))
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

  test("SPARK-37283: Don't try to store a V1 table in Hive compatible format " +
    "if the table contains Hive incompatible types") {
    import DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
    import YearMonthIntervalType.{MONTH, YEAR}
    withTable("t") {
      val logAppender = new LogAppender(
        s"Check whether a message is shown and it says that the V1 table contains " +
          "Hive incompatible types if the table contains Hive incompatible types")
      logAppender.setThreshold(Level.WARN)
      withLogAppender(logAppender) {
        sql(
          """
            |CREATE TABLE t(
            |  c1 INTERVAL DAY TO MINUTE,
            |  c2 STRING,
            |  c3 INTERVAL YEAR TO MONTH,
            |  c4 INT,
            |  c5 INTERVAL HOUR,
            |  c6 INTERVAL MONTH,
            |  c7 STRUCT<a: INT, b: STRING>,
            |  c8 STRUCT<a: INT, b: INTERVAL HOUR TO SECOND>,
            |  c9 ARRAY<INT>,
            |  c10 ARRAY<INTERVAL YEAR>,
            |  c11 MAP<INT, STRING>,
            |  c12 MAP<INT, INTERVAL DAY>,
            |  c13 MAP<INTERVAL MINUTE TO SECOND, STRING>,
            |  c14 TIMESTAMP_NTZ
            |) USING Parquet""".stripMargin)
      }
      val expectedMsg = "Hive incompatible types found: interval day to minute, " +
        "interval year to month, interval hour, interval month, " +
        "struct<a:int,b:interval hour to second>, " +
        "array<interval year>, map<int,interval day>, " +
        "map<interval minute to second,string>, timestamp_ntz. " +
        s"Persisting data source table `$SESSION_CATALOG_NAME`.`default`.`t` into Hive " +
        "metastore in Spark SQL specific format, which is NOT compatible with Hive."
      val actualMessages = logAppender.loggingEvents
        .map(_.getMessage.getFormattedMessage)
        .filter(_.contains("incompatible"))
      assert(actualMessages.contains(expectedMsg))
      assert(hiveClient.getTable("default", "t").schema
        .forall(_.dataType == ArrayType(StringType)))

      val df = sql("SELECT * FROM t")
      assert(df.schema ===
        StructType(Seq(
          StructField("c1", DayTimeIntervalType(DAY, MINUTE)),
          StructField("c2", StringType),
          StructField("c3", YearMonthIntervalType(YEAR, MONTH)),
          StructField("c4", IntegerType),
          StructField("c5", DayTimeIntervalType(HOUR)),
          StructField("c6", YearMonthIntervalType(MONTH)),
          StructField("c7",
            StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", StringType)))),
          StructField("c8",
            StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", DayTimeIntervalType(HOUR, SECOND))))),
          StructField("c9", ArrayType(IntegerType)),
          StructField("c10", ArrayType(YearMonthIntervalType(YEAR))),
          StructField("c11", MapType(IntegerType, StringType)),
          StructField("c12", MapType(IntegerType, DayTimeIntervalType(DAY))),
          StructField("c13", MapType(DayTimeIntervalType(MINUTE, SECOND), StringType)),
          StructField("c14", TimestampNTZType))))
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
}
