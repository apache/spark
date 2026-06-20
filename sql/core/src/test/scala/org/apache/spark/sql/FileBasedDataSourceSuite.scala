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

package org.apache.spark.sql

import java.io.{File, FileNotFoundException}
import java.net.URI
import java.nio.file.{Files, StandardOpenOption}
import java.time.{LocalDateTime, ZoneOffset}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}

import org.apache.spark.{SparkException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.TestingUDT.{IntervalUDT, NullData, NullUDT}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.expressions.IntegralLiteralTestUtils.{negativeInt, positiveInt}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TimestampNanosTestUtils.foreachNanosPrecision
import org.apache.spark.sql.execution.{FileSourceScanLike, SimpleMode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.TimestampNanosVal
import org.apache.spark.util.HadoopFSUtils

class FileBasedDataSourceSuite extends SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.ORC_IMPLEMENTATION.key, "native")
  }

  override def afterAll(): Unit = {
    try {
      spark.sessionState.conf.unsetConf(SQLConf.ORC_IMPLEMENTATION)
    } finally {
      super.afterAll()
    }
  }

  private val allFileBasedDataSources = Seq("orc", "parquet", "csv", "json", "text")
  private val formatMapping = Map(
    "csv" -> "CSV",
    "json" -> "JSON",
    "parquet" -> "Parquet",
    "orc" -> "ORC",
    "text" -> "Text",
    "xml" -> "XML"
  )
  private val nameWithSpecialChars = "sp&cial%c hars"

  allFileBasedDataSources.foreach { format =>
    test(s"Writing empty datasets should not fail - $format") {
      withTempPath { dir =>
        Seq("str").toDS().limit(0).write.format(format).save(dir.getCanonicalPath)
      }
    }
  }

  // `TEXT` data source always has a single column whose name is `value`.
  allFileBasedDataSources.filterNot(_ == "text").foreach { format =>
    test(s"SPARK-23072 Write and read back unicode column names - $format") {
      withTempPath { path =>
        val dir = path.getCanonicalPath

        // scalastyle:off nonascii
        val df = Seq("a").toDF("한글")
        // scalastyle:on nonascii

        df.write.format(format).option("header", "true").save(dir)
        val answerDf = spark.read.format(format).option("header", "true").load(dir)

        assert(DataTypeUtils.sameType(df.schema, answerDf.schema))
        checkAnswer(df, answerDf)
      }
    }
  }

  // Only ORC/Parquet support this. `CSV` and `JSON` returns an empty schema.
  // `TEXT` data source always has a single column whose name is `value`.
  Seq("orc", "parquet").foreach { format =>
    test(s"SPARK-15474 Write and read back non-empty schema with empty dataframe - $format") {
      withTempPath { file =>
        val path = file.getCanonicalPath
        val emptyDf = Seq((true, 1, "str")).toDF().limit(0)
        emptyDf.write.format(format).save(path)

        val df = spark.read.format(format).load(path)
        assert(DataTypeUtils.sameType(df.schema, emptyDf.schema))
        checkAnswer(df, emptyDf)
      }
    }
  }

  Seq("orc", "parquet").foreach { format =>
    test(s"SPARK-23271 empty RDD when saved should write a metadata only file - $format") {
      withTempPath { outputPath =>
        val df = spark.emptyDataFrame.select(lit(1).as("i"))
        df.write.format(format).save(outputPath.toString)
        val partFiles = outputPath.listFiles()
          .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
        assert(partFiles.length === 1)

        // Now read the file.
        val df1 = spark.read.format(format).load(outputPath.toString)
        checkAnswer(df1, Seq.empty[Row])
        assert(df1.schema.equals(df.schema.asNullable))
      }
    }
  }

  allFileBasedDataSources.foreach { format =>
    test(s"SPARK-23372 error while writing empty schema files using $format") {
      withTempPath { outputPath =>
        checkError(
          exception = intercept[AnalysisException] {
            spark.emptyDataFrame.write.format(format).save(outputPath.toString)
          },
          condition = "EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE",
          parameters = Map("format" -> formatMapping(format))
        )
      }

      // Nested empty schema
      withTempPath { outputPath =>
        val schema = StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StructType(Nil)),
          StructField("c", IntegerType)
        ))
        val df = spark.createDataFrame(sparkContext.emptyRDD[Row], schema)
        checkError(
          exception = intercept[AnalysisException] {
            df.write.format(format).save(outputPath.toString)
          },
          condition = "EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE",
          parameters = Map("format" -> formatMapping(format))
        )
      }
    }
  }

  val emptySchemaSupportedDataSources = Seq("orc", "csv", "json")
  emptySchemaSupportedDataSources.foreach { format =>
    val emptySchemaValidationConf = SQLConf.ALLOW_EMPTY_SCHEMAS_FOR_WRITES.key
    test("SPARK-38651 allow writing empty schema files " +
      s"using $format when ${emptySchemaValidationConf} is enabled") {
      withSQLConf(emptySchemaValidationConf -> "true") {
        withTempPath { outputPath =>
          spark.emptyDataFrame.write.format(format).save(outputPath.toString)
        }
      }
    }
  }

  allFileBasedDataSources.foreach { format =>
    test(s"SPARK-22146 read files containing special characters using $format") {
      withTempDir { dir =>
        val tmpFile = s"$dir/$nameWithSpecialChars"
        spark.createDataset(Seq("a", "b")).write.format(format).save(tmpFile)
        val fileContent = spark.read.format(format).load(tmpFile)
        checkAnswer(fileContent, Seq(Row("a"), Row("b")))
      }
    }
  }

  // Separate test case for formats that support multiLine as an option.
  Seq("json", "csv").foreach { format =>
    test("SPARK-23148 read files containing special characters " +
      s"using $format with multiline enabled") {
      withTempDir { dir =>
        val tmpFile = s"$dir/$nameWithSpecialChars"
        spark.createDataset(Seq("a", "b")).write.format(format).save(tmpFile)
        val reader = spark.read.format(format).option("multiLine", true)
        val fileContent = reader.load(tmpFile)
        checkAnswer(fileContent, Seq(Row("a"), Row("b")))
      }
    }
  }

  allFileBasedDataSources.foreach { format =>
    testQuietly(s"Enabling/disabling ignoreMissingFiles using $format") {
      def testIgnoreMissingFiles(options: Map[String, String]): Unit = {
        withTempDir { dir =>
          val basePath = dir.getCanonicalPath

          Seq("0").toDF("a").write.format(format).save(new Path(basePath, "second").toString)
          Seq("1").toDF("a").write.format(format).save(new Path(basePath, "fourth").toString)

          val firstPath = new Path(basePath, "first")
          val thirdPath = new Path(basePath, "third")
          val fs = thirdPath.getFileSystem(spark.sessionState.newHadoopConf())
          Seq("2").toDF("a").write.format(format).save(firstPath.toString)
          Seq("3").toDF("a").write.format(format).save(thirdPath.toString)
          val files = Seq(firstPath, thirdPath).flatMap { p =>
            fs.listStatus(p).filter(_.isFile).map(_.getPath)
          }

          val df = spark.read.options(options).format(format).load(
            new Path(basePath, "first").toString,
            new Path(basePath, "second").toString,
            new Path(basePath, "third").toString,
            new Path(basePath, "fourth").toString)

          // Make sure all data files are deleted and can't be opened.
          files.foreach(f => fs.delete(f, false))
          assert(fs.delete(thirdPath, true))
          for (f <- files) {
            intercept[FileNotFoundException](fs.open(f))
          }

          checkAnswer(df, Seq(Row("0"), Row("1")))
        }
      }

      // Test set ignoreMissingFiles via SQL Conf and Data Source reader options
      for {
        (ignore, options, sqlConf) <- Seq(
          // Set via SQL Conf: leave options empty
          ("true", Map.empty[String, String], "true"),
          ("false", Map.empty[String, String], "false"),
          // Set via reader options: explicitly set SQL Conf to opposite
          ("true", Map("ignoreMissingFiles" -> "true"), "false"),
          ("false", Map("ignoreMissingFiles" -> "false"), "true"))
        sources <- Seq("", format)
      } {
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> sources,
          SQLConf.IGNORE_MISSING_FILES.key -> sqlConf) {
          if (ignore.toBoolean) {
            testIgnoreMissingFiles(options)
          } else {
            checkErrorMatchPVals(
              exception = intercept[SparkException] {
                testIgnoreMissingFiles(options)
              },
              condition = "FAILED_READ_FILE.FILE_NOT_EXIST",
              parameters = Map("path" -> ".*")
            )
          }
        }
      }
    }
  }

  Seq("json", "orc").foreach { format =>
    test(s"SPARK-32889: column name supports special characters using $format") {
      Seq("$", " ", ",", ";", "{", "}", "(", ")", "\n", "\t", "=").foreach { name =>
        withTempDir { dir =>
          val dataDir = new File(dir, "file").getCanonicalPath
          Seq(1).toDF(name).write.format(format).save(dataDir)
          val schema = spark.read.format(format).load(dataDir).schema
          assert(schema.size == 1)
          assertResult(name)(schema.head.name)
        }
      }
    }
  }

  // Text file format only supports string type
  test("SPARK-24691 error handling for unsupported types - text") {
    withTempDir { dir =>
      // write path
      val textDir = new File(dir, "text").getCanonicalPath
      checkError(
        exception = intercept[AnalysisException] {
          Seq(1).toDF().write.text(textDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`value`",
          "columnType" -> "\"INT\"",
          "format" -> "Text")
      )

      checkError(
        exception = intercept[AnalysisException] {
          Seq(1.2).toDF().write.text(textDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`value`",
          "columnType" -> "\"DOUBLE\"",
          "format" -> "Text")
      )

      checkError(
        exception = intercept[AnalysisException] {
          Seq(true).toDF().write.text(textDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`value`",
          "columnType" -> "\"BOOLEAN\"",
          "format" -> "Text")
      )

      checkError(
        exception = intercept[AnalysisException] {
          Seq(1).toDF("a").selectExpr("struct(a)").write.text(textDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`struct(a)`",
          "columnType" -> "\"STRUCT<a: INT NOT NULL>\"",
          "format" -> "Text")
      )

      checkError(
        exception = intercept[AnalysisException] {
          Seq((Map("Tesla" -> 3))).toDF("cars").write.mode("overwrite").text(textDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`cars`",
          "columnType" -> "\"MAP<STRING, INT>\"",
          "format" -> "Text")
      )

      checkError(
        exception = intercept[AnalysisException] {
          Seq((Array("Tesla", "Chevy", "Ford"))).toDF("brands")
            .write.mode("overwrite").text(textDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`brands`",
          "columnType" -> "\"ARRAY<STRING>\"",
          "format" -> "Text")
      )

      // read path
      Seq("aaa").toDF().write.mode("overwrite").text(textDir)
      checkError(
        exception = intercept[AnalysisException] {
          val schema = StructType(StructField("a", IntegerType, true) :: Nil)
          spark.read.schema(schema).text(textDir).collect()
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`a`",
          "columnType" -> "\"INT\"",
          "format" -> "Text")
      )

      checkError(
        exception = intercept[AnalysisException] {
          val schema = StructType(StructField("a", DoubleType, true) :: Nil)
          spark.read.schema(schema).text(textDir).collect()
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`a`",
          "columnType" -> "\"DOUBLE\"",
          "format" -> "Text")
      )

      checkError(
        exception = intercept[AnalysisException] {
          val schema = StructType(StructField("a", BooleanType, true) :: Nil)
          spark.read.schema(schema).text(textDir).collect()
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`a`",
          "columnType" -> "\"BOOLEAN\"",
          "format" -> "Text")
      )
    }
  }

  // Unsupported data types of csv, json, orc, and parquet are as follows;
  //  csv -> R/W: Null, Array, Map, Struct
  //  json -> R/W: Interval
  //  orc -> R/W: Interval, W: Null
  //  parquet -> R/W: Interval, Null
  test("SPARK-24204 error handling for unsupported Array/Map/Struct types - csv") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      checkError(
        exception = intercept[AnalysisException] {
          Seq((1, "Tesla")).toDF("a", "b").selectExpr("struct(a, b)").write.csv(csvDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`struct(a, b)`",
          "columnType" -> "\"STRUCT<a: INT NOT NULL, b: STRING>\"",
          "format" -> "CSV")
      )

      checkError(
        exception = intercept[AnalysisException] {
          val schema = StructType.fromDDL("a struct<b: Int>")
          spark.range(1).write.mode("overwrite").csv(csvDir)
          spark.read.schema(schema).csv(csvDir).collect()
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`a`",
          "columnType" -> "\"STRUCT<b: INT>\"",
          "format" -> "CSV")
      )

      checkError(
        exception = intercept[AnalysisException] {
          Seq((1, Map("Tesla" -> 3))).toDF("id", "cars").write.mode("overwrite").csv(csvDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`cars`",
          "columnType" -> "\"MAP<STRING, INT>\"",
          "format" -> "CSV")
      )

      checkError(
        exception = intercept[AnalysisException] {
          val schema = StructType.fromDDL("a map<int, int>")
          spark.range(1).write.mode("overwrite").csv(csvDir)
          spark.read.schema(schema).csv(csvDir).collect()
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`a`",
          "columnType" -> "\"MAP<INT, INT>\"",
          "format" -> "CSV")
      )

      checkError(
        exception = intercept[AnalysisException] {
          Seq((1, Array("Tesla", "Chevy", "Ford"))).toDF("id", "brands")
            .write.mode("overwrite").csv(csvDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`brands`",
          "columnType" -> "\"ARRAY<STRING>\"",
          "format" -> "CSV")
      )

      checkError(
        exception = intercept[AnalysisException] {
          val schema = StructType.fromDDL("a array<int>")
          spark.range(1).write.mode("overwrite").csv(csvDir)
          spark.read.schema(schema).csv(csvDir).collect()
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`a`",
          "columnType" -> "\"ARRAY<INT>\"",
          "format" -> "CSV")
      )

      checkError(
        exception = intercept[AnalysisException] {
          Seq((1, new TestUDT.MyDenseVector(Array(0.25, 2.25, 4.25)))).toDF("id", "vectors")
            .write.mode("overwrite").csv(csvDir)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`vectors`",
          "columnType" -> "UDT(\"ARRAY<DOUBLE>\")",
          "format" -> "CSV")
      )

      checkError(
        exception = intercept[AnalysisException] {
          val schema = StructType(StructField("a", new TestUDT.MyDenseVectorUDT(), true) :: Nil)
          spark.range(1).write.mode("overwrite").csv(csvDir)
          spark.read.schema(schema).csv(csvDir).collect()
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`a`",
          "columnType" -> "UDT(\"ARRAY<DOUBLE>\")",
          "format" -> "CSV")
      )
    }
  }

  test("SPARK-24204 error handling for unsupported Interval data types - csv, json, parquet, orc") {
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      Seq(true, false).foreach { useV1 =>
        val useV1List = if (useV1) {
          "csv,json,orc,parquet"
        } else {
          ""
        }
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> useV1List,
          SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
          // write path
          Seq("csv", "json", "parquet", "orc").foreach { format =>
            checkError(
              exception = intercept[AnalysisException] {
                sql("select interval 1 days").write.format(format).mode("overwrite").save(tempDir)
              },
              condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
              parameters = Map(
                "format" -> formatMapping(format),
                "columnName" -> "`INTERVAL '1 days'`",
                "columnType" -> "\"INTERVAL\"")
            )
          }

          // read path
          Seq("parquet", "csv").foreach { format =>
            val formatParameter = format match {
              case "parquet" => "Parquet"
              case _ => "CSV"
            }
            checkError(
              exception = intercept[AnalysisException] {
                val schema = StructType(StructField("a", CalendarIntervalType, true) :: Nil)
                spark.range(1).write.format(format).mode("overwrite").save(tempDir)
                spark.read.schema(schema).format(format).load(tempDir).collect()
              },
              condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
              parameters = Map(
                "columnName" -> "`a`",
                "columnType" -> "\"INTERVAL\"",
                "format" -> formatParameter
              )
            )
            checkError(
              exception = intercept[AnalysisException] {
                val schema = StructType(StructField("a", new IntervalUDT(), true) :: Nil)
                spark.range(1).write.format(format).mode("overwrite").save(tempDir)
                spark.read.schema(schema).format(format).load(tempDir).collect()
              },
              condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
              parameters = Map(
                "columnName" -> "`a`",
                "columnType" -> "UDT(\"INTERVAL\")",
                "format" -> formatParameter
              )
            )
          }
        }
      }
    }
  }

  test("SPARK-24204 error handling for unsupported Null data types - csv, orc") {
    Seq(true, false).foreach { useV1 =>
      val useV1List = if (useV1) {
        "csv,orc"
      } else {
        ""
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
        withTempDir { dir =>
          val tempDir = new File(dir, "files").getCanonicalPath

          Seq("csv", "orc").foreach { format =>
            // write path
            checkError(
              exception = intercept[AnalysisException] {
                sql("select null").write.format(format).mode("overwrite").save(tempDir)
              },
              condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
              parameters = Map(
                "columnName" -> "`NULL`",
                "columnType" -> "\"VOID\"",
                "format" -> formatMapping(format)
              )
            )

            checkError(
              exception = intercept[AnalysisException] {
                spark.udf.register("testType", () => new NullData())
                sql("select testType()").write.format(format).mode("overwrite").save(tempDir)
              },
              condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
              parameters = Map(
                "columnName" -> "`testType()`",
                "columnType" -> "UDT(\"VOID\")",
                "format" -> formatMapping(format)
              )
            )

            // read path
            checkError(
              exception = intercept[AnalysisException] {
                val schema = StructType(StructField("a", NullType, true) :: Nil)
                spark.range(1).write.format(format).mode("overwrite").save(tempDir)
                spark.read.schema(schema).format(format).load(tempDir).collect()
              },
              condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
              parameters = Map(
                "columnName" -> "`a`",
                "columnType" -> "\"VOID\"",
                "format" -> formatMapping(format)
              )
            )

            checkError(
              exception = intercept[AnalysisException] {
                val schema = StructType(StructField("a", new NullUDT(), true) :: Nil)
                spark.range(1).write.format(format).mode("overwrite").save(tempDir)
                spark.read.schema(schema).format(format).load(tempDir).collect()
              },
              condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
              parameters = Map(
                "columnName" -> "`a`",
                "columnType" -> "UDT(\"VOID\")",
                "format" -> formatMapping(format)
              )
            )
          }
        }
      }
    }
  }

  Seq("parquet", "orc").foreach { format =>
    test(s"Spark native readers should respect spark.sql.caseSensitive - ${format}") {
      withTempDir { dir =>
        val tableName = s"spark_25132_${format}_native"
        val tableDir = dir.getCanonicalPath + s"/$tableName"
        withTable(tableName) {
          val end = 5
          val data = spark.range(end).selectExpr("id as A", "id * 2 as b", "id * 3 as B")
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            data.write.format(format).mode("overwrite").save(tableDir)
          }
          sql(s"CREATE TABLE $tableName (a LONG, b LONG) USING $format LOCATION '$tableDir'")

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
            checkAnswer(sql(s"select a from $tableName"), data.select("A"))
            checkAnswer(sql(s"select A from $tableName"), data.select("A"))

            // RuntimeException is triggered at executor side, which is then wrapped as
            // SparkException at driver side
            checkError(
              exception = intercept[SparkException] {
                sql(s"select b from $tableName").collect()
              }.getCause.asInstanceOf[SparkRuntimeException],
              condition = "_LEGACY_ERROR_TEMP_2093",
              parameters = Map("requiredFieldName" -> "b", "matchedOrcFields" -> "[b, B]")
            )
            checkError(
              exception = intercept[SparkException] {
                sql(s"select B from $tableName").collect()
              }.getCause.asInstanceOf[SparkRuntimeException],
              condition = "_LEGACY_ERROR_TEMP_2093",
              parameters = Map("requiredFieldName" -> "b", "matchedOrcFields" -> "[b, B]")
            )
          }

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            checkAnswer(sql(s"select a from $tableName"), (0 until end).map(_ => Row(null)))
            checkAnswer(sql(s"select b from $tableName"), data.select("b"))
          }
        }
      }
    }
  }

  test("SPARK-25237 compute correct input metrics in FileScanRDD") {
    // TODO: Test CSV V2 as well after it implements [[SupportsReportStatistics]].
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "csv") {
      withTempPath { p =>
        val path = p.getAbsolutePath
        spark.range(1000).repartition(1).write.csv(path)
        val bytesReads = new mutable.ArrayBuffer[Long]()
        val bytesReadListener = new SparkListener() {
          override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
            bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
          }
        }
        sparkContext.addSparkListener(bytesReadListener)
        try {
          spark.read.csv(path).limit(1).collect()
          sparkContext.listenerBus.waitUntilEmpty()
          assert(bytesReads.sum === 7860)
        } finally {
          sparkContext.removeSparkListener(bytesReadListener)
        }
      }
    }
  }

  test("SPARK-30362: test input metrics for DSV2") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      Seq("json", "orc", "parquet").foreach { format =>
        withTempPath { path =>
          val dir = path.getCanonicalPath
          spark.range(0, 10).write.format(format).save(dir)
          val df = spark.read.format(format).load(dir)
          val bytesReads = new mutable.ArrayBuffer[Long]()
          val recordsRead = new mutable.ArrayBuffer[Long]()
          val bytesReadListener = new SparkListener() {
            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
              bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
              recordsRead += taskEnd.taskMetrics.inputMetrics.recordsRead
            }
          }
          sparkContext.addSparkListener(bytesReadListener)
          try {
            df.collect()
            sparkContext.listenerBus.waitUntilEmpty()
            assert(bytesReads.sum > 0)
            assert(recordsRead.sum == 10)
          } finally {
            sparkContext.removeSparkListener(bytesReadListener)
          }
        }
      }
    }
  }

  test("SPARK-37585: test input metrics for DSV2 with output limits") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      Seq("json", "orc", "parquet").foreach { format =>
        withTempPath { path =>
          val dir = path.getCanonicalPath
          spark.range(0, 100).write.format(format).save(dir)
          val df = spark.read.format(format).load(dir)
          val bytesReads = new mutable.ArrayBuffer[Long]()
          val recordsRead = new mutable.ArrayBuffer[Long]()
          val bytesReadListener = new SparkListener() {
            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
              bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
              recordsRead += taskEnd.taskMetrics.inputMetrics.recordsRead
            }
          }
          sparkContext.addSparkListener(bytesReadListener)
          try {
            df.limit(10).collect()
            sparkContext.listenerBus.waitUntilEmpty()
            assert(bytesReads.sum > 0)
            assert(recordsRead.sum > 0)
          } finally {
            sparkContext.removeSparkListener(bytesReadListener)
          }
        }
      }
    }
  }

  test("Do not use cache on overwrite") {
    Seq("", "orc").foreach { useV1SourceReaderList =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceReaderList) {
        withTempDir { dir =>
          val path = dir.toString
          spark.range(1000).write.mode("overwrite").orc(path)
          val df = spark.read.orc(path).cache()
          assert(df.count() == 1000)
          spark.range(10).write.mode("overwrite").orc(path)
          assert(df.count() == 10)
          assert(spark.read.orc(path).count() == 10)
        }
      }
    }
  }

  test("Do not use cache on append") {
    Seq("", "orc").foreach { useV1SourceReaderList =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceReaderList) {
        withTempDir { dir =>
          val path = dir.toString
          spark.range(1000).write.mode("append").orc(path)
          val df = spark.read.orc(path).cache()
          assert(df.count() == 1000)
          spark.range(10).write.mode("append").orc(path)
          assert(df.count() == 1010)
          assert(spark.read.orc(path).count() == 1010)
        }
      }
    }
  }

  test("UDF input_file_name()") {
    Seq("", "orc").foreach { useV1SourceReaderList =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceReaderList) {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          spark.range(10).write.orc(path)
          val row = spark.read.orc(path).select(input_file_name()).first()
          assert(row.getString(0).contains(path))
        }
      }
    }
  }

  test("Option recursiveFileLookup: recursive loading correctly") {

    val expectedFileList = mutable.ListBuffer[String]()

    def createFile(dir: File, fileName: String, format: String): Unit = {
      val path = new File(dir, s"${fileName}.${format}")
      Files.write(
        path.toPath,
        s"content of ${path.toString}".getBytes,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE
      )
      val fsPath = new Path(path.getAbsoluteFile.toURI).toString
      expectedFileList.append(fsPath)
    }

    def createDir(path: File, dirName: String, level: Int): Unit = {
      val dir = new File(path, s"dir${dirName}-${level}")
      dir.mkdir()
      createFile(dir, s"file${level}", "bin")
      createFile(dir, s"file${level}", "text")

      if (level < 4) {
        // create sub-dir
        createDir(dir, "sub0", level + 1)
        createDir(dir, "sub1", level + 1)
      }
    }

    withTempPath { path =>
      path.mkdir()
      createDir(path, "root", 0)

      val dataPath = new File(path, "dirroot-0").getAbsolutePath
      val fileList = spark.read.format("binaryFile")
        .option("recursiveFileLookup", true)
        .load(dataPath)
        .select("path").collect().map(_.getString(0))

      assert(fileList.toSet === expectedFileList.toSet)

      withClue("SPARK-32368: 'recursiveFileLookup' and 'pathGlobFilter' can be case insensitive") {
        val fileList2 = spark.read.format("binaryFile")
          .option("RecuRsivefileLookup", true)
          .option("PaThglobFilter", "*.bin")
          .load(dataPath)
          .select("path").collect().map(_.getString(0))

        assert(fileList2.toSet === expectedFileList.filter(_.endsWith(".bin")).toSet)
      }
    }
  }

  test("Option recursiveFileLookup: disable partition inferring") {
    val dataPath = Thread.currentThread().getContextClassLoader
      .getResource("test-data/text-partitioned").toString

    val df = spark.read.format("binaryFile")
      .option("recursiveFileLookup", true)
      .load(dataPath)

    assert(!df.columns.contains("year"), "Expect partition inferring disabled")
    val fileList = df.select("path").collect().map(_.getString(0))

    val expectedFileList = Array(
      dataPath + "/year=2014/data.txt",
      dataPath + "/year=2015/data.txt"
    ).map(path => new Path(path).toString)

    assert(fileList.toSet === expectedFileList.toSet)
  }

  test("recursiveFileLookup with a partitioned catalog table is rejected") {
    withTable("part_tbl") {
      sql(
        """
          |CREATE TABLE part_tbl (id INT, value STRING)
          |USING parquet
          |PARTITIONED BY (year INT)
          |""".stripMargin)
      sql("INSERT INTO part_tbl PARTITION (year = 2024) VALUES (1, 'a')")
      sql("INSERT INTO part_tbl PARTITION (year = 2025) VALUES (2, 'b')")
      checkError(
        exception = intercept[AnalysisException] {
          spark.read
            .option("recursiveFileLookup", "true")
            .table("part_tbl")
            .collect()
        },
        condition = "RECURSIVE_FILE_LOOKUP_NOT_SUPPORTED_FOR_PARTITIONED_DATA_SOURCE",
        parameters = Map.empty[String, String]
      )
    }
  }

  test("Return correct results when data columns overlap with partition columns") {
    Seq("parquet", "orc", "json").foreach { format =>
      withTempPath { path =>
        val tablePath = new File(s"${path.getCanonicalPath}/cOl3=c/cOl1=a/cOl5=e")
        Seq((1, 2, 3, 4, 5)).toDF("cOl1", "cOl2", "cOl3", "cOl4", "cOl5")
          .write.format(format).save(tablePath.getCanonicalPath)

        val df = spark.read.format(format).load(path.getCanonicalPath)
          .select("CoL1", "Col2", "CoL5", "CoL3")
        checkAnswer(df, Row("a", 2, "e", "c"))
      }
    }
  }

  test("Return correct results when data columns overlap with partition columns (nested data)") {
    Seq("parquet", "orc", "json").foreach { format =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        withTempPath { path =>
          val tablePath = new File(s"${path.getCanonicalPath}/c3=c/c1=a/c5=e")

          val inputDF = sql("SELECT 1 c1, 2 c2, 3 c3, named_struct('c4_1', 2, 'c4_2', 3) c4, 5 c5")
          inputDF.write.format(format).save(tablePath.getCanonicalPath)

          val resultDF = spark.read.format(format).load(path.getCanonicalPath)
            .select("c1", "c4.c4_1", "c5", "c3")
          checkAnswer(resultDF, Row("a", 2, "e", "c"))
        }
      }
    }
  }

  test("sizeInBytes should be the total size of all files") {
    Seq("orc", "").foreach { useV1SourceReaderList =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceReaderList) {
        withTempDir { dir =>
          dir.delete()
          spark.range(1000).write.orc(dir.toString)
          val df = spark.read.orc(dir.toString)
          assert(df.queryExecution.optimizedPlan.stats.sizeInBytes === BigInt(getLocalDirSize(dir)))
        }
      }
    }
  }

  test("SPARK-22790,SPARK-27668: spark.sql.sources.compressionFactor takes effect") {
    Seq(1.0, 0.5).foreach { compressionFactor =>
      withSQLConf(SQLConf.FILE_COMPRESSION_FACTOR.key -> compressionFactor.toString,
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "457") {
        withTempPath { workDir =>
          // the file size is 504 bytes
          val workDirPath = workDir.getAbsolutePath
          val data1 = Seq(100, 200, 300, 400).toDF("count")
          data1.write.orc(workDirPath + "/data1")
          val df1FromFile = spark.read.orc(workDirPath + "/data1")
          val data2 = Seq(100, 200, 300, 400).toDF("count")
          data2.write.orc(workDirPath + "/data2")
          val df2FromFile = spark.read.orc(workDirPath + "/data2")
          val joinedDF = df1FromFile.join(df2FromFile, Seq("count"))
          if (compressionFactor == 0.5) {
            val bJoinExec = collect(joinedDF.queryExecution.executedPlan) {
              case bJoin: BroadcastHashJoinExec => bJoin
            }
            assert(bJoinExec.nonEmpty)
            val smJoinExec = collect(joinedDF.queryExecution.executedPlan) {
              case smJoin: SortMergeJoinExec => smJoin
            }
            assert(smJoinExec.isEmpty)
          } else {
            // compressionFactor is 1.0
            val bJoinExec = collect(joinedDF.queryExecution.executedPlan) {
              case bJoin: BroadcastHashJoinExec => bJoin
            }
            assert(bJoinExec.isEmpty)
            val smJoinExec = collect(joinedDF.queryExecution.executedPlan) {
              case smJoin: SortMergeJoinExec => smJoin
            }
            assert(smJoinExec.nonEmpty)
          }
        }
      }
    }
  }

  test("SPARK-36568: FileScan statistics estimation takes read schema into account") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempDir { dir =>
        spark.range(1000).map(x => (x / 100, x, x)).toDF("k", "v1", "v2").
          write.partitionBy("k").mode(SaveMode.Overwrite).orc(dir.toString)
        val dfAll = spark.read.orc(dir.toString)
        val dfK = dfAll.select("k")
        val dfV1 = dfAll.select("v1")
        val dfV2 = dfAll.select("v2")
        val dfV1V2 = dfAll.select("v1", "v2")

        def sizeInBytes(df: DataFrame): BigInt = df.queryExecution.optimizedPlan.stats.sizeInBytes

        assert(sizeInBytes(dfAll) === BigInt(getLocalDirSize(dir)))
        assert(sizeInBytes(dfK) < sizeInBytes(dfAll))
        assert(sizeInBytes(dfV1) < sizeInBytes(dfAll))
        assert(sizeInBytes(dfV2) === sizeInBytes(dfV1))
        assert(sizeInBytes(dfV1V2) < sizeInBytes(dfAll))
      }
    }
  }

  test("File source v2: support partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      allFileBasedDataSources.foreach { format =>
        withTempPath { dir =>
          Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
            .toDF("value", "p1", "p2")
            .write
            .format(format)
            .partitionBy("p1", "p2")
            .option("header", true)
            .save(dir.getCanonicalPath)
          val df = spark
            .read
            .format(format)
            .option("header", true)
            .load(dir.getCanonicalPath)
            .where("p1 = 1 and p2 = 2 and value != \"a\"")

          val filterCondition = df.queryExecution.optimizedPlan.collectFirst {
            case f: Filter => f.condition
          }
          assert(filterCondition.isDefined)
          // The partitions filters should be pushed down and no need to be reevaluated.
          assert(!filterCondition.get.exists {
            case a: AttributeReference => a.name == "p1" || a.name == "p2"
            case _ => false
          })

          val fileScan = df.queryExecution.executedPlan collectFirst {
            case BatchScanExec(_, f: FileScan, _, _, _, _) => f
          }
          assert(fileScan.nonEmpty)
          assert(fileScan.get.partitionFilters.nonEmpty)
          assert(fileScan.get.dataFilters.nonEmpty)
          assert(fileScan.get.planInputPartitions().forall { partition =>
            partition.asInstanceOf[FilePartition].files.forall { file =>
              file.urlEncodedPath.contains("p1=1") &&
                file.urlEncodedPath.contains("p2=2")
            }
          })
          checkAnswer(df, Row("b", 1, 2))
        }
      }
    }
  }

  test("File source v2: support passing data filters to FileScan without partitionFilters") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      allFileBasedDataSources.foreach { format =>
        withTempPath { dir =>
          Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
            .toDF("value", "p1", "p2")
            .write
            .format(format)
            .partitionBy("p1", "p2")
            .option("header", true)
            .save(dir.getCanonicalPath)
          val df = spark
            .read
            .format(format)
            .option("header", true)
            .load(dir.getCanonicalPath)
            .where("value = 'a'")

          val filterCondition = df.queryExecution.optimizedPlan.collectFirst {
            case f: Filter => f.condition
          }
          assert(filterCondition.isDefined)

          val fileScan = df.queryExecution.executedPlan collectFirst {
            case BatchScanExec(_, f: FileScan, _, _, _, _) => f
          }
          assert(fileScan.nonEmpty)
          assert(fileScan.get.partitionFilters.isEmpty)
          assert(fileScan.get.dataFilters.nonEmpty)
          checkAnswer(df, Row("a", 1, 2))
        }
      }
    }
  }

  test("SPARK-31116: Select nested schema with case insensitive mode") {
    // This test case failed at only Parquet. ORC is added for test coverage parity.
    Seq("orc", "parquet").foreach { format =>
      Seq("true", "false").foreach { nestedSchemaPruningEnabled =>
        withSQLConf(
          SQLConf.CASE_SENSITIVE.key -> "false",
          SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> nestedSchemaPruningEnabled) {
          withTempPath { dir =>
            val path = dir.getCanonicalPath

            // Prepare values for testing nested parquet data
            spark
              .range(1L)
              .selectExpr("NAMED_STRUCT('lowercase', id, 'camelCase', id + 1) AS StructColumn")
              .write
              .format(format)
              .save(path)

            val exactSchema = "StructColumn struct<lowercase: LONG, camelCase: LONG>"

            checkAnswer(spark.read.schema(exactSchema).format(format).load(path), Row(Row(0, 1)))

            // In case insensitive manner, parquet's column cases are ignored
            val innerColumnCaseInsensitiveSchema =
              "StructColumn struct<Lowercase: LONG, camelcase: LONG>"
            checkAnswer(
              spark.read.schema(innerColumnCaseInsensitiveSchema).format(format).load(path),
              Row(Row(0, 1)))

            val rootColumnCaseInsensitiveSchema =
              "structColumn struct<lowercase: LONG, camelCase: LONG>"
            checkAnswer(
              spark.read.schema(rootColumnCaseInsensitiveSchema).format(format).load(path),
              Row(Row(0, 1)))
          }
        }
      }
    }
  }

  test("test casts pushdown on orc/parquet for integral types") {
    def checkPushedFilters(
        format: String,
        df: DataFrame,
        filters: Array[sources.Filter],
        noScan: Boolean = false): Unit = {
      val scanExec = df.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec])
      if (noScan) {
        assert(scanExec.isEmpty)
        return
      }
      val scan = scanExec.get.asInstanceOf[BatchScanExec].scan
      format match {
        case "orc" =>
          assert(scan.isInstanceOf[OrcScan])
          assert(scan.asInstanceOf[OrcScan].pushedFilters === filters)
        case "parquet" =>
          assert(scan.isInstanceOf[ParquetScan])
          assert(scan.asInstanceOf[ParquetScan].pushedFilters === filters)
        case _ =>
          fail(s"unknown format $format")
      }
    }

    Seq("orc", "parquet").foreach { format =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        withTempPath { dir =>
          spark.range(100).map(i => (i.toShort, i.toString)).toDF("id", "s")
            .write
            .format(format)
            .save(dir.getCanonicalPath)
          val df = spark.read.format(format).load(dir.getCanonicalPath)

          // cases when value == MAX
          var v = Short.MaxValue
          checkPushedFilters(format, df.where($"id" > v.toInt), Array(), noScan = true)
          checkPushedFilters(format, df.where($"id" >= v.toInt),
            Array(sources.IsNotNull("id"), sources.EqualTo("id", v)))
          checkPushedFilters(format, df.where($"id" === v.toInt),
            Array(sources.IsNotNull("id"), sources.EqualTo("id", v)))
          checkPushedFilters(format, df.where($"id" <=> v.toInt),
            Array(sources.EqualNullSafe("id", v)))
          checkPushedFilters(format, df.where($"id" <= v.toInt),
            Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where($"id" < v.toInt),
            Array(sources.IsNotNull("id"), sources.Not(sources.EqualTo("id", v))))

          // cases when value > MAX
          var v1: Int = positiveInt
          checkPushedFilters(format, df.where($"id" > v1), Array(), noScan = true)
          checkPushedFilters(format, df.where($"id" >= v1), Array(), noScan = true)
          checkPushedFilters(format, df.where($"id" === v1), Array(), noScan = true)
          checkPushedFilters(format, df.where($"id" <=> v1), Array(), noScan = true)
          checkPushedFilters(format, df.where($"id" <= v1), Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where($"id" < v1), Array(sources.IsNotNull("id")))

          // cases when value = MIN
          v = Short.MinValue
          checkPushedFilters(format, df.where(lit(v.toInt) < $"id"),
            Array(sources.IsNotNull("id"), sources.Not(sources.EqualTo("id", v))))
          checkPushedFilters(format, df.where(lit(v.toInt) <= $"id"),
            Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where(lit(v.toInt) === $"id"),
            Array(sources.IsNotNull("id"),
            sources.EqualTo("id", v)))
          checkPushedFilters(format, df.where(lit(v.toInt) <=> $"id"),
            Array(sources.EqualNullSafe("id", v)))
          checkPushedFilters(format, df.where(lit(v.toInt) >= $"id"),
            Array(sources.IsNotNull("id"), sources.EqualTo("id", v)))
          checkPushedFilters(format, df.where(lit(v.toInt) > $"id"), Array(), noScan = true)

          // cases when value < MIN
          v1 = negativeInt
          checkPushedFilters(format, df.where(lit(v1) < $"id"),
            Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where(lit(v1) <= $"id"),
            Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where(lit(v1) === $"id"), Array(), noScan = true)
          checkPushedFilters(format, df.where(lit(v1) >= $"id"), Array(), noScan = true)
          checkPushedFilters(format, df.where(lit(v1) > $"id"), Array(), noScan = true)

          // cases when value is within range (MIN, MAX)
          checkPushedFilters(format, df.where($"id" > 30), Array(sources.IsNotNull("id"),
            sources.GreaterThan("id", 30)))
          checkPushedFilters(format, df.where(lit(100) >= $"id"),
            Array(sources.IsNotNull("id"), sources.LessThanOrEqual("id", 100)))
        }
      }
    }
  }

  test("SPARK-32827: Set max metadata string length") {
    withTempDir { dir =>
      val tableName = "t"
      val path = s"${dir.getCanonicalPath}/$tableName"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName(c INT) USING PARQUET LOCATION '$path'")
        withSQLConf(SQLConf.MAX_METADATA_STRING_LENGTH.key -> "5") {
          val explain = spark.table(tableName).queryExecution.explainString(SimpleMode)
          assert(!explain.contains(path))
          // metadata has abbreviated by ...
          assert(explain.contains("..."))
        }

        withSQLConf(SQLConf.MAX_METADATA_STRING_LENGTH.key -> "1000") {
          val explain = spark.table(tableName).queryExecution.explainString(SimpleMode)
          assert(explain.contains(path))
          assert(!explain.contains("..."))
        }
      }
    }
  }

  test("SPARK-35669: special char in CSV header with filter pushdown") {
    withTempPath { path =>
      val pathStr = path.getCanonicalPath
      Seq("a / b,a`b", "v1,v2").toDF().coalesce(1).write.text(pathStr)
      val df = spark.read.option("header", true).csv(pathStr)
        .where($"a / b".isNotNull and $"`a``b`".isNotNull)
      checkAnswer(df, Row("v1", "v2"))
    }
  }

  test("SPARK-41017: filter pushdown with nondeterministic predicates") {
    withTempPath { path =>
      val pathStr = path.getCanonicalPath
      spark.range(10).write.parquet(pathStr)
      Seq("parquet", "").foreach { useV1SourceList =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceList) {
          val scan = spark.read.parquet(pathStr)
          val df = scan.where(rand() > 0.5 && $"id" > 5)
          val filters = df.queryExecution.executedPlan.collect {
            case f: FileSourceScanLike => f.dataFilters
            case b: BatchScanExec => b.scan.asInstanceOf[FileScan].dataFilters
          }.flatten
          assert(filters.contains(GreaterThan(scan.logicalPlan.output.head, Literal(5L))))
        }
      }
    }
  }

  test("SPARK-51590: unsupported the TIME data types in data sources") {
    val datasources = Seq("text")
    Seq(true, false).foreach { useV1 =>
      val useV1List = if (useV1) {
        datasources.mkString(",")
      } else {
        ""
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
        withTempDir { dir =>
          val tempDir = new File(dir, "files").getCanonicalPath
          datasources.foreach { format =>
            checkError(
              exception = intercept[AnalysisException] {
                sql("select time'12:01:02' as t")
                  .write.format(format).mode("overwrite").save(tempDir)
              },
              condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
              parameters = Map(
                "columnName" -> "`t`",
                "columnType" -> s"\"${TimeType().sql}\"",
                "format" -> formatMapping(format)))
          }
        }
      }
    }
  }

  test("Geospatial types are not supported in file data sources other than Parquet") {
    // All of these file formats do NOT support geospatial types (GEOMETRY and GEOGRAPHY).
    val unsupportedDataSources = Seq("csv", "json", "orc", "text", "xml")
    // Test both v1 and v2 data sources.
    Seq(true, false).foreach { useV1 =>
      val useV1List = if (useV1) {
        unsupportedDataSources.mkString(",")
      } else {
        ""
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
        withTempDir { dir =>
          // Temporary directory for writing the test data.
          val tempDir = new File(dir, "files").getCanonicalPath
          // Test data: WKB representation of POINT(1 2).
          val wkb = "0101000000000000000000F03F0000000000000040"
          // Test GEOMETRY and GEOGRAPHY data types.
          val geoTestCases = Seq(
            (s"ST_GeomFromWKB(X'$wkb')", "\"GEOMETRY(0)\""),
            (s"ST_GeogFromWKB(X'$wkb')", "\"GEOGRAPHY(4326)\"")
          )
          unsupportedDataSources.foreach { format =>
            geoTestCases.foreach { case (expr, expectedType) =>
              checkError(
                exception = intercept[AnalysisException] {
                  sql(s"select $expr as g").write.format(format).mode("overwrite").save(tempDir)
                },
                condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
                parameters = Map(
                  "columnName" -> "`g`",
                  "columnType" -> expectedType,
                  "format" -> formatMapping(format)))
            }
          }
        }
      }
    }
  }

  test("SPARK-57166: nanosecond timestamp types are not supported in selected file data sources") {
    // Parquet and ORC support nanosecond-capable timestamps, while these formats still reject them.
    val unsupportedDataSources = Seq("json")
    val nanosTypes = Seq(TimestampNTZNanosType(9), TimestampLTZNanosType(9))
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      // Test both v1 and v2 data sources.
      Seq(true, false).foreach { useV1 =>
        val useV1List = if (useV1) {
          unsupportedDataSources.mkString(",")
        } else {
          ""
        }
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
          unsupportedDataSources.foreach { format =>
            nanosTypes.foreach { nanosType =>
              val expectedType = s""""${nanosType.sql}""""
              withTempDir { dir =>
                // Write path: a nanos-typed column cannot be written. The nanos literal is built
                // directly from its internal value to avoid relying on cast/parser support.
                val nanosLiteral =
                  Literal.create(new TimestampNanosVal(0L, 0.toShort), nanosType)
                val df = spark.range(1).select(Column(nanosLiteral).as("ts"))
                val writeDir = new File(dir, "write").getCanonicalPath
                checkError(
                  exception = intercept[AnalysisException] {
                    df.write.format(format).mode("overwrite").save(writeDir)
                  },
                  condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
                  parameters = Map(
                    "columnName" -> "`ts`",
                    "columnType" -> expectedType,
                    "format" -> formatMapping(format)))

                // Read path: a user-specified nanos schema is rejected. Write a benign file first
                // so schema validation (not file listing) is what fails.
                val readDir = new File(dir, "read").getCanonicalPath
                // XML requires a `rowTag` option on both the read and write paths.
                val extraOptions =
                  if (format == "xml") Map("rowTag" -> "row") else Map.empty[String, String]
                Seq("a").toDF("ts").write.format(format).options(extraOptions)
                  .mode("overwrite").save(readDir)
                checkError(
                  exception = intercept[AnalysisException] {
                    spark.read.schema(new StructType().add("ts", nanosType))
                      .format(format).options(extraOptions).load(readDir).collect()
                  },
                  condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
                  parameters = Map(
                    "columnName" -> "`ts`",
                    "columnType" -> expectedType,
                    "format" -> formatMapping(format)))
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-57166: ORC supports nanosecond timestamp types in v1 and v2") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      // Validate both v1 and v2 ORC paths.
      Seq(true, false).foreach { useV1 =>
        val useV1List = if (useV1) "orc" else ""
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
          foreachNanosPrecision { precision =>
            Seq(TimestampNTZNanosType(precision), TimestampLTZNanosType(precision)).foreach {
              nanosType =>
                withTempDir { dir =>
                  // Build the row from an external java.time value; the column schema carries the
                  // precision and truncates the sub-microsecond digits, matching the ORC suites.
                  val wallClock = LocalDateTime.of(1970, 1, 1, 0, 20, 34, 567890123)
                  val value: Any = nanosType match {
                    case _: TimestampNTZNanosType => wallClock
                    case _: TimestampLTZNanosType => wallClock.toInstant(ZoneOffset.UTC)
                  }
                  val df = spark.createDataFrame(
                    spark.sparkContext.parallelize(Seq(Row(value))),
                    new StructType().add("ts", nanosType))
                  val path = new File(dir, s"orc_nanos_${nanosType.typeName}").getCanonicalPath
                  df.write.format("orc").mode("overwrite").save(path)
                  val readBack = spark.read.schema(new StructType().add("ts", nanosType))
                    .format("orc").load(path)
                  checkAnswer(readBack, df)
                }
            }
          }
        }
      }
    }
  }

  test("SPARK-57457: CSV supports nanosecond timestamp types in v1 and v2") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      Seq(true, false).foreach { useV1 =>
        val useV1List = if (useV1) "csv" else ""
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
          foreachNanosPrecision { precision =>
            // CSV is text-based: the format string must carry enough fractional-second digits to
            // represent the full precision. Use exactly `precision` S-characters so the emitted
            // string is compact and unambiguously round-trips at the given precision.
            val fracPat = "S" * precision
            Seq(TimestampNTZNanosType(precision), TimestampLTZNanosType(precision)).foreach {
              nanosType =>
                withTempDir { dir =>
                  val wallClock = LocalDateTime.of(1970, 1, 1, 0, 20, 34, 567890123)
                  val value: Any = nanosType match {
                    case _: TimestampNTZNanosType => wallClock
                    case _: TimestampLTZNanosType => wallClock.toInstant(ZoneOffset.UTC)
                  }
                  val df = spark.createDataFrame(
                    spark.sparkContext.parallelize(Seq(Row(value))),
                    new StructType().add("ts", nanosType))
                  val path = new File(dir, s"csv_nanos_${nanosType.typeName}").getCanonicalPath
                  val (fmtKey, fmtVal) = nanosType match {
                    case _: TimestampNTZNanosType =>
                      ("timestampNTZFormat", s"yyyy-MM-dd'T'HH:mm:ss.$fracPat")
                    case _: TimestampLTZNanosType =>
                      ("timestampFormat", s"yyyy-MM-dd'T'HH:mm:ss.${fracPat}XXX")
                  }
                  df.write.format("csv").option(fmtKey, fmtVal).mode("overwrite").save(path)
                  val readBack = spark.read
                    .schema(new StructType().add("ts", nanosType))
                    .option(fmtKey, fmtVal)
                    .format("csv").load(path)
                  checkAnswer(readBack, df)
                }
            }
          }
        }
      }
    }
  }

  // Asserts the ignoredPathSegmentRegex contract for `format`: the default regex hides the
  // '_'-prefixed file; a never-matching per-read option or session conf each surface it; the
  // option overrides the conf.
  private def checkIgnoredPathSegmentRegexContract(format: String, basePath: String): Unit = {
    val defaultRegex = HadoopFSUtils.DEFAULT_IGNORED_PATH_SEGMENT_REGEX

    // Default: the hidden file is filtered out.
    checkAnswer(spark.read.format(format).load(basePath), Seq(Row("visible")))

    // A never-matching per-read option surfaces the hidden data.
    checkAnswer(
      spark.read.option("ignoredPathSegmentRegex", "(?!)").format(format).load(basePath),
      Seq(Row("visible"), Row("hidden")))

    // A never-matching session conf surfaces the hidden data too.
    withSQLConf(SQLConf.IGNORED_PATH_SEGMENT_REGEX.key -> "(?!)") {
      checkAnswer(
        spark.read.format(format).load(basePath),
        Seq(Row("visible"), Row("hidden")))
    }

    // Precedence: the per-read option overrides the session conf in both directions.
    withSQLConf(SQLConf.IGNORED_PATH_SEGMENT_REGEX.key -> defaultRegex) {
      checkAnswer(
        spark.read.option("ignoredPathSegmentRegex", "(?!)").format(format).load(basePath),
        Seq(Row("visible"), Row("hidden")))
    }
    withSQLConf(SQLConf.IGNORED_PATH_SEGMENT_REGEX.key -> "(?!)") {
      checkAnswer(
        spark.read.option("ignoredPathSegmentRegex", defaultRegex).format(format).load(basePath),
        Seq(Row("visible")))
    }
  }

  test("Option ignoredPathSegmentRegex: surfaces hidden files only when relaxed - json") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      // Write JSON files directly: a visible record and a '_'-prefixed hidden record.
      Files.write(new File(dir, "visible.json").toPath, """{"a":"visible"}""".getBytes)
      Files.write(new File(dir, "_hidden.json").toPath, """{"a":"hidden"}""".getBytes)

      checkIgnoredPathSegmentRegexContract("json", basePath)
    }
  }

  test("Option ignoredPathSegmentRegex: surfaces hidden files only when relaxed - parquet") {
    withTempDir { dir =>
      // Copy only the part-*.parquet from a scratch write into basePath -- this drops Spark's own
      // _SUCCESS / _temporary marker files, so a relaxed ignoredPathSegmentRegex surfaces only the
      // intended hidden file. Flat layout (files directly under basePath) mirrors the JSON test.
      val basePath = dir.getCanonicalPath

      def copyPartFile(value: String, destName: String): Unit = {
        withTempDir { scratch =>
          // withTempDir pre-creates the directory, so overwrite to avoid PATH_ALREADY_EXISTS.
          Seq(value).toDF("a").write.format("parquet").mode("overwrite")
            .save(scratch.getCanonicalPath)
          val partFile = scratch.listFiles()
            .find(f => f.isFile && f.getName.startsWith("part-") && f.getName.endsWith(".parquet"))
            .getOrElse(fail(s"no part file produced under ${scratch.getCanonicalPath}"))
          Files.copy(partFile.toPath, new File(dir, destName).toPath)
        }
      }
      copyPartFile("visible", "visible.parquet")
      copyPartFile("hidden", "_hidden.parquet")

      checkIgnoredPathSegmentRegexContract("parquet", basePath)
    }
  }

  test("Option ignoredPathSegmentRegex: reads an unmodified Spark-written parquet dir") {
    withTempDir { dir =>
      // A normal Spark write leaves a zero-length _SUCCESS marker next to the part files. With
      // a never-matching ignoredPathSegmentRegex, the marker is surfaced by listing but must
      // neither be picked as a schema inference footer candidate nor scanned as data.
      val basePath = dir.getCanonicalPath
      // withTempDir pre-creates the directory, so overwrite to avoid PATH_ALREADY_EXISTS.
      Seq("a", "b").toDF("col").write.mode("overwrite").parquet(basePath)
      assert(new File(dir, "_SUCCESS").exists())

      checkAnswer(
        spark.read.option("ignoredPathSegmentRegex", "(?!)").parquet(basePath),
        Seq(Row("a"), Row("b")))
    }
  }

  test("Option ignoredPathSegmentRegex: reads a partitioned Spark-written parquet dir") {
    withTempDir { dir =>
      // partitionBy leaves the zero-length _SUCCESS marker at the table root next to the 'p='
      // partition dirs. Under a never-matching ignoredPathSegmentRegex the marker survives listing,
      // but partition discovery still works (parsePartition stops at the base path) and the
      // zero-length marker is neither a schema inference footer candidate nor scanned as data.
      val basePath = dir.getCanonicalPath
      // withTempDir pre-creates the directory, so overwrite to avoid PATH_ALREADY_EXISTS.
      Seq((1, "a"), (2, "b")).toDF("p", "v").write.mode("overwrite").partitionBy("p")
        .parquet(basePath)
      assert(new File(dir, "_SUCCESS").exists())

      // The read-back schema appends the discovered partition column after the data column.
      val expected = Seq(Row("a", 1), Row("b", 2))
      checkAnswer(spark.read.parquet(basePath), expected)
      checkAnswer(
        spark.read.option("ignoredPathSegmentRegex", "(?!)").parquet(basePath),
        expected)
    }
  }

  test("Option ignoredPathSegmentRegex: custom regex replaces the default filter - json") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      // The default '^[._]' hides both the '_'- and '.'-prefixed files. A custom '^_' regex
      // replaces it entirely: it keeps hiding the '_'-prefixed file but surfaces the
      // '.'-prefixed one.
      Files.write(new File(dir, "visible.json").toPath, """{"a":"visible"}""".getBytes)
      Files.write(new File(dir, "_under.json").toPath, """{"a":"under"}""".getBytes)
      Files.write(new File(dir, ".dot.json").toPath, """{"a":"dot"}""".getBytes)

      checkAnswer(spark.read.format("json").load(basePath), Seq(Row("visible")))
      checkAnswer(
        spark.read.option("ignoredPathSegmentRegex", "^_").format("json").load(basePath),
        Seq(Row("visible"), Row("dot")))
    }
  }

  test("Option ignoredPathSegmentRegex: invalid regexes are rejected") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      Files.write(new File(dir, "visible.json").toPath, """{"a":"visible"}""".getBytes)

      // An invalid per-read regex is surfaced as a clear IllegalArgumentException when the read
      // resolves the relation, instead of a raw PatternSyntaxException deep in file listing.
      val optionError = intercept[IllegalArgumentException] {
        spark.read.option("ignoredPathSegmentRegex", "[").format("json").load(basePath)
      }
      assert(optionError.getMessage.contains("ignoredPathSegmentRegex"))

      // The session conf rejects invalid values eagerly via checkValue.
      val confError = intercept[IllegalArgumentException] {
        spark.conf.set(SQLConf.IGNORED_PATH_SEGMENT_REGEX.key, "[")
      }
      assert(confError.getMessage.contains("valid Java regular expression"))
    }
  }

  test("Option ignoredPathSegmentRegex: an empty value disables hidden-file filtering") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      Files.write(new File(dir, "visible.json").toPath, """{"a":"visible"}""".getBytes)
      Files.write(new File(dir, "_hidden.json").toPath, """{"a":"hidden"}""".getBytes)

      // The default '^[._]' hides the '_'-prefixed file.
      checkAnswer(spark.read.format("json").load(basePath), Seq(Row("visible")))
      // An empty per-read option disables the generic hidden-file filter and surfaces it.
      checkAnswer(
        spark.read.option("ignoredPathSegmentRegex", "").format("json").load(basePath),
        Seq(Row("visible"), Row("hidden")))
      // An empty session conf does too.
      withSQLConf(SQLConf.IGNORED_PATH_SEGMENT_REGEX.key -> "") {
        checkAnswer(spark.read.format("json").load(basePath), Seq(Row("visible"), Row("hidden")))
      }
    }
  }

  test("SPARK-57458: XML supports nanosecond timestamp types") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      foreachNanosPrecision { precision =>
        Seq(TimestampNTZNanosType(precision), TimestampLTZNanosType(precision)).foreach {
          nanosType =>
            withTempDir { dir =>
              val wallClock = LocalDateTime.of(1970, 1, 1, 0, 20, 34, 567890123)
              val (value, fmtKey, fmtVal) = nanosType match {
                case _: TimestampNTZNanosType =>
                  (wallClock.asInstanceOf[Any],
                    "timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS")
                case _: TimestampLTZNanosType =>
                  (wallClock.toInstant(ZoneOffset.UTC).asInstanceOf[Any],
                    "timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX")
              }
              val df = spark.createDataFrame(
                spark.sparkContext.parallelize(Seq(Row(value))),
                new StructType().add("ts", nanosType))
              val path = new File(dir, s"xml_nanos_${nanosType.typeName}").getCanonicalPath
              df.write.format("xml").option("rowTag", "row")
                .option(fmtKey, fmtVal).mode("overwrite").save(path)
              val readBack = spark.read.schema(new StructType().add("ts", nanosType))
                .format("xml").option("rowTag", "row").option(fmtKey, fmtVal).load(path)
              checkAnswer(readBack, df)
            }
        }
      }
    }
  }
}

object TestingUDT {

  @SQLUserDefinedType(udt = classOf[IntervalUDT])
  class IntervalData extends Serializable

  class IntervalUDT extends UserDefinedType[IntervalData] {

    override def sqlType: DataType = CalendarIntervalType
    override def serialize(obj: IntervalData): Any =
      throw SparkUnsupportedOperationException()
    override def deserialize(datum: Any): IntervalData =
      throw SparkUnsupportedOperationException()
    override def userClass: Class[IntervalData] = classOf[IntervalData]
  }

  @SQLUserDefinedType(udt = classOf[NullUDT])
  private[sql] class NullData extends Serializable

  private[sql] class NullUDT extends UserDefinedType[NullData] {

    override def sqlType: DataType = NullType
    override def serialize(obj: NullData): Any =
      throw SparkUnsupportedOperationException()
    override def deserialize(datum: Any): NullData =
      throw SparkUnsupportedOperationException()
    override def userClass: Class[NullData] = classOf[NullData]
  }
}

class FakeFileSystemRequiringDSOption extends LocalFileSystem {
  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    require(conf.get("ds_option", "") == "value")
  }
}
