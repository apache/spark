/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
    }
  }

  test("SPARK-57419-EXT: Stream read text rows nested inside .tar.gz archives") {
    import java.io.{File, FileOutputStream}
    import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
    import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream

    // Utility closure helper inside the test scope to assemble localized tar payloads
    def createTarGzFile(dir: File, fileName: String, contents: Map[String, String]): String = {
      val tarGzFile = new File(dir, fileName)
      val fos = new FileOutputStream(tarGzFile)
      val gzos = new GzipCompressorOutputStream(fos)
      val tos = new TarArchiveOutputStream(gzos)
      try {
        contents.foreach { case (innerFileName, body) =>
          val bytes = body.getBytes("UTF-8")
          val entry = new TarArchiveEntry(innerFileName)
          entry.setSize(bytes.length)
          tos.putArchiveEntry(entry)
          tos.write(bytes)
          tos.closeArchiveEntry()
        }
      } finally {
        tos.close()
        gzos.close()
        fos.close()
      }
      tarGzFile.getAbsolutePath
    }

    withTempDir { dir =>
      val archivePath = createTarGzFile(dir, "logs.tar.gz", Map(
        "app1.log" -> "line_1\nline_2",
        "app2.log" -> "line_3\nline_4"
      ))

      // 1. Verify standard line-by-line sequential extraction behavior when the flag is true
      withSQLConf("spark.sql.files.archive.reader.enabled" -> "true") {
        val df = spark.read.text(archivePath)
        checkAnswer(df, Seq(Row("line_1"), Row("line_2"), Row("line_3"), Row("line_4")))
      }

      // 2. Verify wholetext formatting options map embedded streams into unique single rows
      withSQLConf("spark.sql.files.archive.reader.enabled" -> "true") {
        val dfWhole = spark.read.option("wholetext", "true").text(archivePath)
        checkAnswer(dfWhole, Seq(Row("line_1\nline_2"), Row("line_3\nline_4")))
      }

      // 3. Fallback tracking route validation when the core configuration flag is explicitly disabled
      withSQLConf("spark.sql.files.archive.reader.enabled" -> "false") {
        intercept[Exception] {
          spark.read.text(archivePath).collect()
        }
      }
    }
  }
}