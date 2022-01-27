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

import java.io.File
import java.net.URI
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem

import org.apache.spark.SparkException
import org.apache.spark.sql.TestingUDT.{IntervalUDT, NullData, NullUDT}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.IntegralLiteralTestUtils.{negativeInt, positiveInt}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._


abstract class FileDataSourceSuiteBase extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  protected def format: String

  // subclasses can override this to exclude certain tests by name
  // useful when inheriting a test suite but do not want to run all tests in it
  def excluded: Seq[String] = Seq.empty

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.conf.setConf(SQLConf.ORC_IMPLEMENTATION, "native")
    tests.foreach { case (name, testCode) =>
      if (!excluded.contains(name)) {
        test(name) {
          testCode
        }
      }
    }
  }

  override def afterAll(): Unit = {
    try {
      spark.sessionState.conf.unsetConf(SQLConf.ORC_IMPLEMENTATION)
    } finally {
      super.afterAll()
    }
  }

  private val nameWithSpecialChars = "sp&cial%c hars"

  lazy val tests: Map[String, () => Any] = Map(
    // `TEXT` data source always has a single column whose name is `value`.
    "SPARK-23072: Write and read back unicode column names" ->
      (() => withTempPath { path =>
        val dir = path.getCanonicalPath

        // scalastyle:off nonascii
        val df = Seq("a").toDF("한글")
        // scalastyle:on nonascii

        df.write.format(format).option("header", "true").save(dir)
        val answerDf = spark.read.format(format).option("header", "true").load(dir)

        assert(df.schema.sameType(answerDf.schema))
        checkAnswer(df, answerDf)
      }),
    // Only ORC/Parquet support this. `CSV` and `JSON` returns an empty schema.
    // `TEXT` data source always has a single column whose name is `value`.
    "SPARK-15474: Write and read back non-empty schema with empty dataframe" ->
      (() => withTempPath { file =>
        val path = file.getCanonicalPath
        val emptyDf = Seq((true, 1, "str")).toDF().limit(0)
        emptyDf.write.format(format).save(path)

        val df = spark.read.format(format).load(path)
        assert(df.schema.sameType(emptyDf.schema))
        checkAnswer(df, emptyDf)
      }),
    "SPARK-23271: empty RDD when saved should write a metadata only file" ->
      (() => withTempPath { outputPath =>
        val df = spark.emptyDataFrame.select(lit(1).as("i"))
        df.write.format(format).save(outputPath.toString)
        val partFiles = outputPath.listFiles()
          .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
        assert(partFiles.length === 1)

        // Now read the file.
        val df1 = spark.read.format(format).load(outputPath.toString)
        checkAnswer(df1, Seq.empty[Row])
        assert(df1.schema.equals(df.schema.asNullable))
      }),
    // Separate test case for formats that support multiLine as an option.
    "SPARK-23148: read files containing special characters with multiline enabled" ->
      (() => withTempDir { dir =>
        val tmpFile = s"$dir/$nameWithSpecialChars"
        spark.createDataset(Seq("a", "b")).write.format(format).save(tmpFile)
        val reader = spark.read.format(format).option("multiLine", true)
        val fileContent = reader.load(tmpFile)
        checkAnswer(fileContent, Seq(Row("a"), Row("b")))
      }),
    "SPARK-32889: column name supports special characters" ->
      (() => Seq("$", " ", ",", ";", "{", "}", "(", ")", "\n", "\t", "=").foreach { name =>
        withTempDir { dir =>
          val dataDir = new File(dir, "file").getCanonicalPath
          Seq(1).toDF(name).write.format(format).save(dataDir)
          val schema = spark.read.format(format).load(dataDir).schema
          assert(schema.size == 1)
          assertResult(name)(schema.head.name)
        }
      }),
    "Spark native readers should respect spark.sql.caseSensitive" ->
      (() => withTempDir { dir =>
        val tableName = s"spark_25132_${format}_native"
        val tableDir = dir.getCanonicalPath + s"/$tableName"
        withTable(tableName) {
          val end = 5
          val data = spark.range(end).selectExpr("id as A", "id * 2 as b", "id * 3 as B")
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            data.write.format(format).mode("overwrite").save(tableDir)
          }
          sql(
            s"""
               | CREATE TABLE $tableName (a LONG, b LONG)
               | USING $format LOCATION '$tableDir'
            """.stripMargin)

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
            checkAnswer(sql(s"select a from $tableName"), data.select("A"))
            checkAnswer(sql(s"select A from $tableName"), data.select("A"))

            // RuntimeException is triggered at executor side, which is then wrapped as
            // SparkException at driver side
            val e1 = intercept[SparkException] {
              sql(s"select b from $tableName").collect()
            }
            assert(
              e1.getCause.isInstanceOf[RuntimeException] &&
                e1.getCause.getMessage.contains(
                  """Found duplicate field(s) "b": [b, B] in case-insensitive mode"""))
            val e2 = intercept[SparkException] {
              sql(s"select B from $tableName").collect()
            }
            assert(
              e2.getCause.isInstanceOf[RuntimeException] &&
                e2.getCause.getMessage.contains(
                  """Found duplicate field(s) "b": [b, B] in case-insensitive mode"""))
          }

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            checkAnswer(sql(s"select a from $tableName"), (0 until end).map(_ => Row(null)))
            checkAnswer(sql(s"select b from $tableName"), data.select("b"))
          }
        }
      }),
    "SPARK-24204: error handling for unsupported Interval data types" ->
      (() => withTempDir { dir =>
        val tempDir = new File(dir, "files").getCanonicalPath
        // TODO: test file source V2 after write path is fixed.
        Seq(true).foreach { useV1 =>
          val useV1List = if (useV1) {
            format
          } else {
            ""
          }

          def validateErrorMessage(msg: String): Unit = {
            val msg1 = "cannot save interval data type into external storage."
            val msg2 = "data source does not support interval data type."
            assert(msg.toLowerCase(Locale.ROOT).contains(msg1) ||
              msg.toLowerCase(Locale.ROOT).contains(msg2))
          }

          withSQLConf(
            SQLConf.USE_V1_SOURCE_LIST.key -> useV1List,
            SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
            // write path
            Seq("csv", "json", "parquet", "orc").foreach { format =>
              val msg = intercept[AnalysisException] {
                sql("select interval 1 days").write.format(format).mode("overwrite").save(tempDir)
              }.getMessage
              validateErrorMessage(msg)
            }

            // read path
            Seq("parquet", "csv").foreach { format =>
              var msg = intercept[AnalysisException] {
                val schema = StructType(StructField("a", CalendarIntervalType, true) :: Nil)
                spark.range(1).write.format(format).mode("overwrite").save(tempDir)
                spark.read.schema(schema).format(format).load(tempDir).collect()
              }.getMessage
              validateErrorMessage(msg)

              msg = intercept[AnalysisException] {
                val schema = StructType(StructField("a", new IntervalUDT(), true) :: Nil)
                spark.range(1).write.format(format).mode("overwrite").save(tempDir)
                spark.read.schema(schema).format(format).load(tempDir).collect()
              }.getMessage
              validateErrorMessage(msg)
            }
          }
        }
      }),
    "SPARK-24204: error handling for unsupported Null data types" ->
      (() =>
        // TODO: test file source V2 after write path is fixed.
        Seq(true).foreach { useV1 =>
          val useV1List = if (useV1) {
            "csv,orc,parquet"
          } else {
            ""
          }

          def errorMessage(format: String): String = {
            s"$format data source does not support void data type."
          }

          withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
            withTempDir { dir =>
              val tempDir = new File(dir, "files").getCanonicalPath

              Seq("parquet", "csv", "orc").foreach { format =>
                // write path
                var msg = intercept[AnalysisException] {
                  sql("select null").write.format(format).mode("overwrite").save(tempDir)
                }.getMessage
                assert(msg.toLowerCase(Locale.ROOT)
                  .contains(errorMessage(format)))

                msg = intercept[AnalysisException] {
                  spark.udf.register("testType", () => new NullData())
                  sql("select testType()").write.format(format).mode("overwrite").save(tempDir)
                }.getMessage
                assert(msg.toLowerCase(Locale.ROOT)
                  .contains(errorMessage(format)))

                // read path
                msg = intercept[AnalysisException] {
                  val schema = StructType(StructField("a", NullType, true) :: Nil)
                  spark.range(1).write.format(format).mode("overwrite").save(tempDir)
                  spark.read.schema(schema).format(format).load(tempDir).collect()
                }.getMessage
                assert(msg.toLowerCase(Locale.ROOT)
                  .contains(errorMessage(format)))

                msg = intercept[AnalysisException] {
                  val schema = StructType(StructField("a", new NullUDT(), true) :: Nil)
                  spark.range(1).write.format(format).mode("overwrite").save(tempDir)
                  spark.read.schema(schema).format(format).load(tempDir).collect()
                }.getMessage
                assert(msg.toLowerCase(Locale.ROOT)
                  .contains(errorMessage(format)))
              }
            }
          }
        }),
    "Return correct results when data columns overlap with partition columns" ->
      (() => withTempPath { path =>
        val tablePath = new File(s"${path.getCanonicalPath}/cOl3=c/cOl1=a/cOl5=e")
        Seq((1, 2, 3, 4, 5)).toDF("cOl1", "cOl2", "cOl3", "cOl4", "cOl5")
          .write.format(format).save(tablePath.getCanonicalPath)

        val df = spark.read.format(format).load(path.getCanonicalPath)
          .select("CoL1", "Col2", "CoL5", "CoL3")
        checkAnswer(df, Row("a", 2, "e", "c"))
      }),
    "Return correct results when data columns overlap with partition columns (nested data)" ->
      (() => withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        withTempPath { path =>
          val tablePath = new File(s"${path.getCanonicalPath}/c3=c/c1=a/c5=e")

          val inputDF = sql("SELECT 1 c1, 2 c2, 3 c3, named_struct('c4_1', 2, 'c4_2', 3) c4, 5 c5")
          inputDF.write.format(format).save(tablePath.getCanonicalPath)

          val resultDF = spark.read.format(format).load(path.getCanonicalPath)
            .select("c1", "c4.c4_1", "c5", "c3")
          checkAnswer(resultDF, Row("a", 2, "e", "c"))
        }
      }),
    "SPARK-31116: Select nested schema with case insensitive mode" ->
      (() => Seq("true", "false").foreach { nestedSchemaPruningEnabled =>
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
      }),
    "test casts pushdown on orc/parquet for integral types" ->
      (() => withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        withTempPath { dir =>
          spark.range(100).map(i => (i.toShort, i.toString)).toDF("id", "s")
            .write
            .format(format)
            .save(dir.getCanonicalPath)
          val df = spark.read.format(format).load(dir.getCanonicalPath)

          // cases when value == MAX
          var v = Short.MaxValue
          checkPushedFilters(format, df.where('id > v.toInt), Array(), noScan = true)
          checkPushedFilters(format, df.where('id >= v.toInt), Array(sources.IsNotNull("id"),
            sources.EqualTo("id", v)))
          checkPushedFilters(format, df.where('id === v.toInt), Array(sources.IsNotNull("id"),
            sources.EqualTo("id", v)))
          checkPushedFilters(format, df.where('id <=> v.toInt),
            Array(sources.EqualNullSafe("id", v)))
          checkPushedFilters(format, df.where('id <= v.toInt), Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where('id < v.toInt), Array(sources.IsNotNull("id"),
            sources.Not(sources.EqualTo("id", v))))

          // cases when value > MAX
          var v1: Int = positiveInt
          checkPushedFilters(format, df.where('id > v1), Array(), noScan = true)
          checkPushedFilters(format, df.where('id >= v1), Array(), noScan = true)
          checkPushedFilters(format, df.where('id === v1), Array(), noScan = true)
          checkPushedFilters(format, df.where('id <=> v1), Array(), noScan = true)
          checkPushedFilters(format, df.where('id <= v1), Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where('id < v1), Array(sources.IsNotNull("id")))

          // cases when value = MIN
          v = Short.MinValue
          checkPushedFilters(format, df.where(lit(v.toInt) < 'id), Array(sources.IsNotNull("id"),
            sources.Not(sources.EqualTo("id", v))))
          checkPushedFilters(format, df.where(lit(v.toInt) <= 'id), Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where(lit(v.toInt) === 'id), Array(sources.IsNotNull("id"),
            sources.EqualTo("id", v)))
          checkPushedFilters(format, df.where(lit(v.toInt) <=> 'id),
            Array(sources.EqualNullSafe("id", v)))
          checkPushedFilters(format, df.where(lit(v.toInt) >= 'id), Array(sources.IsNotNull("id"),
            sources.EqualTo("id", v)))
          checkPushedFilters(format, df.where(lit(v.toInt) > 'id), Array(), noScan = true)

          // cases when value < MIN
          v1 = negativeInt
          checkPushedFilters(format, df.where(lit(v1) < 'id), Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where(lit(v1) <= 'id), Array(sources.IsNotNull("id")))
          checkPushedFilters(format, df.where(lit(v1) === 'id), Array(), noScan = true)
          checkPushedFilters(format, df.where(lit(v1) >= 'id), Array(), noScan = true)
          checkPushedFilters(format, df.where(lit(v1) > 'id), Array(), noScan = true)

          // cases when value is within range (MIN, MAX)
          checkPushedFilters(format, df.where('id > 30), Array(sources.IsNotNull("id"),
            sources.GreaterThan("id", 30)))
          checkPushedFilters(format, df.where(lit(100) >= 'id), Array(sources.IsNotNull("id"),
            sources.LessThanOrEqual("id", 100)))
        }
      })
  )

  test("Writing empty datasets should not fail") {
    withTempPath { dir =>
      Seq("str").toDS().limit(0).write.format(format).save(dir.getCanonicalPath)
    }
  }

  test("SPARK-23372 error while writing empty schema files using") {
    withTempPath { outputPath =>
      val errMsg = intercept[AnalysisException] {
        spark.emptyDataFrame.write.format(format).save(outputPath.toString)
      }
      assert(errMsg.getMessage.contains(
        "Datasource does not support writing empty or nested empty schemas"))
    }

    // Nested empty schema
    withTempPath { outputPath =>
      val schema = StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StructType(Nil)),
        StructField("c", IntegerType)
      ))
      val df = spark.createDataFrame(sparkContext.emptyRDD[Row], schema)
      val errMsg = intercept[AnalysisException] {
        df.write.format(format).save(outputPath.toString)
      }
      assert(errMsg.getMessage.contains(
        "Datasource does not support writing empty or nested empty schemas"))
    }
  }

  test("SPARK-22146 read files containing special characters using") {
    withTempDir { dir =>
      val tmpFile = s"$dir/$nameWithSpecialChars"
      spark.createDataset(Seq("a", "b")).write.format(format).save(tmpFile)
      val fileContent = spark.read.format(format).load(tmpFile)
      checkAnswer(fileContent, Seq(Row("a"), Row("b")))
    }
  }

  test("File source v2: support partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
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
        assert(filterCondition.get.collectFirst {
          case a: AttributeReference if a.name == "p1" || a.name == "p2" => a
        }.isEmpty)

        val fileScan = df.queryExecution.executedPlan collectFirst {
          case BatchScanExec(_, f: FileScan, _) => f
        }
        assert(fileScan.nonEmpty)
        assert(fileScan.get.partitionFilters.nonEmpty)
        assert(fileScan.get.dataFilters.nonEmpty)
        assert(fileScan.get.planInputPartitions().forall { partition =>
          partition.asInstanceOf[FilePartition].files.forall { file =>
            file.filePath.contains("p1=1") && file.filePath.contains("p2=2")
          }
        })
        checkAnswer(df, Row("b", 1, 2))
      }
    }
  }

  test("File source v2: support passing data filters to FileScan without partitionFilters") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
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
          case BatchScanExec(_, f: FileScan, _) => f
        }
        assert(fileScan.nonEmpty)
        assert(fileScan.get.partitionFilters.isEmpty)
        assert(fileScan.get.dataFilters.nonEmpty)
        checkAnswer(df, Row("a", 1, 2))
      }
    }
  }

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
}

object TestingUDT {

  @SQLUserDefinedType(udt = classOf[IntervalUDT])
  class IntervalData extends Serializable

  class IntervalUDT extends UserDefinedType[IntervalData] {

    override def sqlType: DataType = CalendarIntervalType
    override def serialize(obj: IntervalData): Any =
      throw new UnsupportedOperationException("Not implemented")
    override def deserialize(datum: Any): IntervalData =
      throw new UnsupportedOperationException("Not implemented")
    override def userClass: Class[IntervalData] = classOf[IntervalData]
  }

  @SQLUserDefinedType(udt = classOf[NullUDT])
  private[sql] class NullData extends Serializable

  private[sql] class NullUDT extends UserDefinedType[NullData] {

    override def sqlType: DataType = NullType
    override def serialize(obj: NullData): Any =
      throw new UnsupportedOperationException("Not implemented")
    override def deserialize(datum: Any): NullData =
      throw new UnsupportedOperationException("Not implemented")
    override def userClass: Class[NullData] = classOf[NullData]
  }
}

class FakeFileSystemRequiringDSOption extends LocalFileSystem {
  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    require(conf.get("ds_option", "") == "value")
  }
}
