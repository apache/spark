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
import java.util.Locale

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.TestingUDT.{IntervalData, IntervalUDT, NullData, NullUDT}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._


class FileBasedDataSourceSuite extends QueryTest with SharedSQLContext with BeforeAndAfterAll {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.conf.setConf(SQLConf.ORC_IMPLEMENTATION, "native")
  }

  override def afterAll(): Unit = {
    try {
      spark.sessionState.conf.unsetConf(SQLConf.ORC_IMPLEMENTATION)
    } finally {
      super.afterAll()
    }
  }

  private val allFileBasedDataSources = Seq("orc", "parquet", "csv", "json", "text")
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

        assert(df.schema.sameType(answerDf.schema))
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
        assert(df.schema.sameType(emptyDf.schema))
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
      def testIgnoreMissingFiles(): Unit = {
        withTempDir { dir =>
          val basePath = dir.getCanonicalPath

          Seq("0").toDF("a").write.format(format).save(new Path(basePath, "first").toString)
          Seq("1").toDF("a").write.format(format).save(new Path(basePath, "second").toString)

          val thirdPath = new Path(basePath, "third")
          val fs = thirdPath.getFileSystem(spark.sessionState.newHadoopConf())
          Seq("2").toDF("a").write.format(format).save(thirdPath.toString)
          val files = fs.listStatus(thirdPath).filter(_.isFile).map(_.getPath)

          val df = spark.read.format(format).load(
            new Path(basePath, "first").toString,
            new Path(basePath, "second").toString,
            new Path(basePath, "third").toString)

          // Make sure all data files are deleted and can't be opened.
          files.foreach(f => fs.delete(f, false))
          assert(fs.delete(thirdPath, true))
          for (f <- files) {
            intercept[FileNotFoundException](fs.open(f))
          }

          checkAnswer(df, Seq(Row("0"), Row("1")))
        }
      }

      withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> "true") {
        testIgnoreMissingFiles()
      }

      withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> "false") {
        val exception = intercept[SparkException] {
          testIgnoreMissingFiles()
        }
        assert(exception.getMessage().contains("does not exist"))
      }
    }
  }

  // Text file format only supports string type
  test("SPARK-24691 error handling for unsupported types - text") {
    withTempDir { dir =>
      // write path
      val textDir = new File(dir, "text").getCanonicalPath
      var msg = intercept[AnalysisException] {
        Seq(1).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support int data type"))

      msg = intercept[AnalysisException] {
        Seq(1.2).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support double data type"))

      msg = intercept[AnalysisException] {
        Seq(true).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support boolean data type"))

      msg = intercept[AnalysisException] {
        Seq(1).toDF("a").selectExpr("struct(a)").write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support struct<a:int> data type"))

      msg = intercept[AnalysisException] {
        Seq((Map("Tesla" -> 3))).toDF("cars").write.mode("overwrite").text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support map<string,int> data type"))

      msg = intercept[AnalysisException] {
        Seq((Array("Tesla", "Chevy", "Ford"))).toDF("brands")
          .write.mode("overwrite").text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support array<string> data type"))

      // read path
      Seq("aaa").toDF.write.mode("overwrite").text(textDir)
      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", IntegerType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support int data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", DoubleType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support double data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", BooleanType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support boolean data type"))
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
      var msg = intercept[AnalysisException] {
        Seq((1, "Tesla")).toDF("a", "b").selectExpr("struct(a, b)").write.csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support struct<a:int,b:string> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType.fromDDL("a struct<b: Int>")
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support struct<b:int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, Map("Tesla" -> 3))).toDF("id", "cars").write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support map<string,int> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType.fromDDL("a map<int, int>")
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support map<int,int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, Array("Tesla", "Chevy", "Ford"))).toDF("id", "brands")
          .write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support array<string> data type"))

      msg = intercept[AnalysisException] {
         val schema = StructType.fromDDL("a array<int>")
         spark.range(1).write.mode("overwrite").csv(csvDir)
         spark.read.schema(schema).csv(csvDir).collect()
       }.getMessage
      assert(msg.contains("CSV data source does not support array<int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, new UDT.MyDenseVector(Array(0.25, 2.25, 4.25)))).toDF("id", "vectors")
          .write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support array<double> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", new UDT.MyDenseVectorUDT(), true) :: Nil)
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support array<double> data type."))
    }
  }

  test("SPARK-24204 error handling for unsupported Interval data types - csv, json, parquet, orc") {
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath

      // write path
      Seq("csv", "json", "parquet", "orc").foreach { format =>
        var msg = intercept[AnalysisException] {
          sql("select interval 1 days").write.format(format).mode("overwrite").save(tempDir)
        }.getMessage
        assert(msg.contains("Cannot save interval data type into external storage."))

        msg = intercept[AnalysisException] {
          spark.udf.register("testType", () => new IntervalData())
          sql("select testType()").write.format(format).mode("overwrite").save(tempDir)
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support calendarinterval data type."))
      }

      // read path
      Seq("parquet", "csv").foreach { format =>
        var msg = intercept[AnalysisException] {
          val schema = StructType(StructField("a", CalendarIntervalType, true) :: Nil)
          spark.range(1).write.format(format).mode("overwrite").save(tempDir)
          spark.read.schema(schema).format(format).load(tempDir).collect()
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support calendarinterval data type."))

        msg = intercept[AnalysisException] {
          val schema = StructType(StructField("a", new IntervalUDT(), true) :: Nil)
          spark.range(1).write.format(format).mode("overwrite").save(tempDir)
          spark.read.schema(schema).format(format).load(tempDir).collect()
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support calendarinterval data type."))
      }
    }
  }

  test("SPARK-24204 error handling for unsupported Null data types - csv, parquet, orc") {
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath

      Seq("orc").foreach { format =>
        // write path
        var msg = intercept[AnalysisException] {
          sql("select null").write.format(format).mode("overwrite").save(tempDir)
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support null data type."))

        msg = intercept[AnalysisException] {
          spark.udf.register("testType", () => new NullData())
          sql("select testType()").write.format(format).mode("overwrite").save(tempDir)
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support null data type."))

        // read path
        // We expect the types below should be passed for backward-compatibility

        // Null type
        var schema = StructType(StructField("a", NullType, true) :: Nil)
        spark.range(1).write.format(format).mode("overwrite").save(tempDir)
        spark.read.schema(schema).format(format).load(tempDir).collect()

        // UDT having null data
        schema = StructType(StructField("a", new NullUDT(), true) :: Nil)
        spark.range(1).write.format(format).mode("overwrite").save(tempDir)
        spark.read.schema(schema).format(format).load(tempDir).collect()
      }

      Seq("parquet", "csv").foreach { format =>
        // write path
        var msg = intercept[AnalysisException] {
          sql("select null").write.format(format).mode("overwrite").save(tempDir)
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support null data type."))

        msg = intercept[AnalysisException] {
          spark.udf.register("testType", () => new NullData())
          sql("select testType()").write.format(format).mode("overwrite").save(tempDir)
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support null data type."))

        // read path
        msg = intercept[AnalysisException] {
          val schema = StructType(StructField("a", NullType, true) :: Nil)
          spark.range(1).write.format(format).mode("overwrite").save(tempDir)
          spark.read.schema(schema).format(format).load(tempDir).collect()
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support null data type."))

        msg = intercept[AnalysisException] {
          val schema = StructType(StructField("a", new NullUDT(), true) :: Nil)
          spark.range(1).write.format(format).mode("overwrite").save(tempDir)
          spark.read.schema(schema).format(format).load(tempDir).collect()
        }.getMessage
        assert(msg.toLowerCase(Locale.ROOT)
          .contains(s"$format data source does not support null data type."))
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
      }
    }
  }

  test("SPARK-31935: Hadoop file system config should be effective in data source options") {
    withSQLConf(
      "fs.file.impl" -> classOf[FakeFileSystemRequiringDSOption].getName,
      "fs.file.impl.disable.cache" -> "true") {
      withTempDir { dir =>
        val path = "file:" + dir.getCanonicalPath.stripPrefix("file:")
        spark.range(10).write.option("ds_option", "value").mode("overwrite").parquet(path)
        checkAnswer(
          spark.read.option("ds_option", "value").parquet(path), spark.range(10).toDF())
      }
    }
  }

  test("SPARK-25237 compute correct input metrics in FileScanRDD") {
    withTempPath { p =>
      val path = p.getAbsolutePath
      spark.range(1000).repartition(1).write.csv(path)
      val bytesReads = new mutable.ArrayBuffer[Long]()
      val bytesReadListener = new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
          bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
        }
      }
      sparkContext.addSparkListener(bytesReadListener)
      try {
        spark.read.csv(path).limit(1).collect()
        sparkContext.listenerBus.waitUntilEmpty(1000L)
        assert(bytesReads.sum === 7860)
      } finally {
        sparkContext.removeSparkListener(bytesReadListener)
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
      throw new NotImplementedError("Not implemented")
    override def deserialize(datum: Any): IntervalData =
      throw new NotImplementedError("Not implemented")
    override def userClass: Class[IntervalData] = classOf[IntervalData]
  }

  @SQLUserDefinedType(udt = classOf[NullUDT])
  private[sql] class NullData extends Serializable

  private[sql] class NullUDT extends UserDefinedType[NullData] {

    override def sqlType: DataType = NullType
    override def serialize(obj: NullData): Any = throw new NotImplementedError("Not implemented")
    override def deserialize(datum: Any): NullData =
      throw new NotImplementedError("Not implemented")
    override def userClass: Class[NullData] = classOf[NullData]
  }
}

class FakeFileSystemRequiringDSOption extends LocalFileSystem {
  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    require(conf.get("ds_option", "") == "value")
  }
}
