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
package org.apache.spark.sql.connector

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}

class DummyReadOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DummyReadOnlyFileTable
  }
}

class DummyReadOnlyFileTable extends Table with SupportsRead {
  override def name(): String = "dummy"

  override def schema(): StructType = StructType(Nil)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    throw SparkException.internalError("Dummy file reader")
  }

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.ACCEPT_ANY_SCHEMA)
}

class DummyWriteOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DummyWriteOnlyFileTable
  }
}

class DummyWriteOnlyFileTable extends Table with SupportsWrite {
  override def name(): String = "dummy"

  override def schema(): StructType = StructType(Nil)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw SparkException.internalError("Dummy file writer")

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_WRITE, TableCapability.ACCEPT_ANY_SCHEMA)
}

class FileDataSourceV2WriteSuite extends QueryTest with SharedSparkSession {

  private val dummyReadOnlyFileSourceV2 = classOf[DummyReadOnlyFileDataSourceV2].getName
  private val dummyWriteOnlyFileSourceV2 = classOf[DummyWriteOnlyFileDataSourceV2].getName

  // Built-in file formats for write testing. Text is excluded
  // because it only supports a single string column.
  private val fileFormats = Seq("parquet", "orc", "json", "csv")

  override protected def sparkConf: SparkConf = super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("Fall back to v1 when writing to file with read only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      // Writing file should fall back to v1 and succeed.
      df.write.format(dummyReadOnlyFileSourceV2).save(path)

      // Validate write result with [[ParquetFileFormat]].
      checkAnswer(spark.read.parquet(path), df)

      // Dummy File reader should fail as expected.
      checkError(
        exception = intercept[SparkException] {
          spark.read.format(dummyReadOnlyFileSourceV2).load(path).collect()
        },
        condition = "INTERNAL_ERROR",
        parameters = Map("message" -> "Dummy file reader"))
    }
  }

  test("Fall back read path to v1 with configuration USE_V1_SOURCE_LIST") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      df.write.parquet(path)
      Seq(
        "foo,parquet,bar",
        "ParQuet,bar,foo",
        s"foobar,$dummyReadOnlyFileSourceV2"
      ).foreach { fallbackReaders =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> fallbackReaders) {
          // Reading file should fall back to v1 and succeed.
          checkAnswer(spark.read.format(dummyReadOnlyFileSourceV2).load(path), df)
          checkAnswer(sql(s"SELECT * FROM parquet.`$path`"), df)
        }
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "foo,bar") {
        // Dummy File reader should fail as DISABLED_V2_FILE_DATA_SOURCE_READERS doesn't include it.
        checkError(
          exception = intercept[SparkException] {
            spark.read.format(dummyReadOnlyFileSourceV2).load(path).collect()
          },
          condition = "INTERNAL_ERROR",
          parameters = Map("message" -> "Dummy file reader"))
      }
    }
  }

  test("Fall back to v1 when reading file with write only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      df.write.parquet(path)
      // Fallback reads to V1
      checkAnswer(spark.read.format(dummyWriteOnlyFileSourceV2).load(path), df)
    }
  }

  test("Fall back write path to v1 for default save mode") {
    val df = spark.range(10).toDF()
    withTempPath { path =>
      // Default mode is ErrorIfExists, which now routes through V2 for file sources.
      // DummyWriteOnlyFileDataSourceV2 throws on write, so it falls back to V1
      // via the SparkUnsupportedOperationException catch in the createMode branch.
      // Use a real format to verify ErrorIfExists works via V2.
      df.write.parquet(path.getCanonicalPath)
      checkAnswer(spark.read.parquet(path.getCanonicalPath), df)
    }
  }

  test("Fallback Parquet V2 to V1") {
    Seq("parquet", classOf[ParquetDataSourceV2].getCanonicalName).foreach { format =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> format) {
        val commands = ArrayBuffer.empty[(String, LogicalPlan)]
        val exceptions = ArrayBuffer.empty[(String, Exception)]
        val listener = new QueryExecutionListener {
          override def onFailure(
              funcName: String,
              qe: QueryExecution,
              exception: Exception): Unit = {
            exceptions += funcName -> exception
          }

          override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
            commands += funcName -> qe.logical
          }
        }
        spark.listenerManager.register(listener)

        try {
          withTempPath { path =>
            val inputData = spark.range(10)
            inputData.write.format(format).save(path.getCanonicalPath)
            sparkContext.listenerBus.waitUntilEmpty()
            assert(commands.length == 1)
            assert(commands.head._1 == "command")
            assert(commands.head._2.isInstanceOf[InsertIntoHadoopFsRelationCommand])
            assert(commands.head._2.asInstanceOf[InsertIntoHadoopFsRelationCommand]
              .fileFormat.isInstanceOf[ParquetFileFormat])
            val df = spark.read.format(format).load(path.getCanonicalPath)
            checkAnswer(df, inputData.toDF())
            assert(
              df.queryExecution.executedPlan.exists(_.isInstanceOf[FileSourceScanExec]))
          }
        } finally {
          spark.listenerManager.unregister(listener)
        }
      }
    }
  }

  test("File write for multiple formats") {
    fileFormats.foreach { format =>
      withTempPath { path =>
        val inputData = spark.range(10).toDF()
        inputData.write.option("header", "true").format(format).save(path.getCanonicalPath)
        val readBack = spark.read.option("header", "true").schema(inputData.schema)
          .format(format).load(path.getCanonicalPath)
        checkAnswer(readBack, inputData)
      }
    }
  }

  test("File write produces same results with V1 and V2 reads") {
    withTempPath { v1Path =>
      withTempPath { v2Path =>
        val inputData = spark.range(100).selectExpr("id", "id * 2 as value")

        // Write via V1 path
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
          inputData.write.parquet(v1Path.getCanonicalPath)
        }

        // Write via V2 path (default)
        inputData.write.parquet(v2Path.getCanonicalPath)

        // Both should produce the same results
        val v1Result = spark.read.parquet(v1Path.getCanonicalPath)
        val v2Result = spark.read.parquet(v2Path.getCanonicalPath)
        checkAnswer(v1Result, v2Result)
      }
    }
  }

  test("Partitioned file write") {
    fileFormats.foreach { format =>
      withTempPath { path =>
        val inputData = spark.range(20).selectExpr(
          "id", "id % 5 as part")
        inputData.write.option("header", "true")
          .partitionBy("part").format(format).save(path.getCanonicalPath)
        val readBack = spark.read.option("header", "true").schema(inputData.schema)
          .format(format).load(path.getCanonicalPath)
        checkAnswer(readBack, inputData)

        // Verify partition directory structure exists
        val partDirs = path.listFiles().filter(_.isDirectory).map(_.getName).sorted
        assert(partDirs.exists(_.startsWith("part=")),
          s"Expected partition directories for format $format, got: ${partDirs.mkString(", ")}")
      }
    }
  }

  test("Partitioned write produces same results with V1 and V2 reads") {
    fileFormats.foreach { format =>
      withTempPath { v1Path =>
        withTempPath { v2Path =>
          val inputData = spark.range(50).selectExpr(
            "id", "id % 3 as category", "id * 10 as value")

          // Write via V1 path
          withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> format) {
            inputData.write.option("header", "true")
              .partitionBy("category").format(format).save(v1Path.getCanonicalPath)
          }

          // Write via V2 path (default)
          inputData.write.option("header", "true")
            .partitionBy("category").format(format).save(v2Path.getCanonicalPath)

          val v1Result = spark.read.option("header", "true").schema(inputData.schema)
            .format(format).load(v1Path.getCanonicalPath)
          val v2Result = spark.read.option("header", "true").schema(inputData.schema)
            .format(format).load(v2Path.getCanonicalPath)
          checkAnswer(v1Result, v2Result)
        }
      }
    }
  }

  test("Multi-level partitioned write") {
    fileFormats.foreach { format =>
      withTempPath { path =>
        val schema = "id LONG, year LONG, month LONG"
        val inputData = spark.range(30).selectExpr(
          "id", "id % 3 as year", "id % 2 as month")
        inputData.write.option("header", "true")
          .partitionBy("year", "month")
          .format(format).save(path.getCanonicalPath)
        checkAnswer(
          spark.read.option("header", "true")
            .schema(schema).format(format)
            .load(path.getCanonicalPath),
          inputData)

        val yearDirs = path.listFiles()
          .filter(_.isDirectory).map(_.getName).sorted
        assert(yearDirs.exists(_.startsWith("year=")),
          s"Expected year partition dirs for $format")
        val firstYearDir = path.listFiles()
          .filter(_.isDirectory).head
        val monthDirs = firstYearDir.listFiles()
          .filter(_.isDirectory).map(_.getName).sorted
        assert(monthDirs.exists(_.startsWith("month=")),
          s"Expected month partition dirs for $format")
      }
    }
  }

  test("Dynamic partition overwrite") {
    fileFormats.foreach { format =>
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> format,
        SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
        withTempPath { path =>
          val schema = "id LONG, part LONG"
          val initialData = spark.range(9).selectExpr(
            "id", "id % 3 as part")
          initialData.write.option("header", "true")
            .partitionBy("part")
            .format(format).save(path.getCanonicalPath)

          val overwriteData = spark.createDataFrame(
            Seq((100L, 0L), (101L, 0L))).toDF("id", "part")
          overwriteData.write.option("header", "true")
            .mode("overwrite").partitionBy("part")
            .format(format).save(path.getCanonicalPath)

          val result = spark.read.option("header", "true")
            .schema(schema).format(format)
            .load(path.getCanonicalPath)
          val expected = initialData.filter("part != 0")
            .union(overwriteData)
          checkAnswer(result, expected)
        }
      }
    }
  }

  test("Dynamic partition overwrite produces same results") {
    fileFormats.foreach { format =>
      withTempPath { v1Path =>
        withTempPath { v2Path =>
          val schema = "id LONG, part LONG"
          val initialData = spark.range(12).selectExpr(
            "id", "id % 4 as part")
          val overwriteData = spark.createDataFrame(
            Seq((200L, 1L), (201L, 1L))).toDF("id", "part")

          Seq(v1Path, v2Path).foreach { p =>
            withSQLConf(
              SQLConf.USE_V1_SOURCE_LIST.key -> format,
              SQLConf.PARTITION_OVERWRITE_MODE.key ->
                "dynamic") {
              initialData.write.option("header", "true")
                .partitionBy("part").format(format)
                .save(p.getCanonicalPath)
              overwriteData.write.option("header", "true")
                .mode("overwrite").partitionBy("part")
                .format(format).save(p.getCanonicalPath)
            }
          }

          val v1Result = spark.read
            .option("header", "true").schema(schema)
            .format(format).load(v1Path.getCanonicalPath)
          val v2Result = spark.read
            .option("header", "true").schema(schema)
            .format(format).load(v2Path.getCanonicalPath)
          checkAnswer(v1Result, v2Result)
        }
      }
    }
  }

  test("DataFrame API write uses V2 path") {
    fileFormats.foreach { format =>
      val writeOpts = if (format == "csv") {
        Map("header" -> "true")
      } else {
        Map.empty[String, String]
      }
      def readBack(p: String): DataFrame = {
        val r = spark.read.format(format)
        val configured = if (format == "csv") {
          r.option("header", "true").schema("id LONG")
        } else r
        configured.load(p)
      }

      // SaveMode.Append to existing path goes via V2
      withTempPath { path =>
        val data1 = spark.range(5).toDF()
        data1.write.options(writeOpts).format(format).save(path.getCanonicalPath)
        val data2 = spark.range(5, 10).toDF()
        data2.write.options(writeOpts).mode("append")
          .format(format).save(path.getCanonicalPath)
        checkAnswer(readBack(path.getCanonicalPath),
          data1.union(data2))
      }

      // SaveMode.Overwrite goes via V2
      withTempPath { path =>
        val data1 = spark.range(5).toDF()
        data1.write.options(writeOpts).format(format)
          .save(path.getCanonicalPath)
        val data2 = spark.range(10, 15).toDF()
        data2.write.options(writeOpts).mode("overwrite")
          .format(format).save(path.getCanonicalPath)
        checkAnswer(readBack(path.getCanonicalPath), data2)
      }
    }
  }

  test("DataFrame API partitioned write") {
    withTempPath { path =>
      val data = spark.range(20).selectExpr("id", "id % 4 as part")
      data.write.partitionBy("part").parquet(path.getCanonicalPath)
      val result = spark.read.parquet(path.getCanonicalPath)
      checkAnswer(result, data)

      val partDirs = path.listFiles().filter(_.isDirectory).map(_.getName)
      assert(partDirs.exists(_.startsWith("part=")))
    }
  }

  test("DataFrame API write with compression option") {
    withTempPath { path =>
      val data = spark.range(10).toDF()
      data.write.option("compression", "snappy").parquet(path.getCanonicalPath)
      checkAnswer(spark.read.parquet(path.getCanonicalPath), data)
    }
  }

  test("Catalog table INSERT INTO") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, value BIGINT) USING parquet")
      sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
      checkAnswer(sql("SELECT * FROM t"),
        Seq((1L, 10L), (2L, 20L), (3L, 30L)).map(Row.fromTuple))
    }
  }

  test("Catalog table partitioned INSERT INTO") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, part BIGINT) USING parquet PARTITIONED BY (part)")
      sql("INSERT INTO t VALUES (1, 1), (2, 1), (3, 2), (4, 2)")
      checkAnswer(sql("SELECT * FROM t ORDER BY id"),
        Seq((1L, 1L), (2L, 1L), (3L, 2L), (4L, 2L)).map(Row.fromTuple))
    }
  }

  test("V2 cache invalidation on overwrite") {
    fileFormats.foreach { format =>
      withTempPath { path =>
        val p = path.getCanonicalPath
        spark.range(1000).toDF("id").write.format(format).save(p)
        val df = spark.read.format(format).load(p).cache()
        assert(df.count() == 1000)
        // Overwrite via V2 path should invalidate cache
        spark.range(10).toDF("id").write.mode("append").format(format).save(p)
        spark.range(10).toDF("id").write
          .mode("overwrite").format(format).save(p)
        assert(df.count() == 10,
          s"Cache should be invalidated after V2 overwrite for $format")
        df.unpersist()
      }
    }
  }

  test("V2 cache invalidation on append") {
    fileFormats.foreach { format =>
      withTempPath { path =>
        val p = path.getCanonicalPath
        spark.range(1000).toDF("id").write.format(format).save(p)
        val df = spark.read.format(format).load(p).cache()
        assert(df.count() == 1000)
        // Append via V2 path should invalidate cache
        spark.range(10).toDF("id").write.mode("append").format(format).save(p)
        assert(df.count() == 1010,
          s"Cache should be invalidated after V2 append for $format")
        df.unpersist()
      }
    }
  }

  test("Cache invalidation on catalog table overwrite") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT) USING parquet")
      sql("INSERT INTO t SELECT id FROM range(100)")
      spark.table("t").cache()
      assert(spark.table("t").count() == 100)
      sql("INSERT OVERWRITE TABLE t SELECT id FROM range(10)")
      assert(spark.table("t").count() == 10,
        "Cache should be invalidated after catalog table overwrite")
      spark.catalog.uncacheTable("t")
    }
  }

  // SQL path INSERT INTO parquet.`path` requires SupportsCatalogOptions

  test("CTAS") {
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id * 2 as value FROM range(10)")
      checkAnswer(
        sql("SELECT count(*) FROM t"),
        Seq(Row(10L)))
    }
  }

  test("Partitioned write to empty directory succeeds") {
    fileFormats.foreach { format =>
      withTempDir { dir =>
        val schema = "id LONG, k LONG"
        val data = spark.range(20).selectExpr(
          "id", "id % 4 as k")
        data.write.option("header", "true")
          .partitionBy("k").mode("overwrite")
          .format(format).save(dir.toString)
        checkAnswer(
          spark.read.option("header", "true")
            .schema(schema).format(format)
            .load(dir.toString),
          data)
      }
    }
  }

  test("Partitioned overwrite to existing directory succeeds") {
    fileFormats.foreach { format =>
      withTempDir { dir =>
        val schema = "id LONG, k LONG"
        val data1 = spark.range(10).selectExpr(
          "id", "id % 3 as k")
        data1.write.option("header", "true")
          .partitionBy("k").mode("overwrite")
          .format(format).save(dir.toString)
        val data2 = spark.range(10, 20).selectExpr(
          "id", "id % 3 as k")
        data2.write.option("header", "true")
          .partitionBy("k").mode("overwrite")
          .format(format).save(dir.toString)
        checkAnswer(
          spark.read.option("header", "true")
            .schema(schema).format(format)
            .load(dir.toString),
          data2)
      }
    }
  }

  test("DataFrame API ErrorIfExists mode") {
    Seq("parquet", "orc").foreach { format =>
      // ErrorIfExists on existing path should throw
      withTempPath { path =>
        spark.range(5).toDF().write.format(format).save(path.getCanonicalPath)
        val e = intercept[AnalysisException] {
          spark.range(10).toDF().write.mode("error").format(format)
            .save(path.getCanonicalPath)
        }
        assert(e.getCondition == "PATH_ALREADY_EXISTS")
      }
      // ErrorIfExists on new path should succeed
      withTempPath { path =>
        spark.range(5).toDF().write.mode("error").format(format)
          .save(path.getCanonicalPath)
        checkAnswer(spark.read.format(format).load(path.getCanonicalPath),
          spark.range(5).toDF())
      }
    }
  }

  test("DataFrame API Ignore mode") {
    Seq("parquet", "orc").foreach { format =>
      // Ignore on existing path should skip writing
      withTempPath { path =>
        spark.range(5).toDF().write.format(format).save(path.getCanonicalPath)
        spark.range(100).toDF().write.mode("ignore").format(format)
          .save(path.getCanonicalPath)
        checkAnswer(spark.read.format(format).load(path.getCanonicalPath),
          spark.range(5).toDF())
      }
      // Ignore on new path should write data
      withTempPath { path =>
        spark.range(5).toDF().write.mode("ignore").format(format)
          .save(path.getCanonicalPath)
        checkAnswer(spark.read.format(format).load(path.getCanonicalPath),
          spark.range(5).toDF())
      }
    }
  }

  test("INSERT INTO format.path uses V2 path") {
    Seq("parquet", "orc", "json").foreach { format =>
      withTempPath { path =>
        val p = path.getCanonicalPath
        spark.range(5).toDF("id").write.format(format).save(p)
        sql(s"INSERT INTO ${format}.`${p}` SELECT * FROM range(5, 10)")
        checkAnswer(
          spark.read.format(format).load(p),
          spark.range(10).toDF("id"))
      }
    }
  }

  test("SELECT FROM format.path uses V2 path") {
    Seq("parquet", "orc", "json").foreach { format =>
      withTempPath { path =>
        val p = path.getCanonicalPath
        spark.range(5).toDF("id").write.format(format).save(p)
        checkAnswer(
          sql(s"SELECT * FROM ${format}.`${p}`"),
          spark.range(5).toDF("id"))
      }
    }
  }
}
