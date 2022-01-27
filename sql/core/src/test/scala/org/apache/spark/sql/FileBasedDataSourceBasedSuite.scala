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
import java.nio.file.{Files, StandardOpenOption}

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.execution.SimpleMode
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.internal.SQLConf

/**
 * This class contains tests for all file-based data sources but not related to source format.
 *
 */
class FileBasedDataSourceBasedSuite extends FileBasedDataSourceSuiteBase {
  override protected def format: String = ""
  import testImplicits._

  testQuietly(s"Enabling/disabling ignoreMissingFiles using") {
    def testIgnoreMissingFiles(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath

        Seq("0").toDF("a").write.format(format)
          .save(new Path(basePath, "second").toString)
        Seq("1").toDF("a").write.format(format)
          .save(new Path(basePath, "fourth").toString)

        val firstPath = new Path(basePath, "first")
        val thirdPath = new Path(basePath, "third")
        val fs = thirdPath.getFileSystem(spark.sessionState.newHadoopConf())
        Seq("2").toDF("a").write.format(format).save(firstPath.toString)
        Seq("3").toDF("a").write.format(format).save(thirdPath.toString)
        val files = Seq(firstPath, thirdPath).flatMap { p =>
          fs.listStatus(p).filter(_.isFile).map(_.getPath)
        }

        val df = spark.read.format(format).load(
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

    for {
      ignore <- Seq("true", "false")
      sources <- Seq("", format)
    } {
      withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> ignore,
        SQLConf.USE_V1_SOURCE_LIST.key -> sources) {
        if (ignore.toBoolean) {
          testIgnoreMissingFiles()
        } else {
          val exception = intercept[SparkException] {
            testIgnoreMissingFiles()
          }
          assert(exception.getMessage().contains("does not exist"))
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
          val row = spark.read.orc(path).select(input_file_name).first()
          assert(row.getString(0).contains(path))
        }
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
}
