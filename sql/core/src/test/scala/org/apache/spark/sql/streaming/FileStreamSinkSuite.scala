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

package org.apache.spark.sql.streaming

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.apache.hadoop.mapreduce.JobContext

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.paths.SparkPath
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, FileScan, FileTable}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

abstract class FileStreamSinkSuite extends StreamTest {
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

  protected def checkQueryExecution(df: DataFrame): Unit

  test("unpartitioned writing and batch reading") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      query =
        df.writeStream
          .option("checkpointLocation", checkpointDir)
          .format("parquet")
          .start(outputDir)

      inputData.addData(1, 2, 3)

      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }

      val outputDf = spark.read.parquet(outputDir).as[Int]
      checkDatasetUnorderly(outputDf, 1, 2, 3)

    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("SPARK-21167: encode and decode path correctly") {
    val inputData = MemoryStream[String]
    val ds = inputData.toDS()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    val query = ds.map(s => (s, s.length))
      .toDF("value", "len")
      .writeStream
      .partitionBy("value")
      .option("checkpointLocation", checkpointDir)
      .format("parquet")
      .start(outputDir)

    try {
      // The output is partitioned by "value", so the value will appear in the file path.
      // This is to test if we handle spaces in the path correctly.
      inputData.addData("hello world")
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }
      val outputDf = spark.read.parquet(outputDir)
      checkDatasetUnorderly(outputDf.as[(Int, String)], ("hello world".length, "hello world"))
    } finally {
      query.stop()
    }
  }

  test("partitioned writing and batch reading") {
    val inputData = MemoryStream[Int]
    val ds = inputData.toDS()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir)
          .format("parquet")
          .start(outputDir)

      inputData.addData(1, 2, 3)
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }

      val outputDf = spark.read.parquet(outputDir)
      val expectedSchema = new StructType()
        .add(StructField("value", IntegerType, nullable = false))
        .add(StructField("id", IntegerType))
      assert(outputDf.schema === expectedSchema)

      // Verify the data is correctly read
      checkDatasetUnorderly(
        outputDf.as[(Int, Int)],
        (1000, 1), (2000, 2), (3000, 3))

      checkQueryExecution(outputDf)
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("partitioned writing and batch reading with 'basePath'") {
    withTempDir { outputDir =>
      withTempDir { checkpointDir =>
        val outputPath = outputDir.getAbsolutePath
        val inputData = MemoryStream[Int]
        val ds = inputData.toDS()

        var query: StreamingQuery = null

        try {
          query =
            ds.map(i => (i, -i, i * 1000))
              .toDF("id1", "id2", "value")
              .writeStream
              .partitionBy("id1", "id2")
              .option("checkpointLocation", checkpointDir.getAbsolutePath)
              .format("parquet")
              .start(outputPath)

          inputData.addData(1, 2, 3)
          failAfter(streamingTimeout) {
            query.processAllAvailable()
          }

          val readIn = spark.read.option("basePath", outputPath).parquet(s"$outputDir/*/*")
          checkDatasetUnorderly(
            readIn.as[(Int, Int, Int)],
            (1000, 1, -1), (2000, 2, -2), (3000, 3, -3))
        } finally {
          if (query != null) {
            query.stop()
          }
        }
      }
    }
  }

  // This tests whether FileStreamSink works with aggregations. Specifically, it tests
  // whether the correct streaming QueryExecution (i.e. IncrementalExecution) is used to
  // to execute the trigger for writing data to file sink. See SPARK-18440 for more details.
  test("writing with aggregation") {

    // Since FileStreamSink currently only supports append mode, we will test FileStreamSink
    // with aggregations using event time windows and watermark, which allows
    // aggregation + append mode.
    val inputData = MemoryStream[Long]
    val inputDF = inputData.toDF().toDF("time")
    val outputDf = inputDF
      .selectExpr("timestamp_seconds(time) AS timestamp")
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "5 seconds"))
      .count()
      .select("window.start", "window.end", "count")

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      query =
        outputDf.writeStream
          .option("checkpointLocation", checkpointDir)
          .format("parquet")
          .start(outputDir)


      def addTimestamp(timestampInSecs: Int*): Unit = {
        inputData.addData(timestampInSecs.map(_ * 1L): _*)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }
      }

      def check(expectedResult: ((Long, Long), Long)*): Unit = {
        val outputDf = spark.read.parquet(outputDir)
          .selectExpr(
            "CAST(start as BIGINT) AS start",
            "CAST(end as BIGINT) AS end",
            "count")
          .orderBy("start") // sort the DataFrame in order to compare with the expected one.
        checkDataset(
          outputDf.as[(Long, Long, Long)],
          expectedResult.map(x => (x._1._1, x._1._2, x._2)): _*)
      }

      addTimestamp(100) // watermark = None before this, watermark = 100 - 10 = 90 after this
      check() // nothing emitted yet

      addTimestamp(104, 123) // watermark = 90 before this, watermark = 123 - 10 = 113 after this
      check((100L, 105L) -> 2L)  // no-data-batch emits results on 100-105,

      addTimestamp(140) // wm = 113 before this, emit results on 100-105, wm = 130 after this
      check((100L, 105L) -> 2L, (120L, 125L) -> 1L)  // no-data-batch emits results on 120-125

    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("Update and Complete output mode not supported") {
    val df = MemoryStream[Int].toDF().groupBy().count()
    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath

    withTempDir { dir =>

      def testOutputMode(mode: String): Unit = {
        checkError(
          exception = intercept[AnalysisException] {
            df.writeStream.format("parquet").outputMode(mode).start(dir.getCanonicalPath)
          },
          condition = "STREAMING_OUTPUT_MODE.UNSUPPORTED_DATASOURCE",
          sqlState = "42KDE",
          parameters = Map("className" -> "parquet", "outputMode" -> mode))
      }
      testOutputMode("update")
      testOutputMode("complete")
    }
  }

  test("parquet") {
    testFormat(None) // should not throw error as default format parquet when not specified
    testFormat(Some("parquet"))
  }

  test("orc") {
    testFormat(Some("orc"))
  }

  test("text") {
    testFormat(Some("text"))
  }

  test("json") {
    testFormat(Some("json"))
  }

  def testFormat(format: Option[String]): Unit = {
    val inputData = MemoryStream[Int]
    val ds = inputData.toDS()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      val writer = ds.map(i => (i, i * 1000)).toDF("id", "value").writeStream
      if (format.nonEmpty) {
        writer.format(format.get)
      }
      query = writer.option("checkpointLocation", checkpointDir).start(outputDir)
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("FileStreamSink.ancestorIsMetadataDirectory()") {
    val hadoopConf = spark.sessionState.newHadoopConf()
    def assertAncestorIsMetadataDirectory(path: String): Unit =
      assert(FileStreamSink.ancestorIsMetadataDirectory(new Path(path), hadoopConf))
    def assertAncestorIsNotMetadataDirectory(path: String): Unit =
      assert(!FileStreamSink.ancestorIsMetadataDirectory(new Path(path), hadoopConf))

    assertAncestorIsMetadataDirectory(s"/${FileStreamSink.metadataDir}")
    assertAncestorIsMetadataDirectory(s"/${FileStreamSink.metadataDir}/")
    assertAncestorIsMetadataDirectory(s"/a/${FileStreamSink.metadataDir}")
    assertAncestorIsMetadataDirectory(s"/a/${FileStreamSink.metadataDir}/")
    assertAncestorIsMetadataDirectory(s"/a/b/${FileStreamSink.metadataDir}/c")
    assertAncestorIsMetadataDirectory(s"/a/b/${FileStreamSink.metadataDir}/c/")

    assertAncestorIsNotMetadataDirectory(s"/a/b/c")
    assertAncestorIsNotMetadataDirectory(s"/a/b/c/${FileStreamSink.metadataDir}extra")
  }

  test("SPARK-20460 Check name duplication in schema") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val inputData = MemoryStream[(Int, Int)]
        val df = inputData.toDF()

        val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
        val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

        var query: StreamingQuery = null
        try {
          query =
            df.writeStream
              .option("checkpointLocation", checkpointDir)
              .format("json")
              .start(outputDir)

          inputData.addData((1, 1))

          failAfter(streamingTimeout) {
            query.processAllAvailable()
          }
        } finally {
          if (query != null) {
            query.stop()
          }
        }

        checkError(
          exception = intercept[AnalysisException] {
            spark.read.schema(s"$c0 INT, $c1 INT").json(outputDir).as[(Int, Int)]
          },
          condition = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))
      }
    }
  }

  test("SPARK-23288 writing and checking output metrics") {
    Seq("parquet", "orc", "text", "json").foreach { format =>
      val inputData = MemoryStream[String]
      val df = inputData.toDF()

      withTempDir { outputDir =>
        withTempDir { checkpointDir =>

          var query: StreamingQuery = null

          var numTasks = 0
          var recordsWritten: Long = 0L
          var bytesWritten: Long = 0L
          try {
            spark.sparkContext.addSparkListener(new SparkListener() {
              override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
                val outputMetrics = taskEnd.taskMetrics.outputMetrics
                recordsWritten += outputMetrics.recordsWritten
                bytesWritten += outputMetrics.bytesWritten
                numTasks += 1
              }
            })

            query =
              df.writeStream
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .format(format)
                .start(outputDir.getCanonicalPath)

            inputData.addData("1", "2", "3")
            inputData.addData("4", "5")

            failAfter(streamingTimeout) {
              query.processAllAvailable()
            }
            spark.sparkContext.listenerBus.waitUntilEmpty(streamingTimeout.toMillis)

            assert(numTasks > 0)
            assert(recordsWritten === 5)
            // This is heavily file type/version specific but should be filled
            assert(bytesWritten > 0)
          } finally {
            if (query != null) {
              query.stop()
            }
          }
        }
      }
    }
  }

  test("special characters in output path") {
    withTempDir { tempDir =>
      val checkpointDir = new File(tempDir, "chk")
      val outputDir = new File(tempDir, "output @#output")
      val inputData = MemoryStream[Int]
      inputData.addData(1, 2, 3)
      val q = inputData.toDF()
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .format("parquet")
        .start(outputDir.getCanonicalPath)
      try {
        q.processAllAvailable()
      } finally {
        q.stop()
      }
      // The "_spark_metadata" directory should be in "outputDir"
      assert(outputDir.listFiles.map(_.getName).contains(FileStreamSink.metadataDir))
      val outputDf = spark.read.parquet(outputDir.getCanonicalPath).as[Int]
      checkDatasetUnorderly(outputDf, 1, 2, 3)
    }
  }

  testQuietly("cleanup incomplete output for aborted task") {
    withTempDir { tempDir =>
      val checkpointDir = new File(tempDir, "chk")
      val outputDir = new File(tempDir, "output")
      val inputData = MemoryStream[Int]
      inputData.addData(1, 2, 3)
      val q = inputData.toDS().map(_ / 0)
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .format("parquet")
        .start(outputDir.getCanonicalPath)

      intercept[StreamingQueryException] {
        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }

      val outputFiles = Files.walk(outputDir.toPath).iterator().asScala
        .filter(_.toString.endsWith(".parquet"))
      assert(outputFiles.toList.isEmpty, "Incomplete files should be cleaned up.")
    }
  }

  testQuietly("cleanup complete but invalid output for aborted job") {
    withSQLConf(("spark.sql.streaming.commitProtocolClass",
      classOf[PendingCommitFilesTrackingManifestFileCommitProtocol].getCanonicalName)) {
      withTempDir { tempDir =>
        val checkpointDir = new File(tempDir, "chk")
        val outputDir = new File(tempDir, "output @#output")
        val inputData = MemoryStream[Int]
        inputData.addData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val q = inputData.toDS()
          .repartition(10)
          .map { value =>
            // we intend task failure after some tasks succeeds
            if (value == 5) {
              // put some delay to let other task commits before this task fails
              Thread.sleep(100)
              value / 0
            } else {
              value
            }
          }
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("parquet")
          .start(outputDir.getCanonicalPath)

        intercept[StreamingQueryException] {
          try {
            q.processAllAvailable()
          } finally {
            q.stop()
          }
        }

        import PendingCommitFilesTrackingManifestFileCommitProtocol._
        val outputFileNames = Files.walk(outputDir.toPath).iterator().asScala
          .filter(_.toString.endsWith(".parquet"))
          .map(_.getFileName.toString)
          .toSet
        val trackingFileNames = tracking.map(SparkPath.fromUrlString(_).toPath.getName).toSet

        // there would be possible to have race condition:
        // - some tasks complete while abortJob is being called
        // we can't delete complete files for these tasks (it's OK since this is a best effort)
        assert(outputFileNames.intersect(trackingFileNames).isEmpty,
          "abortJob should clean up files reported as successful.")
      }
    }
  }

  test("Handle FileStreamSink metadata correctly for empty partition") {
    Seq("parquet", "orc", "text", "json").foreach { format =>
      val inputData = MemoryStream[String]
      val df = inputData.toDF()

      withTempDir { outputDir =>
        withTempDir { checkpointDir =>
          var query: StreamingQuery = null
          try {
            // repartition to more than the input to leave empty partitions
            query =
              df.repartition(10)
                .writeStream
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .format(format)
                .start(outputDir.getCanonicalPath)

            inputData.addData("1", "2", "3")
            inputData.addData("4", "5")

            failAfter(streamingTimeout) {
              query.processAllAvailable()
            }
          } finally {
            if (query != null) {
              query.stop()
            }
          }

          val outputDirPath = new Path(outputDir.getCanonicalPath)
          val hadoopConf = spark.sessionState.newHadoopConf()
          val fs = outputDirPath.getFileSystem(hadoopConf)
          val logPath = FileStreamSink.getMetadataLogPath(fs, outputDirPath, conf)

          val sinkLog = new FileStreamSinkLog(FileStreamSinkLog.VERSION, spark, logPath.toString)

          val allFiles = sinkLog.allFiles()
          // only files from non-empty partition should be logged
          assert(allFiles.length < 10)
          assert(allFiles.forall(file => fs.exists(file.sparkPath.toPath)))

          // the query should be able to read all rows correctly with metadata log
          val outputDf = spark.read.format(format).load(outputDir.getCanonicalPath)
            .selectExpr("CAST(value AS INT)").as[Int]
          checkDatasetUnorderly(outputDf, 1, 2, 3, 4, 5)
        }
      }
    }
  }

  test("formatCheck fail should not fail the query") {
    withSQLConf(
      "fs.file.impl" -> classOf[FailFormatCheckFileSystem].getName,
      "fs.file.impl.disable.cache" -> "true") {
      withTempDir { tempDir =>
        val path = new File(tempDir, "text").getCanonicalPath
        Seq("foo").toDF().write.format("text").save(path)
        spark.read.format("text").load(path)
      }
    }
  }

  test("fail to check glob path should not fail the query") {
    withSQLConf(
      "fs.file.impl" -> classOf[FailFormatCheckFileSystem].getName,
      "fs.file.impl.disable.cache" -> "true") {
      withTempDir { tempDir =>
        val path = new File(tempDir, "text").getCanonicalPath
        Seq("foo").toDF().write.format("text").save(path)
        spark.read.format("text").load(path + "/*")
      }
    }
  }

  test("SPARK-35565: Ignore metadata directory when reading sink output") {
    Seq(true, false).foreach { ignoreMetadata =>
      withSQLConf(SQLConf.FILESTREAM_SINK_METADATA_IGNORED.key -> ignoreMetadata.toString) {
        val inputData = MemoryStream[String]
        val df = inputData.toDF()

        withTempDir { outputDir =>
          withTempDir { checkpointDir =>
            var query: StreamingQuery = null
            try {
              query =
                df.writeStream
                  .option("checkpointLocation", checkpointDir.getCanonicalPath)
                  .format("text")
                  .start(outputDir.getCanonicalPath)

              inputData.addData("1", "2", "3")
              inputData.addData("4", "5")

              failAfter(streamingTimeout) {
                query.processAllAvailable()
              }
            } finally {
              if (query != null) {
                query.stop()
              }
            }

            val additionalFile = new File(outputDir, "additional.txt")
            stringToFile(additionalFile, "6")
            additionalFile.exists()

            val outputDf = spark.read.format("text").load(outputDir.getCanonicalPath)
              .selectExpr("CAST(value AS INT)").as[Int]
            if (ignoreMetadata) {
              checkDatasetUnorderly(outputDf, 1, 2, 3, 4, 5, 6)
            } else {
              checkDatasetUnorderly(outputDf, 1, 2, 3, 4, 5)
            }
          }
        }
      }
    }
  }

  test("SPARK-48991: Move path initialization into try-catch block") {
    val logAppender = new LogAppender("Assume no metadata directory.")
    Seq(null, "", "file:tmp").foreach { path =>
      withLogAppender(logAppender) {
        assert(!FileStreamSink.hasMetadata(Seq(path), spark.sessionState.newHadoopConf(), conf))
      }

      assert(logAppender.loggingEvents.map(_.getMessage.getFormattedMessage).contains(
        "Assume no metadata directory. Error while looking for metadata directory in the path:" +
        s" $path."))
    }
  }
}

object PendingCommitFilesTrackingManifestFileCommitProtocol {
  val tracking: ArrayBuffer[String] = new ArrayBuffer[String]()

  def cleanPendingCommitFiles(): Unit = tracking.clear()
  def addPendingCommitFiles(paths: Seq[String]): Unit = tracking ++= paths
}

class PendingCommitFilesTrackingManifestFileCommitProtocol(jobId: String, path: String)
  extends ManifestFileCommitProtocol(jobId, path) {
  import PendingCommitFilesTrackingManifestFileCommitProtocol._

  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)
    cleanPendingCommitFiles()
  }

  override def onTaskCommit(taskCommit: FileCommitProtocol.TaskCommitMessage): Unit = {
    super.onTaskCommit(taskCommit)
    addPendingCommitFiles(taskCommit.obj.asInstanceOf[Seq[SinkFileStatus]].map(_.path))
  }
}

@SlowSQLTest
class FileStreamSinkV1Suite extends FileStreamSinkSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "csv,json,orc,text,parquet")

  override def checkQueryExecution(df: DataFrame): Unit = {
    // Verify that MetadataLogFileIndex is being used and the correct partitioning schema has
    // been inferred
    val hadoopdFsRelations = df.queryExecution.analyzed.collect {
      case RelationAndCatalogTable(_, baseRelation: HadoopFsRelation, _) => baseRelation
    }
    assert(hadoopdFsRelations.size === 1)
    assert(hadoopdFsRelations.head.location.isInstanceOf[MetadataLogFileIndex])
    assert(hadoopdFsRelations.head.partitionSchema.exists(_.name == "id"))
    assert(hadoopdFsRelations.head.dataSchema.exists(_.name == "value"))

    /** Check some condition on the partitions of the FileScanRDD generated by a DF */
    def checkFileScanPartitions(df: DataFrame)(func: Seq[FilePartition] => Unit): Unit = {
      val getFileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[FileScanRDD]
      }.headOption.getOrElse {
        fail(s"No FileScan in query\n${df.queryExecution}")
      }
      func(getFileScanRDD.filePartitions)
    }

    // Read without pruning
    checkFileScanPartitions(df) { partitions =>
      // There should be as many distinct partition values as there are distinct ids
      assert(partitions.flatMap(_.files.map(_.partitionValues)).distinct.size === 3)
    }

    // Read with pruning, should read only files in partition dir id=1
    checkFileScanPartitions(df.filter("id = 1")) { partitions =>
      val filesToBeRead = partitions.flatMap(_.files)
      assert(filesToBeRead.map(_.urlEncodedPath).forall(_.contains("/id=1/")))
      assert(filesToBeRead.map(_.partitionValues).distinct.size === 1)
    }

    // Read with pruning, should read only files in partition dir id=1 and id=2
    checkFileScanPartitions(df.filter("id in (1,2)")) { partitions =>
      val filesToBeRead = partitions.flatMap(_.files)
      assert(!filesToBeRead.map(_.urlEncodedPath).exists(_.contains("/id=3/")))
      assert(filesToBeRead.map(_.partitionValues).distinct.size === 2)
    }
  }
}

@SlowSQLTest
class FileStreamSinkV2Suite extends FileStreamSinkSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  override def checkQueryExecution(df: DataFrame): Unit = {
    // Verify that MetadataLogFileIndex is being used and the correct partitioning schema has
    // been inferred
    val table = df.queryExecution.analyzed.collect {
      case DataSourceV2Relation(table: FileTable, _, _, _, _) => table
    }
    assert(table.size === 1)
    assert(table.head.fileIndex.isInstanceOf[MetadataLogFileIndex])
    assert(table.head.fileIndex.partitionSchema.exists(_.name == "id"))
    assert(table.head.dataSchema.exists(_.name == "value"))

    /** Check some condition on the partitions of the FileScanRDD generated by a DF */
    def checkFileScanPartitions(df: DataFrame)(func: Seq[FilePartition] => Unit): Unit = {
      val fileScan = df.queryExecution.executedPlan.collect {
        case batch: BatchScanExec if batch.scan.isInstanceOf[FileScan] =>
          batch.scan.asInstanceOf[FileScan]
      }.headOption.getOrElse {
        fail(s"No FileScan in query\n${df.queryExecution}")
      }
      func(fileScan.planInputPartitions().map(_.asInstanceOf[FilePartition]).toImmutableArraySeq)
    }

    // Read without pruning
    checkFileScanPartitions(df) { partitions =>
      // There should be as many distinct partition values as there are distinct ids
      assert(partitions.flatMap(_.files.map(_.partitionValues)).distinct.size === 3)
    }
    // TODO: test partition pruning when file source V2 supports it.
  }
}

/**
 * A special file system that fails when accessing metadata log directory or using a glob path to
 * access.
 */
class FailFormatCheckFileSystem extends RawLocalFileSystem {
  override def getFileStatus(f: Path): FileStatus = {
    if (f.getName == FileStreamSink.metadataDir) {
      throw new IOException("cannot access metadata log")
    }
    if (SparkHadoopUtil.get.isGlobPath(f)) {
      throw new IOException("fail to access a glob path")
    }
    super.getFileStatus(f)
  }
}
