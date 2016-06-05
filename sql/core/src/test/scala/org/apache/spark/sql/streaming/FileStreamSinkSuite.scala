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

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, RegexFileFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class FileStreamSinkSuite extends StreamTest {
  import testImplicits._

  val COMPRESSION_CODECS = Seq("none", "gzip")

  test("FileStreamSinkWriter - csv - unpartitioned data - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec => testUnpartitionedData(new CSVFileFormat, codec) }
  }

  test("FileStreamSinkWriter - json - unpartitioned data - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec => testUnpartitionedData(new JsonFileFormat, codec) }
  }

  test("FileStreamSinkWriter - parquet - unpartitioned data - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec => testUnpartitionedData(new ParquetFileFormat, codec) }
  }

  test("FileStreamSinkWriter - text - unpartitioned data - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec => testUnpartitionedData(new TextFileFormat, codec) }
  }

  private def testUnpartitionedData(
      fileFormat: FileFormat with DataSourceRegister,
      codec: String) {
    val path = Utils.createTempDir()
    path.delete()

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    val testingText = fileFormat.isInstanceOf[TextFileFormat]

    def writeRange(start: Int, end: Int, numPartitions: Int): Seq[String] = {

      // The `text` format accepts only one unpartitioned column, and requires the contents be
      // strings; so we occasionally treat `text` format differently from the other formats.
      val df = if (testingText) {
          // For `text` format, we'll have only one unpartitioned column
          spark
            .range(start, end, 1, numPartitions)
            .map(_.toString).toDF("id")
        }
        else {
          // For the other formats, we'll have two unpartitioned columns
          spark
            .range(start, end, 1, numPartitions)
            .map(_.toString).toDF("id")
            .select($"id", lit("foo").as("value"))
        }

      val writer = new FileStreamSinkWriter(
        df,
        fileFormat,
        path.toString,
        partitionColumnNames = Nil,
        hadoopConf,
        Map("compression" -> codec))
      // `path` would be like "file:///some/path/..." if run on Hadoop 2.2.0
      // `path` would be like "file:/some/path/..."   if run on Hadoop 2.3.0 and onwards
      // so path.stripPrefix("file://").stripPrefix("file:") would give us "/some/path/..."
      writer.write().map(_.path.stripPrefix("file://").stripPrefix("file:"))
    }

    // Write and check whether new files are written correctly
    val files1 = writeRange(0, 10, 2)
    assert(files1.size === 2, s"unexpected number of files: $files1")
    checkFilesExist(path, files1, "file not written")
    checkAnswer(
        spark.read.format(fileFormat.shortName()).load(path.getCanonicalPath),
        (0 until 10).map(_.toString).map { if (testingText) Row(_) else Row(_, "foo")} )

    // Append and check whether new files are written correctly and old files still exist
    val files2 = writeRange(10, 20, 3)
    assert(files2.size === 3, s"unexpected number of files: $files2")
    assert(files2.intersect(files1).isEmpty, "old files returned")
    checkFilesExist(path, files2, s"New file not written")
    checkFilesExist(path, files1, s"Old file not found")
    checkAnswer(
        spark.read.format(fileFormat.shortName()).load(path.getCanonicalPath),
        (0 until 20).map(_.toString).map { if (testingText) Row(_) else Row(_, "foo")} )
  }

  test("FileStreamSinkWriter - csv - partitioned data - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec => testPartitionedData(new csv.CSVFileFormat(), codec) }
  }

  test("FileStreamSinkWriter - json - partitioned data - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec => testPartitionedData(new JsonFileFormat, codec) }
  }

  test("FileStreamSinkWriter - parquet - partitioned data - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec => testPartitionedData(new ParquetFileFormat, codec) }
  }

  test("FileStreamSinkWriter - text - partitioned data - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec => testPartitionedData(new TextFileFormat, codec) }
  }

  private def testPartitionedData(
      fileFormat: FileFormat with DataSourceRegister,
      codec: String) {
    implicit val e = ExpressionEncoder[java.lang.Long]
    val path = Utils.createTempDir()
    path.delete()

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    val testingText = fileFormat.isInstanceOf[TextFileFormat]

    def writeRange(start: Int, end: Int, numPartitions: Int): Seq[String] = {
      val df = if (testingText) {
          // For `text` format, we'll have only one unpartitioned column
          spark
            .range(start, end, 1, numPartitions)
            .flatMap(x => Iterator(x, x, x)).toDF("id")
            .select($"id", lit("foo").as("value"))
        } else {
          // For the other formats, we'll have two unpartitioned columns
          spark
            .range(start, end, 1, numPartitions)
            .flatMap(x => Iterator(x, x, x)).toDF("id")
            .select($"id", lit("foo").as("value1"), lit("bar").as("value2"))
        }
      require(df.rdd.partitions.size === numPartitions)
      val writer = new FileStreamSinkWriter(
        df,
        fileFormat,
        path.toString,
        partitionColumnNames = Seq("id"),
        hadoopConf,
        Map("compression" -> codec))
      // `path` would be like "file:///some/path/..." if run on Hadoop 2.2.0
      // `path` would be like "file:/some/path/..."   if run on Hadoop 2.3.0 and onwards
      // so path.stripPrefix("file://").stripPrefix("file:") would give us "/some/path/..."
      writer.write().map(_.path.stripPrefix("file://").stripPrefix("file:"))
    }

    def checkOneFileWrittenPerKey(keys: Seq[Int], filesWritten: Seq[String]): Unit = {
      keys.foreach { id =>
        assert(
          filesWritten.count(_.contains(s"/id=$id/")) == 1,
          s"no file for id=$id. all files: \n\t${filesWritten.mkString("\n\t")}"
        )
      }
    }

    // Write and check whether new files are written correctly
    val files1 = writeRange(0, 10, 2)
    assert(files1.size === 10, s"unexpected number of files:\n${files1.mkString("\n")}")
    checkFilesExist(path, files1, "file not written")
    checkOneFileWrittenPerKey(0 until 10, files1)

    val answer1 = if (testingText) {
        (0 until 10).flatMap(x => Iterator(x, x, x)).map(Row("foo", _))
      } else {
        (0 until 10).flatMap(x => Iterator(x, x, x)).map(Row("foo", "bar", _))
      }
    checkAnswer(spark.read.format(fileFormat.shortName()).load(path.getCanonicalPath), answer1)

    // Append and check whether new files are written correctly and old files still exist
    val files2 = writeRange(0, 20, 3)
    assert(files2.size === 20, s"unexpected number of files:\n${files2.mkString("\n")}")
    assert(files2.intersect(files1).isEmpty, "old files returned")
    checkFilesExist(path, files2, s"New file not written")
    checkFilesExist(path, files1, s"Old file not found")
    checkOneFileWrittenPerKey(0 until 20, files2)

    val answer2 = if (testingText) {
        (0 until 20).flatMap(x => Iterator(x, x, x)).map(Row("foo", _))
      }
      else {
        (0 until 20).flatMap(x => Iterator(x, x, x)).map(Row("foo", "bar", _))
      }

    checkAnswer(
        spark.read.format(fileFormat.shortName()).load(path.getCanonicalPath),
        answer1 ++ answer2)
  }

  test("csv - unpartitioned writing and batch reading - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec =>
      testUnpartitionedWritingAndBatchReading(new CSVFileFormat, codec)
    }
  }

  test("json - unpartitioned writing and batch reading - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec =>
      testUnpartitionedWritingAndBatchReading(new JsonFileFormat, codec)
    }
  }

  test("parquet - unpartitioned writing and batch reading - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec =>
      testUnpartitionedWritingAndBatchReading(new ParquetFileFormat, codec)
    }
  }

  test("text - unpartitioned writing and batch reading - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec =>
      testUnpartitionedWritingAndBatchReading(new TextFileFormat, codec)
    }
  }

  private def testUnpartitionedWritingAndBatchReading(
      fileFormat: FileFormat with DataSourceRegister,
      codec: String) {

    val inputData = MemoryStream[String]
    val df = inputData.toDF()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: ContinuousQuery = null

    try {
      query =
        df.write
          .format(fileFormat.shortName())
          .option("checkpointLocation", checkpointDir)
          .option("compression", codec)
          .startStream(outputDir)

      inputData.addData("1", "2", "3")

      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }

      val outputDf = spark.read.format(fileFormat.shortName()).load(outputDir).as[String]
      checkDataset(outputDf, "1", "2", "3")

    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("FileStreamSink - csv - partitioned writing and batch reading - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec =>
      testPartitionedWritingAndBatchReading(new CSVFileFormat, codec)
    }
  }

  test("FileStreamSink - json - partitioned writing and batch reading - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach { codec =>
      testPartitionedWritingAndBatchReading(new JsonFileFormat, codec)
    }
  }

  test("FileStreamSink - parquet - partitioned writing and batch reading - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach {
      codec => testPartitionedWritingAndBatchReading(new ParquetFileFormat, codec)
    }
  }

  test("FileStreamSink - text - partitioned writing and batch reading - codecs: none/gzip") {
    COMPRESSION_CODECS.foreach {
      codec => testPartitionedWritingAndBatchReading(new TextFileFormat, codec)
    }
  }

  private def testPartitionedWritingAndBatchReading(
      fileFormat: FileFormat with DataSourceRegister,
      codec: String) {

    val inputData = MemoryStream[Int]
    val ds = inputData.toDS()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: ContinuousQuery = null

    try {
      query =
        ds.map(i => (i, (i * 1000).toString))
          .toDF("id", "value")
          .write
          .format(fileFormat.shortName())
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir)
          .option("header", "true") // this is only for `cvs` to save column names into files
          .startStream(outputDir)

      inputData.addData(1, 2, 3)
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }

      val outputDf =
        sqlContext
          .read
          .format(fileFormat.shortName())
          .option("compression", codec)
          .option("header", "true") // this is only for `cvs` to load column names back from files
          .load(outputDir)

      val expectedSchema = new StructType()
        .add(StructField("value", StringType))
        .add(StructField("id", IntegerType))
      assert(outputDf.schema === expectedSchema)

      // Verify that MetadataLogFileCatalog is being used and the correct partitioning schema has
      // been inferred
      val hadoopdFsRelations = outputDf.queryExecution.analyzed.collect {
        case LogicalRelation(baseRelation, _, _) if baseRelation.isInstanceOf[HadoopFsRelation] =>
          baseRelation.asInstanceOf[HadoopFsRelation]
      }
      assert(hadoopdFsRelations.size === 1)
      assert(hadoopdFsRelations.head.location.isInstanceOf[MetadataLogFileCatalog])
      assert(hadoopdFsRelations.head.partitionSchema.exists(_.name == "id"))
      assert(hadoopdFsRelations.head.dataSchema.exists(_.name == "value"))

      // Verify the data is correctly read
      checkDataset(
        outputDf.as[(String, Int)],
        ("1000", 1), ("2000", 2), ("3000", 3))

      /** Check some condition on the partitions of the FileScanRDD generated by a DF */
      def checkFileScanPartitions(df: DataFrame)(func: Seq[FilePartition] => Unit): Unit = {
        val getFileScanRDD = df.queryExecution.executedPlan.collect {
          case scan: DataSourceScanExec if scan.rdd.isInstanceOf[FileScanRDD] =>
            scan.rdd.asInstanceOf[FileScanRDD]
        }.headOption.getOrElse {
          fail(s"No FileScan in query\n${df.queryExecution}")
        }
        func(getFileScanRDD.filePartitions)
      }

      // Read without pruning
      checkFileScanPartitions(outputDf) { partitions =>
        // There should be as many distinct partition values as there are distinct ids
        assert(partitions.flatMap(_.files.map(_.partitionValues)).distinct.size === 3)
      }

      // Read with pruning, should read only files in partition dir id=1
      checkFileScanPartitions(outputDf.filter("id = 1")) { partitions =>
        val filesToBeRead = partitions.flatMap(_.files)
        assert(filesToBeRead.map(_.filePath).forall(_.contains("/id=1/")))
        assert(filesToBeRead.map(_.partitionValues).distinct.size === 1)
      }

      // Read with pruning, should read only files in partition dir id=1 and id=2
      checkFileScanPartitions(outputDf.filter("id in (1,2)")) { partitions =>
        val filesToBeRead = partitions.flatMap(_.files)
        assert(!filesToBeRead.map(_.filePath).exists(_.contains("/id=3/")))
        assert(filesToBeRead.map(_.partitionValues).distinct.size === 2)
      }
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("FileStreamSink - supported formats") {
    def testFormat(format: Option[String]): Unit = {
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()

      val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
      val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

      var query: ContinuousQuery = null

      try {
        val writer =
          ds.map(i => (i, i * 1000))
            .toDF("id", "value")
            .write
        if (format.nonEmpty) {
          writer.format(format.get)
        }
        query = writer
            .option("checkpointLocation", checkpointDir)
            .startStream(outputDir)
      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }

    testFormat(None) // should not throw error as default format parquet when not specified
    testFormat(Some("csv"))
    testFormat(Some("json"))
    testFormat(Some("parquet"))
    testFormat(Some("text"))
    val e = intercept[UnsupportedOperationException] {
      testFormat(Some("jdbc"))
    }
    Seq("jdbc", "not support", "stream").foreach { s =>
      assert(e.getMessage.contains(s))
    }
  }

  private def checkFilesExist(dir: File, expectedFiles: Seq[String], msg: String): Unit = {
    import scala.collection.JavaConverters._
    val files =
      // To filter out files like '.some_file.txt.gz.crc', but to keep 'some_file.txt.gz'
      FileUtils.listFiles(dir, new RegexFileFilter("[^.].*"), DirectoryFileFilter.DIRECTORY)
        .asScala
        .map(_.getCanonicalPath)

    expectedFiles.foreach { f =>
      // we examine the prefix, but not the extension
      assert(files.map(_.startsWith(f)).reduce(_ || _),
        s"\n$msg\nexpected file:\n\t$f\nfound files:\n${files.mkString("\n\t")}")
    }
  }

  test("getSinkFileStatusUsingGlob()") {
    val dir = Utils.createTempDir(namePrefix = "streaming").getCanonicalPath

    val uuid = UUID.randomUUID().toString
    val file = new File(dir, uuid + ".txt.gz")
    file.createNewFile()

    val fileSinkFileStatus =
      FileStreamSink.getSinkFileStatusUsingGlob(Seq(new Path(dir, uuid)), new Configuration)
    assert(fileSinkFileStatus != null)
    assert(fileSinkFileStatus.length == 1)
    assert(
      // `path` would be like "file:///some/path/..." if run on Hadoop 2.2.0
      // `path` would be like "file:/some/path/..."   if run on Hadoop 2.3.0 and onwards
      // so path.stripPrefix("file://").stripPrefix("file:") would give us "/some/path/..."
      fileSinkFileStatus.head.path.stripPrefix("file://").stripPrefix("file:")
        == file.getCanonicalPath)
    assert(fileSinkFileStatus.head.isDir == false)
    assert(fileSinkFileStatus.head.action == "add")

    val file2 = new File(dir, uuid + ".unknown")
    file2.createNewFile()

    // We would expect only one file being matched, so it should fail if more than one file match
    val e = intercept[AssertionError](
      FileStreamSink.getSinkFileStatusUsingGlob(Seq(new Path(dir, uuid)), new Configuration))
    Seq("unexpected number", "matching").foreach { s =>
      assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
    }
  }
}
