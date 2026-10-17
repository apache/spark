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

package org.apache.spark.sql.execution.datasources.text

import java.io.{File, FileOutputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.{CompressionCodecFactory, CompressionInputStream, Decompressor, DefaultCodec, GzipCodec}

import org.apache.spark.{SparkConf, SparkIllegalArgumentException, SparkUnsupportedOperationException, TestUtils}
import org.apache.spark.io.{HadoopCodecStreams, ZStdCompressionCodec}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.util.HadoopCompressionCodec
import org.apache.spark.sql.catalyst.util.HadoopCompressionCodec.{BZIP2, DEFLATE, GZIP, LZ4, NONE, SNAPPY, UNCOMPRESSED}
import org.apache.spark.sql.execution.datasources.CommonFileDataSourceSuite
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, FileScan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.Utils

abstract class TextSuite extends SharedSparkSession with CommonFileDataSourceSuite {
  import testImplicits._

  override protected def dataSourceFormat = "text"

  test("reading text file") {
    verifyFrame(spark.read.format("text").load(testFile))
  }

  test("SQLContext.read.text() API") {
    verifyFrame(spark.read.text(testFile))
  }

  test("SPARK-12562 verify write.text() can handle column name beyond `value`") {
    val df = spark.read.text(testFile).withColumnRenamed("value", "adwrasdf")

    val tempFile = Utils.createTempDir()
    tempFile.delete()
    df.write.text(tempFile.getCanonicalPath)
    verifyFrame(spark.read.text(tempFile.getCanonicalPath))

    Utils.deleteRecursively(tempFile)
  }

  test("error handling for invalid schema") {
    val tempFile = Utils.createTempDir()
    tempFile.delete()

    val df = spark.range(2)
    intercept[AnalysisException] {
      df.write.text(tempFile.getCanonicalPath)
    }

    intercept[AnalysisException] {
      spark.range(2).select(df("id"), df("id") + 1).write.text(tempFile.getCanonicalPath)
    }
  }

  test("reading partitioned data using read.textFile()") {
    val partitionedData = Thread.currentThread().getContextClassLoader
      .getResource("test-data/text-partitioned").toString
    val ds = spark.read.textFile(partitionedData)
    val data = ds.collect()

    assert(ds.schema == new StructType().add("value", StringType))
    assert(data.length == 2)
  }

  test("support for partitioned reading using read.text()") {
    val partitionedData = Thread.currentThread().getContextClassLoader
      .getResource("test-data/text-partitioned").toString
    val df = spark.read.text(partitionedData)
    val data = df.filter("year = '2015'").select("value").collect()

    assert(data(0) == Row("2015-test"))
    assert(data.length == 1)
  }

  test("SPARK-13503 Support to specify the option for compression codec for TEXT") {
    val testDf = spark.read.text(testFile)
    val extensionNameMap = Seq(BZIP2, DEFLATE, GZIP, LZ4, SNAPPY)
      .map(codec => codec.lowerCaseName() -> codec.getCompressionCodec.getDefaultExtension)
    extensionNameMap.foreach {
      case (codecName, extension) =>
        val tempDir = Utils.createTempDir()
        val tempDirPath = tempDir.getAbsolutePath
        testDf.write.option("compression", codecName).mode(SaveMode.Overwrite).text(tempDirPath)
        val compressedFiles = new File(tempDirPath).listFiles()
        assert(compressedFiles.exists(_.getName.endsWith(s".txt$extension")))
        verifyFrame(spark.read.text(tempDirPath))
    }

    withTempDir { dir =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          testDf.write.option("compression", "illegal").mode(
            SaveMode.Overwrite).text(dir.getAbsolutePath)
        },
        condition = "CODEC_NOT_AVAILABLE.WITH_AVAILABLE_CODECS_SUGGESTION",
        parameters = Map(
          "codecName" -> "illegal",
          "availableCodecs" -> "bzip2, deflate, uncompressed, snappy, none, lz4, gzip")
      )
    }
  }

  test("SPARK-13543 Write the output as uncompressed via option()") {
    val extraOptions = Map[String, String](
      "mapreduce.output.fileoutputformat.compress" -> "true",
      "mapreduce.output.fileoutputformat.compress.type" -> CompressionType.BLOCK.toString,
      "mapreduce.map.output.compress" -> "true",
      "mapreduce.output.fileoutputformat.compress.codec" -> classOf[GzipCodec].getName,
      "mapreduce.map.output.compress.codec" -> classOf[GzipCodec].getName
    )
    withTempDir { dir =>
      val testDf = spark.read.text(testFile)
      val tempDirPath = dir.getAbsolutePath
      testDf.write.option("compression", NONE.lowerCaseName())
        .options(extraOptions).mode(SaveMode.Overwrite).text(tempDirPath)
      val compressedFiles = new File(tempDirPath).listFiles()
      assert(compressedFiles.exists(!_.getName.endsWith(".txt.gz")))
      verifyFrame(spark.read.options(extraOptions).text(tempDirPath))
    }
  }

  test("case insensitive option") {
    val extraOptions = Map[String, String](
      "mApReDuCe.output.fileoutputformat.compress" -> "true",
      "mApReDuCe.output.fileoutputformat.compress.type" -> CompressionType.BLOCK.toString,
      "mApReDuCe.map.output.compress" -> "true",
      "mApReDuCe.output.fileoutputformat.compress.codec" -> classOf[GzipCodec].getName,
      "mApReDuCe.map.output.compress.codec" -> classOf[GzipCodec].getName
    )
    withTempDir { dir =>
      val testDf = spark.read.text(testFile)
      val tempDirPath = dir.getAbsolutePath
      testDf.write.option("CoMpReSsIoN", NONE.lowerCaseName())
        .options(extraOptions).mode(SaveMode.Overwrite).text(tempDirPath)
      val compressedFiles = new File(tempDirPath).listFiles()
      assert(compressedFiles.exists(!_.getName.endsWith(".txt.gz")))
      verifyFrame(spark.read.options(extraOptions).text(tempDirPath))
    }
  }

  test("SPARK-14343: select partitioning column") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val ds1 = spark.range(1).selectExpr("CONCAT('val_', id)")
      ds1.write.text(s"$path/part=a")
      ds1.write.text(s"$path/part=b")

      checkAnswer(
        spark.read.format("text").load(path).select($"part"),
        Row("a") :: Row("b") :: Nil)
    }
  }

  test("SPARK-15654: should not split gz files") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val df1 = spark.range(0, 1000).selectExpr("CAST(id AS STRING) AS s")
      df1.write.option("compression", GZIP.lowerCaseName()).mode("overwrite").text(path)

      val expected = df1.collect()
      Seq(10, 100, 1000).foreach { bytes =>
        withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> bytes.toString) {
          val df2 = spark.read.format("text").load(path)
          checkAnswer(df2, expected)
        }
      }
    }
  }

  def testLineSeparator(lineSep: String): Unit = {
    test(s"SPARK-23577: Support line separator - lineSep: '$lineSep'") {
      // Read
      val values = Seq("a", "b", "\nc")
      val data = values.mkString(lineSep)
      val dataWithTrailingLineSep = s"$data$lineSep"
      Seq(data, dataWithTrailingLineSep).foreach { lines =>
        withTempPath { path =>
          Files.write(path.toPath, lines.getBytes(StandardCharsets.UTF_8))
          val df = spark.read.option("lineSep", lineSep).text(path.getAbsolutePath)
          checkAnswer(df, Seq("a", "b", "\nc").toDF())
        }
      }

      // Write
      withTempPath { path =>
        values.toDF().coalesce(1)
          .write.option("lineSep", lineSep).text(path.getAbsolutePath)
        val partFile = TestUtils.recursiveList(path).filter(f => f.getName.startsWith("part-")).head
        val readBack = new String(Files.readAllBytes(partFile.toPath), StandardCharsets.UTF_8)
        assert(readBack === s"a${lineSep}b${lineSep}\nc${lineSep}")
      }

      // Roundtrip
      withTempPath { path =>
        val df = values.toDF()
        df.write.option("lineSep", lineSep).text(path.getAbsolutePath)
        val readBack = spark.read.option("lineSep", lineSep).text(path.getAbsolutePath)
        checkAnswer(df, readBack)
      }
    }
  }

  // scalastyle:off nonascii
  Seq("|", "^", "::", "!!!@3", 0x1E.toChar.toString, "아").foreach { lineSep =>
    testLineSeparator(lineSep)
  }
  // scalastyle:on nonascii

  private def testFile: String = {
    Thread.currentThread().getContextClassLoader.getResource("test-data/text-suite.txt").toString
  }

  /** Verifies data and schema. */
  private def verifyFrame(df: DataFrame): Unit = {
    // schema
    assert(df.schema == new StructType().add("value", StringType))

    // verify content
    val data = df.collect()
    assert(data(0) == Row("This is a test file for the text data source"))
    assert(data(1) == Row("1+1"))
    // scalastyle:off nonascii
    assert(data(2) == Row("数据砖头"))
    // scalastyle:on nonascii
    assert(data(3) == Row("\"doh\""))
    assert(data.length == 4)
  }

  test("SPARK-40667: validate Text Options") {
    assert(TextOptions.getAllOptions.size == 5)
    // Please add validation on any new Text options here
    assert(TextOptions.isValidOption("compression"))
    assert(TextOptions.isValidOption("decompression"))
    assert(TextOptions.isValidOption("wholetext"))
    assert(TextOptions.isValidOption("encoding"))
    assert(TextOptions.isValidOption("lineSep"))
  }

  private def createTestFiles(dir: File, fileFormatWriter: Boolean,
    wholeText: Boolean): Seq[Row] = {
    val numRecord = 100
    val codecExtensionMap = HadoopCompressionCodec.values()
      .map(c => (c.lowerCaseName(),
        Option(c.getCompressionCodec).map(_.getDefaultExtension).getOrElse(""))) ++
      Seq(("zstd", ".zst"), ("zstd", ".zstd"), ("gzip", ".gzip"))

    val codecFactory = new CompressionCodecFactory(spark.sessionState.newHadoopConf())
    codecExtensionMap.foreach { case (codec, ext) =>

      val data = (1 to numRecord).map(i => s"$i, value_${codec}$ext").mkString("\n")
      val file = new File(dir, s"test_$codec.txt$ext")

      // file data source writers do not support zstd codec yet.
      if (fileFormatWriter && !codec.equals("zstd")) {
        val df = Seq(data).toDF("value")
        df.coalesce(1).write
          .format("text")
          .option("compression", codec)
          .save(file.getCanonicalPath)

        val compressedFiles = new File(file.getCanonicalPath).listFiles()

        compressedFiles.foreach { file =>
          if (file.isFile && file.getName.startsWith("part")) {
            val newName = file.getName.split("\\.").init.mkString(".") + ext
            val status = file.renameTo(new File(dir, newName))
            assert(status)
          }
        }
      } else {
        val os = new FileOutputStream(file)

        val outputStream = codec match {
          case "zstd" =>
            new ZStdCompressionCodec(sparkConf).compressedOutputStream(os)
          case codec if ext.nonEmpty =>
            val compressionCodec = codecFactory.getCodecByName(codec)
            compressionCodec.createOutputStream(os)
          case _ => os
        }
        outputStream.write(data.getBytes(StandardCharsets.UTF_8))
        outputStream.close()
      }
    }

    val expectedOutput = codecExtensionMap.flatMap {
      case (codec, ext) =>
        val data = (1 to numRecord).map(i => s"$i, value_${codec}$ext")
        if (wholeText) {
          if (fileFormatWriter && !codec.equals("zstd")) {
            Seq(Row(data.mkString("", "\n", "\n")))
          } else {
            Seq(Row(data.mkString("\n")))
          }
        } else {
          data.map(Row(_))
        }
    }.toSeq
    val numRows = if (wholeText) 1 else numRecord
    assert(expectedOutput.length == codecExtensionMap.length * numRows)
    expectedOutput
  }

  test("Test all supported codec and extension including zst, zstd and gzip") {
    for (
      wholeText <- Seq(true, false);
      fileFormatWriter <- Seq(true, false)
    ) {
      logInfo(s"Testing with wholeText=$wholeText, fileFormatWriter=$fileFormatWriter")

      withTempDir { dir =>
        val options = Map(
          "wholeText" -> wholeText.toString,
          "recursiveFileLookup" -> "true"
        )

        val expectedOutput = createTestFiles(dir, fileFormatWriter, wholeText)
        val df = spark.read.options(options).text(dir.getCanonicalPath)
        checkAnswer(df, expectedOutput)
      }
    }
  }

  // Writes `data` to `file` compressed with `codec` (a short name; "" means no compression), using
  // Spark's built-in Zstandard codec for zstd (which Hadoop may not be compiled with) and Hadoop's
  // CompressionCodecFactory otherwise. The file name intentionally does NOT carry the codec's
  // extension, so extension-based detection cannot pick the right codec and the
  // `decompression` option is what makes the read succeed.
  private def writeCompressed(file: File, data: String, codec: String): Unit = {
    val codecFactory = new CompressionCodecFactory(spark.sessionState.newHadoopConf())
    val os = new FileOutputStream(file)
    val out = codec match {
      case "" => os
      case "zstd" | "zstandard" => new ZStdCompressionCodec(sparkConf).compressedOutputStream(os)
      case name => codecFactory.getCodecByName(name).createOutputStream(os)
    }
    out.write(data.getBytes(StandardCharsets.UTF_8))
    out.close()
  }

  test("SPARK-60217 decompression option overrides file name extension") {
    val numRecord = 100
    val data = (1 to numRecord).map(i => s"$i, value_$i")
    // Every read-supported codec, including zstd/zstandard which the writers do not support.
    val codecs = HadoopCompressionCodec.values().map(_.lowerCaseName())
      .filterNot(c => c == NONE.lowerCaseName() || c == UNCOMPRESSED.lowerCaseName()) ++
      Seq("zstd", "zstandard")
    for (wholeText <- Seq(true, false); codec <- codecs) {
      withTempDir { dir =>
        // Misleading `.txt` name (no codec suffix): extension-based detection would read it as
        // plain text and fail. The `decompression` option forces the correct codec.
        val file = new File(dir, "data.txt")
        writeCompressed(file, data.mkString("\n"), codec)

        val df = spark.read
          .option("decompression", codec)
          .option("wholeText", wholeText.toString)
          .text(file.getCanonicalPath)
        val expected = if (wholeText) Seq(Row(data.mkString("\n"))) else data.map(Row(_))
        checkAnswer(df, expected)
      }
    }
  }

  test("SPARK-60217 decompression option none reads a file with a codec extension as-is") {
    withTempDir { dir =>
      val data = "not really gzip compressed"
      // A plain-text file that happens to carry a `.gz` extension. Extension-based detection would
      // try (and fail) to gunzip it; `decompression` = none must read the raw bytes.
      val file = new File(dir, "plain.txt.gz")
      writeCompressed(file, data, "")

      Seq(NONE, UNCOMPRESSED).foreach { codec =>
        val df = spark.read
          .option("decompression", codec.lowerCaseName())
          .text(file.getCanonicalPath)
        checkAnswer(df, Row(data) :: Nil)
      }
    }
  }

  test("SPARK-60217 decompression option rejects an invalid codec") {
    withTempPath { dir =>
      Seq("value").toDF().write.text(dir.getCanonicalPath)
      // The suggested codec list is built from a Map, so its order is not asserted here; the read
      // set additionally includes zstd/zstandard on top of the write-supported codecs.
      checkErrorMatchPVals(
        exception = intercept[SparkIllegalArgumentException] {
          spark.read.option("decompression", "illegal").text(dir.getCanonicalPath).collect()
        },
        condition = "CODEC_NOT_AVAILABLE.WITH_AVAILABLE_CODECS_SUGGESTION",
        parameters = Map(
          "codecName" -> "illegal",
          "availableCodecs" -> ".*zstd.*")
      )
    }
  }

  test("SPARK-60217 decompression option decides whether a file is split") {
    val lines = (1 to 2000).map(i => s"line_$i")
    // (codec the file is written with, file name, `decompression`, whether it is split)
    Seq(
      // bzip2 is splittable even though the `.txt` name says nothing about it.
      (BZIP2.lowerCaseName(), "data.txt", BZIP2.lowerCaseName(), true),
      // gzip is not splittable, so a forced gzip `.txt` file is read as a single split.
      (GZIP.lowerCaseName(), "data.txt", GZIP.lowerCaseName(), false),
      // `none` reads a plain file named `.gz` as-is, so it is split like any plain file.
      ("", "data.txt.gz", NONE.lowerCaseName(), true)
    ).foreach { case (writeCodec, name, readCodec, split) =>
      withTempDir { dir =>
        val file = new File(dir, name)
        writeCompressed(file, lines.mkString("\n"), writeCodec)
        withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "256") {
          val df = spark.read.option("decompression", readCodec).text(file.getCanonicalPath)
          val numPartitions = df.rdd.getNumPartitions
          assert(if (split) numPartitions > 1 else numPartitions == 1,
            s"decompression=$readCodec on $name: $numPartitions partitions")
          checkAnswer(df, lines.map(Row(_)))
        }
      }
    }
  }

  test("SPARK-60217 a forced codec that fails is not replaced by zstd based on the extension") {
    withTempDir { dir =>
      // A zstd file with a `.zst` name, read with a forced codec whose streams cannot be created.
      // The zstd fallback for codecs that fail must not kick in, since a codec was forced.
      val file = new File(dir, "data.txt.zst")
      writeCompressed(file, "a\nb", "zstd")
      Seq(false, true).foreach { wholeText =>
        val e = intercept[Exception] {
          spark.read
            .option("decompression", classOf[FailingDecompressionCodec].getName)
            .option("wholetext", wholeText.toString)
            .text(file.getCanonicalPath).collect()
        }
        assert(Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
          .exists(_.getMessage == FailingDecompressionCodec.message), s"wholetext=$wholeText")
      }
    }
  }

  test("SPARK-60217 decompression option is rejected with the legacy line reader") {
    withTempPath { dir =>
      Seq("value").toDF().write.text(dir.getCanonicalPath)
      // Hadoop's LineRecordReader infers the codec from the extension and would ignore the option.
      withSQLConf(SQLConf.HADOOP_LINE_RECORD_READER_ENABLED.key -> "false") {
        checkError(
          exception = intercept[SparkUnsupportedOperationException] {
            spark.read.option("decompression", "gzip").text(dir.getCanonicalPath).collect()
          },
          condition = "UNSUPPORTED_FEATURE.DECOMPRESSION_CODEC_WITH_LEGACY_LINE_READER",
          parameters = Map(
            "option" -> "\"decompression\"",
            "config" -> s"\"${SQLConf.HADOOP_LINE_RECORD_READER_ENABLED.key}\""))
      }
    }
  }

  test("SPARK-60217 an inherited internal decompression codec key is not honored") {
    withTempDir { dir =>
      val file = new File(dir, "plain.txt")
      writeCompressed(file, "plain text", "")
      // Session confs are copied into the read's Hadoop conf; the read must unset the internal key
      // since it did not set `decompression`, so the plain file is not gunzipped.
      withSQLConf(HadoopCodecStreams.DECOMPRESSION_CODEC_CONF -> classOf[GzipCodec].getName) {
        checkAnswer(spark.read.text(file.getCanonicalPath), Row("plain text") :: Nil)
      }
    }
  }

  test("SPARK-60217 compression option does not force a codec on read") {
    withTempDir { dir =>
      // Unlike `decompression`, the write-side `compression` option (which may be persisted
      // in a table's options) must not force a codec on read: a plain file is still read as-is.
      val file = new File(dir, "plain.txt")
      writeCompressed(file, "plain text", "")

      val df = spark.read
        .option("compression", GZIP.lowerCaseName())
        .text(file.getCanonicalPath)
      checkAnswer(df, Row("plain text") :: Nil)
    }
  }
}

class TextV1Suite extends TextSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "text")
}

@ExtendedSQLTest
class TextV2Suite extends TextSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("SPARK-60217 unsplittable reason reports a forced decompression codec") {
    withTempDir { dir =>
      val file = new File(dir, "data.txt")
      Files.write(file.toPath, "line".getBytes(StandardCharsets.UTF_8))
      def reason(options: Map[String, String]): String = {
        val df = spark.read.options(options).text(file.getCanonicalPath)
        val scan = df.queryExecution.optimizedPlan.collectFirst {
          case r: DataSourceV2ScanRelation => r.scan.asInstanceOf[FileScan]
        }.get
        scan.getFileUnSplittableReason(new Path(file.getCanonicalPath))
      }
      // The `.txt` name alone is splittable, so the forced codec is what prevents splitting.
      assert(reason(Map("decompression" -> GZIP.lowerCaseName())) ==
        "the file is compressed by unsplittable compression codec")
      // A forced `none` keeps the file splittable, so wholetext is the reason.
      assert(reason(Map("decompression" -> NONE.lowerCaseName(), "wholetext" -> "true")) ==
        "the text datasource is set wholetext mode")
    }
  }
}

/** A codec that fails to create any input stream, like a codec whose native library is missing. */
class FailingDecompressionCodec extends DefaultCodec {
  override def createInputStream(in: InputStream): CompressionInputStream =
    throw new UnsupportedOperationException(FailingDecompressionCodec.message)

  override def createInputStream(
      in: InputStream,
      decompressor: Decompressor): CompressionInputStream =
    throw new UnsupportedOperationException(FailingDecompressionCodec.message)
}

object FailingDecompressionCodec {
  val message = "Cannot create a FailingDecompressionCodec input stream"
}
