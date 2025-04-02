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

package org.apache.spark.sql.execution.datasources

import java.io.File

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, FileSourceConstantMetadataStructField, FileSourceGeneratedMetadataStructField, Literal}
import org.apache.spark.sql.classic.{DataFrame, Dataset}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/** Verifies the ability for a FileFormat to define custom metadata types */
class FileSourceCustomMetadataStructSuite extends QueryTest with SharedSparkSession {
  import FileSourceCustomMetadataStructSuite._

  val extraConstantMetadataFields = Seq(
    FileSourceConstantMetadataStructField("foo", IntegerType),
    FileSourceConstantMetadataStructField("bar", StringType))

  // Shamelessly recycle the parquet row index generated field.
  val extraGeneratedMetadataFields = Seq(ParquetFileFormat.ROW_INDEX_FIELD.copy(name = "baz"))

  val FILE_SCHEMA = new StructType()
    .add("fileNum", IntegerType)
    .add("x", LongType)

  def withTempData(
    testFileFormat: String, fileSchema: StructType)(
    f: (DataFrame, FileStatus, FileStatus) => Unit): Unit = {

    withTempDir { dir =>
      val f0 = new File(dir, "data/f0").getCanonicalPath
      spark.range(101, 103)
        .select(lit(0).as("fileNum"), col("id").as("x"), when(col("id") % 2 === 0, lit(9)).as("y"))
        .coalesce(1)
        .write.format(testFileFormat).save(f0)

      val f1 = new File(dir, "data/f1").getCanonicalPath
      spark.range(111, 113)
        .select(lit(1).as("fileNum"), col("id").as("x"), when(col("id") % 2 === 0, lit(7)).as("y"))
        .coalesce(1)
        .write.format(testFileFormat).save(f1)

      // 2. read both f0 and f1
      val df = spark.read.format(testFileFormat).schema(fileSchema)
        .load(new File(dir, "data").getCanonicalPath + "/*")

      val hadoopConf = spark.sessionState.newHadoopConfWithOptions(Map.empty)
      def getFileStatus(subdir: String): FileStatus = {
        val file = new File(dir, subdir)
          .listFiles()
          .filter(_.getName.endsWith(s".$testFileFormat"))
          .head
        val path = new Path(file.toURI)
        val fs = path.getFileSystem(hadoopConf)
        fs.getFileStatus(path)
      }

      val realF0 = getFileStatus("data/f0")
      val realF1 = getFileStatus("data/f1")
      assert(
        df.inputFiles.map(new Path(_).getName).toSet ==
        Seq(realF0, realF1).map(_.getPath.getName).toSet)
      f(df, realF0, realF1)
    }
  }

  def createDF(format: FileFormat, files: Seq[FileStatusWithMetadata]): DataFrame = {
    val index = TestFileIndex(files)
    val fsRelation = HadoopFsRelation(
      index, index.partitionSchema, FILE_SCHEMA, None, format, Map.empty)(spark)
    Dataset.ofRows(spark, LogicalRelation(fsRelation))
  }

  test("extra constant metadata fields") {
    withTempData("parquet", FILE_SCHEMA) { (_, f0, f1) =>
      val format = new TestFileFormat(extraConstantMetadataFields)
      val files = Seq(
        // NOTE: The implementation accepts both raw values and literals for any field, and
        // StringType fields can be String or UTF8String.
        FileStatusWithMetadata(f0, Map("foo" -> 0, "bar" -> UTF8String.fromString("000"))),
        FileStatusWithMetadata(f1, Map("foo" -> Literal(1), "bar" -> "111")))
      val df = createDF(format, files)

      // Query in declared order
      checkAnswer(
        df.select("fileNum", "x", "_metadata.row_index", "_metadata.foo", "_metadata.bar"),
        Seq(
          Row(0, 101L, 0L, 0, "000"),
          Row(0, 102L, 1L, 0, "000"),
          Row(1, 111L, 0L, 1, "111"),
          Row(1, 112L, 1L, 1, "111")))

      // Query in permuted order
      checkAnswer(
        df.select("_metadata.bar", "fileNum", "_metadata.row_index", "_metadata.foo", "x"),
        Seq(
          Row("000", 0, 0L, 0, 101L),
          Row("000", 0, 1L, 0, 102L),
          Row("111", 1, 0L, 1, 111L),
          Row("111", 1, 1L, 1, 112L)))

      // Query with repeated metadata fields
      checkAnswer(
        df.select(
          "_metadata.bar", "_metadata.foo", "fileNum", "x", "_metadata.row_index",
          "_metadata.foo", "_metadata.bar"),
        Seq(
          Row("000", 0, 0, 101L, 0L, 0, "000"),
          Row("000", 0, 0, 102L, 1L, 0, "000"),
          Row("111", 1, 1, 111L, 0L, 1, "111"),
          Row("111", 1, 1, 112L, 1L, 1, "111")))
    }
  }

  test("[SPARK-43226] extra constant metadata fields with extractors") {
    withTempData("parquet", FILE_SCHEMA) { (_, f0, f1) =>
      val format = new TestFileFormat(extraConstantMetadataFields) {
        val extractPartitionNumber = { pf: PartitionedFile =>
          pf.toPath.toString.split("/").collectFirst {
            case "f0" => 9990
            case "f1" => 9991
          }.get
        }
        val extractPartitionName = { pf: PartitionedFile =>
          pf.toPath.toString.split("/").collectFirst {
            case "f0" => "f0f"
            case "f1" => "f1f"
          }.get
        }
        override def fileConstantMetadataExtractors: Map[String, PartitionedFile => Any] = {
          super.fileConstantMetadataExtractors ++ Map(
            "foo" -> extractPartitionNumber, "bar" -> extractPartitionName)
        }
      }
      val files = Seq(FileStatusWithMetadata(f0), FileStatusWithMetadata(f1))
      val df = createDF(format, files)

      checkAnswer(
        df.select("fileNum", "x", "_metadata.row_index", "_metadata.foo", "_metadata.bar"),
        Seq(
          Row(0, 101L, 0L, 9990, "f0f"),
          Row(0, 102L, 1L, 9990, "f0f"),
          Row(1, 111L, 0L, 9991, "f1f"),
          Row(1, 112L, 1L, 9991, "f1f")))
    }
  }

  test("filters and projections on extra constant metadata fields") {
    withTempData("parquet", FILE_SCHEMA) { (_, f0, f1) =>
      val format = new TestFileFormat(extraConstantMetadataFields)
      val files = Seq(
        FileStatusWithMetadata(f0, Map("foo" -> 0, "bar" -> "000")),
        FileStatusWithMetadata(f1, Map("foo" -> 1, "bar" -> "111")))
      val df = createDF(format, files)

      checkAnswer(
        df.select(
            col("fileNum"), col("x"), col("_metadata.row_index"),
            col("_metadata.foo").as("foofoo"), col("_metadata.bar").as("barbar"))
          .where("foofoo != 10 and barbar != '999'"),
        Seq(
          Row(0, 101L, 0L, 0, "000"),
          Row(0, 102L, 1L, 0, "000"),
          Row(1, 111L, 0L, 1, "111"),
          Row(1, 112L, 1L, 1, "111")))

      checkAnswer(
        df.select(
            col("fileNum"), col("x"), col("_metadata.row_index"),
            col("_metadata.foo").as("foofoo"), col("_metadata.bar").as("barbar"))
          .where("foofoo != 0"),
        Seq(
          Row(1, 111L, 0L, 1, "111"),
          Row(1, 112L, 1L, 1, "111")))

      checkAnswer(
        df.select(
            col("fileNum"), col("x"), col("_metadata.row_index"),
            col("_metadata.foo").as("foofoo"), col("_metadata.bar").as("barbar"))
          .where("barbar != '111'"),
        Seq(
          Row(0, 101L, 0L, 0, "000"),
          Row(0, 102L, 1L, 0, "000")))
    }
  }

  test("nullable extra constant metadata fields") {
    withTempData("parquet", FILE_SCHEMA) { (_, f0, f1) =>
      val format = new TestFileFormat(extraConstantMetadataFields)
      val files = Seq(
        FileStatusWithMetadata(f0, Map("foo" -> 0)), // no entry for bar
        FileStatusWithMetadata(f1, Map("foo" -> null, "bar" -> "111"))) // set foo null
      val df = createDF(format, files)

      // Query in declared order
      checkAnswer(
        df.select("fileNum", "x", "_metadata.row_index", "_metadata.foo", "_metadata.bar"),
        Seq(
          Row(0, 101L, 0L, 0, null),
          Row(0, 102L, 1L, 0, null),
          Row(1, 111L, 0L, null, "111"),
          Row(1, 112L, 1L, null, "111")))

      // Query in permuted order
      checkAnswer(
        df.select("_metadata.bar", "fileNum", "_metadata.row_index", "_metadata.foo", "x"),
        Seq(
          Row(null, 0, 0L, 0, 101L),
          Row(null, 0, 1L, 0, 102L),
          Row("111", 1, 0L, null, 111L),
          Row("111", 1, 1L, null, 112L)))

      // Query with repeated metadata fields
      checkAnswer(
        df.select(
          "_metadata.bar", "_metadata.foo", "fileNum", "x", "_metadata.row_index",
          "_metadata.foo", "_metadata.bar"),
        Seq(
          Row(null, 0, 0, 101L, 0L, 0, null),
          Row(null, 0, 0, 102L, 1L, 0, null),
          Row("111", null, 1, 111L, 0L, null, "111"),
          Row("111", null, 1, 112L, 1L, null, "111")))
    }
  }

  test("extra generated metadata field") {
    withTempData("parquet", FILE_SCHEMA) { (_, f0, f1) =>
      val format = new TestFileFormat(extraGeneratedMetadataFields)
      val files = Seq(
        // NOTE: The generated column should ignore this mapping
        FileStatusWithMetadata(f0, Map("baz" -> 1001L)),
        FileStatusWithMetadata(f1, Map("baz" -> 1002L)))
      val df = createDF(format, files)

      checkAnswer(
        df.select("fileNum", "x", "_metadata.baz"),
        Seq(
          Row(0, 101L, 0L),
          Row(0, 102L, 1L),
          Row(1, 111L, 0L),
          Row(1, 112L, 1L)))
    }
  }

  test("nullable extra generated metadata fields") {
    val schema = FILE_SCHEMA.add("y", IntegerType)
    withTempData("parquet", schema) { (_, f0, f1) =>
      // The column "y" contains some nulls. If we use it as the internal column that backs
      // _metadata.baz, the latter should still be queryable, and should also return nulls.
      val extraGeneratedMetadataFields =
        Seq(FileSourceGeneratedMetadataStructField("baz", "y", IntegerType))

      val format = new TestFileFormat(extraGeneratedMetadataFields)
      val files = Seq(
        // NOTE: The generated column should ignore this mapping
        FileStatusWithMetadata(f0, Map("baz" -> 0)),
        FileStatusWithMetadata(f1, Map("baz" -> "111")))
      val df = createDF(format, files)

      // Query in declared order
      checkAnswer(
        df.select("fileNum", "x", "_metadata.baz"),
        Seq(
          Row(0, 101L, null),
          Row(0, 102L, 9),
          Row(1, 111L, null),
          Row(1, 112L, 7)))
    }
  }

  test("filter and projection on extra generated metadata field") {
    withTempData("parquet", FILE_SCHEMA) { (_, f0, f1) =>
      val format = new TestFileFormat(extraGeneratedMetadataFields)
      val files = Seq(
        // NOTE: The generated column should ignore this mapping
        FileStatusWithMetadata(f0, Map("baz" -> 1001L)),
        FileStatusWithMetadata(f1, Map("baz" -> 1002L)))
      val df = createDF(format, files)

      checkAnswer(
        df.select(col("fileNum"), col("x"), col("_metadata.baz").as("bazbaz"))
          .where("bazbaz != 999"),
        Seq(
          Row(0, 101L, 0L),
          Row(0, 102L, 1L),
          Row(1, 111L, 0L),
          Row(1, 112L, 1L)))

      checkAnswer(
        df.select(col("fileNum"), col("x"), col("_metadata.baz").as("bazbaz"))
          .where("bazbaz != 0"),
        Seq(
          Row(0, 102L, 1L),
          Row(1, 112L, 1L)))
    }
  }

  test("mix of extra constant and generated metadata fields") {
    withTempData("parquet", FILE_SCHEMA) { (_, f0, f1) =>
      val format = new TestFileFormat(extraGeneratedMetadataFields ++ extraConstantMetadataFields)
      val files = Seq(
        FileStatusWithMetadata(f0, Map("foo" -> 0, "bar" -> "000")),
        FileStatusWithMetadata(f1, Map("foo" -> 1, "bar" -> "111")))
      val df = createDF(format, files)

      checkAnswer(
        df.select("fileNum", "x", "_metadata.foo", "_metadata.bar", "_metadata.baz"),
        Seq(
          Row(0, 101L, 0, "000", 0L),
          Row(0, 102L, 0, "000", 1L),
          Row(1, 111L, 1, "111", 0L),
          Row(1, 112L, 1, "111", 1L)))
    }
  }

  test("generated columns and extractors take precedence over metadata map values") {
    withTempData("parquet", FILE_SCHEMA) { (_, f0, f1) =>
      import FileFormat.{FILE_NAME, FILE_SIZE}
      import ParquetFileFormat.ROW_INDEX

      val format = new TestFileFormat(extraConstantMetadataFields)
      val files = Seq(
        FileStatusWithMetadata(f0, Map(FILE_SIZE -> 0L, FILE_NAME -> "000")),
        FileStatusWithMetadata(f1, Map(ROW_INDEX -> 17L, FILE_NAME -> "111")))
      val df = createDF(format, files)

      checkAnswer(
        df.select(
          "fileNum", "x", s"_metadata.${ROW_INDEX}", s"_metadata.${FILE_SIZE}",
          s"_metadata.${FILE_NAME}"),
        Seq(
          Row(0, 101L, 0L, f0.getLen, f0.getPath.getName),
          Row(0, 102L, 1L, f0.getLen, f0.getPath.getName),
          Row(1, 111L, 0L, f1.getLen, f1.getPath.getName),
          Row(1, 112L, 1L, f1.getLen, f1.getPath.getName)))
    }
  }
}

object FileSourceCustomMetadataStructSuite {
  case class TestFileIndex(files: Seq[FileStatusWithMetadata]) extends FileIndex {
    // The main override of interest for our testing, but we only care about the listing
    // itself -- partition and data filters are intentionally ignored.
    override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression])
        : Seq[PartitionDirectory] = {
      Seq(PartitionDirectory(InternalRow.empty, files))
    }

    // mildly interesting overrides
    override def sizeInBytes: Long = files.map(_.getLen).sum
    override val rootPaths: Seq[Path] = files.map(_.getPath)
    override def inputFiles: Array[String] = rootPaths.map(_.toString).toArray

    // uninteresting overrides
    override def refresh(): Unit = {}
    override def partitionSchema: StructType = new StructType()
  }

  class TestFileFormat(extraMetadataFields: Seq[StructField]) extends ParquetFileFormat {
    override def metadataSchemaFields: Seq[StructField] = {
      super.metadataSchemaFields ++ extraMetadataFields
    }
  }

}
