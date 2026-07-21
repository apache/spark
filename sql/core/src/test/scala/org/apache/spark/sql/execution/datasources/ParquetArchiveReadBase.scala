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
import java.nio.file.Files

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * Binds [[ArchiveReadSuiteBase]]'s hooks to Parquet (entries unpacked to a local file for footer
 * random access). Parquet is self-describing, so the base's schema-inference tests run too.
 */
trait ParquetArchiveReadBase extends ArchiveReadSuiteBase {

  import testImplicits._

  override protected def format: String = "parquet"

  override protected def fileExtension: String = "parquet"

  override protected def readOptions: Map[String, String] = Map.empty

  override protected def readSchema: String = "id INT, name STRING"

  // Parquet has authoritative per-file schemas and only unions under `mergeSchema`, so it opts out
  // of the by-name default-inference union (covered instead by the mergeSchema test below).
  override protected def supportsSchemaMerge: Boolean = false

  // Parquet samples one part-file for non-merge inference (SPARK-11500).
  override protected def inferenceSamplesOneFile: Boolean = true

  override protected def encodeFile(
      df: DataFrame,
      writeOptions: Map[String, String]): Array[Byte] = {
    val dir = Utils.createTempDir(namePrefix = "archive-test-encode")
    try {
      df.coalesce(1).write.format("parquet")
        .options(writeOptions).mode("overwrite").save(dir.getCanonicalPath)
      val parts = dir.listFiles().filter { f =>
        f.isFile && !f.getName.startsWith("_") && !f.getName.startsWith(".") &&
          !f.getName.endsWith(".crc")
      }
      assert(parts.length == 1,
        s"expected exactly one data file, got: ${parts.map(_.getName).toList}")
      Files.readAllBytes(parts.head.toPath)
    } finally Utils.deleteRecursively(dir)
  }

  for (vectorized <- Seq(true, false)) {
    test(s"archive reads return the same rows with vectorized reader = $vectorized") {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
        assertArchiveMatchesDir(
          Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob")))))
      }
    }
  }

  test("an abandoned read (LIMIT) over an archive returns partial rows and cleans up") {
    def archiveTempDirs(localDir: File): Set[String] =
      Option(localDir.listFiles()).getOrElse(Array.empty)
        .filter(_.getName.startsWith("parquet-archive")).map(_.getName).toSet
    withArchiveFile() { archive =>
      val parts = (0 until 4).map(i => entryName(i) -> encodeFile(sampleDf((i, s"v$i"))))
      writeArchive(archive, parts)
      val localDir = new File(Utils.getLocalDir(spark.sparkContext.getConf))
      val before = archiveTempDirs(localDir)
      assert(read(archive.getCanonicalPath).limit(2).collect().length == 2)
      // This read's per-entry temp dir (prefix `parquet-archive`) must be removed on task
      // completion, so no new one survives.
      assert((archiveTempDirs(localDir) -- before).isEmpty,
        "the read's temp dir was not cleaned up")
    }
  }

  test("extensionless entries are read and inferred like a directory of part-files") {
    val data = sampleDf((1, "Alice"), (2, "Bob"))
    withArchiveFile() { archive =>
      writeArchive(archive, Seq("part-00000" -> encodeFile(data)))
      checkAnswer(read(archive.getCanonicalPath), data)
      assert(inferredSchema(Seq(archive.getCanonicalPath)).fieldNames.toSet == Set("id", "name"),
        "an extensionless entry should be inferred like a directory of part-files")
    }
  }

  private def parquetArchiveTempDirs(prefix: String): Set[String] = {
    val localDir = new File(Utils.getLocalDir(spark.sparkContext.getConf))
    Option(localDir.listFiles()).getOrElse(Array.empty)
      .filter(_.getName.startsWith(prefix)).map(_.getName).toSet
  }

  test("a corrupt archive cleans up its read temp dir rather than leaking it") {
    // A corrupt archive throws before the read returns an iterator, but must not leak the temp dir.
    withArchiveFile(corruptArchiveExtension) { archive =>
      writeCorruptArchive(archive)
      val before = parquetArchiveTempDirs("parquet-archive")
      intercept[SparkException](read(archive.getCanonicalPath).collect())
      assert((parquetArchiveTempDirs("parquet-archive") -- before).isEmpty,
        "a corrupt archive leaked its read temp dir")
    }
  }

  test("a corrupt archive cleans up its inference temp dir rather than leaking it") {
    // Inference localizes entries too (readArchiveFooters), on a worker without a TaskContext; a
    // corrupt archive throws during that eager localize and must not leak parquet-archive-infer.
    withArchiveFile(corruptArchiveExtension) { archive =>
      writeCorruptArchive(archive)
      val before = parquetArchiveTempDirs("parquet-archive-infer")
      intercept[SparkException](inferredSchema(Seq(archive.getCanonicalPath)))
      assert((parquetArchiveTempDirs("parquet-archive-infer") -- before).isEmpty,
        "a corrupt archive leaked its inference temp dir")
    }
  }

  test("archive entries with differing fields read like a directory") {
    // Reading fills a missing column with null even with supportsSchemaMerge off.
    val withName = sampleDf((1, "Alice"), (2, "Bob"))
    val idOnly = Seq(3).toDF("id")
    assertArchiveMatchesDir(
      Seq(entryName(0) -> encodeFile(withName), entryName(1) -> encodeFile(idOnly)))
  }

  test("archive inference unions differing fields across entries with mergeSchema=true") {
    // mergeSchema=true folds every entry's footer; over an archive, one unpacked entry at a time.
    val withName = sampleDf((1, "Alice"), (2, "Bob"))
    val idExtra = Seq((3, 30)).toDF("id", "extra")
    val entries = Seq(entryName(0) -> encodeFile(withName), entryName(1) -> encodeFile(idExtra))
    val merge = Map("mergeSchema" -> "true")
    withArchiveFile() { archive =>
      writeArchive(archive, entries)
      val archiveSchema = inferredSchema(Seq(archive.getCanonicalPath), merge)
      withTempDir { dir =>
        entries.foreach { case (n, b) => Files.write(new File(dir, n).toPath, b) }
        assert(archiveSchema.fieldNames.toSet == Set("id", "name", "extra"),
          s"expected the union of entry fields, got $archiveSchema")
        assert(archiveSchema == inferredSchema(Seq(dir.getCanonicalPath), merge),
          s"archive mergeSchema inference diverged from a directory read; got $archiveSchema")
      }
    }
  }

  test("inference skips a missing archive among good ones (ignoreMissingFiles)") {
    // Exercised on ParquetFileFormat.inferSchema(files) directly: inference now runs on the
    // executor, so a hand-built missing status reaches the archive open and throws
    // FileNotFoundException at task time. The public read path re-lists and drops the missing path
    // before inference, so the missing-at-open window is only reachable through this entry point. A
    // missing archive is governed by ignoreMissingFiles, not ignoreCorruptFiles.
    withArchiveFile() { good =>
      writeArchive(good, Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice")))))
      val missing = new File(good.getParentFile, s"missing.${archiveExtensions.head}")
      val archives = Seq(
        new FileStatus(good.length(), false, 0, 0, good.lastModified(), new Path(good.toURI)),
        new FileStatus(1, false, 0, 0, 0, new Path(missing.toURI)))
      def infer(ignoreMissing: Boolean, ignoreCorrupt: Boolean = false): Option[StructType] =
        new ParquetFileFormat().inferSchema(spark, readOptions ++ Map(
          "ignoreMissingFiles" -> ignoreMissing.toString,
          "ignoreCorruptFiles" -> ignoreCorrupt.toString,
          "mergeSchema" -> "true"), archives)
      assert(infer(ignoreMissing = true).exists(_.fieldNames.contains("id")),
        "expected the surviving archive's schema")
      intercept[Exception](infer(ignoreMissing = false))
      intercept[Exception](infer(ignoreMissing = false, ignoreCorrupt = true))
    }
  }

  Seq(true, false).foreach { ignoreCorrupt =>
    test(s"ignoreCorruptFiles=$ignoreCorrupt: inference skips a corrupt entry's whole archive") {
      // A corrupt entry condemns its whole archive during inference (no partial ingestion), so the
      // `extra` column carried by the bad archive's valid sibling entry must not surface. A good
      // archive is unaffected. mergeSchema=true folds every entry's footer.
      val merge = Map("mergeSchema" -> "true")
      val sibling = Seq((3, "Carol", "x")).toDF("id", "name", "extra")
      withTempDir { dir =>
        writeArchive(new File(dir, s"good.${archiveExtensions.head}"),
          Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob")))))
        writeArchive(new File(dir, s"bad.${archiveExtensions.head}"), Seq(
          entryName(0) -> encodeFile(sibling),
          entryName(1) -> "This is not a valid Parquet file".getBytes("UTF-8")))
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorrupt.toString) {
          if (ignoreCorrupt) {
            assert(inferredSchema(Seq(dir.getCanonicalPath), merge).fieldNames.toSet ==
              Set("id", "name"),
              "the whole corrupt archive, including its valid sibling entry, should be skipped")
          } else {
            intercept[Exception](inferredSchema(Seq(dir.getCanonicalPath), merge))
          }
        }
      }
    }

    test(s"ignoreCorruptFiles=$ignoreCorrupt: read skips the rest of an archive at a bad entry") {
      // The streaming read matches the other formats: entries before the corrupt one are ingested,
      // then the rest of that archive is skipped (not whole-archive atomic like inference). A
      // separate good archive is read in full.
      val alive = sampleDf((1, "Alice"), (2, "Bob"))
      val beforeCorrupt = sampleDf((3, "Carol"))
      withTempDir { dir =>
        writeArchive(new File(dir, s"good.${archiveExtensions.head}"),
          Seq(entryName(0) -> encodeFile(alive)))
        writeArchive(new File(dir, s"bad.${archiveExtensions.head}"), Seq(
          entryName(0) -> encodeFile(beforeCorrupt),
          entryName(1) -> "This is not a valid Parquet file".getBytes("UTF-8")))
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorrupt.toString) {
          if (ignoreCorrupt) {
            checkAnswer(read(dir.getCanonicalPath), alive.union(beforeCorrupt))
          } else {
            intercept[SparkException](read(dir.getCanonicalPath).collect())
          }
        }
      }
    }
  }
}
