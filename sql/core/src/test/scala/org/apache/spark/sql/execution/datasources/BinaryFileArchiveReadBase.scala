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
import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Reads of binary files packed in archives via the [[SupportsArchiveFormat]] path. The `wholeFile`
 * option (default true) selects the behavior: true keeps today's whole-archive-as-one-record
 * contract; false emits one row per inner entry -- `content` is the entry's unpacked bytes, `path`
 * is `<archive>!/<entryName>`, and `length` is the entry's size (`modificationTime` stays the
 * parent archive's, since entry timestamps are optional).
 *
 * Unlike CSV/JSON this does not extend [[ArchiveReadSuiteBase]]: binaryFile's four-column row shape
 * (path/modificationTime/length/content) differs from the text formats' shape, so the shared tests
 * there do not apply.
 */
trait BinaryFileArchiveReadBase extends QueryTest with SharedSparkSession {

  /** Archive extensions to exercise; the head is the default. Supplied by the container trait. */
  protected def archiveExtensions: Seq[String]

  /** Writes `entries` (name -> bytes) into the archive at `dest`. From the container trait. */
  protected def writeArchive(dest: File, entries: Seq[(String, Array[Byte])]): Unit

  /** Writes bytes that are not a readable archive at `dest`. From the container trait. */
  protected def writeCorruptArchive(dest: File): Unit

  /** Extension of the archive [[writeCorruptArchive]] produces (corruption is format-specific). */
  protected def corruptArchiveExtension: String

  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ARCHIVE_FORMAT_READER_ENABLED.key, "true")

  private def bytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)

  private def withArchiveFile(
      extension: String = archiveExtensions.head)(f: File => Unit): Unit =
    withTempDir(dir => f(new File(dir, s"archive.$extension")))

  private def read(path: String, options: Map[String, String]): DataFrame =
    spark.read.format("binaryFile").options(options).load(path)

  test("wholeFile=true (default) reads the whole archive as a single record") {
    archiveExtensions.foreach { ext =>
      withArchiveFile(ext) { archive =>
        writeArchive(archive, Seq("a.bin" -> bytes("aaa"), "b.bin" -> bytes("bb")))
        val df = read(archive.getCanonicalPath, Map.empty)
        assert(df.count() == 1L)
        val row = df.select("path", "length", "content").head()
        assert(row.getString(0) == archive.toPath.toUri.toString ||
          row.getString(0).endsWith(archive.getName))
        assert(row.getLong(1) == archive.length())
        // The single record is the raw archive bytes, not any one entry's bytes.
        assert(row.getAs[Array[Byte]](2).length == archive.length())
      }
    }
  }

  test("wholeFile=false emits one row per entry with the entry's unpacked content") {
    archiveExtensions.foreach { ext =>
      withArchiveFile(ext) { archive =>
        writeArchive(archive, Seq("a.bin" -> bytes("aaa"), "b.bin" -> bytes("bbbb")))
        checkAnswer(
          read(archive.getCanonicalPath, Map("wholeFile" -> "false")).select("content"),
          Seq(Row(bytes("aaa")), Row(bytes("bbbb"))))
      }
    }
  }

  test("wholeFile=false sources path and length from each entry, modtime from the parent") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq("a.bin" -> bytes("aaa"), "b.bin" -> bytes("bbbb")))
      val rows = read(archive.getCanonicalPath, Map("wholeFile" -> "false"))
        .select("path", "length", "modificationTime").collect()
      assert(rows.length == 2)
      // path is `<archive>!/<entryName>`, distinct per entry.
      val paths = rows.map(_.getString(0))
      assert(paths.forall(_.contains(s"${archive.getName}!/")))
      assert(paths.map(_.split("!/").last).sorted === Array("a.bin", "b.bin"))
      // length is the entry's own unpacked size (3 for "aaa", 4 for "bbbb"), not the archive's.
      val byEntry = rows.map(r => r.getString(0).split("!/").last -> r.getLong(1)).toMap
      assert(byEntry === Map("a.bin" -> 3L, "b.bin" -> 4L))
      // modificationTime stays the parent archive's, identical across entries.
      assert(rows.map(_.getTimestamp(2)).distinct.length == 1)
    }
  }

  test("wholeFile=false _metadata exposes the parent archive's values for every row") {
    archiveExtensions.foreach { ext =>
      withArchiveFile(ext) { archive =>
        writeArchive(archive, Seq("a.bin" -> bytes("aaa"), "b.bin" -> bytes("bbbb")))
        val rows = read(archive.getCanonicalPath, Map("wholeFile" -> "false"))
          .select("_metadata.file_path", "_metadata.file_name", "_metadata.file_size",
            "_metadata.file_block_start", "_metadata.file_block_length",
            "_metadata.file_modification_time")
          .collect()
        assert(rows.length == 2)

        val fileSize = archive.length()
        rows.foreach { r =>
          // The `path`/`length` data columns are per entry here, but _metadata stays parent-only:
          // it is derived from the single PartitionedFile.
          assert(r.getString(0).endsWith(archive.getName) && !r.getString(0).contains("!/"),
            s"file_path should be the archive file, got ${r.getString(0)}")
          assert(r.getString(1) == archive.getName, s"file_name mismatch: ${r.getString(1)}")
          assert(r.getLong(2) == fileSize, s"file_size mismatch: ${r.getLong(2)} != $fileSize")
          assert(r.getLong(3) == 0L, s"file_block_start should be 0, got ${r.getLong(3)}")
          assert(r.getLong(4) == fileSize,
            s"file_block_length should be the archive size, got ${r.getLong(4)}")
          assert(r.getAs[java.sql.Timestamp](5).getTime == archive.lastModified(),
            "file_modification_time should be the archive's mtime")
        }
        assert(rows.map(_.toSeq).distinct.length == 1,
          "every row must carry the same parent-archive metadata")
      }
    }
  }

  test("wholeFile=false on an empty archive yields no rows") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq.empty)
      checkAnswer(read(archive.getCanonicalPath, Map("wholeFile" -> "false")), Seq.empty[Row])
    }
  }

  test("wholeFile=false skips hidden entries") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(
        "a.bin" -> bytes("keep"),
        "_hidden.bin" -> bytes("drop"),
        ".dotfile.bin" -> bytes("drop")))
      checkAnswer(
        read(archive.getCanonicalPath, Map("wholeFile" -> "false")).select("content"),
        Seq(Row(bytes("keep"))))
    }
  }

  test("wholeFile=false enforces SOURCES_BINARY_FILE_MAX_LENGTH per entry") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq("big.bin" -> bytes("0123456789")))
      withSQLConf(SQLConf.SOURCES_BINARY_FILE_MAX_LENGTH.key -> "4") {
        val e = intercept[SparkException] {
          read(archive.getCanonicalPath, Map("wholeFile" -> "false")).collect()
        }
        assert(e.getMessage.contains("exceeds") || e.getCause != null)
      }
    }
  }

  test("wholeFile=false honors length filter pushdown against each entry") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq("a.bin" -> bytes("aaa"), "b.bin" -> bytes("bbbb")))
      // Entry lengths are 3 and 4; the filter selects per entry, not against the archive size.
      val df = read(archive.getCanonicalPath, Map("wholeFile" -> "false"))
      checkAnswer(df.where("length = 3").select("content"), Seq(Row(bytes("aaa"))))
      checkAnswer(
        df.where("length <= 4").select("content"), Seq(Row(bytes("aaa")), Row(bytes("bbbb"))))
      checkAnswer(df.where("length > 4").select("content"), Seq.empty[Row])
    }
  }

  test("wholeFile=false reads both archive entries and loose files in the same input") {
    withTempDir { dir =>
      val ext = archiveExtensions.head
      writeArchive(
        new File(dir, s"data.$ext"),
        Seq("a.bin" -> bytes("in-archive-a"), "b.bin" -> bytes("in-archive-b")))
      java.nio.file.Files.write(new File(dir, "loose.bin").toPath, bytes("loose"))
      checkAnswer(
        read(dir.getCanonicalPath, Map("wholeFile" -> "false")).select("content"),
        Seq(Row(bytes("in-archive-a")), Row(bytes("in-archive-b")), Row(bytes("loose"))))
    }
  }

  test("wholeFile=false with count and no content column reads the right number of rows") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(
        "a.bin" -> bytes("a"), "b.bin" -> bytes("bb"), "c.bin" -> bytes("ccc")))
      assert(read(archive.getCanonicalPath, Map("wholeFile" -> "false")).count() == 3L)
    }
  }

  test("an archive always yields a single partition regardless of size") {
    withArchiveFile() { archive =>
      val big = bytes("x" * 4096)
      writeArchive(archive, (0 until 4).map(i => s"part-$i.bin" -> big))
      withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1024") {
        val df = read(archive.getCanonicalPath, Map("wholeFile" -> "false"))
        assert(df.rdd.getNumPartitions == 1,
          s"archive should be a single partition; got ${df.rdd.getNumPartitions}")
        assert(df.count() == 4L)
      }
    }
  }

  Seq(true, false).foreach { ignoreCorrupt =>
    test(s"wholeFile=false ignoreCorruptFiles=$ignoreCorrupt controls skipping a corrupt archive") {
      withArchiveFile(corruptArchiveExtension) { archive =>
        writeCorruptArchive(archive)
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorrupt.toString) {
          val df = read(archive.getCanonicalPath, Map("wholeFile" -> "false"))
          if (ignoreCorrupt) {
            checkAnswer(df, Seq.empty[Row])
          } else {
            intercept[SparkException](df.collect())
          }
        }
      }
    }
  }
}

class BinaryFileTarArchiveReadSuite extends BinaryFileArchiveReadBase with TarArchiveTestUtils {

  override protected def corruptArchiveExtension: String = "tar.gz"
}

class BinaryFileZipArchiveReadSuite extends BinaryFileArchiveReadBase with ZipArchiveTestUtils {

  override protected def corruptArchiveExtension: String = "zip"
}

class BinaryFileSevenZArchiveReadSuite
  extends BinaryFileArchiveReadBase with SevenZArchiveTestUtils {

  override protected def corruptArchiveExtension: String = "7z"
}
