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
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * Reads of text files packed in archives, streamed through the [[SupportsArchiveFormat]] path.
 * Entries are streamed (never unpacked to disk), and the central contract verified throughout is
 * parity with reading the same files from a directory.
 *
 * Unlike CSV/JSON this does not extend [[ArchiveReadSuiteBase]]: the text data source has a single
 * fixed `value` column (one row per line, or per entry with `wholetext`) and no schema inference,
 * so the structured, two-column tests there do not apply. The container-writing hooks below are
 * supplied by a container trait the concrete suite mixes in ([[TarArchiveTestUtils]] /
 * [[ZipArchiveTestUtils]]); `corruptArchiveExtension` is supplied by the concrete suite.
 */
trait TextArchiveReadBase extends QueryTest with SharedSparkSession {

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

  private def textBytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)

  /** Provides an archive-extensioned path inside a fresh temp dir to `f`. */
  private def withArchiveFile(
      extension: String = archiveExtensions.head)(f: File => Unit): Unit = {
    val dir = Utils.createTempDir(namePrefix = "archive-test")
    try f(new File(dir, s"archive.$extension")) finally Utils.deleteRecursively(dir)
  }

  private def read(path: String, options: Map[String, String] = Map.empty): DataFrame =
    spark.read.options(options).text(path)

  test("read an archive of multiple text entries matches the union of the lines") {
    archiveExtensions.foreach { ext =>
      withArchiveFile(ext) { archive =>
        writeArchive(archive, Seq(
          "a.txt" -> textBytes("line1\nline2\n"),
          "b.txt" -> textBytes("line3\n"),
          "c.txt" -> textBytes("line4\nline5\n")))
        checkAnswer(
          read(archive.getCanonicalPath),
          Seq("line1", "line2", "line3", "line4", "line5").map(Row(_)))
      }
    }
  }

  test("archive entries read like a directory of the same files") {
    val entries = Seq("a.txt" -> textBytes("a1\na2\n"), "b.txt" -> textBytes("b1\n"))
    withArchiveFile() { archive =>
      writeArchive(archive, entries)
      val fromArchive = read(archive.getCanonicalPath)
      withTempDir { dir =>
        entries.foreach { case (n, b) => Files.write(new File(dir, n).toPath, b) }
        checkAnswer(fromArchive, read(dir.getCanonicalPath).collect().toSeq)
      }
    }
  }

  test("an empty archive yields no rows") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq.empty)
      checkAnswer(read(archive.getCanonicalPath), Seq.empty[Row])
    }
  }

  test("an archive and loose text files in the same directory are all read") {
    withTempDir { dir =>
      val ext = archiveExtensions.head
      writeArchive(
        new File(dir, s"data.$ext"),
        Seq("a.txt" -> textBytes("in-archive-1\nin-archive-2\n")))
      Files.write(new File(dir, "loose.txt").toPath, textBytes("loose-1\n"))
      checkAnswer(
        read(dir.getCanonicalPath),
        Seq("in-archive-1", "in-archive-2", "loose-1").map(Row(_)))
    }
  }

  test("wholetext reads each entry as a single row") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(
        "a.txt" -> textBytes("l1\nl2"),
        "b.txt" -> textBytes("only")))
      checkAnswer(
        read(archive.getCanonicalPath, Map("wholetext" -> "true")),
        Seq(Row("l1\nl2"), Row("only")))
    }
  }

  test("a custom line separator splits entries into rows") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq("a.txt" -> textBytes("x;y;z")))
      checkAnswer(
        read(archive.getCanonicalPath, Map("lineSep" -> ";")),
        Seq(Row("x"), Row("y"), Row("z")))
    }
  }

  test("count over an archive reads the right number of rows with an empty required schema") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(
        "a.txt" -> textBytes("1\n2\n3\n"),
        "b.txt" -> textBytes("4\n")))
      assert(read(archive.getCanonicalPath).count() == 4L)
    }
  }

  test("an archive always yields a single partition regardless of size") {
    withArchiveFile() { archive =>
      val big = (1 to 1000).map(i => s"value-$i").mkString("\n")
      writeArchive(archive, (0 until 4).map(i => s"part-$i.txt" -> textBytes(big + "\n")))
      withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1024") {
        val readDf = read(archive.getCanonicalPath)
        assert(readDf.rdd.getNumPartitions == 1,
          s"archive should be a single partition; got ${readDf.rdd.getNumPartitions}")
        assert(readDf.count() == 4000L)
      }
    }
  }

  Seq(true, false).foreach { ignoreCorrupt =>
    test(s"ignoreCorruptFiles=$ignoreCorrupt controls whether a corrupt archive is skipped") {
      withArchiveFile(corruptArchiveExtension) { archive =>
        writeCorruptArchive(archive)
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorrupt.toString) {
          if (ignoreCorrupt) {
            checkAnswer(read(archive.getCanonicalPath), Seq.empty[Row])
          } else {
            intercept[SparkException](read(archive.getCanonicalPath).collect())
          }
        }
      }
    }
  }

  Seq(true, false).foreach { ignoreMissing =>
    test(s"ignoreMissingFiles=$ignoreMissing controls whether a missing archive is skipped") {
      withArchiveFile() { archive =>
        writeArchive(archive, Seq("a.txt" -> textBytes("line1\nline2\n")))
        withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> ignoreMissing.toString) {
          // The archive is listed when the DataFrame is built, then deleted before the scan opens
          // it, so the reader hits a missing file -- handled by `FileScanRDD`, like any file.
          val df = read(archive.getCanonicalPath)
          assert(archive.delete(), s"failed to delete $archive")
          if (ignoreMissing) {
            checkAnswer(df, Seq.empty[Row])
          } else {
            intercept[SparkException](df.collect())
          }
        }
      }
    }
  }
}
