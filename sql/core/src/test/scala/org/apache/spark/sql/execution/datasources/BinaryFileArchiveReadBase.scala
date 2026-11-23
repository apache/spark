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

import java.io.{File, IOException}
import java.lang.management.ManagementFactory
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Random

import com.sun.management.ThreadMXBean
import org.apache.hadoop.fs.Path
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter
import org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

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

  private def withContentCopyFlags[T](
      singleContentCopy: Boolean,
      streamRead: Boolean,
      minFileSize: Long)(f: => T): T = withSQLConf(
    SQLConf.BINARY_FILE_SINGLE_CONTENT_COPY_ENABLED.key -> singleContentCopy.toString,
    SQLConf.BINARY_FILE_STREAM_CONTENT_READ_ENABLED.key -> streamRead.toString,
    SQLConf.BINARY_FILE_STREAM_CONTENT_READ_MIN_FILE_SIZE.key ->
      minFileSize.toString)(f)

  /** The rows the read function of a wholeFile=false scan returns for `archive`. */
  private def readEntries(archive: File): Seq[InternalRow] = {
    val reader = new BinaryFileFormat().buildReaderWithPartitionValues(
      sparkSession = spark,
      dataSchema = BinaryFileFormat.schema,
      partitionSchema = StructType(Nil),
      requiredSchema = BinaryFileFormat.schema,
      filters = Nil,
      options = Map("wholeFile" -> "false"),
      hadoopConf = spark.sessionState.newHadoopConf())
    val file = mock(classOf[PartitionedFile])
    when(file.toPath).thenReturn(new Path(archive.toURI))
    reader(file).toList
  }

  /** The fewest bytes this thread allocates over three runs of `f`, after a warm-up run. */
  private def minAllocatedBytes(f: => Unit): Long = {
    val threads = ManagementFactory.getThreadMXBean.asInstanceOf[ThreadMXBean]
    assume(threads.isThreadAllocatedMemorySupported && threads.isThreadAllocatedMemoryEnabled)
    f
    Seq.fill(3) {
      val start = threads.getThreadAllocatedBytes(Thread.currentThread().threadId())
      f
      threads.getThreadAllocatedBytes(Thread.currentThread().threadId()) - start
    }.min
  }

  /** Changes the size the central directory of the zip at `archive` lists for its last entry. */
  private def shiftZipEntrySize(archive: File, delta: Int): Unit = {
    val zip = Files.readAllBytes(archive.toPath)
    // The last central directory file header is the entry's; its uncompressed size is at 24.
    val header = zip.lastIndexOfSlice(Seq[Byte](0x50, 0x4b, 0x01, 0x02))
    assert(header >= 0, "the archive must have a central directory")
    val buffer = ByteBuffer.wrap(zip).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(header + 24, buffer.getInt(header + 24) + delta)
    Files.write(archive.toPath, zip)
  }

  test("SPARK-62256: archive entries read the same through the single content copy") {
    // A 4096-byte minimum streams the larger entries, including one of more than one read chunk,
    // and reads the others as whole-file arrays.
    val entries = Seq(0, 1001, 4096, UnsafeWriter.STREAM_READ_CHUNK_BYTES + 1).zipWithIndex.map {
      case (numBytes, seed) =>
        s"entry-$seed.bin" ->
          Array.tabulate[Byte](numBytes)(i => ((i * 31 + seed) % 251 + 1).toByte)
    }
    archiveExtensions.foreach { ext =>
      withArchiveFile(ext) { archive =>
        writeArchive(archive, entries)
        def entryRows(): DataFrame = read(archive.getCanonicalPath, Map("wholeFile" -> "false"))
          .select("path", "length", "content")
        val expected = withContentCopyFlags(
            singleContentCopy = false, streamRead = false, minFileSize = 0L) {
          entryRows().collect()
        }
        assert(expected.map(r => r.getString(0).split("!/").last -> r.getAs[Array[Byte]](2).toSeq)
          .toMap === entries.map { case (name, bytes) => name -> bytes.toSeq }.toMap, ext)
        withContentCopyFlags(singleContentCopy = true, streamRead = true, minFileSize = 4096L) {
          withClue(s"$ext: ") {
            checkAnswer(entryRows(), expected.toSeq)
          }
        }
      }
    }
  }

  if (archiveExtensions.contains("tgz")) {
    test("SPARK-62256: a truncated archive entry stays a corrupt file to ignoreCorruptFiles") {
      withArchiveFile("tgz") { archive =>
        // Incompressible, so cutting the archive in half ends the gzip stream inside the entry,
        // which gzip reports by throwing an EOFException rather than by ending the stream.
        val content = new Array[Byte](64 * 1024)
        new Random(42).nextBytes(content)
        writeArchive(archive, Seq("entry.bin" -> content))
        Files.write(archive.toPath, Files.readAllBytes(archive.toPath).take(32 * 1024))
        def entryRows(): DataFrame = read(archive.getCanonicalPath, Map("wholeFile" -> "false"))
          .select("content")
        // The legacy read, the whole-file array, and the stream read.
        Seq((false, false), (true, false), (true, true)).foreach {
          case (singleContentCopy, streamRead) =>
            withContentCopyFlags(singleContentCopy, streamRead, minFileSize = 0L) {
              val clue = s"single copy $singleContentCopy, stream read $streamRead"
              withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
                withClue(s"$clue: ") {
                  checkAnswer(entryRows(), Seq.empty[Row])
                }
              }
              withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
                intercept[SparkException](entryRows().collect())
              }
            }
        }
      }
    }

    test("SPARK-62256: a truncated archive entry fails before its declared size is allocated") {
      withArchiveFile("tgz") { archive =>
        val declared = 64 * UnsafeWriter.STREAM_READ_CHUNK_BYTES
        val content = new Array[Byte](declared)
        new Random(42).nextBytes(content)
        writeArchive(archive, Seq("entry.bin" -> content))
        Files.write(archive.toPath, Files.readAllBytes(archive.toPath).take(32 * 1024))
        Seq((false, false), (true, false), (true, true)).foreach {
          case (singleContentCopy, streamRead) =>
            withContentCopyFlags(singleContentCopy, streamRead, minFileSize = 0L) {
              val allocated = minAllocatedBytes {
                intercept[IOException](readEntries(archive))
                ()
              }
              assert(allocated < declared / 8, s"single copy $singleContentCopy, stream read " +
                s"$streamRead: allocated $allocated bytes for an entry declaring $declared")
            }
        }
      }
    }
  }

  if (archiveExtensions.contains("zip")) {
    test("SPARK-62256: a zip entry whose size header is wrong reads what the legacy read does") {
      val content = Array.tabulate[Byte](4096)(i => (i % 251 + 1).toByte)
      // The zip entry stream returns all of the entry's data whatever its size header says.
      Seq(8, -8).foreach { delta =>
        withArchiveFile("zip") { archive =>
          writeArchive(archive, Seq("entry.bin" -> content))
          shiftZipEntrySize(archive, delta)
          def entryRows(): DataFrame = read(archive.getCanonicalPath, Map("wholeFile" -> "false"))
            .select("length", "content")
          // The legacy read, the whole-file array, and the stream read.
          Seq((false, false), (true, false), (true, true)).foreach {
            case (singleContentCopy, streamRead) =>
              withContentCopyFlags(singleContentCopy, streamRead, minFileSize = 0L) {
                withClue(s"size header off by $delta, single copy $singleContentCopy, " +
                    s"stream read $streamRead: ") {
                  checkAnswer(entryRows(), Row(content.length.toLong + delta, content))
                }
              }
          }
        }
      }
    }

    test("SPARK-62256: a zip entry whose header overstates its size allocates what it holds") {
      val content = Array.tabulate[Byte](4096)(i => (i % 251 + 1).toByte)
      val declared = 64 * UnsafeWriter.STREAM_READ_CHUNK_BYTES
      withArchiveFile("zip") { archive =>
        writeArchive(archive, Seq("entry.bin" -> content))
        shiftZipEntrySize(archive, declared - content.length)
        val legacy = withContentCopyFlags(
            singleContentCopy = false, streamRead = false, minFileSize = 0L) {
          readEntries(archive)
        }
        val schema = BinaryFileFormat.schema
        assert(legacy.map(_.getLong(schema.fieldIndex("length"))) === Seq(declared.toLong))
        assert(legacy.map(_.getBinary(schema.fieldIndex("content")).toSeq) === Seq(content.toSeq))
        Seq((false, false), (true, false), (true, true)).foreach {
          case (singleContentCopy, streamRead) =>
            withContentCopyFlags(singleContentCopy, streamRead, minFileSize = 0L) {
              val clue = s"single copy $singleContentCopy, stream read $streamRead"
              var rows = Seq.empty[InternalRow]
              val allocated = minAllocatedBytes { rows = readEntries(archive) }
              assert(rows === legacy, clue)
              assert(allocated < declared / 8,
                s"$clue: allocated $allocated bytes for an entry declaring $declared")
            }
        }
      }
    }
  }

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

  test("wholeFile=false honors archivePathFilter, applied on top of hidden-entry filtering") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(
        "keep/a.bin" -> bytes("a"),
        "keep/_hidden.bin" -> bytes("drop"), // matches the glob but hidden
        "other/b.bin" -> bytes("drop")))
      checkAnswer(
        read(archive.getCanonicalPath, Map("wholeFile" -> "false", "archivePathFilter" -> "keep/*"))
          .select("content"),
        Seq(Row(bytes("a"))))
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
